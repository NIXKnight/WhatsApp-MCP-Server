import { createServer, type IncomingMessage, type ServerResponse } from 'node:http';
import type { AddressInfo } from 'node:net';
import { checked, AcceptanceSchema, ControllerError, systemClock, type Clock, type DiagnosticPort, type IntakeStore, type ReadinessPort, type IntakeLifecycle } from '../domain.js';
import type { ControllerConfig } from '../config.js';
import { createSignatureVerifier } from './signature.js';
import { parsePayload } from './payload.js';

function singleHeader(req: IncomingMessage, name: string): string | undefined {
  const values: string[] = [];
  for (let i = 0; i < req.rawHeaders.length; i += 2) {
    if (req.rawHeaders[i]?.toLowerCase() === name) values.push(req.rawHeaders[i + 1] ?? '');
  }
  return values.length === 1 ? values[0] : undefined;
}
function readBody(req: IncomingMessage, limit: number, timeoutMs: number): Promise<Buffer> {
  return new Promise((resolve, reject) => {
    let bytes = 0;
    const chunks: Buffer[] = [];
    const cleanup = () => {
      clearTimeout(timer); req.off('data', data); req.off('end', end); req.off('aborted', abort); req.off('error', abort);
    };
    const fail = (code: 'body_too_large' | 'body_invalid' | 'request_timeout') => {
      cleanup(); chunks.length = 0; req.resume(); reject(new ControllerError(code));
    };
    const data = (chunk: Buffer) => {
      bytes += chunk.length;
      if (bytes > limit) fail('body_too_large'); else chunks.push(chunk);
    };
    const end = () => { cleanup(); resolve(Buffer.concat(chunks, bytes)); };
    const abort = () => fail('body_invalid');
    const timer = setTimeout(() => fail('request_timeout'), timeoutMs);
    timer.unref();
    req.on('data', data); req.once('end', end); req.once('aborted', abort); req.once('error', abort);
  });
}
function reply(res: ServerResponse, status: number, body: Readonly<Record<string, string>>, close = false): void {
  if (res.destroyed || res.writableEnded) return;
  res.setHeader('Content-Type', 'application/json'); res.setHeader('Cache-Control', 'no-store');
  if (close) res.setHeader('Connection', 'close');
  res.writeHead(status); res.end(JSON.stringify(body));
}
export interface WebhookServer extends IntakeLifecycle { listen(port?: number, host?: string): Promise<AddressInfo> }
export function createWebhookServer(config: ControllerConfig, store: IntakeStore, readiness: ReadinessPort, diagnostics: DiagnosticPort, clock: Clock = systemClock): WebhookServer {
  const verify = createSignatureVerifier(config.auth.secret, config.auth.freshnessSeconds, clock);
  const http = { ...config.http };
  const policy = { groups: [...config.admission.groups], operatorJids: [...config.admission.operatorJids] };
  let stopping = false, active = 0, listening = false;
  let closing: Promise<void> | undefined;
  const tasks = new Set<Promise<void>>();
  const emit: DiagnosticPort['emit'] = input => { try { diagnostics.emit(input); } catch { /* Diagnostic sink is not an admission authority. */ } };
  const ready = () => { try { return !stopping && readiness.isReady(); } catch { return false; } };
  const handle = async (req: IncomingMessage, res: ServerResponse): Promise<void> => {
    const url = req.url;
    if (url === '/ready' && req.method === 'GET') {
      const ok = ready(); reply(res, ok ? 200 : 503, { status: ok ? 'ready' : 'unavailable' }, true); req.resume(); return;
    }
    if (url !== http.webhookPath) { reply(res, 404, { error: 'not_found' }, true); req.resume(); return; }
    if (req.method !== 'POST') { res.setHeader('Allow', 'POST'); reply(res, 405, { error: 'method_not_allowed' }, true); req.resume(); return; }
    if (!ready() || active >= http.maxInflight) { reply(res, 503, { error: stopping ? 'intake_closed' : 'not_ready' }, true); req.resume(); return; }
    active++;
    try {
      const type = singleHeader(req, 'content-type'), encoding = singleHeader(req, 'content-encoding');
      if (type === undefined || !/^application\/json(?:\s*;\s*charset=utf-8)?$/i.test(type) || (encoding !== undefined && encoding !== 'identity') || req.headers['content-encoding'] !== undefined && encoding === undefined) {
        reply(res, 415, { error: 'unsupported_media_type' }, true); req.resume(); return;
      }
      const length = singleHeader(req, 'content-length');
      if (length !== undefined && (!/^(0|[1-9]\d*)$/.test(length) || !Number.isSafeInteger(Number(length)))) throw new ControllerError('body_invalid');
      if (length !== undefined && Number(length) > http.maxBodyBytes) throw new ControllerError('body_too_large');
      const headers = { timestamp: singleHeader(req, 'x-webhook-timestamp'), signature: singleHeader(req, 'x-webhook-signature-v2') };
      const requestId = singleHeader(req, 'x-request-id');
      if (headers.timestamp === undefined || headers.signature === undefined || requestId === undefined || !/^[0-9a-f]{32}$/.test(requestId)) throw new ControllerError('signature_invalid');
      const body = await readBody(req, http.maxBodyBytes, http.requestTimeoutMs);
      verify(body, headers);
      const event = parsePayload(body, policy);
      if (requestId !== event.event_id) throw new ControllerError('payload_invalid');
      // Recheck after an in-progress body read. Shutdown never cancels an accept
      // that already began; a client disconnect likewise cannot undo durability.
      if (!ready()) throw new ControllerError(stopping ? 'intake_closed' : 'not_ready');
      let result;
      try { result = checked(AcceptanceSchema, await store.accept(event), 'store_unavailable'); } catch { throw new ControllerError('store_unavailable'); }
      if (result.status === 'unavailable') throw new ControllerError('store_unavailable');
      if (result.status === 'conflict') throw new ControllerError('identity_conflict');
      const duplicate = result.status === 'duplicate';
      reply(res, duplicate ? 200 : 202, { status: result.status, delivery_id: event.event_id });
      emit({ class: duplicate ? 'intake_duplicate' : 'intake_accepted', eventId: event.event_id, chat: event.canonical_chat_jid, httpStatus: duplicate ? 200 : 202 });
    } catch (error) {
      const code = error instanceof ControllerError ? error.code : 'internal_failed';
      const status = code === 'signature_invalid' ? 401 : code === 'policy_denied' ? 403 : code === 'body_too_large' ? 413 : code === 'request_timeout' ? 408 : code === 'identity_conflict' ? 409 : ['store_unavailable', 'not_ready', 'intake_closed'].includes(code) ? 503 : code === 'internal_failed' ? 500 : 400;
      reply(res, status, { error: code }, true); req.resume(); emit({ class: code, httpStatus: status });
    } finally { active--; }
  };
  const server = createServer({ maxHeaderSize: 8192, requestTimeout: http.requestTimeoutMs, headersTimeout: http.requestTimeoutMs, keepAliveTimeout: 1000 }, (req, res) => {
    // Late transport errors after body-listener cleanup must not crash the process.
    req.on('error', () => {}); res.on('error', () => {});
    const task = handle(req, res).catch(() => { reply(res, 500, { error: 'internal_failed' }, true); req.resume(); });
    tasks.add(task); void task.then(() => tasks.delete(task));
  });
  server.on('clientError', (_error, socket) => { if (socket.writable) socket.end('HTTP/1.1 400 Bad Request\r\nConnection: close\r\nContent-Length: 0\r\n\r\n'); });
  // Includes post-listen errors without exposing address or exception strings.
  server.on('error', () => { emit({ class: 'listen_failed' }); });
  return {
    listen(port = http.port, host = http.host) {
      if (stopping || listening || !Number.isInteger(port) || port < 0 || port > 65535 || !['127.0.0.1', '::1', '0.0.0.0'].includes(host)) return Promise.reject(new ControllerError('listen_failed'));
      listening = true;
      return new Promise((resolve, reject) => {
        const failed = () => { server.off('listening', started); server.off('error', failed); listening = false; reject(new ControllerError('listen_failed')); };
        const started = () => { server.off('error', failed); resolve(server.address() as AddressInfo); };
        server.once('error', failed); server.once('listening', started);
        try { server.listen(port, host); } catch { failed(); }
      });
    },
    stopIntake() { stopping = true; },
    close() {
      if (closing) return closing;
      stopping = true;
      closing = (async () => {
        await new Promise<void>(resolve => { server.close(() => resolve()); server.closeIdleConnections(); });
        await Promise.all([...tasks]);
      })();
      return closing;
    }
  };
}
