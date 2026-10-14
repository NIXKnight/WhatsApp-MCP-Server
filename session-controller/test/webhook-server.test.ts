import { createHmac } from 'node:crypto';
import { request } from 'node:http';
import { afterEach, describe, expect, it } from 'vitest';
import { loadConfig } from '../src/config.js';
import { createDiagnostics } from '../src/diagnostics.js';
import { fingerprintEvent, type Acceptance, type CompletionProof, type IntakeStore, type NotificationV2, type NotificationWire } from '../src/domain.js';
import { parsePayload } from '../src/intake/payload.js';
import { createWebhookServer, type WebhookServer } from '../src/intake/server.js';
const testKey = 'TEST_ONLY_NON_SENSITIVE_HMAC_FIXTURE';
const now = 2000000000000;
const policy = { groups: ['100@g.us'], operatorJids: ['300@lid', '400@s.whatsapp.net'] };
function event(): NotificationWire {
  return { schema_version: 2, event_type: 'whatsapp.message.received', event_id: 'a'.repeat(32), chat_jid: '100@g.us', canonical_chat_jid: '100@g.us', message_id: 'TEST_M1', event_timestamp: '2033-05-18T03:33:20Z', received_at: '2033-05-18T03:33:20.123456789Z', chat_sequence: 1, sender_jid: '500@bot', chat_kind: 'group', is_from_me: false };
}
function config() {
  return loadConfig({ BRIDGE_NOTIFY_SECRET: testKey, BRIDGE_NOTIFY_GROUP_ALLOWLIST: policy.groups.join(','), BRIDGE_NOTIFY_OPERATOR_JIDS: policy.operatorJids.join(','), SESSION_CONTROLLER_REDIS_URL: 'redis://127.0.0.1:6379', SESSION_CONTROLLER_SAFE_WORKING_WINDOW_TOKENS: '32000', SESSION_CONTROLLER_PI_CWD: '/tmp/synthetic-project', SESSION_CONTROLLER_PI_AGENT_DIR: '/tmp/synthetic-agent', SESSION_CONTROLLER_SESSION_DIR: '/tmp/synthetic-sessions', SESSION_CONTROLLER_SYSTEM_PROMPT_PATH: '/tmp/synthetic-policy.md', SESSION_CONTROLLER_PRE_COMPACTION_PATH: '/tmp/synthetic-pre.md', SESSION_CONTROLLER_COMPACTION_SUMMARY_PATH: '/tmp/synthetic-summary.md', SESSION_CONTROLLER_MCP_COMMAND: 'uv', SESSION_CONTROLLER_MCP_ARGS_JSON: '["run","whatsapp-mcp"]', SESSION_CONTROLLER_MCP_CWD: '/tmp/synthetic-mcp' });
}
function gate<T>() {
  let resolve!: (value: T) => void;
  const promise = new Promise<T>(r => { resolve = r; });
  return { promise, resolve };
}
interface Response { status: number; body: Record<string, string> }
const servers: WebhookServer[] = [];
afterEach(async () => { await Promise.all(servers.splice(0).map(s => s.close())); });
async function fixture(accept: (event: NotificationV2) => Promise<Acceptance>, isReady = () => true, maxInflight = 128) {
  const c = config(); c.http.maxInflight = maxInflight;
  const logs: string[] = [];
  const server = createWebhookServer(c, { accept }, { isReady }, createDiagnostics(line => logs.push(line), { now: () => now }), { now: () => now });
  servers.push(server);
  const address = await server.listen(0, '127.0.0.1');
  return { server, port: address.port, logs };
}
function signed(raw: Buffer): Record<string, string> {
  const timestamp = String(now / 1000);
  return { 'Content-Type': 'application/json', 'X-Webhook-Timestamp': timestamp, 'X-Webhook-Signature-V2': createHmac('sha256', testKey).update(timestamp + '.').update(raw).digest('hex'), 'X-Request-ID': event().event_id };
}
function send(port: number, raw = Buffer.from(JSON.stringify(event())), overrides: Record<string, string | string[]> = {}, path = '/webhooks/whatsapp', method = 'POST'): Promise<Response> {
  return new Promise((resolve, reject) => {
    const req = request({ host: '127.0.0.1', port, method, path, headers: { ...(overrides['Transfer-Encoding'] === undefined ? { 'Content-Length': String(raw.length) } : {}), ...signed(raw), ...overrides }, agent: false }, res => {
      const chunks: Buffer[] = [];
      res.on('data', (chunk: Buffer) => chunks.push(chunk));
      res.once('error', () => reject(new Error('test_response_failed')));
      res.once('end', () => {
        try { resolve({ status: res.statusCode ?? 0, body: JSON.parse(Buffer.concat(chunks).toString('utf8')) as Record<string, string> }); }
        catch { reject(new Error('test_response_invalid')); }
      });
    });
    req.once('error', () => reject(new Error('test_request_failed')));
    req.end(raw);
  });
}
describe('strict payload admission', () => {
  it('retains actual and canonical operator aliases, and accepts manual own events', () => {
    const privateEvent = { ...event(), chat_kind: 'operator_private', chat_jid: '400@s.whatsapp.net', canonical_chat_jid: '300@lid', sender_jid: '400@s.whatsapp.net', sender_alt_jid: '300@lid' };
    expect(parsePayload(Buffer.from(JSON.stringify(privateEvent)), policy).chat_jid).toBe('400@s.whatsapp.net');
    const own = { ...privateEvent, is_from_me: true, sender_jid: '900@hosted', sender_alt_jid: '9000@lid' };
    expect(parsePayload(Buffer.from(JSON.stringify(own)), policy).is_from_me).toBe(true);
  });
  it.each([
    { schema_version: 1 }, { extra: 'TEST_RAW_NOT_FOR_DIAGNOSTICS' }, { chat_sequence: 0 }, { chat_sequence: '1' }, { chat_sequence: 9007199254740992 },
    { is_from_me: 'false' }, { canonical_chat_jid: '200@g.us' }, { sender_jid: '500:7@s.whatsapp.net' }, { sender_alt_jid: '' },
    { event_timestamp: '2033-02-29T00:00:00Z' }, { received_at: '2033-01-01T24:00:00Z' }, { received_at: '2033-01-01T00:00:00+24:00' },
    { message_id: 'é'.repeat(257) }, { event_id: 'TEST_RAW_ID' }
  ])('rejects malformed metadata without coercion', patch => {
    expect(() => parsePayload(Buffer.from(JSON.stringify({ ...event(), ...patch })), policy)).toThrow('payload_invalid');
  });
  it('rejects malformed UTF-8 and unauthorized inbound private identities', () => {
    expect(() => parsePayload(Buffer.from([0xff]), policy)).toThrow('payload_invalid');
    const p = { ...event(), chat_kind: 'operator_private', chat_jid: '400@s.whatsapp.net', canonical_chat_jid: '300@lid' };
    expect(() => parsePayload(Buffer.from(JSON.stringify(p)), policy)).toThrow('policy_denied');
    expect(() => parsePayload(Buffer.from(JSON.stringify({ ...p, sender_jid: '400@s.whatsapp.net', canonical_chat_jid: '400@s.whatsapp.net' })), policy)).toThrow('policy_denied');
  });
});
describe('immutable-event fingerprint foundation (adapter dedupe deferred)', () => {
  it('freezes the versioned hash vector and ignores JSON order/whitespace with or without the optional alias', () => {
    expect(fingerprintEvent(event())).toBe('sha256:v1:afee26aa236ff63660ac50838f6c7f48e27b9749b7a2daa35ce9865f64d88bd8');
    for (const wire of [event(), { ...event(), sender_alt_jid: '500@lid' }]) {
      const reversed = Object.fromEntries(Object.entries(wire).reverse());
      const parsed = parsePayload(Buffer.from(JSON.stringify(reversed, null, 2)), policy);
      expect(fingerprintEvent(parsed)).toBe(fingerprintEvent(wire));
    }
  });
  const changes: [string, Partial<NotificationWire>][] = [
    ['event_id', { event_id: 'b'.repeat(32) }],
    ['message_id', { message_id: 'TEST_M2' }],
    ['event_timestamp', { event_timestamp: '2033-05-18T03:33:21Z' }],
    ['event_timestamp spelling', { event_timestamp: '2033-05-18T03:33:20.000Z' }],
    ['received_at nanoseconds', { received_at: '2033-05-18T03:33:20.123456788Z' }],
    ['chat_sequence', { chat_sequence: 2 }],
    ['sender_jid', { sender_jid: '600@bot' }],
    ['sender_alt_jid added', { sender_alt_jid: '500@lid' }],
    ['is_from_me', { is_from_me: true }],
    ['chat_jid', { chat_jid: '200@g.us' }],
    ['canonical_chat_jid', { canonical_chat_jid: '200@g.us' }],
    ['chat_kind/routing', { chat_kind: 'operator_private', chat_jid: '400@s.whatsapp.net', canonical_chat_jid: '300@lid' }]
  ];
  it.each(changes)('retained completion proof diverges for changed %s after transient event deletion', (_field, patch) => {
    // Only serialized proof survives this scope; there is no retained event or
    // Redis/Pi implementation here. Individual routing patches exercise hash
    // coverage, not admission; parsePayload remains the policy authority.
    const serialized = (() => {
      const original = parsePayload(Buffer.from(JSON.stringify(event())), policy);
      const proof: CompletionProof = { marker: { version: 1, eventId: original.event_id, eventFingerprint: fingerprintEvent(original), canonicalChatId: original.canonical_chat_jid, chatSequence: original.chat_sequence, completion: 'turn', completedAt: '2033-05-18T03:33:22Z', confirmedSends: [] }, entryId: 'TEST_COMPLETION1', session: { canonicalChatId: original.canonical_chat_jid, sessionId: 'TEST_SESSION1', sessionFile: '/tmp/synthetic-sessions/session1.jsonl' } };
      return JSON.stringify(proof);
    })();
    const durable = JSON.parse(serialized) as CompletionProof;
    expect(fingerprintEvent(event())).toBe(durable.marker.eventFingerprint);
    expect(fingerprintEvent({ ...event(), ...patch } as NotificationWire)).not.toBe(durable.marker.eventFingerprint);
    expect(serialized.includes('TEST_M1')).toBe(false);
    expect(serialized.includes('sender_jid')).toBe(false);
    expect(serialized.includes('received_at')).toBe(false);
  });
  it('distinguishes actual private aliases and optional sender alias changes/removal without losing canonical routing', () => {
    const wire = parsePayload(Buffer.from(JSON.stringify({ ...event(), chat_kind: 'operator_private', chat_jid: '400@s.whatsapp.net', canonical_chat_jid: '300@lid', sender_jid: '400@s.whatsapp.net', sender_alt_jid: '300@lid' })), policy);
    const original = fingerprintEvent(wire);
    const replay = parsePayload(Buffer.from(JSON.stringify({ ...wire, chat_jid: '300@lid' })), policy);
    expect(replay.event_id).toBe(wire.event_id);
    expect(replay.chat_sequence).toBe(wire.chat_sequence);
    expect(replay.canonical_chat_jid).toBe(wire.canonical_chat_jid);
    expect(fingerprintEvent(replay)).not.toBe(original);
    expect(fingerprintEvent({ ...wire, sender_alt_jid: '400@s.whatsapp.net' })).not.toBe(original);
    const withoutAlias = { ...wire }; delete withoutAlias.sender_alt_jid;
    expect(fingerprintEvent(withoutAlias)).not.toBe(original);
  });
  it.each([{ schema_version: 1 }, { event_type: 'TEST_OTHER_EVENT' }, { content: 'TEST_ONLY_CONTENT' }])('rejects changed fixed wire fields or raw content instead of hashing an invalid event', patch => {
    expect(() => fingerprintEvent({ ...event(), ...patch } as NotificationWire)).toThrow('payload_invalid');
  });
});
describe('native webhook protocol and lifecycle', () => {
  it('waits for durable acceptance before responding; fresh retry becomes duplicate', async () => {
    const entered = gate<void>(), persisted = gate<Acceptance>();
    let calls = 0, responded = false;
    const f = await fixture(async () => { calls++; if (calls === 1) { entered.resolve(); return persisted.promise; } return { status: 'duplicate' }; });
    const pending = send(f.port).then(r => { responded = true; return r; });
    await entered.promise; expect(responded).toBe(false);
    persisted.resolve({ status: 'accepted' });
    expect(await pending).toEqual({ status: 202, body: { status: 'accepted', delivery_id: event().event_id } });
    expect(await send(f.port)).toEqual({ status: 200, body: { status: 'duplicate', delivery_id: event().event_id } });
    expect(calls).toBe(2);
  });
  it.each(['unavailable', 'throw', 'malformed', 'conflict'])('maps store %s safely without claiming acceptance', async mode => {
    const f = await fixture(async () => {
      if (mode === 'throw') throw new Error('TEST_RAW_STORE_NOT_FOR_DIAGNOSTICS');
      if (mode === 'malformed') return { status: 'coalesced' } as unknown as Acceptance;
      return { status: mode === 'conflict' ? 'conflict' : 'unavailable' };
    });
    const result = await send(f.port);
    expect(result).toEqual({ status: mode === 'conflict' ? 409 : 503, body: { error: mode === 'conflict' ? 'identity_conflict' : 'store_unavailable' } });
    expect(f.logs.join('').includes('TEST_RAW_STORE')).toBe(false);
    expect(f.logs.join('').includes('100@g.us')).toBe(false);
  });
  it('authenticates before parsing, denies disallowed groups, and bounds signed bodies', async () => {
    let calls = 0;
    const f = await fixture(async () => { calls++; return { status: 'accepted' }; });
    expect((await send(f.port, Buffer.from('not-json'), { 'X-Webhook-Signature-V2': '0'.repeat(64) })).status).toBe(401);
    expect((await send(f.port, Buffer.from('not-json'))).status).toBe(400);
    const denied = { ...event(), chat_jid: '200@g.us', canonical_chat_jid: '200@g.us' };
    expect((await send(f.port, Buffer.from(JSON.stringify(denied)))).status).toBe(403);
    expect((await send(f.port, Buffer.alloc(65537, 32), { 'Transfer-Encoding': 'chunked' })).status).toBe(413);
    expect((await send(f.port, Buffer.alloc(65537, 32), { 'Content-Length': '65537' })).status).toBe(413);
    expect((await send(f.port, undefined, { 'X-Request-ID': 'b'.repeat(32) })).status).toBe(400);
    expect(calls).toBe(0);
  });
  it('rejects duplicate authentication headers and unsupported representations', async () => {
    let calls = 0;
    const f = await fixture(async () => { calls++; return { status: 'accepted' }; });
    const raw = Buffer.from(JSON.stringify(event())); const h = signed(raw);
    for (const name of ['X-Webhook-Timestamp', 'X-Webhook-Signature-V2', 'X-Request-ID']) expect((await send(f.port, raw, { [name]: [h[name] ?? '', h[name] ?? ''] })).status).toBe(401);
    expect((await send(f.port, raw, { 'Content-Encoding': 'gzip' })).status).toBe(415);
    expect((await send(f.port, raw, { 'Content-Type': 'text/plain' })).status).toBe(415);
    expect((await send(f.port, raw, {}, '/webhooks/whatsapp?test=1')).status).toBe(404);
    expect((await send(f.port, raw, {}, '/webhooks/whatsapp', 'GET')).status).toBe(405);
    expect(calls).toBe(0);
  });
  it('reports readiness dynamically and treats thrown readiness checks as unavailable', async () => {
    let ready = false;
    const f = await fixture(async () => ({ status: 'accepted' }), () => ready);
    expect((await send(f.port, Buffer.alloc(0), {}, '/ready', 'GET')).status).toBe(503);
    expect((await send(f.port)).status).toBe(503);
    ready = true;
    expect((await send(f.port, Buffer.alloc(0), {}, '/ready', 'GET')).status).toBe(200);
    f.server.stopIntake();
    expect((await send(f.port, Buffer.alloc(0), {}, '/ready', 'GET')).status).toBe(503);
    expect((await send(f.port)).status).toBe(503);
    const thrown = await fixture(async () => ({ status: 'accepted' }), () => { throw new Error('TEST_READINESS_ERROR'); });
    expect((await send(thrown.port)).status).toBe(503);
  });
  it('bounds active intake, closes admission, and drains accepted work without a timer', async () => {
    const entered = gate<void>(), settled = gate<Acceptance>();
    const f = await fixture(async () => { entered.resolve(); return settled.promise; }, () => true, 1);
    const pending = send(f.port); await entered.promise;
    expect((await send(f.port)).status).toBe(503);
    let closed = false;
    const closing = f.server.close(); expect(f.server.close()).toBe(closing);
    void closing.then(() => { closed = true; });
    await Promise.resolve(); expect(closed).toBe(false);
    settled.resolve({ status: 'accepted' });
    expect((await pending).status).toBe(202);
    await closing; expect(closed).toBe(true);
  });
  it('continues durable acceptance after a client disconnect and drains its promise', async () => {
    const entered = gate<void>(), settled = gate<Acceptance>(), disconnected = gate<void>();
    const f = await fixture(async () => { entered.resolve(); return settled.promise; });
    const raw = Buffer.from(JSON.stringify(event()));
    const req = request({ host: '127.0.0.1', port: f.port, method: 'POST', path: '/webhooks/whatsapp', headers: { ...signed(raw), 'Content-Length': String(raw.length) }, agent: false });
    req.on('error', () => {}); req.once('close', () => disconnected.resolve()); req.end(raw);
    await entered.promise; req.destroy(); await disconnected.promise;
    let drained = false;
    const closing = f.server.close(); void closing.then(() => { drained = true; });
    await Promise.resolve(); expect(drained).toBe(false);
    settled.resolve({ status: 'accepted' }); await closing;
    expect(drained).toBe(true);
    expect(f.logs.some(line => JSON.parse(line).class === 'intake_accepted')).toBe(true);
  });
  it('rejects invalid/repeated listen calls with fixed errors and closes before listen', async () => {
    const f = await fixture(async () => ({ status: 'accepted' }));
    await expect(f.server.listen()).rejects.toThrow('listen_failed');
    const server = createWebhookServer(config(), { accept: async () => ({ status: 'accepted' }) }, { isReady: () => true }, { emit: () => {} });
    servers.push(server);
    await expect(server.listen(-1)).rejects.toThrow('listen_failed');
    await server.close();
    await expect(server.listen(0)).rejects.toThrow('listen_failed');
  });
  it('does not let diagnostics failures convert persisted acceptance into rejection', async () => {
    const store: IntakeStore = { accept: async () => ({ status: 'accepted' }) };
    const server = createWebhookServer(config(), store, { isReady: () => true }, { emit: () => { throw new Error('TEST_DIAGNOSTIC_ERROR'); } }, { now: () => now });
    servers.push(server); const address = await server.listen(0, '127.0.0.1');
    expect((await send(address.port)).status).toBe(202);
  });
});
