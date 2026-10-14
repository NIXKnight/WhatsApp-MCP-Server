import { afterEach, beforeEach, describe, expect, it } from 'vitest';
import { spawn, type ChildProcess } from 'node:child_process';
import { mkdtemp, rm } from 'node:fs/promises';
import { isAbsolute } from 'node:path';
import { createServer } from 'node:net';
import { once } from 'node:events';
import { createClient } from 'redis';
import {
  canonicalChatId, eventId, fingerprintEvent, type NotificationV2, type ClaimedWork,
  type CompletionProof, type CompletionIndexPort, type PiHostPort
} from '../../src/domain.js';
import { connectRedis, requireBaseline, type RedisConnections } from '../../src/redis/client.js';
import { createRepository, type RedisRepository } from '../../src/redis/repository.js';
import { createRecovery } from '../../src/scheduler/recovery.js';
import { runChatWorker } from '../../src/scheduler/chat-worker.js';
import { timerWait } from '../../src/scheduler/coordinator.js';
import { APP, GROUP, keys } from '../../src/redis/keys.js';
const binary = process.env['SYNTHETIC_REDIS_TEST_BINARY'];
// Never connect to a supplied URL/service. Only spawn an explicit executable
// with a new /tmp directory, loopback ephemeral port and synthetic records.
const suite = binary === undefined ? describe.skip : describe;
const chat = canonicalChatId('100@g.us'), other = canonicalChatId('200@g.us');
const policy = { groups: [chat, other], operatorJids: ['300@lid', '400@s.whatsapp.net'] };
const appId = 'a'.repeat(32), workerId = 'b'.repeat(32);
const OWNERSHIP_MS = 120000; // Headroom for durable multi-operation assertions, not an expiry trigger.
const pause = (ms: number) => new Promise<void>(resolve => setTimeout(resolve, ms));
function event(n = 1, c = chat, id = n.toString(16).padStart(32, '0')): NotificationV2 {
  const privateChat = !c.endsWith('@g.us');
  return { schema_version: 2, event_type: 'whatsapp.message.received', event_id: eventId(id), message_id: 'TEST_MESSAGE_' + n,
    chat_sequence: n, chat_kind: privateChat ? 'operator_private' : 'group', chat_jid: c, canonical_chat_jid: c,
    sender_jid: privateChat ? '300@lid' : '500@s.whatsapp.net', is_from_me: false,
    event_timestamp: '2033-05-18T03:33:20.123456789Z', received_at: '2033-05-18T03:33:21Z' } as NotificationV2;
}
function proof(work: ClaimedWork): CompletionProof {
  return { marker: { version: 1, eventId: work.event.event_id, eventFingerprint: fingerprintEvent(work.event),
    canonicalChatId: work.event.canonical_chat_jid, chatSequence: work.event.chat_sequence, completion: 'turn',
    completedAt: '2033-05-18T03:33:22Z', confirmedSends: [] }, entryId: 'TEST_ENTRY',
    session: { canonicalChatId: work.event.canonical_chat_jid, sessionId: 'TEST_SESSION', sessionFile: '/tmp/synthetic-session.jsonl' } };
}
suite('fenced Redis 7.2 PEL and unfinished-turn recovery', () => {
  let directory: string, port: number, child: ChildProcess | undefined, repository: RedisRepository;
  let admin: ReturnType<typeof createClient>;
  const opened: RedisConnections[] = [];
  let evidence: Map<string, CompletionProof>;
  let index: CompletionIndexPort;
  const config = () => ({ url: `redis://127.0.0.1:${port}/0`, waitAofMs: 1000, blockMs: 100 });
  const evidenceKey = (c: string, id: string) => c + '|' + id;
  const saveProof = (p: CompletionProof) => evidence.set(evidenceKey(p.marker.canonicalChatId, p.marker.eventId), p);
  async function start() {
    if (!binary || !isAbsolute(binary)) throw new Error('synthetic_binary_required');
    child = spawn(binary, ['--bind', '127.0.0.1', '--port', String(port), '--dir', directory, '--appendonly', 'yes', '--appendfsync', 'always', '--maxmemory-policy', 'noeviction', '--save', '', '--daemonize', 'no'], { stdio: 'ignore', env: { PATH: '/usr/bin:/bin' } });
    child.on('error', () => {});
    for (let i = 0; i < 200; i++) {
      const probe = createClient({ url: config().url, disableOfflineQueue: true, socket: { reconnectStrategy: false, connectTimeout: 100 } });
      probe.on('error', () => {});
      try { await probe.connect(); await requireBaseline(probe); await probe.close(); return; }
      catch { if (probe.isOpen) probe.destroy(); await pause(10); }
    }
    throw new Error('synthetic_start_failed');
  }
  async function stop(signal: NodeJS.Signals = 'SIGTERM') {
    const running = child; child = undefined;
    if (running && running.exitCode === null && running.signalCode === null) { const exit = once(running, 'exit'); running.kill(signal); await exit; }
  }
  async function connect(indexPort = index) {
    const c = await connectRedis(config()); opened.push(c);
    return { c, r: await createRepository(c, policy, indexPort) };
  }
  async function claim(e = event(), leaseMs = OWNERSHIP_MS): Promise<ClaimedWork> {
    expect((await repository.accept(e)).status).toBe('accepted');
    const app = await repository.acquireApp(appId, OWNERSHIP_MS); expect(app).toBeDefined();
    if (!app) throw new Error('synthetic_app_failed');
    const result = await repository.claimNext(e.canonical_chat_jid, app, workerId, leaseMs, 1000);
    expect(result.status).toBe('claimed'); if (result.status !== 'claimed') throw new Error('synthetic_claim_failed');
    return result.work;
  }
  beforeEach(async () => {
    evidence = new Map(); index = { findEvent: async (c, id) => evidence.get(evidenceKey(c, id)), classifyOwn: async () => 'manual' };
    directory = await mkdtemp('/tmp/whatsapp-synthetic-redis-');
    const socket = createServer(); socket.listen(0, '127.0.0.1'); await once(socket, 'listening');
    const address = socket.address(); if (!address || typeof address === 'string') throw new Error('synthetic_port_failed');
    port = address.port; await new Promise<void>((resolve, reject) => socket.close(error => error ? reject(error) : resolve()));
    await start(); const result = await connect(); repository = result.r;
    admin = createClient({ url: config().url, disableOfflineQueue: true, socket: { reconnectStrategy: false } });
    admin.on('error', () => {}); await admin.connect();
  }, 15000);
  afterEach(async () => {
    for (const c of opened.splice(0)) await c.close();
    if (admin?.isOpen) admin.destroy(); await stop(); if (directory) await rm(directory, { recursive: true, force: true });
  }, 15000);
  async function expireChat(c = chat) { await admin.sendCommand(['HSET', keys(c).lease, 'until', '1']); }
  async function pending() {
    return await admin.sendCommand(['XPENDING', keys(chat).stream, GROUP]) as [number, unknown, unknown, unknown];
  }
  it('raw Redis 7.2 XAUTOCLAIM JUSTID has cursor, IDs, deleted IDs', async () => {
    await repository.accept(event());
    await repository.waitForWork(chat, workerId, 1);
    const r = await admin.sendCommand(['XAUTOCLAIM', keys(chat).stream, GROUP, 'TEST_RECOVERY', '0', '0-0', 'COUNT', '32', 'JUSTID']) as unknown[];
    expect(r).toHaveLength(3); expect(r[0]).toBe('0-0'); expect(r[1]).toHaveLength(1); expect(r[2]).toEqual([]);
  });
  it('recovery requires current app and idle chat, never steals active lease', async () => {
    const w = await claim();
    const before = await admin.sendCommand(['HGETALL', keys(chat).lease]);
    expect((await repository.recoverPending(chat, w.ownership.app, workerId, '0-0', 32)).status).toBe('busy');
    expect(await admin.sendCommand(['HGETALL', keys(chat).lease])).toEqual(before);
    const forged = { ...w.ownership.app, epoch: '9007199254740993' };
    expect((await repository.recoverPending(chat, forged, workerId, '0-0', 32)).status).toBe('lost');
    await admin.sendCommand(['HSET', APP, 'until', '1']);
    expect((await repository.recoverPending(chat, w.ownership.app, workerId, '0-0', 32)).status).toBe('lost');
  });
  it('expired lease PEL recovery is hint only; exact contiguous claim grants new fence', async () => {
    const w = await claim(); await expireChat();
    const r = await repository.recoverPending(chat, w.ownership.app, 'c'.repeat(32), '0-0', 32);
    expect(r).toEqual({ status: 'ok', next: '0-0', claimed: 1, deleted: 0 });
    expect(await repository.checkWork(w)).toBe(false); expect((await pending())[0]).toBe(1);
    const next = await repository.claimNext(chat, w.ownership.app, 'd'.repeat(32), OWNERSHIP_MS, 1000);
    expect(next.status).toBe('claimed'); if (next.status !== 'claimed') throw new Error('synthetic_claim_failed');
    expect(next.work.event.event_id).toBe(w.event.event_id); expect(next.work.ownership.chat.epoch).not.toBe(w.ownership.chat.epoch);
    expect(await repository.checkWork(w)).toBe(false); expect(await repository.checkWork(next.work)).toBe(true);
  });
  it('out-of-order PEL wakes and recovered IDs cannot bypass a missing sequence', async () => {
    await repository.accept(event(2)); await repository.waitForWork(chat, workerId, 1);
    const app = await repository.acquireApp(appId, OWNERSHIP_MS); if (!app) throw new Error('synthetic_app_failed');
    const recover = createRecovery(repository); expect(await recover(chat, app, workerId)).toBe(true);
    expect((await repository.claimNext(chat, app, workerId, OWNERSHIP_MS, 1000)).status).toBe('waiting');
    await admin.sendCommand(['HSET', keys(chat).state, 'gap', '1']);
    expect((await repository.claimNext(chat, app, workerId, OWNERSHIP_MS, 1000)).status).toBe('paused');
    expect((await repository.readChat(chat)).pause).toBe('sequence_gap');
    await repository.accept(event(1, other, 'e'.repeat(32)));
    expect((await repository.claimNext(other, app, workerId, OWNERSHIP_MS, 1000)).status).toBe('claimed');
  });
  it('Redis 7 deleted-ID third reply pauses that chat instead of losing cursor work', async () => {
    const w = await claim(); await expireChat();
    await admin.sendCommand(['XDEL', keys(chat).stream, w.streamId]);
    const r = await repository.recoverPending(chat, w.ownership.app, workerId, '0-0', 32);
    expect(r).toEqual({ status: 'ok', next: '0-0', claimed: 0, deleted: 1 });
    expect((await repository.readChat(chat)).pause).toBe('sequence_gap');
    expect((await repository.claimNext(chat, w.ownership.app, workerId, OWNERSHIP_MS, 1000)).status).toBe('paused');
    expect(await admin.sendCommand(['HLEN', keys(chat).events])).toBe(1);
  });
  it('durable begin-turn is non-overwriting and stale owners cannot begin', async () => {
    const w = await claim(); expect(await repository.turnState(w)).toBe('fresh');
    expect(await repository.beginTurn(w)).toBe(true); expect(await repository.beginTurn(w)).toBe(false);
    expect(await repository.turnState(w)).toBe('started'); await expireChat();
    expect(await repository.beginTurn(w)).toBe(false); expect(await repository.turnState(w)).toBe('lost');
  });
  function recoveryHost(p?: CompletionProof): PiHostPort {
    return { findEvent: async () => p, classifyOwn: async () => 'manual', close: async () => {},
      open: async () => { throw new Error('synthetic_open_forbidden'); } };
  }
  async function worker(w: ClaimedWork, host: PiHostPort) {
    await runChatWorker(repository, w, { leaseMs: OWNERSHIP_MS, renewMs: 100, appCurrent: () => true,
      instructions: () => 'SYNTHETIC_POLICY', host, wait: timerWait, diagnostics: { emit: () => {} },
      turns: { attach: async () => {}, prepare: async () => {}, run: async (_s, _c, prompt) => prompt(), confirmed: async () => [] } });
  }
  it('owned Redis SIGKILL/AOF restart preserves unfinished turn; recovery never prompts it', async () => {
    const w = await claim(); await repository.bindSession(w, proof(w).session); expect(await repository.beginTurn(w)).toBe(true);
    await repository.close(); admin.destroy(); await stop('SIGKILL'); await start();
    const result = await connect(); repository = result.r;
    admin = createClient({ url: config().url, disableOfflineQueue: true, socket: { reconnectStrategy: false } }); admin.on('error', () => {}); await admin.connect();
    expect((await pending())[0]).toBe(1); await expireChat();
    const r = await repository.claimNext(chat, w.ownership.app, 'd'.repeat(32), OWNERSHIP_MS, 1000);
    expect(r.status).toBe('claimed'); if (r.status !== 'claimed') throw new Error('synthetic_claim_failed');
    expect(await repository.turnState(r.work)).toBe('started'); await worker(r.work, recoveryHost());
    expect((await repository.readChat(chat)).pause).toBe('mutation_uncertain');
    expect(await admin.sendCommand(['XLEN', keys(chat).stream])).toBe(1);
  });
  it('matching durable marker recovers started turn by proof-before-delete without Pi', async () => {
    const w = await claim(), p = proof(w); await repository.bindSession(w, p.session); await repository.beginTurn(w); saveProof(p); await expireChat();
    const r = await repository.claimNext(chat, w.ownership.app, 'd'.repeat(32), OWNERSHIP_MS, 1000);
    if (r.status !== 'claimed') throw new Error('synthetic_claim_failed');
    await worker(r.work, recoveryHost(p));
    expect(await admin.sendCommand(['XLEN', keys(chat).stream])).toBe(0); expect((await pending())[0]).toBe(0);
    expect(await admin.sendCommand(['HGET', keys(chat).state, 'turnEvent'])).toBeNull(); expect((await repository.readChat(chat)).lastCompletedSequence).toBe(1);
  });
  it('unfinished journal still pauses rather than resending after PEL recovery', async () => {
    const w = await claim();
    await repository.journal.begin(w, { version: 1, attemptId: 'c'.repeat(32), eventId: w.event.event_id,
      canonicalChatId: chat, toolCallId: 'TEST_TOOL', tool: 'send_message', destination: other,
      startedAt: '2033-05-18T03:33:22Z', ownership: w.ownership, status: 'attempting' });
    await expireChat(); expect((await repository.recoverPending(chat, w.ownership.app, workerId, '0-0', 32)).status).toBe('ok');
    expect((await repository.claimNext(chat, w.ownership.app, workerId, OWNERSHIP_MS, 1000)).status).toBe('paused');
    expect((await repository.readChat(chat)).pause).toBe('mutation_uncertain');
    expect((await repository.journal.list(chat))[0]?.status).toBe('attempting');
  });
  it('expired reply cannot grant authority after the durability barrier', async () => {
    const w = await claim(); const c = await connectRedis(config()); opened.push(c);
    const late = await createRepository(c, policy, index, { now: () => Number.MAX_SAFE_INTEGER });
    expect(await late.checkWork(w)).toBe(false);
    expect(await repository.checkWork(w)).toBe(true);
  });

});
