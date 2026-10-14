import { afterEach, beforeEach, describe, expect, it } from 'vitest';
import { spawn, type ChildProcess } from 'node:child_process';
import { mkdtemp, rm } from 'node:fs/promises';
import { isAbsolute } from 'node:path';
import { createServer } from 'node:net';
import { once } from 'node:events';
import { createClient } from 'redis';
import {
  canonicalChatId, eventId, fingerprintEvent, type NotificationV2, type ClaimedWork,
  type CompletionProof, type CompletionIndexPort, type MutationAttempt, type CheckpointRecord
} from '../../src/domain.js';
import { connectRedis, requireBaseline, type RedisConnections, type CommandPort } from '../../src/redis/client.js';
import { createRepository, readSessionMappings, type RedisRepository } from '../../src/redis/repository.js';
import { APP, GLOBAL_EVENTS, GROUP, keys } from '../../src/redis/keys.js';
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
function checkpoint(work: ClaimedWork): CheckpointRecord {
  return { checkpointId: 'd'.repeat(32), phase: 'prepared', checkpoint: { version: 1,
    canonicalChatId: work.event.canonical_chat_jid, lastCompletedSequence: work.event.chat_sequence - 1,
    policyVersion: 'test-v1', templateVersion: 'test-v1', createdAt: '2033-05-18T03:33:22Z',
    goals: [], decisions: [], constraints: [], verifiedFacts: [], pendingWork: [], approvalBoundaries: [], nextSteps: [], confirmedToolOutcomes: [] } };
}
function attempt(work: ClaimedWork): MutationAttempt {
  return { version: 1, attemptId: 'c'.repeat(32), eventId: work.event.event_id, canonicalChatId: work.event.canonical_chat_jid,
    toolCallId: 'TEST_CALL', tool: 'send_message', destination: other, startedAt: '2033-05-18T03:33:22Z', ownership: work.ownership, status: 'attempting' };
}
suite('isolated synthetic Redis 7.2 AOF integration', () => {
  let directory: string, port: number, child: ChildProcess | undefined, connections: RedisConnections, repository: RedisRepository;
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
    await start(); const result = await connect(); connections = result.c; repository = result.r;
    admin = createClient({ url: config().url, disableOfflineQueue: true, socket: { reconnectStrategy: false } });
    admin.on('error', () => {}); await admin.connect();
  }, 15000);
  afterEach(async () => {
    for (const c of opened.splice(0)) await c.close();
    if (admin?.isOpen) admin.destroy(); await stop(); if (directory) await rm(directory, { recursive: true, force: true });
  }, 15000);
  it('creates exactly configured streams/groups; aliases share one private stream', async () => {
    expect(repository.chats).toEqual([chat, other, '300@lid']);
    for (const c of repository.chats) expect(await admin.sendCommand(['XINFO', 'GROUPS', keys(c).stream])).toHaveLength(1);
    const e = event(1, canonicalChatId('300@lid'));
    const alias = { ...e, chat_jid: '400@s.whatsapp.net' } as NotificationV2;
    expect((await repository.accept(alias)).status).toBe('accepted');
    expect(await admin.xLen(keys(e.canonical_chat_jid).stream)).toBe(1);
    expect((await repository.accept({ ...alias, canonical_chat_jid: canonicalChatId('400@s.whatsapp.net') } as NotificationV2)).status).toBe('unavailable');
  });
  it('dedupes concurrently and rejects changed metadata, sequence and cross-chat identity', async () => {
    const statuses = await Promise.all(Array.from({ length: 12 }, () => repository.accept(event())));
    expect(statuses.filter(s => s.status === 'accepted')).toHaveLength(1);
    expect(statuses.filter(s => s.status === 'duplicate')).toHaveLength(11);
    const base = event();
    for (const changed of [ { ...base, message_id: 'TEST_CHANGED' }, { ...base, received_at: '2033-05-18T03:33:22Z' },
      { ...base, event_timestamp: '2033-05-18T03:33:20.123456788Z' }, { ...base, sender_alt_jid: '500@lid' },
      { ...base, is_from_me: true }, { ...base, chat_sequence: 2 } ]) expect((await repository.accept(changed)).status).toBe('conflict');
    expect((await repository.accept(event(1, chat, 'e'.repeat(32)))).status).toBe('conflict');
    expect((await repository.accept(event(1, other, base.event_id))).status).toBe('conflict');
    expect(await admin.xLen(keys(chat).stream)).toBe(1);
  });
  it('retains waiting backlog without TTL/cap and never acknowledges unavailable Pi lookup', async () => {
    index.findEvent = async () => { throw new Error('TEST_INDEX_FAILURE'); };
    expect((await repository.accept(event())).status).toBe('unavailable');
    expect(await admin.xLen(keys(chat).stream)).toBe(0);
  });
  it('durability gates acceptance and duplicates on the same serialized writer connection', async () => {
    const trace: string[] = []; let release: (() => void) | undefined;
    const gate = new Promise<void>(resolve => { release = resolve; }); let intercepted = false;
    const wrapped: RedisConnections = { ...connections, durable: operation => connections.durable(async () => {
      const result = await operation(); trace.push('lua'); if (!intercepted) { intercepted = true; await gate; } trace.push('before_wait'); return result;
    }) };
    // Initialization also durable; use a gate only after wrapper creation finishes.
    release?.(); const r = await createRepository(wrapped, policy, index);
    let settle: (() => void) | undefined;
    const blocked = new Promise<void>(resolve => { settle = resolve; });
    let entered: (() => void) | undefined; const begun = new Promise<void>(resolve => { entered = resolve; });
    const real = connections.writer.sendCommand.bind(connections.writer);
    connections.writer.sendCommand = async args => { if (args[0] === 'WAITAOF') { trace.push('wait'); entered?.(); await blocked; } return real(args); };
    let done = false; const accepting = r.accept(event()).then(value => { done = true; return value; });
    await begun; expect(done).toBe(false); expect(await admin.xLen(keys(chat).stream)).toBe(1);
    try {
      const mappings = await Promise.race([
        readSessionMappings(connections, policy),
        pause(500).then(() => { throw new Error('mapping_read_blocked'); })
      ]);
      expect(mappings).toEqual([
        { chat }, { chat: other }, { chat: canonicalChatId('300@lid') }
      ]);
    } finally { settle?.(); }
    expect((await accepting).status).toBe('accepted');
    let waits = 0;
    connections.writer.sendCommand = async args => { if (args[0] === 'WAITAOF') waits++; return real(args); };
    expect((await r.accept(event())).status).toBe('duplicate'); expect(waits).toBe(1);
    expect(trace).toContain('wait');
  });
  it('does not acknowledge a WAITAOF timeout/failure or queue work after connection loss', async () => {
    const real = connections.writer.sendCommand.bind(connections.writer);
    connections.writer.sendCommand = async args => args[0] === 'WAITAOF' ? [0, 0] : real(args);
    expect((await repository.accept(event())).status).toBe('unavailable'); expect(connections.isReady()).toBe(false);
    expect(await admin.xLen(keys(chat).stream)).toBe(1);
    const reopened = await connect(); expect((await reopened.r.accept(event())).status).toBe('duplicate');
    await stop('SIGKILL');
    expect((await reopened.r.accept(event(2))).status).toBe('unavailable'); expect(reopened.c.isReady()).toBe(false);
  });
  it('blocks missing earlier sequence, uses exact next entry after out-of-order PEL delivery', async () => {
    expect((await repository.accept(event(2))).status).toBe('accepted');
    const app = await repository.acquireApp(appId, OWNERSHIP_MS); if (!app) throw new Error('synthetic_app_failed');
    expect((await repository.claimNext(chat, app, workerId, OWNERSHIP_MS, 1000)).status).toBe('waiting');
    expect((await repository.accept(event())).status).toBe('accepted');
    await repository.waitForWork(chat, workerId, 1);
    const r = await repository.claimNext(chat, app, workerId, OWNERSHIP_MS, 1000);
    expect(r.status).toBe('claimed'); if (r.status !== 'claimed') throw new Error('synthetic_claim_failed');
    expect(r.work.event.chat_sequence).toBe(1);
    const p = proof(r.work); await repository.bindSession(r.work, p.session); saveProof(p);
    expect(await repository.complete(r.work, p)).toBe(true);
    const next = await repository.claimNext(chat, app, workerId, OWNERSHIP_MS, 1000);
    expect(next.status).toBe('claimed'); if (next.status === 'claimed') expect(next.work.event.chat_sequence).toBe(2);
  });
  it('persists a gap pause only on affected chat', async () => {
    await repository.accept(event(2)); await repository.accept(event(1, other, 'f'.repeat(32)));
    const app = await repository.acquireApp(appId, OWNERSHIP_MS); if (!app) throw new Error('synthetic_app_failed');
    expect((await repository.claimNext(chat, app, workerId, OWNERSHIP_MS, 1000)).status).toBe('waiting');
    expect((await repository.readChat(chat)).gapSinceMs).toBeDefined();
    await admin.hSet(keys(chat).state, 'gap', '1'); // Owned synthetic gap clock, no wall-clock race.
    expect((await repository.claimNext(chat, app, workerId, OWNERSHIP_MS, 1)).status).toBe('paused');
    expect((await repository.readChat(chat)).pause).toBe('sequence_gap');
    expect((await repository.claimNext(other, app, workerId, OWNERSHIP_MS, 1)).status).toBe('claimed');
  });
  it('fences stale app/chat owners for every mutation, including journal/checkpoint/deletion', async () => {
    const work = await claim(); const p = proof(work); await repository.bindSession(work, p.session); saveProof(p);
    await repository.journal.begin(work, attempt(work)); await repository.checkpoints.prepare(work, checkpoint(work));
    const staleApp = { ...work, ownership: { ...work.ownership, app: { ...work.ownership.app, epoch: '999' } } };
    const staleChat = { ...work, ownership: { ...work.ownership, chat: { ...work.ownership.chat, epoch: '999' } } };
    for (const stale of [staleApp, staleChat]) {
      expect(await repository.renew(stale, 1000)).toBeUndefined(); expect(await repository.pause(stale, 'session_failed')).toBe(false);
      expect(await repository.bindSession(stale, p.session)).toBe(false); expect(await repository.complete(stale, p)).toBe(false);
      expect(await repository.journal.begin(stale, { ...attempt(stale), attemptId: 'e'.repeat(32) })).toBe(false);
      expect(await repository.journal.confirm(stale, 'c'.repeat(32), 'TEST_SENT')).toBe(false);
      expect(await repository.journal.uncertain(stale, 'c'.repeat(32))).toBe(false);
      expect(await repository.checkpoints.prepare(stale, checkpoint(stale))).toBe(false);
      expect(await repository.checkpoints.transition(stale, 'd'.repeat(32), 'compacted', 'TEST_COMPACT')).toBe(false);
    }
    expect(await repository.renewApp({ ...work.ownership.app, epoch: '999' }, 1000)).toBeUndefined();
    expect(await repository.releaseApp({ ...work.ownership.app, epoch: '999' })).toBe(false);
    expect(await admin.xLen(keys(chat).stream)).toBe(1);
  });
  it('keeps epochs exact beyond JS/Lua floating-point precision and rejects expired renewals', async () => {
    await admin.hSet(APP, 'epoch', '9007199254740992');
    const app = await repository.acquireApp(appId, OWNERSHIP_MS); expect(app?.epoch).toBe('9007199254740993');
    if (!app) throw new Error('synthetic_app_failed');
    await admin.hSet(APP, 'until', '1'); // Owned synthetic state: expire only after epoch assertion.
    expect(await repository.renewApp(app, 1000)).toBeUndefined();
    const next = await repository.acquireApp(appId, OWNERSHIP_MS); expect(next?.epoch).toBe('9007199254740994');
    if (!next) throw new Error('synthetic_app_failed'); await repository.accept(event());
    await admin.hSet(keys(chat).lease, 'epoch', '9007199254740992');
    const r = await repository.claimNext(chat, next, workerId, OWNERSHIP_MS, 1000);
    if (r.status !== 'claimed') throw new Error('synthetic_claim_failed'); expect(r.work.ownership.chat.epoch).toBe('9007199254740993');
    await admin.hSet(keys(chat).lease, 'until', '1');
    expect(await repository.renew(r.work, 1000)).toBeUndefined();
    const reclaimed = await repository.claimNext(chat, next, workerId, OWNERSHIP_MS, 1000);
    expect(reclaimed.status).toBe('claimed'); if (reclaimed.status === 'claimed') expect(reclaimed.work.ownership.chat.epoch).toBe('9007199254740994');
  });
  it('requires strict durable matching Pi proof before deletion and compares duplicate digest after deletion', async () => {
    const work = await claim(), p = proof(work); await repository.bindSession(work, p.session);
    expect(await repository.complete(work, p)).toBe(false); expect(await admin.xLen(keys(chat).stream)).toBe(1);
    saveProof(p); expect(await repository.complete(work, { ...p, marker: { ...p.marker, eventFingerprint: 'sha256:v1:' + '0'.repeat(64) } })).toBe(false);
    expect(await repository.complete(work, p)).toBe(true);
    const k = keys(chat); expect(await admin.xLen(k.stream)).toBe(0); expect(await admin.hLen(k.events)).toBe(0); expect(await admin.hLen(k.tools)).toBe(0);
    expect(await admin.hLen(GLOBAL_EVENTS)).toBe(0); expect((await repository.readChat(chat)).lastCompletedSequence).toBe(1);
    const pel = await admin.sendCommand(['XPENDING', k.stream, GROUP]); expect(Array.isArray(pel) && pel[0]).toBe(0);
    expect((await repository.accept(event())).status).toBe('duplicate');
    expect((await repository.accept({ ...event(), sender_alt_jid: '500@lid' })).status).toBe('conflict');
    expect((await repository.accept(event(1, other, work.event.event_id))).status).toBe('conflict'); expect(await admin.xLen(k.stream)).toBe(0);
  });
  it('malformed/absent legacy evidence fails closed instead of resurrecting completed work', async () => {
    const work = await claim(), p = proof(work); await repository.bindSession(work, p.session); saveProof(p); await repository.complete(work, p);
    const invalid = { ...p, marker: { ...p.marker, eventFingerprint: undefined } };
    index.findEvent = async () => invalid as unknown as CompletionProof;
    expect((await repository.accept(event())).status).toBe('unavailable'); expect(await admin.xLen(keys(chat).stream)).toBe(0);
  });
  it('completion/intake race retries its snapshot and does not resurrect work', async () => {
    const work = await claim(), p = proof(work); await repository.bindSession(work, p.session);
    const second = await connect(); let release: (() => void) | undefined;
    const gate = new Promise<void>(resolve => { release = resolve; }); let entered: (() => void) | undefined;
    const started = new Promise<void>(resolve => { entered = resolve; }); let blocked = false;
    index.findEvent = async (c, id) => {
      const found = evidence.get(evidenceKey(c, id));
      if (!blocked && c === chat) { blocked = true; entered?.(); await gate; }
      return found;
    };
    const intake = repository.accept(event()); await started; saveProof(p);
    expect(await second.r.complete(work, p)).toBe(true); release?.();
    expect((await intake).status).toBe('duplicate'); expect(await admin.xLen(keys(chat).stream)).toBe(0);
  });
  it('global identity revision closes cross-chat completion/intake races', async () => {
    const work = await claim(), p = proof(work); await repository.bindSession(work, p.session);
    const second = await connect(); let release: (() => void) | undefined;
    const gate = new Promise<void>(resolve => { release = resolve; }); let enter: (() => void) | undefined;
    const begun = new Promise<void>(resolve => { enter = resolve; }); let blocked = false;
    index.findEvent = async (c, id) => {
      const found = evidence.get(evidenceKey(c, id));
      if (!blocked && c === chat) { blocked = true; enter?.(); await gate; }
      return found;
    };
    const intake = repository.accept(event(1, other, work.event.event_id)); await begun;
    saveProof(p); expect(await second.r.complete(work, p)).toBe(true); release?.();
    expect((await intake).status).toBe('conflict'); expect(await admin.xLen(keys(other).stream)).toBe(0);
  });
  it('completed identity rejects every changed variable field and missing evidence', async () => {
    const work = await claim(), p = proof(work); await repository.bindSession(work, p.session); saveProof(p);
    expect(await repository.complete(work, p)).toBe(true);
    const base = event();
    for (const changed of [
      { ...base, message_id: 'TEST_CHANGED' }, { ...base, received_at: '2033-05-18T03:33:22Z' },
      { ...base, event_timestamp: '2033-05-18T03:33:20.123456788Z' }, { ...base, sender_jid: '501@s.whatsapp.net' },
      { ...base, sender_alt_jid: '500@lid' }, { ...base, is_from_me: true }, { ...base, chat_sequence: 2 }
    ]) expect((await repository.accept(changed)).status).toBe('conflict');
    evidence.clear(); expect((await repository.accept(base)).status).toBe('conflict');
    expect(await admin.xLen(keys(chat).stream)).toBe(0);
  });
  it('expired application lease blocks work mutations even while chat lease is live', async () => {
    const work = await claim(), p = proof(work); await repository.bindSession(work, p.session); saveProof(p);
    await admin.hSet(APP, 'until', '1');
    expect(await repository.complete(work, p)).toBe(false);
    expect(await repository.pause(work, 'session_failed')).toBe(false);
    expect(await repository.renew(work, 1000)).toBeUndefined();
    expect(await admin.xLen(keys(chat).stream)).toBe(1);
  });
  it('completed private events retain exact actual alias and optional alias fingerprint', async () => {
    const privateChat = canonicalChatId('300@lid');
    const base = { ...event(1, privateChat), chat_jid: '400@s.whatsapp.net', sender_alt_jid: '400@s.whatsapp.net' } as NotificationV2;
    const work = await claim(base), p = proof(work); await repository.bindSession(work, p.session); saveProof(p);
    expect(await repository.complete(work, p)).toBe(true);
    expect((await repository.accept(base)).status).toBe('duplicate');
    expect((await repository.accept({ ...base, chat_jid: privateChat })).status).toBe('conflict');
    expect((await repository.accept({ ...base, sender_alt_jid: '300@lid' })).status).toBe('conflict');
    const { sender_alt_jid: unused, ...removed } = base; void unused;
    expect((await repository.accept(removed as NotificationV2)).status).toBe('conflict');
    expect(await admin.xLen(keys(privateChat).stream)).toBe(0);
  });
  it('namespace type corruption aborts Lua before any partial admission', async () => {
    await admin.del(keys(chat).sequences); await admin.set(keys(chat).sequences, 'TEST_WRONG_TYPE');
    expect((await repository.accept(event())).status).toBe('unavailable');
    expect(await admin.xLen(keys(chat).stream)).toBe(0);
    expect(await admin.hLen(keys(chat).events)).toBe(0);
  });
  it('durable journal transitions block unconfirmed completion and crash recovery never resends', async () => {
    const work = await claim(), p = proof(work); await repository.bindSession(work, p.session); saveProof(p);
    expect(await repository.journal.begin(work, attempt(work))).toBe(true); expect(await repository.journal.begin(work, attempt(work))).toBe(false);
    expect(await repository.complete(work, p)).toBe(false);
    await admin.hSet(keys(chat).lease, 'until', '1'); // Expire only after journal/cleanup prerequisites.
    expect((await repository.claimNext(chat, work.ownership.app, workerId, OWNERSHIP_MS, 1000)).status).toBe('paused');
    expect((await repository.readChat(chat)).pause).toBe('mutation_uncertain'); expect((await repository.journal.list(chat))[0]?.status).toBe('attempting');
  });
  it('confirmed journal correlation must be present in durable completion proof before tool deletion', async () => {
    const work = await claim(), p = proof(work); await repository.bindSession(work, p.session);
    await repository.journal.begin(work, attempt(work)); expect(await repository.journal.confirm(work, 'c'.repeat(32), 'TEST_SENT')).toBe(true);
    saveProof(p); expect(await repository.complete(work, p)).toBe(false);
    p.marker.confirmedSends.push({ destination: other, messageId: 'TEST_SENT' }); saveProof(p);
    expect(await repository.complete(work, p)).toBe(true); expect(await repository.journal.list(chat)).toEqual([]);
  });
  it('uncertain journal state remains durable and cannot be automatically confirmed/retried', async () => {
    const work = await claim(); await repository.journal.begin(work, attempt(work));
    expect(await repository.journal.uncertain(work, 'c'.repeat(32))).toBe(true);
    expect(await repository.journal.confirm(work, 'c'.repeat(32), 'TEST_SENT')).toBe(false);
    expect((await repository.journal.list(chat))[0]?.status).toBe('uncertain'); expect((await repository.readChat(chat)).pause).toBe('mutation_uncertain');
  });
  it('persists mapping/checkpoint; enforces prepared-compacted-restored transitions', async () => {
    const work = await claim(), p = proof(work), c = checkpoint(work); await repository.bindSession(work, p.session);
    expect(await repository.bindSession(work, { ...p.session, sessionId: 'TEST_OTHER' })).toBe(false);
    expect(await repository.checkpoints.prepare(work, c)).toBe(true);
    expect(await repository.checkpoints.transition(work, c.checkpointId, 'restored', 'TEST_COMPACT')).toBe(false);
    expect(await repository.checkpoints.prepare(work, { ...c, checkpointId: 'e'.repeat(32) })).toBe(false);
    expect(await repository.checkpoints.transition(work, c.checkpointId, 'compacted', 'TEST_COMPACT')).toBe(true);
    expect(await repository.checkpoints.transition(work, c.checkpointId, 'restored', 'TEST_WRONG')).toBe(false);
    expect(await repository.checkpoints.transition(work, c.checkpointId, 'restored', 'TEST_COMPACT')).toBe(true);
    saveProof(p); await repository.complete(work, p);
    expect((await repository.checkpoints.latest(chat))?.phase).toBe('restored'); expect((await repository.readChat(chat)).session).toEqual(p.session);
    expect(await readSessionMappings(connections, policy)).toEqual([
      { chat, binding: p.session }, { chat: other }, { chat: canonicalChatId('300@lid') }
    ]);
    await admin.hSet(keys(chat).state, 'session', '{"canonicalChatId":"RAW_MAPPING_MARKER"');
    await expect(readSessionMappings(connections, policy)).rejects.toThrow(/^session_failed$/);
  });
  it('separate blocking reader does not prevent durable intake; close drains and is idempotent', async () => {
    const waiting = repository.waitForWork(chat, workerId, 1000);
    expect((await repository.accept(event())).status).toBe('accepted'); await waiting;
    let unlock: (() => void) | undefined; const gate = new Promise<void>(resolve => { unlock = resolve; });
    let entered: (() => void) | undefined; const begun = new Promise<void>(resolve => { entered = resolve; });
    const real = connections.writer.sendCommand.bind(connections.writer);
    connections.writer.sendCommand = async args => { if (args[0] === 'WAITAOF') { entered?.(); await gate; } return real(args); };
    const intake = repository.accept(event(2)); await begun;
    let closed = false; const closing = repository.close().then(() => { closed = true; }); await Promise.resolve(); expect(closed).toBe(false);
    unlock?.(); expect((await intake).status).toBe('accepted'); await closing; await repository.close();
  });
  it('controller reconnect plus Redis SIGKILL/AOF restart preserves accepted backlog and PEL', async () => {
    const work = await claim(); await repository.accept(event(2));
    for (const c of opened.splice(0)) await c.close(); if (admin.isOpen) admin.destroy();
    await stop('SIGKILL'); await start();
    const result = await connect(); repository = result.r; connections = result.c;
    await admin.connect();
    expect(await admin.xLen(keys(chat).stream)).toBe(2);
    const pel = await admin.sendCommand(['XPENDING', keys(chat).stream, GROUP]); expect(Array.isArray(pel) && pel[0]).toBe(1);
    await admin.hSet(keys(chat).lease, 'until', '1'); // Expire persisted ownership only after backlog/PEL assertions.
    const app = await repository.acquireApp('f'.repeat(32), OWNERSHIP_MS);
    // Old application lease survives AOF; acquire only after explicit valid release.
    expect(app).toBeUndefined(); expect(await repository.releaseApp(work.ownership.app)).toBe(true);
    const current = await repository.acquireApp('f'.repeat(32), OWNERSHIP_MS); if (!current) throw new Error('synthetic_app_failed');
    const recovered = await repository.claimNext(chat, current, workerId, OWNERSHIP_MS, 1000);
    expect(recovered.status).toBe('claimed'); if (recovered.status === 'claimed') expect(recovered.work.event.chat_sequence).toBe(1);
    expect((await repository.accept(event(2))).status).toBe('duplicate');
  }, 15000);
  it('large waiting backlog has no expiration or trimming, including paused state', async () => {
    for (let n = 1; n <= 128; n++) expect((await repository.accept(event(n))).status).toBe('accepted');
    for (const k of Object.values(keys(chat)).filter(k => !k.endsWith(':lease') && !k.endsWith(':checkpoint') && !k.endsWith(':tools'))) expect(await admin.pTTL(k)).toBe(-1);
    expect(await admin.xLen(keys(chat).stream)).toBe(128);
    const app = await repository.acquireApp(appId, OWNERSHIP_MS); if (!app) throw new Error('synthetic_app_failed');
    const r = await repository.claimNext(chat, app, workerId, OWNERSHIP_MS, 1000); if (r.status !== 'claimed') throw new Error('synthetic_claim_failed');
    await repository.pause(r.work, 'session_failed'); expect(await admin.xLen(keys(chat).stream)).toBe(128);
  });
  it.each([['appendfsync', 'everysec'], ['maxmemory-policy', 'allkeys-lru']] as const)('rejects unsafe %s baseline at startup and after drift', async (key, value) => {
    await admin.sendCommand(['CONFIG', 'SET', key, value]);
    await expect(connectRedis(config())).rejects.toThrow('store_unavailable');
    expect((await repository.accept(event())).status).toBe('unavailable');
    await admin.sendCommand(['CONFIG', 'SET', key, key === 'appendfsync' ? 'always' : 'noeviction']);
  });
});
// These pure reply-shape tests cannot contact any Redis instance.
describe('baseline reply validation', () => {
  it.each(['7.1.9', '6.2.0', 'invalid'])('rejects version %s before startup', async version => {
    const fake: CommandPort = { sendCommand: async () => `redis_version:${version}\r\n` };
    await expect(requireBaseline(fake)).rejects.toThrow('store_unavailable');
  });
  it('rejects missing AOF or malformed CONFIG reply', async () => {
    const fake: CommandPort = { sendCommand: async args => args[0] === 'INFO' ? 'redis_version:7.2.6\r\n' : ['appendonly', 'no'] };
    await expect(requireBaseline(fake)).rejects.toThrow('store_unavailable');
  });
});
