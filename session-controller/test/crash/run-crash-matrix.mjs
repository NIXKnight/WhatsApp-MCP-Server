#!/usr/bin/env node
import { spawn } from 'node:child_process';
import { once } from 'node:events';
import { appendFileSync, existsSync, readFileSync } from 'node:fs';
import { mkdtemp, open, readFile, rm } from 'node:fs/promises';
import { createServer } from 'node:net';
import { dirname, isAbsolute, join, resolve } from 'node:path';
import { pathToFileURL } from 'node:url';
import { createRequire } from 'node:module';
import { Duplex } from 'node:stream';
const argv = new Map(); for (let i = 2; i < process.argv.length; i += 2) argv.set(process.argv[i], process.argv[i + 1]);
const root = resolve(argv.get('--controller-root') ?? process.env['SYNTHETIC_CONTROLLER_ROOT'] ?? process.cwd());
const rawBinary = argv.get('--redis-binary') ?? process.env['PHASE10_OWNED_REDIS_BINARY'];
if (!rawBinary || !isAbsolute(rawBinary)) { process.stderr.write('<REDACTED>\n'); process.exit(1); }
const binary = resolve(rawBinary);
const internal = process.env['SYNTHETIC_CRASH_WORKER']; let createClient;
const line = value => process.stdout.write(`${JSON.stringify(value)}\n`); const sleep = ms => new Promise(r => setTimeout(r, ms));
const childCloses = new WeakMap();
function trackChild(child) { const closed = new Promise(resolveClose => child.once('close', (code, signal) => resolveClose({ code, signal }))); childCloses.set(child, closed); return child; }
function childClose(child) { const closed = childCloses.get(child); if (!closed) throw new Error('owned_child_close_untracked'); return closed; }
const moduleAt = relative => import(pathToFileURL(join(root, 'dist', 'src', relative)).href);
async function modules() { const [domain, client, repository, keyModule, recovery, completion, sessionHost, coordinator] = await Promise.all([
  moduleAt('domain.js'), moduleAt('redis/client.js'), moduleAt('redis/repository.js'), moduleAt('redis/keys.js'), moduleAt('scheduler/recovery.js'),
  moduleAt('pi/completion-index.js'), moduleAt('pi/session-host.js'), moduleAt('scheduler/coordinator.js')]);
  return { domain, client, repository, keyModule, recovery, completion, sessionHost, coordinator }; }
function event(sequence) { return { schema_version: 2, event_type: 'whatsapp.message.received', event_id: String(sequence).padStart(32, '0'), message_id: `SYNTHETIC_CRASH_${sequence}`, event_timestamp: '2033-05-18T03:33:20Z', received_at: '2033-05-18T03:33:21Z', chat_sequence: sequence, sender_jid: '500@s.whatsapp.net', is_from_me: false, chat_kind: 'group', chat_jid: '100@g.us', canonical_chat_jid: '100@g.us' }; }
function sdk(turnLog) { let serial = 0; const manager = (file, header, entries) => ({ getSessionId: () => header.id, getSessionFile: () => file, getHeader: () => header, getEntries: () => entries, getBranch: () => entries,
  appendCustomEntry(customType, data) { const value = { type: 'custom', id: `ENTRY_${++serial}`, parentId: entries.at(-1)?.id ?? null, timestamp: '2033-05-18T03:33:22Z', customType, data }; entries.push(value); if (existsSync(file)) appendFileSync(file, JSON.stringify(value) + '\n'); return value.id; } });
  return { create(cwd, directory) { const id = `SYNTHETIC_SESSION_${++serial}`; return manager(join(directory, `${id}.jsonl`), { type: 'session', version: 3, id, cwd, timestamp: '2033-05-18T03:33:20Z' }, []); },
    open(file) { const rows = readFileSync(file, 'utf8').trimEnd().split('\n').map(JSON.parse); serial = Math.max(serial, rows.length + 10); return manager(file, rows.shift(), rows); },
    async connect(_manager, current) { await current(); let idle = true; const listeners = new Set(); return { get isIdle() { return idle; }, setAutoCompactionEnabled() {}, async bindExtensions() {}, getAllTools: () => [], subscribe(fn) { listeners.add(fn); return () => listeners.delete(fn); },
      async prompt() { idle = false; const fd = await open(turnLog, 'a', 0o600); try { await fd.writeFile('prompt\n'); await fd.sync(); } finally { await fd.close(); } idle = true; for (const fn of listeners) fn({ type: 'agent_settled', aborted: false }); }, async waitForIdle() {}, dispose() {}, getContextUsage: () => ({ tokens: 1, contextWindow: 10000, percent: 0.01 }), async compact() { throw new Error('external_model_disabled'); } }; }, async ready(_session, current) { await current(); } }; }
async function worker() {
  const port = Number(process.env['SYNTHETIC_REDIS_PORT']), mode = internal, turnLog = process.env['SYNTHETIC_TURN_LOG'], sessionRoot = process.env['SYNTHETIC_SESSION_ROOT'];
  if (!Number.isInteger(port) || !mode || !turnLog || !sessionRoot) throw new Error('worker_config_invalid');
  if (mode === 'never_barrier') {
    const keepAlive = setInterval(() => {}, 1000);
    const clear = () => { clearInterval(keepAlive); };
    process.once('SIGTERM', clear);
    process.once('SIGINT', clear);
    process.once('exit', clear);
    return;
  }
  const m = await modules(), chat = m.domain.canonicalChatId('100@g.us'), policy = { groups: [chat], operatorJids: ['300@lid'] };
  const expected = { pid: Number(process.env['SYNTHETIC_REDIS_PID']), runId: process.env['SYNTHETIC_REDIS_RUN_ID'] };
  if (!Number.isSafeInteger(expected.pid) || expected.pid <= 0 || !/^[0-9a-f]{40}$/.test(expected.runId ?? '')) throw new Error('worker_config_invalid');
  if (mode === 'expire') { const admin = createClient({ url: `redis://127.0.0.1:${port}/0`, disableOfflineQueue: true, socket: { reconnectStrategy: false } }); admin.on('error', () => {}); try { await admin.connect(); await verifyRedisIdentity(admin, expected); await admin.hSet(m.keyModule.APP, 'until', '1'); await admin.hSet(m.keyModule.keys(chat).lease, 'until', '1'); line({ barrier: 'expired' }); } finally { if (admin.isOpen) admin.destroy(); } return; }
  const connections = await m.client.connectRedis({ url: `redis://127.0.0.1:${port}/0`, waitAofMs: 2000, blockMs: 10 }); let index, repo, host, runner;
  try {
  await verifyConnections(connections, expected);
  repo = await m.repository.createRepository(connections, policy, { findEvent: (c, id) => index.findEvent(c, id), classifyOwn: (c, actual, id) => index.classifyOwn(c, actual, id) });
  const rows = await Promise.all(repo.chats.map(async c => { const state = await repo.readChat(c); return { chat: c, ...(state.session ? { binding: state.session } : {}) }; }));
  index = m.completion.createCompletionIndex(sessionRoot, policy, { list: async () => rows.map(r => ({ ...r })) }, async (actual, id) => (await repo.ownMutation(actual, id)) === 'uncertain');
    if (mode === 'accept' || mode === 'claim') { const accepted = await repo.accept(event(Number(process.env['SYNTHETIC_SEQUENCE']))); if (!['accepted', 'duplicate'].includes(accepted.status)) throw new Error('accept_failed'); if (mode === 'accept') { line({ barrier: 'accepted' }); await new Promise(() => {}); }
      const app = await repo.acquireApp('a'.repeat(32), 120000); if (!app) throw new Error('app_failed'); const claimed = await repo.claimNext(chat, app, 'b'.repeat(32), 120000, 1000); if (claimed.status !== 'claimed') throw new Error('claim_failed'); line({ barrier: 'claimed' }); await new Promise(() => {}); }
    host = m.sessionHost.createSessionHost({ cwd: '/tmp', root: sessionRoot, sdk: sdk(turnLog), index, current: async work => { if (!await repo.checkWork(work)) throw new m.domain.ControllerError('lease_lost'); }, bind: async (work, binding) => { const ok = await repo.bindSession(work, binding); if (ok) rows.find(r => r.chat === work.event.canonical_chat_jid).binding = binding; return ok; } });
    let ids = 0; const turns = { attach: async () => {}, prepare: async () => ({ usageRequiredAfterPrompt: false }), run: async (_s, _c, prompt) => prompt(), confirmed: async () => [] };
    runner = m.coordinator.createCoordinator(repo, { scheduler: { concurrency: 1, leaseMs: 120000, renewMs: 10000, gapTimeoutMs: 1000 }, pollMs: 10, ids: { next: () => (++ids).toString(16).padStart(32, '0') }, worker: { instructions: () => 'SYNTHETIC', host, turns, wait: m.coordinator.timerWait, diagnostics: { emit() {} } } });
    await runner.start(); const wanted = Number(process.env['SYNTHETIC_SEQUENCE']); for (let n = 0; n < 1000; n++) { if ((await repo.readChat(chat)).lastCompletedSequence >= wanted) break; await sleep(5); if (n === 999) throw new Error('consume_timeout'); }
    runner.stopScheduling(); await runner.drain(); await host.close(); line({ barrier: 'completed' });
  } finally {
    let first;
    const attempt = async (action, fixed) => { try { await action(); } catch { first ??= new Error(fixed); } };
    await attempt(async () => { if (runner) { runner.stopScheduling(); await runner.drain(); } }, 'worker_drain_failed');
    await attempt(async () => { await host?.close(); }, 'worker_host_cleanup_failed');
    await attempt(() => connections.close(), 'worker_connections_cleanup_failed');
    if (first) throw first;
  }
}
async function reservePort() { const server = createServer(); server.listen(0, '127.0.0.1'); await once(server, 'listening'); const address = server.address(); if (!address || typeof address === 'string') throw new Error('port_failed'); const port = address.port; await new Promise((r, j) => server.close(e => e ? j(e) : r())); return port; }
function parseRedisIdentity(reply) {
  if (typeof reply !== 'string' || Buffer.byteLength(reply, 'utf8') > 16384) throw new Error('owned_redis_identity_invalid');
  const field = name => { const rows = reply.split('\n').map(row => row.replace(/\r$/, '')).filter(row => row.startsWith(`${name}:`)); if (rows.length !== 1) throw new Error('owned_redis_identity_invalid'); return rows[0].slice(name.length + 1); };
  const pidText = field('process_id'), pid = Number(pidText), runId = field('run_id');
  if (field('redis_version') !== '7.2.6' || !/^[1-9]\d*$/.test(pidText) || !Number.isSafeInteger(pid) || !/^[0-9a-f]{40}$/.test(runId)) throw new Error('owned_redis_identity_invalid');
  return { pid, runId };
}
async function verifyRedisIdentity(port, expected) { const actual = parseRedisIdentity(await Promise.race([port.sendCommand(['INFO', 'server']), sleep(1000).then(() => { throw new Error('owned_redis_identity_timeout'); })])); if (actual.pid !== expected.pid || actual.runId !== expected.runId) throw new Error('owned_redis_identity_mismatch'); }
async function verifyConnections(connections, expected) { for (const port of [connections.writer, connections.reader, connections.mapping]) await verifyRedisIdentity(port, expected); }
function assertRedisLive(child) { if (!child || !Number.isSafeInteger(child.pid) || !child.pid || child.exitCode !== null || child.signalCode !== null) throw new Error('owned_redis_child_not_live'); }
async function waitRedis(port, child) {
  for (let n = 0; n < 300; n++) {
    assertRedisLive(child);
    const probe = createClient({ url: `redis://127.0.0.1:${port}/0`, disableOfflineQueue: true, socket: { reconnectStrategy: false, connectTimeout: 100 } }); probe.on('error', () => {}); let connected = false;
    try { await Promise.race([probe.connect(), childClose(child).then(() => { throw new Error('owned_redis_child_not_live'); })]); connected = true; assertRedisLive(child); const identity = parseRedisIdentity(await Promise.race([probe.sendCommand(['INFO', 'server']), childClose(child).then(() => { throw new Error('owned_redis_child_not_live'); }), sleep(1000).then(() => { throw new Error('owned_redis_identity_timeout'); })])); if (identity.pid !== child.pid) throw new Error('owned_redis_identity_mismatch'); await verifyRedisIdentity(probe, identity); assertRedisLive(child); return identity; }
    catch { assertRedisLive(child); if (connected) throw new Error('owned_redis_identity_mismatch'); await sleep(10); }
    finally { if (probe.isOpen) probe.destroy(); }
  }
  throw new Error('redis_start_failed');
}
async function redisChild(port, directory, ownership) {
  const child = trackChild(spawn(binary, ['--bind', '127.0.0.1', '--port', String(port), '--dir', directory, '--appendonly', 'yes', '--appendfsync', 'always', '--maxmemory-policy', 'noeviction', '--save', '', '--daemonize', 'no'], { stdio: 'ignore', env: { PATH: '/usr/bin:/bin' } }));
  const closed = childClose(child).then(result => { ownership.settled = true; return result; });
  ownership.child = child; ownership.closed = closed; ownership.settled = false;
  try { await once(child, 'spawn'); }
  catch {
    await closed;
    if (ownership.child === child) ownership.child = undefined;
    throw new Error('owned_child_spawn_failed');
  }
  return child;
}
async function terminate(child, signal = 'SIGTERM') { if (!child) return; const closed = childClose(child).then(() => true, () => false); if (child.exitCode === null && child.signalCode === null) child.kill(signal); if (await Promise.race([closed, sleep(2000).then(() => false)])) return; if (child.exitCode === null && child.signalCode === null) child.kill('SIGKILL'); if (!await Promise.race([closed, sleep(2000).then(() => false)])) throw new Error('owned_child_exit_timeout'); }
async function redisVersion(ownership) {
  const child = trackChild(spawn(binary, ['--version'], { stdio: ['ignore', 'pipe', 'pipe'], env: { PATH: '/usr/bin:/bin' } }));
  ownership.child = child; ownership.settled = false;
  const captureLimit = 4096; let stdout = '', stderr = '', capturedBytes = 0, overflow = false;
  const capture = (target, value) => { const added = Buffer.byteLength(value, 'utf8'); if (overflow || capturedBytes + added > captureLimit) { overflow = true; return target; } capturedBytes += added; return target + value; };
  child.stdout.setEncoding('utf8'); child.stderr.setEncoding('utf8');
  child.stdout.on('data', value => { stdout = capture(stdout, value); }); child.stderr.on('data', value => { stderr = capture(stderr, value); });
  const closed = childClose(child).then(result => { ownership.settled = true; return result; }); ownership.closed = closed;
  const spawned = new Promise((resolveSpawn, rejectSpawn) => { child.once('spawn', () => resolveSpawn(true)); child.once('error', () => rejectSpawn(new Error('redis_version_probe_failed'))); });
  try {
    if (!await Promise.race([spawned, sleep(2000).then(() => false)])) throw new Error('redis_version_probe_failed');
    const result = await Promise.race([closed, sleep(2000).then(() => undefined)]);
    if (!result) throw new Error('redis_version_probe_failed');
    const version = /(?:^|\s)v=([^\s]+)/.exec(stdout)?.[1];
    if (overflow || result.code !== 0 || stderr !== '' || version !== '7.2.6') throw new Error('redis_version_probe_failed');
    if (ownership.child === child) ownership.child = undefined;
    return version;
  } catch {
    if (!ownership.settled && child.exitCode === null && child.signalCode === null) await terminate(child);
    if (!ownership.settled && !await Promise.race([closed.then(() => true), sleep(2000).then(() => false)])) throw new Error('owned_child_exit_timeout');
    if (ownership.child === child) ownership.child = undefined;
    throw new Error('redis_version_probe_failed');
  }
}
function childBarrier(mode, port, turnLog, sessionRoot, sequence, expected, identity) { const child = trackChild(spawn(process.execPath, [resolve(process.argv[1]), '--controller-root', root, '--redis-binary', binary], { stdio: ['ignore', 'pipe', 'ignore'], env: { PATH: process.env['PATH'] ?? '/usr/bin:/bin', SYNTHETIC_CRASH_WORKER: mode, SYNTHETIC_REDIS_PORT: String(port), SYNTHETIC_REDIS_PID: String(identity?.pid ?? ''), SYNTHETIC_REDIS_RUN_ID: identity?.runId ?? '', SYNTHETIC_TURN_LOG: turnLog, SYNTHETIC_SESSION_ROOT: sessionRoot, SYNTHETIC_SEQUENCE: String(sequence) } }));
  const barrierCaptureLimit = 4096; let buffered = '', settled = false, stopping = false;
  const barrier = new Promise((ok, fail) => {
    const finish = (fn, value) => { if (settled) return; settled = true; clearTimeout(timeout); fn(value); };
    const stopAndFail = error => { if (settled || stopping) return; stopping = true; void terminate(child).then(
      () => finish(fail, error),
      terminationError => finish(fail, terminationError instanceof Error && terminationError.message === 'owned_child_exit_timeout' ? terminationError : new Error('owned_child_exit_timeout'))
    ); };
    const timeout = setTimeout(() => stopAndFail(new Error('barrier_timeout')), process.env['SYNTHETIC_CRASH_TEST_BARRIER_TIMEOUT'] === '1' ? 250 : 10000);
    child.once('error', () => finish(fail, new Error('owned_child_spawn_failed')));
    child.stdout.setEncoding('utf8'); child.stdout.on('data', chunk => {
      if (settled || stopping) return;
      if (Buffer.byteLength(buffered, 'utf8') + Buffer.byteLength(chunk, 'utf8') > barrierCaptureLimit) { stopAndFail(new Error('barrier_output_overflow')); return; }
      buffered += chunk; const end = buffered.indexOf('\n'); if (end < 0) return;
      try { const value = JSON.parse(buffered.slice(0, end)); value.barrier === expected ? finish(ok, value) : finish(fail, new Error('barrier_invalid')); } catch { finish(fail, new Error('barrier_invalid')); }
    });
    child.once('exit', code => { if (!stopping && code !== 0) finish(fail, new Error('child_failed')); });
  }); return { child, barrier }; }
async function delayedCloseTest() {
  // fd 3 belongs to this parent. The ready holder inherits its read side and
  // stdout/stderr, and cannot exit until this parent ends the release pipe.
  const holder = "process.stdin.resume();process.stdin.once('end',()=>process.exit(0));process.send('ready');process.disconnect();";
  const leader = `const {spawn}=require('node:child_process');const held=spawn(process.execPath,['-e',${JSON.stringify(holder)}],{stdio:[3,1,2,'ipc']});held.once('error',()=>process.exit(1));held.once('message',message=>{if(message!=='ready')process.exit(1);held.unref();process.exit(0);});`;
  let child, releasePipe, closing, deadline;
  try {
    child = trackChild(spawn(process.execPath, ['-e', leader], { stdio: ['ignore', 'pipe', 'pipe', 'pipe'] }));
    const pipe = child.stdio[3]; if (!(pipe instanceof Duplex)) throw new Error('delayed_close_fixture_invalid');
    releasePipe = pipe; pipe.on('error', () => {});
    const closePromise = childClose(child); let closeSettled = false, terminationSettled = false;
    void closePromise.then(() => { closeSettled = true; });
    await Promise.race([once(child, 'exit'), new Promise((_, reject) => { deadline = setTimeout(() => reject(new Error('delayed_close_exit_timeout')), 2000); })]);
    clearTimeout(deadline);
    await new Promise(resolveTurn => setImmediate(resolveTurn));
    if (child.exitCode !== 0 || closeSettled) throw new Error('delayed_close_fixture_invalid');
    closing = terminate(child).then(() => { terminationSettled = true; }); void closing.catch(() => {});
    await new Promise(resolveTurn => setImmediate(resolveTurn));
    if (closeSettled || terminationSettled) throw new Error('delayed_close_fixture_invalid');
    pipe.end(); await closing; await closePromise;
    if (!closeSettled || !terminationSettled) throw new Error('owned_child_exit_timeout');
  } finally {
    clearTimeout(deadline); releasePipe?.end();
    const results = await Promise.allSettled([closing, terminate(child)]);
    if (results.some(result => result.status === 'rejected')) throw new Error('owned_child_exit_timeout');
  }
  line({ status: 'close-settled' });
}
async function parent() { if (process.env['SYNTHETIC_CRASH_USE_DOCKER'] === '1') throw new Error('docker_opt_in_requires_external_compose_runner'); const versionProbe = { child: undefined }; const redisOwnership = { child: undefined, closed: undefined, settled: true }; let directory, redis, controller, identity;
  try { const version = await redisVersion(versionProbe); directory = await mkdtemp('/tmp/whatsapp-synthetic-crash-'); const sessionRoot = join(directory, 'sessions'), port = await reservePort(), turnLog = join(directory, 'logical-turns.log');
    redis = await redisChild(port, directory, redisOwnership); identity = await waitRedis(port, redis);
    if (process.env['SYNTHETIC_CRASH_TEST_BARRIER_TIMEOUT'] === '1') { const started = childBarrier('never_barrier', port, turnLog, sessionRoot, 0, 'never', identity); controller = started.child; await started.barrier; throw new Error('barrier_timeout_test_failed'); }
    { const started = childBarrier('accept', port, turnLog, sessionRoot, 1, 'accepted', identity); controller = started.child; await started.barrier; } await terminate(controller, 'SIGKILL'); controller = undefined; { const started = childBarrier('consume', port, turnLog, sessionRoot, 1, 'completed', identity); controller = started.child; await started.barrier; } await terminate(controller); controller = undefined;
    { const started = childBarrier('claim', port, turnLog, sessionRoot, 2, 'claimed', identity); controller = started.child; await started.barrier; } await terminate(controller, 'SIGKILL'); controller = undefined; { const started = childBarrier('expire', port, turnLog, sessionRoot, 2, 'expired', identity); controller = started.child; await started.barrier; } await terminate(controller); controller = undefined; { const started = childBarrier('consume', port, turnLog, sessionRoot, 2, 'completed', identity); controller = started.child; await started.barrier; } await terminate(controller); controller = undefined;
    { const started = childBarrier('accept', port, turnLog, sessionRoot, 3, 'accepted', identity); controller = started.child; await started.barrier; } await terminate(controller, 'SIGKILL'); controller = undefined; await terminate(redis, 'SIGKILL'); if (redisOwnership.child === redis) redisOwnership.child = undefined; redis = undefined; const previousIdentity = identity; redis = await redisChild(port, directory, redisOwnership); identity = await waitRedis(port, redis); if (identity.runId === previousIdentity.runId) throw new Error('redis_restart_identity_invalid'); { const started = childBarrier('consume', port, turnLog, sessionRoot, 3, 'completed', identity); controller = started.child; await started.barrier; } await terminate(controller); controller = undefined;
    const turns = (await readFile(turnLog, 'utf8')).trim().split('\n'); if (turns.length !== 3) throw new Error('logical_turn_count_invalid'); line({ status: 'ok', redisVersion: version, queuedRestart: true, pelRestart: true, redisAofRestart: true, logicalTurns: turns.length });
  } finally {
    let cleanupFailure;
    const attempt = async (work, fixed) => { try { await work(); } catch { cleanupFailure ??= new Error(fixed); } };
    // The controller and Redis both use the owned directory. A handle is clearable
    // only after close settlement; an exit timeout retains both process and path.
    const ownedController = controller; let controllerSettled = !ownedController;
    await attempt(async () => { if (!ownedController) return; await terminate(ownedController); controllerSettled = true; if (controller === ownedController) controller = undefined; }, 'owned_child_exit_timeout');
    const ownedRedis = redis ?? redisOwnership.child; let redisSettled = !ownedRedis;
    await attempt(async () => { if (!ownedRedis) return; await terminate(ownedRedis); if (redisOwnership.closed && !redisOwnership.settled) await redisOwnership.closed; redisSettled = true; if (redis === ownedRedis) redis = undefined; if (redisOwnership.child === ownedRedis) redisOwnership.child = undefined; }, 'owned_child_exit_timeout');
    const probe = versionProbe.child;
    await attempt(async () => { if (!probe) return; await terminate(probe); if (!versionProbe.settled && !await Promise.race([versionProbe.closed.then(() => true), sleep(2000).then(() => false)])) throw new Error('owned_child_exit_timeout'); if (versionProbe.child === probe) versionProbe.child = undefined; }, 'owned_child_exit_timeout');
    const ownedDirectory = directory;
    if (controllerSettled && redisSettled) {
      await attempt(async () => { if (!ownedDirectory) return; await rm(ownedDirectory, { recursive: true, force: true }); if (directory === ownedDirectory) directory = undefined; }, 'temporary_directory_cleanup_failed');
    }
    // Never rm beneath an unsettled directory user. cleanupFailure preserves the
    // first fixed termination failure after every independent safe attempt.
    if (cleanupFailure) throw cleanupFailure;
  } }
try { if (process.env['SYNTHETIC_CRASH_TEST_DELAYED_CLOSE'] === '1') await delayedCloseTest(); else { const requireAtRoot = createRequire(join(root, 'package.json')); ({ createClient } = requireAtRoot('redis')); if (internal) await worker(); else await parent(); } } catch { process.stderr.write('<REDACTED>\n'); process.exitCode = 1; }
