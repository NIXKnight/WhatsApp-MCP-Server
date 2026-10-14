import { describe, it, expect } from 'vitest';
import {
  canonicalChatId, eventId, fingerprintEvent, ControllerError,
  type ClaimedWork, type CompletionProof, type CompletionMarker, type PiHostPort,
  type PiSessionPort, type Fence, type Ownership, type SessionBinding
} from '../../src/domain.js';
import { createCoordinator, timerWait, type CoordinatorRepository } from '../../src/scheduler/coordinator.js';
import { runChatWorker, type WorkerRepository, type WorkerOptions, type TurnContext } from '../../src/scheduler/chat-worker.js';
import { createRecovery } from '../../src/scheduler/recovery.js';
const chat = canonicalChatId('100@g.us'), other = canonicalChatId('200@g.us');
const fence: Fence = { owner: 'a'.repeat(32), epoch: '9007199254740993', expiresAtMs: 2000000000000 };
const ownership: Ownership = { app: fence, chat: { ...fence, owner: 'b'.repeat(32) } };
function work(c = chat): ClaimedWork {
  return { streamId: '1-0', ownership, event: { schema_version: 2, event_type: 'whatsapp.message.received',
    event_id: eventId(c === chat ? '1'.repeat(32) : '2'.repeat(32)), message_id: 'TEST_MESSAGE',
    event_timestamp: '2033-05-18T03:33:20Z', received_at: '2033-05-18T03:33:21Z', chat_sequence: 1,
    chat_kind: 'group', chat_jid: c, canonical_chat_jid: c, sender_jid: '300@lid', is_from_me: false } };
}
function binding(c = chat): SessionBinding { return { canonicalChatId: c, sessionId: 'TEST_SESSION', sessionFile: '/tmp/synthetic-session.jsonl' }; }
function proof(w = work()): CompletionProof {
  return { entryId: 'TEST_ENTRY', session: binding(w.event.canonical_chat_jid), marker: {
    version: 1, eventId: w.event.event_id, eventFingerprint: fingerprintEvent(w.event), canonicalChatId: w.event.canonical_chat_jid,
    chatSequence: 1, completion: 'turn', completedAt: '2033-05-18T03:33:22Z', confirmedSends: [] } };
}
function gate() { let resolve!: () => void; const promise = new Promise<void>(r => { resolve = r; }); return { promise, resolve }; }
async function until(check: () => boolean) {
  for (let i = 0; i < 1000 && !check(); i++) await new Promise(r => setImmediate(r));
  expect(check()).toBe(true);
}
function fixture() {
  const effects: string[] = [];
  let current = true, started = false, evidence: CompletionProof | undefined, activeBinding: SessionBinding | undefined;
  let promptGate: ReturnType<typeof gate> | undefined;
  const repository: WorkerRepository = {
    renew: async () => current ? ownership : undefined, checkWork: async () => current,
    readChat: async c => ({ canonicalChatId: c, lastCompletedSequence: 0, ...(activeBinding ? { session: activeBinding } : {}) }),
    bindSession: async (_w, b) => { effects.push('bind'); activeBinding = b; return current; },
    turnState: async () => started ? 'started' : 'fresh', beginTurn: async () => { effects.push('begin'); started = true; return current; },
    pause: async (_w, reason) => { effects.push('pause:' + reason); return current; },
    complete: async (_w, p) => { effects.push('complete'); return current && p === evidence; }
  };
  const host: PiHostPort = {
    findEvent: async () => evidence, classifyOwn: async () => 'manual', close: async () => {},
    open: async w => {
      effects.push('open'); let guard: TurnContext | undefined;
      const session: PiSessionPort = {
        binding: binding(w.event.canonical_chat_jid), usage: () => undefined,
        compact: async () => { throw new ControllerError('compaction_failed'); },
        prompt: async () => { await guard?.current(); effects.push('prompt'); await promptGate?.promise; },
        appendCompletion: async (marker: CompletionMarker) => { await guard?.current(); effects.push('marker'); evidence = { ...proof(w), marker }; return evidence; },
        dispose: async () => { effects.push('dispose'); }
      };
      Object.assign(session, { setGuard: (c: TurnContext) => { guard = c; } }); return session;
    }
  };
  const options: WorkerOptions = {
    leaseMs: 120000, renewMs: 100, appCurrent: () => current, instructions: () => 'SYNTHETIC_POLICY',
    host, wait: timerWait, diagnostics: { emit: d => effects.push('diagnostic:' + d.class) },
    clock: { now: () => 2000000000000 }, turns: {
      attach: async (s, c) => (s as PiSessionPort & { setGuard(c: TurnContext): void }).setGuard(c),
      prepare: async () => { effects.push('prepare'); },
      run: async (_s, _c, prompt) => prompt(), confirmed: async () => []
    }
  };
  return { repository, options, effects, lose: () => { current = false; },
    setStarted: () => { started = true; }, setEvidence: (p: CompletionProof) => { evidence = p; activeBinding = p.session; },
    holdPrompt: () => { promptGate = gate(); return promptGate; } };
}
describe('ownership-safe worker, injected Pi only', () => {
  it('binds and records durable turn attempt before prompt; marker precedes cleanup', async () => {
    const f = fixture(); await runChatWorker(f.repository, work(), f.options);
    expect(f.effects).toEqual(['open', 'bind', 'prepare', 'begin', 'prompt', 'marker', 'complete', 'dispose']);
  });
  it('existing strict marker completes without opening or repeating Pi', async () => {
    const f = fixture(); f.setEvidence(proof()); await runChatWorker(f.repository, work(), f.options);
    expect(f.effects).toEqual(['complete']);
  });
  it('wrong-fingerprint marker cannot authorize deletion or a repeated prompt', async () => {
    const f = fixture(); const p = proof(); p.marker.eventFingerprint = 'sha256:v1:' + '0'.repeat(64); f.setEvidence(p);
    await runChatWorker(f.repository, work(), f.options);
    expect(f.effects).toEqual(['pause:session_failed', 'diagnostic:session_failed']);
  });
  it('unfinished logical attempt without marker pauses uncertain before opening', async () => {
    const f = fixture(); f.setStarted(); await runChatWorker(f.repository, work(), f.options);
    expect(f.effects).toEqual(['pause:mutation_uncertain', 'diagnostic:mutation_uncertain']);
  });
  it('stale initial app starts no effects', async () => {
    const f = fixture(); f.lose(); await runChatWorker(f.repository, work(), f.options);
    expect(f.effects).toEqual(['diagnostic:lease_lost']);
  });
  it('lost ownership during active prompt prevents marker and completion', async () => {
    const f = fixture(), hold = f.holdPrompt(); const running = runChatWorker(f.repository, work(), f.options);
    await until(() => f.effects.includes('prompt')); f.lose(); hold.resolve(); await running;
    expect(f.effects).not.toContain('marker'); expect(f.effects).not.toContain('complete'); expect(f.effects).not.toContain('pause:lease_lost');
  });
  it('manual own message still prompts', async () => {
    const f = fixture(), w = work(); w.event.is_from_me = true;
    await runChatWorker(f.repository, w, f.options); expect(f.effects).toContain('prompt');
  });
  it('confirmed own message is context-only, uncertain own message pauses', async () => {
    const f = fixture(), w = work(); w.event.is_from_me = true; f.options.host.classifyOwn = async () => 'confirmed';
    await runChatWorker(f.repository, w, f.options); expect(f.effects).not.toContain('prompt'); expect(f.effects).toContain('marker');
    const u = fixture(); u.options.host.classifyOwn = async () => 'uncertain'; await runChatWorker(u.repository, w, u.options);
    expect(u.effects).toEqual(['pause:mutation_uncertain', 'diagnostic:mutation_uncertain']);
  });
  it('renewal continues while prompt is held', async () => {
    const f = fixture(), hold = f.holdPrompt(), tick = gate(); let waiting = false, renewals = 0, first = true;
    f.options.wait = { wait: async (_ms, signal) => { waiting = true; const p = first ? tick.promise : new Promise<void>(() => {}); first = false; await Promise.race([p, new Promise<void>(r => signal.addEventListener('abort', () => r(), { once: true }))]); } };
    const original = f.repository.renew; f.repository.renew = async (...args) => { renewals++; return original(...args); };
    const running = runChatWorker(f.repository, work(), f.options); await until(() => waiting && f.effects.includes('prompt'));
    const before = renewals; tick.resolve(); await until(() => renewals > before); hold.resolve(); await running;
    expect(renewals).toBeGreaterThan(before);
  });
  it('uncertain prompt result is never marked or retried', async () => {
    const f = fixture(); f.options.turns.run = async (_s, _c, prompt) => { await prompt(); throw new Error('synthetic_failure'); };
    await runChatWorker(f.repository, work(), f.options);
    expect(f.effects.filter(x => x === 'prompt')).toHaveLength(1); expect(f.effects).not.toContain('marker'); expect(f.effects).toContain('pause:mutation_uncertain');
  });
});
describe('non-authoritative diagnostics', () => {
  it('throwing sink preserves pause and cleanup despite disposal failure', async () => {
    const f = fixture();
    f.options.diagnostics = { emit: () => { throw new Error('synthetic_sink'); } };
    const open = f.options.host.open;
    f.options.host.open = async (...args) => { const session = await open(...args); session.dispose = async () => { f.effects.push('dispose'); throw new Error('synthetic_dispose'); }; return session; };
    f.options.turns.run = async () => { throw new Error('synthetic_turn'); };
    await expect(runChatWorker(f.repository, work(), f.options)).resolves.toBeUndefined();
    expect(f.effects).toContain('pause:mutation_uncertain'); expect(f.effects).toContain('dispose');
    expect(f.effects).not.toContain('marker'); expect(f.effects).not.toContain('complete');
  });
});
describe('bounded coordinator and recovery hints', () => {
  function coordinatorFixture(concurrency = 2) {
    const f = fixture(), holds = new Map<string, ReturnType<typeof gate>>(), claimed = new Set<string>();
    const attempts = new Map<string, number>(), starts = new Map<string, number>(), completed = new Set<string>();
    let scheduling = 0, max = 0, appRenewFailed = false;
    const repository: CoordinatorRepository = {
      ...f.repository, chats: [chat, other], acquireApp: async () => fence,
      renewApp: async () => appRenewFailed ? undefined : fence, releaseApp: async () => true,
      recoverPending: async () => ({ status: 'ok' as const, next: '0-0', claimed: 0, deleted: 0 }),
      claimNext: async (c: typeof chat) => {
        attempts.set(c, (attempts.get(c) ?? 0) + 1);
        if (completed.has(c)) return { status: 'empty' as const };
        // Adversarial port offers work again even while this chat is active.
        claimed.add(c); scheduling++; max = Math.max(max, scheduling); return { status: 'claimed' as const, work: work(c) };
      }
    };
    f.options.host.open = async w => {
      const h = gate(); holds.set(w.event.canonical_chat_jid, h);
      starts.set(w.event.canonical_chat_jid, (starts.get(w.event.canonical_chat_jid) ?? 0) + 1);
      return { binding: binding(w.event.canonical_chat_jid), usage: () => undefined,
        prompt: async () => h.promise, appendCompletion: async () => proof(w),
        compact: async () => { throw new ControllerError('compaction_failed'); }, dispose: async () => { scheduling--; } };
    };
    f.options.turns.attach = async () => {};
    // Per-chat state avoids fixture-level turn/binding cross-talk.
    repository.readChat = async c => ({ canonicalChatId: c, lastCompletedSequence: 0 });
    repository.bindSession = async () => true; repository.turnState = async () => 'fresh';
    repository.beginTurn = async () => true;
    repository.complete = async w => { completed.add(w.event.canonical_chat_jid); return true; };
    let serial = 0;
    const { appCurrent: _app, leaseMs: _lease, renewMs: _renew, ...worker } = f.options;
    const coordinator = createCoordinator(repository, { scheduler: { concurrency, leaseMs: 120000, renewMs: 100, gapTimeoutMs: 1000 },
      pollMs: 1, ids: { next: () => (++serial).toString(16).padStart(32, '0') }, worker });
    return { coordinator, holds, claimed, attempts, starts, completed, maximum: () => max, failRenew: () => { appRenewFailed = true; }, repository, worker };
  }
  it('distinct chats run concurrently, each chat once; indefinite drain waits for all', async () => {
    const f = coordinatorFixture(); await f.coordinator.start(); await until(() => f.holds.size === 2);
    f.coordinator.stopScheduling(); let drained = false; const draining = f.coordinator.drain().then(() => { drained = true; });
    await new Promise(r => setImmediate(r)); expect(drained).toBe(false); expect(f.maximum()).toBe(2);
    f.holds.get(chat)?.resolve(); await new Promise(r => setImmediate(r)); expect(drained).toBe(false);
    f.holds.get(other)?.resolve(); await draining; expect(f.claimed.size).toBe(2);
  });
  it('active same-chat offers are excluded while another chat completes and polling continues', async () => {
    const f = coordinatorFixture(); let polls = 0;
    const original = f.worker.wait;
    f.worker.wait = { wait: async (ms, signal) => {
      if (ms === 1) { polls++; await new Promise<void>(r => setImmediate(r)); }
      else await original.wait(ms, signal);
    } };
    await f.coordinator.start(); await until(() => f.holds.size === 2);
    const before = polls; f.holds.get(other)!.resolve();
    await until(() => f.completed.has(other) && polls >= before + 3);
    expect(f.completed.has(chat)).toBe(false);
    expect(f.attempts.get(chat)).toBe(1); expect(f.starts.get(chat)).toBe(1);
    expect(f.starts.get(other)).toBe(1);
    f.coordinator.stopScheduling(); f.holds.get(chat)!.resolve(); await f.coordinator.drain();
    expect(f.completed.size).toBe(2);
  });
  it('throwing diagnostics cannot reject app-loss or release-failure drain', async () => {
    for (const loss of [true, false]) {
      const f = coordinatorFixture(1);
      f.worker.diagnostics = { emit: () => { throw new Error('synthetic_sink'); } };
      f.repository.releaseApp = async () => { throw new Error('synthetic_release'); };
      await f.coordinator.start(); await until(() => f.holds.size === 1);
      if (loss) { f.failRenew(); await new Promise(r => setTimeout(r, 120)); }
      f.coordinator.stopScheduling(); for (const h of f.holds.values()) h.resolve();
      await expect(f.coordinator.drain()).resolves.toBeUndefined();
    }
  });
  it('concurrency one bounds cross-chat work', async () => {
    const f = coordinatorFixture(1); await f.coordinator.start(); await until(() => f.holds.size === 1);
    f.coordinator.stopScheduling(); const draining = f.coordinator.drain(); for (const h of f.holds.values()) h.resolve(); await draining;
    expect(f.maximum()).toBe(1); expect(f.claimed.size).toBe(1);
  });
  it('claim loss stops application heartbeat', async () => {
    const f = coordinatorFixture(1), heartbeatTick = gate();
    let heartbeatWaiting = false, renewals = 0, losses = 0;
    f.worker.wait = { wait: async (ms, signal) => {
      if (ms === 100) {
        heartbeatWaiting = true;
        await Promise.race([heartbeatTick.promise, new Promise<void>(r => signal.addEventListener('abort', () => r(), { once: true }))]);
      } else await timerWait.wait(ms, signal);
    } };
    f.worker.diagnostics = { emit: event => { if (event.class === 'lease_lost') losses++; } };
    f.repository.renewApp = async () => { renewals++; return fence; };
    f.repository.claimNext = async () => ({ status: 'lost' as const });

    await f.coordinator.start();
    await until(() => heartbeatWaiting && losses === 1);
    heartbeatTick.resolve();
    await new Promise<void>(r => setImmediate(r));
    expect(renewals).toBe(0); expect(losses).toBe(1);

    await f.coordinator.drain();
  });
  it('loop failure stops application heartbeat', async () => {
    const f = coordinatorFixture(1), heartbeatTick = gate(), failLoop = gate();
    let heartbeatWaiting = false, claimWaiting = false, renewals = 0, losses = 0;
    f.worker.wait = { wait: async (ms, signal) => {
      if (ms === 100) {
        heartbeatWaiting = true;
        await Promise.race([heartbeatTick.promise, new Promise<void>(r => signal.addEventListener('abort', () => r(), { once: true }))]);
      } else await timerWait.wait(ms, signal);
    } };
    f.worker.diagnostics = { emit: event => { if (event.class === 'lease_lost') losses++; } };
    f.repository.renewApp = async () => { renewals++; return fence; };
    f.repository.claimNext = async () => {
      claimWaiting = true;
      await failLoop.promise;
      throw new Error('synthetic_loop_failure');
    };

    await f.coordinator.start();
    await until(() => heartbeatWaiting && claimWaiting);
    failLoop.resolve();
    await until(() => losses === 1);
    heartbeatTick.resolve();
    await new Promise<void>(r => setImmediate(r));
    expect(renewals).toBe(0); expect(losses).toBe(1);

    await f.coordinator.drain();
  });
  it('normal drain renews until active work settles then releases leadership', async () => {
    const f = coordinatorFixture(1), heartbeatTick = gate(), ordering: string[] = [];
    let appHeartbeatSignal: AbortSignal | undefined, renewals = 0, releases = 0, drained = false;
    f.worker.wait = { wait: async (ms, signal) => {
      if (ms === 100 && !appHeartbeatSignal) {
        appHeartbeatSignal = signal;
        await Promise.race([heartbeatTick.promise, new Promise<void>(r => signal.addEventListener('abort', () => r(), { once: true }))]);
        return;
      }
      if (signal.aborted) return;
      await new Promise<void>(r => signal.addEventListener('abort', () => r(), { once: true }));
    } };
    const renewApp = f.repository.renewApp;
    f.repository.renewApp = async (...args) => {
      renewals++; ordering.push('renewApp');
      return renewApp(...args);
    };
    const releaseApp = f.repository.releaseApp;
    f.repository.releaseApp = async (...args) => {
      releases++; ordering.push('releaseApp');
      return releaseApp(...args);
    };
    const open = f.worker.host.open;
    f.worker.host.open = async (...args) => {
      const session = await open(...args), dispose = session.dispose;
      session.dispose = async () => { await dispose(); ordering.push('workerSettled'); };
      return session;
    };

    await f.coordinator.start();
    await until(() => appHeartbeatSignal !== undefined && f.holds.size === 1);
    f.coordinator.stopScheduling();
    const draining = f.coordinator.drain().then(() => { drained = true; });
    await new Promise<void>(r => setImmediate(r));
    expect(drained).toBe(false);

    heartbeatTick.resolve();
    await until(() => renewals === 1);
    expect(releases).toBe(0); expect(drained).toBe(false);

    f.holds.values().next().value?.resolve();
    await draining;
    expect(appHeartbeatSignal?.aborted).toBe(true);
    expect(renewals).toBe(1); expect(releases).toBe(1);
    expect(ordering).toEqual(['renewApp', 'workerSettled', 'releaseApp']);
  });
  it('app renewal failure prevents new scheduling', async () => {
    const f = coordinatorFixture(1), firstTick = gate(), secondTick = gate();
    let heartbeatWaits = 0, renewalCalls = 0, claimCalls = 0, losses = 0;
    f.worker.wait = { wait: async (ms, signal) => {
      if (ms === 100) {
        const tick = heartbeatWaits++ === 0 ? firstTick : secondTick;
        await Promise.race([tick.promise, new Promise<void>(r => signal.addEventListener('abort', () => r(), { once: true }))]);
      } else await timerWait.wait(ms, signal);
    } };
    f.worker.diagnostics = { emit: event => { if (event.class === 'lease_lost') losses++; } };
    f.repository.claimNext = async () => { claimCalls++; return { status: 'empty' as const }; };
    const original = f.repository.renewApp;
    f.repository.renewApp = async (...args) => { renewalCalls++; return original(...args); };

    await f.coordinator.start(); await until(() => heartbeatWaits === 1 && claimCalls > 0);
    f.failRenew(); firstTick.resolve(); await until(() => losses === 1);
    const claimsAtLoss = claimCalls;
    secondTick.resolve();
    await new Promise<void>(r => setImmediate(r));
    await new Promise<void>(r => setImmediate(r));
    expect(renewalCalls).toBe(1); expect(losses).toBe(1);
    expect(heartbeatWaits).toBe(1); expect(claimCalls).toBe(claimsAtLoss);

    await f.coordinator.drain();
  });
  it('stop before start is terminal and idempotent', async () => {
    const f = coordinatorFixture(); f.coordinator.stopScheduling(); f.coordinator.stopScheduling();
    await expect(f.coordinator.start()).rejects.toMatchObject({ code: 'intake_closed' }); await f.coordinator.drain(); expect(f.claimed.size).toBe(0);
  });
  it('cursor continues after empty page; deleted IDs never authorize claim', async () => {
    const starts: string[] = []; let n = 0;
    const recovery = createRecovery({ recoverPending: async (_chat, _app, _consumer, start) => {
      starts.push(start); return { status: 'ok', next: ++n === 1 ? '5-0' : '0-0', claimed: 0, deleted: n === 2 ? 1 : 0 };
    } });
    expect(await recovery(chat, fence, fence.owner)).toBe(true); expect(await recovery(chat, fence, fence.owner)).toBe(false); expect(starts).toEqual(['0-0', '5-0']);
  });
});
