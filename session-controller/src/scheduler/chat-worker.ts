import { isDeepStrictEqual } from 'node:util';
import {
  checked, ControllerError, CompletionProofSchema, fingerprintEvent, systemClock,
  type ClaimedWork, type PiHostPort, type PiSessionPort, type CompletionMarker,
  type PauseReason, type Clock, type WaitPort, type DiagnosticPort
} from '../domain.js';
import type { RedisRepository } from '../redis/repository.js';
export type WorkerRepository = Pick<RedisRepository, 'renew' | 'checkWork' | 'readChat' | 'bindSession' | 'turnState' | 'beginTurn' | 'pause' | 'complete'> & Partial<Pick<RedisRepository, 'ownMutation'>>;
export interface TurnContext {
  /** Ownership gate, not a tool/destination authorization interceptor.
   * Slice 6/7 adapters must call immediately before every prompt, journal,
   * checkpoint and marker effect; stale work must never initiate them.
   */
  current(): Promise<void>;
  work(): ClaimedWork;
}
export interface TurnPorts {
  /** Trusted Slice 6/7 composition binds context.current to session effects,
   * including marker fsync and extension follow-ups, before any prompt.
   * This is ownership fencing only, never capability/destination policy. */
  attach(session: PiSessionPort, context: TurnContext): Promise<void>;
  /** Preparation may defer an unknown usage decision for an exact fresh first
   * turn or after compaction; afterPrompt must validate the real prompt usage. */
  prepare(session: PiSessionPort, context: TurnContext): Promise<{ usageRequiredAfterPrompt: boolean } | void>;
  /** Slice 7 installs journal hooks through this port. It must settle all
   * hooks before returning and must not replay uncertain calls. */
  run(session: PiSessionPort, context: TurnContext, prompt: () => Promise<void>): Promise<void>;
  /** Runs after the real event prompt settles under the journal/fences. */
  afterPrompt?(session: PiSessionPort, context: TurnContext, preparation: { usageRequiredAfterPrompt: boolean }): Promise<void>;
  confirmed(context: TurnContext): Promise<CompletionMarker['confirmedSends']>;
}
/** Native composition: host performs the durable journal run inside prompt;
 * these scheduler ports expose confirmed metadata and injected Slice 8 prepare.
 * loadNativeSdk extras and SessionHostOptions.journal must share this instance. */
export function createJournalTurns(journal: Pick<import('../pi/tool-journal-extension.js').JournalAttachment, 'confirmed'>,
  prepare: TurnPorts['prepare'],
  afterPrompt: NonNullable<TurnPorts['afterPrompt']> = async () => {}): TurnPorts {
  return {
    async attach(_session, context) { await context.current(); },
    prepare,
    async run(_session, context, prompt) { await context.current(); await prompt(); await context.current(); },
    afterPrompt,
    confirmed: context => journal.confirmed(context)
  };
}
export interface WorkerOptions {
  leaseMs: number; renewMs: number;
  appCurrent(): boolean;
  instructions(work: ClaimedWork): string;
  host: PiHostPort & { attach?(session: PiSessionPort, context: TurnContext): Promise<void> }; turns: TurnPorts; wait: WaitPort;
  diagnostics: DiagnosticPort; clock?: Clock;
}
/** One invocation owns one chat slot; the coordinator never overlaps it. */
export async function runChatWorker(repository: WorkerRepository, initial: ClaimedWork, options: WorkerOptions): Promise<void> {
  const emit = (event: Parameters<DiagnosticPort['emit']>[0]) => {
    try { options.diagnostics.emit(event); } catch { /* Diagnostics cannot change lifecycle disposition. */ }
  };
  let work = initial, lost = false, session: PiSessionPort | undefined;
  const done = new AbortController();
  const clock = options.clock ?? systemClock;
  const context: TurnContext = {
    work: () => work,
    async current() {
      if (lost || !options.appCurrent()) throw new ControllerError('lease_lost');
      const renewed = await repository.renew(work, options.leaseMs);
      if (!renewed || lost || !options.appCurrent()) { lost = true; throw new ControllerError('lease_lost'); }
      work = { ...work, ownership: renewed };
      if (!await repository.checkWork(work) || !options.appCurrent()) { lost = true; throw new ControllerError('lease_lost'); }
    }
  };
  const renewal = (async () => {
    while (!done.signal.aborted) {
      await options.wait.wait(options.renewMs, done.signal);
      if (done.signal.aborted) break;
      try { await context.current(); } catch { lost = true; break; }
    }
  })().catch(() => { if (!done.signal.aborted) lost = true; });
  let reason: PauseReason = 'session_failed';
  try {
    await context.current();
    const state = await repository.readChat(work.event.canonical_chat_jid);
    if (state.lastCompletedSequence + 1 !== work.event.chat_sequence || state.pause) throw new ControllerError('sequence_gap');
    const existing = await options.host.findEvent(work.event.canonical_chat_jid, work.event.event_id);
    if (existing !== undefined) {
      const p = checked(CompletionProofSchema, existing, 'session_failed');
      if (p.marker.eventId !== work.event.event_id || p.marker.canonicalChatId !== work.event.canonical_chat_jid ||
          p.marker.chatSequence !== work.event.chat_sequence || p.marker.eventFingerprint !== fingerprintEvent(work.event) ||
          !state.session || !isDeepStrictEqual(p.session, state.session)) throw new ControllerError('session_failed');
      await context.current();
      if (!await repository.complete(work, p)) throw new ControllerError('lease_lost');
      return; // Durable marker already exists: no host.open or repeated logical turn.
    }
    // Unresolved mutations pause each scheduled affected actual destination,
    // including participant turns, without authorizing or rewriting sends.
    if (await repository.ownMutation?.(work.event.chat_jid, work.event.message_id) === 'uncertain') throw new ControllerError('mutation_uncertain');
    const turn = await repository.turnState(work);
    if (turn === 'lost') throw new ControllerError('lease_lost');
    if (turn === 'started') throw new ControllerError('mutation_uncertain');
    // Native host combines all-session active journals and durable completion
    // markers. Unresolved destinations win; manual own events still prompt.
    const own = work.event.is_from_me
      ? await options.host.classifyOwn(work.event.canonical_chat_jid, work.event.chat_jid, work.event.message_id) : 'manual';
    if (own === 'uncertain') throw new ControllerError('mutation_uncertain');
    await context.current();
    session = await options.host.open(work, state.session);
    await context.current();
    if (session.binding.canonicalChatId !== work.event.canonical_chat_jid ||
        (state.session && !isDeepStrictEqual(state.session, session.binding)) ||
        !await repository.bindSession(work, session.binding)) throw new ControllerError('session_failed');
    await context.current(); await options.host.attach?.(session, context);
    await context.current(); await options.turns.attach(session, context);
    if (own !== 'confirmed') {
      reason = 'preparation_failed';
      const preparation = await options.turns.prepare(session, context) ?? { usageRequiredAfterPrompt: false };
      await context.current();
      if (!await repository.beginTurn(work)) throw new ControllerError('mutation_uncertain');
      // Crash from here without a durable marker is uncertain, never auto-prompted.
      reason = 'mutation_uncertain';
      const opened = session; let prompted = false;
      await options.turns.run(opened, context, async () => {
        if (prompted) throw new ControllerError('mutation_uncertain');
        await context.current(); prompted = true;
        await opened.prompt(work.event, options.instructions(work));
      });
      // The real current-event assistant response has settled. If preparation
      // deferred unknown usage, require valid usage before completion.
      await options.turns.afterPrompt?.(opened, context, preparation);
    }
    await context.current();
    const confirmedSends = await options.turns.confirmed(context);
    await context.current();
    const proof = await session.appendCompletion({ version: 1, eventId: work.event.event_id,
      eventFingerprint: fingerprintEvent(work.event), canonicalChatId: work.event.canonical_chat_jid,
      chatSequence: work.event.chat_sequence, completion: own === 'confirmed' ? 'context_only' : 'turn',
      completedAt: new Date(clock.now()).toISOString(), confirmedSends });
    await context.current();
    if (!await repository.complete(work, proof)) throw new ControllerError('lease_lost');
  } catch (error) {
    if (error instanceof ControllerError) {
      const allowed: readonly PauseReason[] = ['lease_lost', 'sequence_gap', 'session_failed', 'mutation_uncertain', 'usage_unknown', 'preparation_failed', 'checkpoint_invalid', 'compaction_failed', 'restore_failed'];
      if (allowed.includes(error.code as PauseReason)) reason = error.code as PauseReason;
    }
    if (!lost && options.appCurrent()) {
      try { await context.current(); await repository.pause(work, reason); } catch { lost = true; }
    }
    emit({ class: lost ? 'lease_lost' : reason });
  } finally {
    done.abort(); await renewal;
    // Disposal is resource cleanup, not a prompt or durable state effect.
    try { await session?.dispose(); } catch { emit({ class: 'session_failed' }); }
  }
}
