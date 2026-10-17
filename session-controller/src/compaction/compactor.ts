import { readFile } from 'node:fs/promises';
import { isDeepStrictEqual } from 'node:util';
import { Type } from 'typebox';
import {
  checked, ControllerError, CheckpointRecordSchema, CheckpointV1Schema, ChatJidSchema,
  MessageIdSchema, MutationToolSchema, OpaqueIdSchema,
  type CheckpointV1, type PiSessionPort
} from '../domain.js';
import type { ControllerConfig } from '../config.js';
import type { TurnContext, TurnPorts } from '../scheduler/chat-worker.js';
import type { RedisRepository } from '../redis/repository.js';
import { checkpointIdentity, validateCheckpoint, type ConfirmedToolOutcome } from './checkpoint.js';
import { createUsagePolicy, type IncomingTokenBudget } from './usage-policy.js';
export interface Compactor { prepare: TurnPorts['prepare']; afterPrompt: NonNullable<TurnPorts['afterPrompt']> }
export interface CompactorOptions {
  config: ControllerConfig;
  repository: Pick<RedisRepository, 'readChat' | 'checkpoints' | 'nativeJournal'>;
  incoming(session: PiSessionPort, context: TurnContext): IncomingTokenBudget;
  readTemplate?: (path: string) => Promise<string>;
}
interface SessionHistoryPort extends PiSessionPort {
  history(lastCompletedSequence: number, currentCheckpoint?: CheckpointV1): Promise<readonly ConfirmedToolOutcome[]>;
}
interface SessionRecoveryPort extends PiSessionPort {
  reconcileCheckpoint(record: import('../domain.js').CheckpointRecord): Promise<{ entryId: string; checkpoint: CheckpointV1 }>;
}
const ConfirmedToolOutcomeSchema = Type.Object({
  attemptId: OpaqueIdSchema, tool: MutationToolSchema, destination: ChatJidSchema, messageId: MessageIdSchema
}, { additionalProperties: false });
async function diskTemplate(path: string): Promise<string> { return readFile(path, 'utf8'); }
async function template(path: string, read: (path: string) => Promise<string>): Promise<string> {
  try { const value = await read(path); if (!value.trim() || Buffer.byteLength(value) > 65536) throw new Error(); return value; }
  catch { throw new ControllerError('preparation_failed'); }
}
function withHistory(session: PiSessionPort): SessionHistoryPort {
  const candidate = session as Partial<SessionHistoryPort>;
  if (typeof candidate.history !== 'function') throw new ControllerError('checkpoint_invalid');
  return session as SessionHistoryPort;
}
function withRecovery(session: PiSessionPort): SessionRecoveryPort {
  const candidate = session as Partial<SessionRecoveryPort>;
  if (typeof candidate.reconcileCheckpoint !== 'function') throw new ControllerError('restore_failed');
  return session as SessionRecoveryPort;
}
function mergeOutcomes(...sources: readonly (readonly ConfirmedToolOutcome[])[]): ConfirmedToolOutcome[] {
  const merged = new Map<string, ConfirmedToolOutcome>();
  for (const source of sources) for (const outcome of source) {
    const checkedOutcome = checked(ConfirmedToolOutcomeSchema, outcome, 'checkpoint_invalid');
    const prior = merged.get(checkedOutcome.attemptId);
    if (prior && !isDeepStrictEqual(prior, checkedOutcome)) throw new ControllerError('checkpoint_invalid');
    merged.set(checkedOutcome.attemptId, checkedOutcome);
    if (merged.size > 256) throw new ControllerError('checkpoint_invalid');
  }
  return [...merged.values()].sort((a, b) => a.attemptId.localeCompare(b.attemptId));
}
export function createCompactor(options: CompactorOptions): Compactor {
  const usage = createUsagePolicy(options.config.compaction);
  const read = options.readTemplate ?? diskTemplate;
  return {
    async prepare(session, context) {
    await context.current();
    const latest = await options.repository.checkpoints.latest(context.work().event.canonical_chat_jid);
    // Reconcile every latest Redis record, including restored, from one exact
    // durable active-branch entry. Recovery never regenerates a summary, calls
    // the model, or invokes compact; Redis transitions depend only on phase.
    // Aborted is terminal historical evidence for an operator-proven orphan. It
    // deliberately has no Pi compaction entry and therefore needs no restore.
    let settled = latest?.phase === 'aborted' ? undefined : latest;
    if (settled) {
      const original = settled;
      await context.current(); const recovered = await withRecovery(session).reconcileCheckpoint(original);
      if (!isDeepStrictEqual(recovered.checkpoint, original.checkpoint)) throw new ControllerError('restore_failed');
      // Reconciliation succeeds before any transition. Advance only from the
      // phase actually read: prepared -> compacted -> restored, compacted ->
      // restored, and restored -> no transition. Recovery never models or compacts.
      switch (settled.phase) {
        case 'prepared': {
          await context.current();
          if (!await options.repository.checkpoints.transition(context.work(), settled.checkpointId, 'compacted', recovered.entryId)) throw new ControllerError('restore_failed');
          await context.current(); const compacted = await options.repository.checkpoints.latest(context.work().event.canonical_chat_jid);
          if (!compacted || compacted.phase !== 'compacted' || compacted.checkpointId !== settled.checkpointId ||
              compacted.compactionEntryId !== recovered.entryId || !isDeepStrictEqual(compacted.checkpoint, recovered.checkpoint)) throw new ControllerError('restore_failed');
          settled = compacted;
          await context.current();
          if (!await options.repository.checkpoints.transition(context.work(), settled.checkpointId, 'restored', recovered.entryId)) throw new ControllerError('restore_failed');
          break;
        }
        case 'compacted':
          await context.current();
          if (!await options.repository.checkpoints.transition(context.work(), settled.checkpointId, 'restored', recovered.entryId)) throw new ControllerError('restore_failed');
          break;
        case 'restored':
          break;
      }
      await context.current();
      const recoveredLatest = await options.repository.checkpoints.latest(context.work().event.canonical_chat_jid);
      if (recoveredLatest?.phase === 'aborted') throw new ControllerError('restore_failed');
      settled = recoveredLatest;
      if (!settled || settled.phase !== 'restored' || settled.compactionEntryId !== recovered.entryId ||
          settled.checkpointId !== original.checkpointId || !isDeepStrictEqual(settled.checkpoint, recovered.checkpoint)) throw new ControllerError('restore_failed');
    }
    const decision = usage.decide(session.usage(), options.incoming(session, context));
    if (decision.status === 'ok') return { usageRequiredAfterPrompt: false };
    if (decision.status === 'unknown' && (settled || !session.isFresh())) throw new ControllerError('usage_unknown');
    await context.current();
    const state = await options.repository.readChat(context.work().event.canonical_chat_jid);
    if (!state.session || !isDeepStrictEqual(state.session, session.binding) || state.lastCompletedSequence !== context.work().event.chat_sequence - 1) throw new ControllerError('checkpoint_invalid');
    await context.current();
    const pending = await options.repository.nativeJournal.list(context.work().event.canonical_chat_jid);
    if (pending.some(value => value.status !== 'confirmed')) throw new ControllerError('checkpoint_invalid');
    // A brand-new mapped session has no model usage until its first real event
    // prompt. Its durable mapping/cursor/journal must still prove that this is
    // exactly that first turn; no synthetic prompt or compaction is permitted.
    if (decision.status === 'unknown') return { usageRequiredAfterPrompt: true };
    await context.current();
    const historical = await withHistory(session).history(state.lastCompletedSequence, settled?.checkpoint);
    const native = pending.map(value => {
      if (value.status !== 'confirmed') throw new ControllerError('checkpoint_invalid');
      return { attemptId: value.attemptId, tool: value.tool, destination: value.destination, messageId: value.messageId };
    });
    const confirmedToolOutcomes = mergeOutcomes(historical, native);
    const seed = checked(CheckpointV1Schema, { version: 1, canonicalChatId: state.canonicalChatId, lastCompletedSequence: state.lastCompletedSequence,
      policyVersion: options.config.pi.policyVersion, templateVersion: options.config.pi.templateVersion, createdAt: new Date().toISOString(),
      goals: [], decisions: [], constraints: [], verifiedFacts: [], pendingWork: [], approvalBoundaries: [], nextSteps: [], confirmedToolOutcomes }, 'checkpoint_invalid');
    try {
      await context.current(); const pre = await template(options.config.pi.preCompactionPath, read);
      await context.current(); const summary = await template(options.config.pi.compactionSummaryPath, read);
      await context.current(); const result = await session.compact(`${pre}\n${summary}`, seed);
      const checkpoint = validateCheckpoint(result.checkpoint, checkpointIdentity(seed), options.config.compaction.checkpointMaxBytes);
      if (!isDeepStrictEqual(checkpoint.confirmedToolOutcomes, confirmedToolOutcomes)) throw new ControllerError('checkpoint_invalid');
      await context.current(); const restored = await options.repository.checkpoints.latest(context.work().event.canonical_chat_jid);
      checked(CheckpointRecordSchema, restored, 'restore_failed');
      if (!restored || restored.phase !== 'restored' || restored.compactionEntryId !== result.entryId || !isDeepStrictEqual(restored.checkpoint, checkpoint)) throw new ControllerError('restore_failed');
      // Pi 1.1 deliberately reports null usage immediately after compaction.
      // The current real event prompt must refill it; never block or synthesize
      // an internal prompt here.
      return { usageRequiredAfterPrompt: true };
    } catch (error) {
      if (error instanceof ControllerError) throw error;
      throw new ControllerError('compaction_failed');
    }
  },
  async afterPrompt(session, context, preparation) {
    if (!preparation.usageRequiredAfterPrompt) return;
    await context.current();
    // Called only after the journaled event prompt has emitted agent_settled.
    // Completion requires the actual assistant response's public usage.
    if (usage.decide(session.usage(), { event: 0, tools: 0, prompt: 0 }).status === 'unknown') {
      throw new ControllerError('usage_unknown');
    }
  } };
}
