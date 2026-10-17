import { createHash, randomBytes } from 'node:crypto';
import { isDeepStrictEqual } from 'node:util';
import type { ExtensionFactory, AgentSession, CompactionResult, ExtensionContext, SessionStartEvent, SessionBeforeCompactEvent, SessionBeforeCompactResult, SessionCompactEvent, SessionCompactFailedEvent, SessionShutdownEvent } from '@earendil-works/pi-coding-agent';
import { convertToLlm, serializeConversation } from '@earendil-works/pi-coding-agent';
import { checked, ControllerError, CheckpointV1Schema, fingerprintEvent, type CheckpointV1, type CheckpointRecord, type SessionBinding } from '../domain.js';
import type { ControllerConfig } from '../config.js';
import type { TurnContext } from '../scheduler/chat-worker.js';
import type { RedisRepository } from '../redis/repository.js';
import type { SdkSession } from '../pi/mcp-loader.js';
import { assertSupportedEntries, checkpointIdentity, renderCheckpointSummary, validateCheckpoint } from './checkpoint.js';
import { captureStableSessionFileGeneration } from './orphan-recovery.js';
interface Armed { checkpointId: string; replacement: Readonly<{ compaction: CompactionResult }>; compactEvent?: SessionCompactEvent; failed: boolean }
export interface CompactionEventPort {
  sessionStart(event: SessionStartEvent, context: ExtensionContext): void;
  beforeCompact(event: SessionBeforeCompactEvent, context: ExtensionContext): SessionBeforeCompactResult | undefined;
  compact(event: SessionCompactEvent, context: ExtensionContext): void;
  compactFailed(event: SessionCompactFailedEvent, context: ExtensionContext): void;
  shutdown(event: SessionShutdownEvent, context: ExtensionContext): void;
}
export interface CheckpointExtension {
  extension: ExtensionFactory;
  events: CompactionEventPort;
  run(sessionId: string, binding: SessionBinding, session: Pick<AgentSession, 'compact'> & Pick<SdkSession, 'compactionPreflight'>, context: TurnContext, seed: CheckpointV1, preparationPrompt: string, summaryPrompt: string): Promise<{ entryId: string; checkpointId: string; checkpoint: CheckpointV1; compacted(): Promise<void>; restored(): Promise<void> }>;
}
const fixed = () => new ControllerError('compaction_failed');
function compactionDetails(value: unknown): value is Record<string, unknown> {
  return value !== null && typeof value === 'object' && !Array.isArray(value);
}
function branchGeneration(value: readonly unknown[]): string {
  try { return createHash('sha256').update('sc:pi-active-branch:v1\0', 'utf8').update(JSON.stringify(value), 'utf8').digest('hex'); }
  catch { throw fixed(); }
}
function freeze<T>(value: T): Readonly<T> {
  if (value && typeof value === 'object' && !Object.isFrozen(value)) {
    for (const child of Object.values(value)) freeze(child); Object.freeze(value);
  }
  return value;
}
export function createCheckpointExtension(repository: Pick<RedisRepository, 'checkpoints'>, config: ControllerConfig['compaction'] & { sessionDir: string }): CheckpointExtension {
  const maximumBytes = config.checkpointMaxBytes;
  const armed = new Map<string, Armed>(), runtimes = new Map<string, ExtensionContext>();
  const events = {
    sessionStart(_event, ctx) { runtimes.set(ctx.sessionManager.getSessionId(), ctx); },
    // Pi catches hook failures and falls back. This handler therefore performs
    // no validation, I/O, allocation, or throw: an armed exact session always
    // receives the pre-cloned, deeply immutable canonical replacement.
    beforeCompact(_event, ctx) { return armed.get(ctx.sessionManager.getSessionId())?.replacement; },
    compact(event, ctx) { const state = armed.get(ctx.sessionManager.getSessionId()); if (state) state.compactEvent = event; },
    compactFailed(_event, ctx) { const state = armed.get(ctx.sessionManager.getSessionId()); if (state) state.failed = true; },
    shutdown(_event, ctx) { runtimes.delete(ctx.sessionManager.getSessionId()); }
  } satisfies CompactionEventPort;
  const extension: ExtensionFactory = pi => {
    pi.on('session_start', events.sessionStart);
    pi.on('session_before_compact', events.beforeCompact);
    pi.on('session_compact', events.compact);
    pi.on('session_compact_failed', events.compactFailed);
    pi.on('session_shutdown', events.shutdown);
  };
  return { extension, events, async run(id, sessionBinding, session, context, seed, preparationPrompt, summaryPrompt) {
    const runtime = runtimes.get(id);
    if (!runtime || runtime.sessionManager.getSessionId() !== id || !runtime.isIdle()) throw fixed();
    if (armed.has(id)) throw fixed();
    const checkpointId = randomBytes(16).toString('hex');
    let isArmed = false;
    try {
      // Every fallible operation, including branch preparation, model work,
      // checkpoint validation/rendering and durable prepare, precedes arming.
      await context.current();
      // This is the exact pure prepareCompaction branch/settings decision that
      // the imminent AgentSession.compact() call will repeat. It must precede
      // both the checkpoint provider and durable `prepared` write: a required
      // but impossible compaction is a fixed capacity/configuration pause, not
      // a provider failure and never a restorable checkpoint.
      const initialPreflight = session.compactionPreflight?.();
      if (initialPreflight !== true && typeof initialPreflight !== 'string') throw new ControllerError('compaction_capacity');
      const branch = runtime.sessionManager.getBranch(); assertSupportedEntries(branch);
      const initialBranchGeneration = branchGeneration(branch);
      const projection = runtime.sessionManager.buildSessionProjection();
      const firstKeptEntryId = projection.entries.at(-1)?.sourceEntry.id;
      const tokensBefore = runtime.getContextUsage()?.tokens;
      if (!firstKeptEntryId || tokensBefore === null || tokensBefore === undefined || !Number.isSafeInteger(tokensBefore) || tokensBefore < 0 || !projection.messages.length) throw fixed();
      // Snapshot the complete finalized model-visible projection before arming;
      // unlike hook-time work this cannot be swallowed into Pi's fallback path.
      const transcript = serializeConversation(convertToLlm(projection.messages));
      const exactSeed = checked(CheckpointV1Schema, seed, 'checkpoint_invalid');
      const request = `${preparationPrompt}\n${summaryPrompt}\nReturn one JSON CheckpointV1 object only. Fixed fields and confirmedToolOutcomes must exactly equal:\n${JSON.stringify(exactSeed)}\nFinalized conversation:\n${transcript}`;
      if (Buffer.byteLength(request, 'utf8') > maximumBytes * 8) throw fixed();
      await context.current(); if (!runtime.model) throw fixed();
      const response = await runtime.modelRegistry.complete(runtime.model, { messages: [{ role: 'user', content: [{ type: 'text', text: request }], timestamp: Date.now() }] },
        { maxTokens: Math.min(8192, Math.max(1024, Math.ceil(maximumBytes / 4))), cacheRetention: 'none', sessionId: id });
      // Provider work is not authority to persist. Re-run the exact pinned Pi
      // decision immediately and require the same branch/settings generation.
      await context.current();
      const finalPreflight = session.compactionPreflight?.();
      if ((finalPreflight !== true && typeof finalPreflight !== 'string') ||
          finalPreflight !== initialPreflight ||
          branchGeneration(runtime.sessionManager.getBranch()) !== initialBranchGeneration) {
        throw new ControllerError('compaction_capacity');
      }
      const text = response.content.filter((v): v is { type: 'text'; text: string } => v.type === 'text').map(v => v.text).join('');
      if (!text.trim() || Buffer.byteLength(text, 'utf8') > maximumBytes) throw fixed();
      let parsed: unknown; try { parsed = JSON.parse(text); } catch { throw fixed(); }
      const expected = checkpointIdentity(exactSeed);
      const checkpoint = validateCheckpoint(parsed, expected, maximumBytes);
      if (!isDeepStrictEqual(checkpoint.confirmedToolOutcomes, exactSeed.confirmedToolOutcomes)) throw fixed();
      const expectedSummary = renderCheckpointSummary(checkpoint, checkpointId, expected, maximumBytes);
      const result: CompactionResult = { summary: expectedSummary, firstKeptEntryId,
        tokensBefore, usage: response.usage, details: { version: 1, checkpointId, checkpoint } };
      const replacement = freeze(structuredClone({ compaction: result }));
      await context.current();
      // This stable named-file generation is the last operation before the
      // durable prepare. Recovery must prove this exact generation, not infer
      // preparation from mutable JSONL timestamps.
      const fileGeneration = await captureStableSessionFileGeneration(config.sessionDir, sessionBinding);
      const work = context.work();
      const record: CheckpointRecord = { checkpointId, phase: 'prepared', checkpoint, recovery: {
        version: 1, eventId: work.event.event_id, eventFingerprint: fingerprintEvent(work.event),
        streamId: work.streamId, session: sessionBinding, generation: fileGeneration
      } };
      if (!await repository.checkpoints.prepare(work, record)) throw fixed();
      await context.current();
      const state: Armed = { checkpointId, replacement, failed: false };
      armed.set(id, state); isArmed = true;
      // No work remains that can fail before this call. The armed handler is
      // synchronous/infallible and cannot permit Pi's default fallback.
      const returned: unknown = await session.compact(`WHATSAPP_CHECKPOINT_V1:${checkpointId}`);
      await context.current();
      const event = state.compactEvent, entry = event?.compactionEntry;
      const details = compactionDetails(entry?.details) ? entry.details : undefined;
      const returnedEnvelope = compactionDetails(returned) ? returned : undefined;
      const returnedDetails = returnedEnvelope && compactionDetails(returnedEnvelope['details']) ? returnedEnvelope['details'] : undefined;
      if (state.failed || !event || !event.fromExtension || event.reason !== 'manual' || event.willRetry || !entry ||
          entry.fromHook !== true || entry.id.length < 1 || entry.id.length > 128 || !details || !returnedEnvelope || !returnedDetails ||
          entry.summary !== expectedSummary || returnedEnvelope['summary'] !== entry.summary ||
          returnedEnvelope['firstKeptEntryId'] !== entry.firstKeptEntryId || returnedEnvelope['tokensBefore'] !== entry.tokensBefore ||
          !isDeepStrictEqual(returnedEnvelope['usage'], entry.usage) || details['version'] !== 1 || details['checkpointId'] !== checkpointId ||
          returnedDetails['version'] !== 1 || returnedDetails['checkpointId'] !== checkpointId ||
          !isDeepStrictEqual(details, result.details) || !isDeepStrictEqual(returnedDetails, details) ||
          !isDeepStrictEqual(details['checkpoint'], checkpoint) || !isDeepStrictEqual(returnedDetails['checkpoint'], checkpoint)) throw fixed();
      return { entryId: entry.id, checkpointId, checkpoint,
        async compacted() { await context.current(); if (!await repository.checkpoints.transition(context.work(), checkpointId, 'compacted', entry.id)) throw fixed(); },
        async restored() {
          await context.current(); const current = await repository.checkpoints.latest(context.work().event.canonical_chat_jid);
          if (!current || current.phase !== 'compacted' || current.checkpointId !== checkpointId || current.compactionEntryId !== entry.id || !isDeepStrictEqual(current.checkpoint, checkpoint)) throw new ControllerError('restore_failed');
          await context.current(); if (!await repository.checkpoints.transition(context.work(), checkpointId, 'restored', entry.id)) throw new ControllerError('restore_failed');
        }
      };
    } catch (error) { if (error instanceof ControllerError) throw error; throw fixed(); }
    finally { if (isArmed) armed.delete(id); }
  } };
}
