import { isDeepStrictEqual } from 'node:util';
import type { SessionEntry } from '@earendil-works/pi-coding-agent';
import { Type } from 'typebox';
import {
  checked, ControllerError, CheckpointV1Schema, CompletionMarkerSchema, EventFingerprintSchema,
  MutationStateSchema, OpaqueIdSchema, SessionBindingSchema,
  type CheckpointV1, type MutationState, type SessionBinding
} from '../domain.js';
import { BINDING_TYPE, COMPLETION_TYPE } from '../pi/completion-index.js';
import { MUTATION_TYPE, type JournalRecord } from '../pi/tool-journal-extension.js';

const supported = new Set(['message', 'model_change', 'thinking_level_change', 'compaction', 'context_edit', 'branch_summary', 'custom', 'custom_message', 'label', 'session_info', 'usage']);
const sensitive = [
  /-----BEGIN [A-Z ]*PRIVATE KEY-----/i,
  /\b(?:authorization|proxy-authorization)\s*:\s*(?:bearer|basic)\s+\S+/i,
  /\b(?:api[_-]?key|access[_-]?token|refresh[_-]?token|client[_-]?secret|password)\s*[:=]\s*["']?\S{8,}/i,
  /\b(?:redis|postgres(?:ql)?|mysql):\/\/[^\s/@:]+:[^\s/@]+@/i,
  /\b(?:ghp|github_pat|sk)-[A-Za-z0-9_-]{16,}\b/
];
const fail = () => new ControllerError('checkpoint_invalid');
const JournalRecordSchema = Type.Object({
  version: Type.Literal(1), eventFingerprint: EventFingerprintSchema,
  session: SessionBindingSchema, state: MutationStateSchema
}, { additionalProperties: false });
const controllerPrefix = 'whatsapp.controller.';
const MAX_ENTRIES = 4096, MAX_OUTCOMES = 256;
export type CheckpointIdentity = Pick<CheckpointV1, 'canonicalChatId' | 'lastCompletedSequence' | 'policyVersion' | 'templateVersion' | 'createdAt'>;
export interface ConfirmedToolOutcome { attemptId: string; tool: MutationState['tool']; destination: string; messageId: string }

export function checkpointIdentity(seed: CheckpointV1): CheckpointIdentity {
  return { canonicalChatId: seed.canonicalChatId, lastCompletedSequence: seed.lastCompletedSequence,
    policyVersion: seed.policyVersion, templateVersion: seed.templateVersion, createdAt: seed.createdAt };
}
export function assertSupportedEntries(entries: readonly unknown[]): void {
  for (const raw of entries) {
    if (!raw || typeof raw !== 'object' || Array.isArray(raw)) throw fail();
    const type = (raw as Record<string, unknown>)['type'];
    if (typeof type !== 'string' || !supported.has(type)) throw fail();
  }
}
export function containsSensitive(value: unknown): boolean {
  const pending: unknown[] = [value]; let seen = 0;
  while (pending.length) {
    if (++seen > 4096) return true;
    const next = pending.pop();
    if (typeof next === 'string') { if (sensitive.some(pattern => pattern.test(next))) return true; continue; }
    if (Array.isArray(next)) { pending.push(...next); continue; }
    if (next && typeof next === 'object') pending.push(...Object.values(next));
  }
  return false;
}
export function validateCheckpoint(value: unknown, expected: CheckpointIdentity, maximumBytes: number): CheckpointV1 {
  if (!Number.isSafeInteger(maximumBytes) || maximumBytes < 1024 || maximumBytes > 262144) throw fail();
  const checkpoint = checked(CheckpointV1Schema, value, 'checkpoint_invalid');
  const noteValues = [checkpoint.goals, checkpoint.decisions, checkpoint.constraints, checkpoint.verifiedFacts,
    checkpoint.pendingWork, checkpoint.approvalBoundaries, checkpoint.nextSteps].flat();
  if (noteValues.some(note => /[\r\n]/u.test(note))) throw fail();
  const attempts = new Set<string>();
  for (const outcome of checkpoint.confirmedToolOutcomes) if (attempts.has(outcome.attemptId)) throw fail(); else attempts.add(outcome.attemptId);
  const fixed = { canonicalChatId: expected.canonicalChatId, lastCompletedSequence: expected.lastCompletedSequence,
    policyVersion: expected.policyVersion, templateVersion: expected.templateVersion, createdAt: expected.createdAt };
  const actual = { canonicalChatId: checkpoint.canonicalChatId, lastCompletedSequence: checkpoint.lastCompletedSequence,
    policyVersion: checkpoint.policyVersion, templateVersion: checkpoint.templateVersion, createdAt: checkpoint.createdAt };
  if (!isDeepStrictEqual(actual, fixed) || containsSensitive(checkpoint)) throw fail();
  const canonical = JSON.stringify(checkpoint);
  if (Buffer.byteLength(canonical, 'utf8') > maximumBytes || JSON.parse(canonical) === undefined) throw fail();
  return structuredClone(checkpoint);
}
function sameMutation(a: MutationState, b: MutationState): boolean {
  const common = (v: MutationState) => ({ version: v.version, attemptId: v.attemptId, eventId: v.eventId,
    canonicalChatId: v.canonicalChatId, toolCallId: v.toolCallId, tool: v.tool, destination: v.destination,
    startedAt: v.startedAt, ownership: v.ownership });
  return isDeepStrictEqual(common(a), common(b));
}
/** Parse only the supplied public active branch. Controller records are ordered
 * evidence: binding, attempting, confirmed, then the matching completion. */
export function confirmedHistory(entries: readonly SessionEntry[], binding: SessionBinding,
  lastCompletedSequence: number, currentCheckpoint?: CheckpointV1): readonly ConfirmedToolOutcome[] {
  if (!Number.isSafeInteger(lastCompletedSequence) || lastCompletedSequence < 0 || entries.length > MAX_ENTRIES) throw fail();
  const exactBinding = checked(SessionBindingSchema, binding, 'checkpoint_invalid');
  let bound = false;
  const transitions = new Map<string, JournalRecord>();
  const completedEvents = new Set<string>();
  const outcomes = new Map<string, ConfirmedToolOutcome>();
  const add = (value: ConfirmedToolOutcome) => {
    const prior = outcomes.get(value.attemptId);
    if (prior && !isDeepStrictEqual(prior, value)) throw fail();
    outcomes.set(value.attemptId, value); if (outcomes.size > MAX_OUTCOMES) throw fail();
  };
  if (currentCheckpoint) {
    const prior = checked(CheckpointV1Schema, currentCheckpoint, 'checkpoint_invalid');
    if (prior.canonicalChatId !== exactBinding.canonicalChatId || prior.lastCompletedSequence > lastCompletedSequence) throw fail();
    for (const value of prior.confirmedToolOutcomes) add(value);
  }
  for (const entry of entries) {
    if (!supported.has(entry.type)) throw fail();
    if (entry.type !== 'custom') continue;
    if (entry.customType === BINDING_TYPE) {
      if (bound || !isDeepStrictEqual(entry.data, exactBinding)) throw fail();
      bound = true; continue;
    }
    if (entry.customType === MUTATION_TYPE) {
      if (!bound) throw fail();
      const record = checked(JournalRecordSchema, entry.data, 'checkpoint_invalid') satisfies JournalRecord;
      if (!isDeepStrictEqual(record.session, exactBinding) || record.state.canonicalChatId !== exactBinding.canonicalChatId || completedEvents.has(record.state.eventId)) throw fail();
      const prior = transitions.get(record.state.attemptId);
      if (!prior) {
        if (record.state.status !== 'attempting') throw fail();
      } else {
        if (prior.state.status !== 'attempting' || record.state.status !== 'confirmed' ||
            prior.eventFingerprint !== record.eventFingerprint || !sameMutation(prior.state, record.state)) throw fail();
      }
      transitions.set(record.state.attemptId, record); continue;
    }
    if (entry.customType === COMPLETION_TYPE) {
      if (!bound) throw fail();
      const marker = checked(CompletionMarkerSchema, entry.data, 'checkpoint_invalid');
      if (marker.canonicalChatId !== exactBinding.canonicalChatId || marker.chatSequence > lastCompletedSequence || completedEvents.has(marker.eventId)) throw fail();
      for (const [attemptId, record] of transitions) {
        const state = record.state;
        if (state.eventId !== marker.eventId) continue;
        if (record.eventFingerprint !== marker.eventFingerprint || state.status !== 'confirmed' ||
            !marker.confirmedSends.some(send => send.destination === state.destination && send.messageId === state.messageId)) throw fail();
        add({ attemptId, tool: state.tool, destination: state.destination, messageId: state.messageId });
      }
      for (const send of marker.confirmedSends) {
        const matches = [...transitions.values()].filter(record => record.state.eventId === marker.eventId && record.state.status === 'confirmed' &&
          record.state.destination === send.destination && record.state.messageId === send.messageId);
        if (matches.length !== 1) throw fail();
      }
      completedEvents.add(marker.eventId); continue;
    }
    if (entry.customType?.startsWith(controllerPrefix)) throw fail();
  }
  if (!bound) throw fail();
  for (const record of transitions.values()) if (record.state.status !== 'confirmed' || !completedEvents.has(record.state.eventId)) throw fail();
  return Object.freeze([...outcomes.values()].sort((a, b) => a.attemptId.localeCompare(b.attemptId)).map(value => Object.freeze(value)));
}
const lines = (title: string, values: readonly string[]) => [`## ${title}`, ...(values.length ? values.map(v => `- ${v}`) : ['- None recorded.'])];
export function renderCheckpointSummary(checkpoint: CheckpointV1, checkpointId: string, expected: CheckpointIdentity, maximumBytes: number): string {
  const c = validateCheckpoint(checkpoint, expected, maximumBytes);
  const exactCheckpointId = checked(OpaqueIdSchema, checkpointId, 'checkpoint_invalid');
  const tools = c.confirmedToolOutcomes.map(v => `${v.tool}: ${v.destination} (${v.messageId})`);
  const summary = ['## Checkpoint Metadata', `- Checkpoint ID: ${exactCheckpointId}`, `- Created at: ${c.createdAt}`,
    ...lines('Goals', c.goals), ...lines('Decisions', c.decisions), ...lines('Constraints', c.constraints),
    ...lines('Verified Facts', c.verifiedFacts), ...lines('Pending Work', c.pendingWork),
    ...lines('Confirmed Tool Outcomes', tools), ...lines('Approval Boundaries', c.approvalBoundaries), ...lines('Next Steps', c.nextSteps),
    '## Continuity', `- Canonical chat: ${c.canonicalChatId}`, `- Last completed sequence: ${c.lastCompletedSequence}`,
    `- Policy version: ${c.policyVersion}`, `- Template version: ${c.templateVersion}`].join('\n');
  // The same byte ceiling covers all rendered identity metadata, including the
  // exact checkpoint ID and creation time, not merely model-authored sections.
  if (Buffer.byteLength(summary, 'utf8') > maximumBytes || containsSensitive(summary)) throw fail();
  return summary;
}
