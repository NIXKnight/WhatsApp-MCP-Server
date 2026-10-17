import { constants, type BigIntStats } from 'node:fs';
import { lstat, open } from 'node:fs/promises';
import { dirname, isAbsolute, relative, resolve } from 'node:path';
import { isDeepStrictEqual } from 'node:util';
import { TextDecoder } from 'node:util';
import { Type } from 'typebox';
import {
  checked, CheckpointV1Schema, CompletionMarkerSchema, ControllerError,
  EventFingerprintSchema, MutationStateSchema, OpaqueIdSchema, SessionBindingSchema,
  SessionFileGenerationSchema, type CheckpointRecord, type EventId, type SessionBinding,
  type SessionFileGeneration
} from '../domain.js';
import { BINDING_TYPE, COMPLETION_TYPE } from '../pi/completion-index.js';
import { MUTATION_TYPE } from '../pi/tool-journal-extension.js';
import { checkpointIdentity, renderCheckpointSummary, validateCheckpoint } from './checkpoint.js';

const types = new Set(['message', 'model_change', 'thinking_level_change', 'compaction', 'context_edit', 'branch_summary', 'custom', 'custom_message', 'label', 'session_info', 'usage']);
const JournalEnvelopeSchema = Type.Object({
  version: Type.Literal(1), eventFingerprint: EventFingerprintSchema,
  session: SessionBindingSchema, state: MutationStateSchema
}, { additionalProperties: false });
const fail = () => new ControllerError('restore_failed');
const MAX_LINE_BYTES = 8 * 1024 * 1024;

function validTime(value: unknown): boolean {
  if (typeof value !== 'string' || !/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d{1,9})?(Z|[+-]\d{2}:\d{2})$/.test(value) || !Number.isFinite(Date.parse(value))) return false;
  const year = Number(value.slice(0, 4)), month = Number(value.slice(5, 7)), day = Number(value.slice(8, 10));
  const leap = year % 4 === 0 && (year % 100 !== 0 || year % 400 === 0);
  const days = [31, leap ? 29 : 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31];
  return month >= 1 && month <= 12 && day >= 1 && day <= (days[month - 1] ?? 0) &&
    Number(value.slice(11, 13)) < 24 && Number(value.slice(14, 16)) < 60 && Number(value.slice(17, 19)) < 60;
}
function record(value: unknown): Record<string, unknown> {
  if (!value || typeof value !== 'object' || Array.isArray(value)) throw fail();
  return value as Record<string, unknown>;
}
function controllerDetails(value: unknown): value is Record<string, unknown> {
  return value !== null && typeof value === 'object' && !Array.isArray(value) &&
    ('checkpointId' in value || 'checkpoint' in value || (value as Record<string, unknown>)['version'] === 1);
}
function assertPath(root: string, binding: SessionBinding): void {
  checked(SessionBindingSchema, binding, 'restore_failed');
  const rel = relative(resolve(root), binding.sessionFile);
  if (!isAbsolute(binding.sessionFile) || !rel || rel.startsWith('..') || isAbsolute(rel) || !binding.sessionFile.endsWith('.jsonl')) throw fail();
}
async function assertAncestors(root: string, binding: SessionBinding): Promise<void> {
  assertPath(root, binding);
  let ancestor = dirname(binding.sessionFile);
  while (true) {
    const status = await lstat(ancestor);
    if (!status.isDirectory() || status.isSymbolicLink()) throw fail();
    if (ancestor === resolve(root)) return;
    const parent = dirname(ancestor); if (parent === ancestor) throw fail(); ancestor = parent;
  }
}
function generation(status: BigIntStats): SessionFileGeneration {
  return checked(SessionFileGenerationSchema, {
    device: status.dev.toString(), inode: status.ino.toString(), size: status.size.toString(),
    mtimeNs: status.mtimeNs.toString(), ctimeNs: status.ctimeNs.toString()
  }, 'restore_failed');
}
function sameGeneration(a: SessionFileGeneration, b: SessionFileGeneration): boolean {
  return isDeepStrictEqual(a, b);
}
function timestampNs(value: string): bigint {
  const match = /^(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2})(?:\.(\d{1,9}))?(Z|[+-]\d{2}:\d{2})$/.exec(value);
  if (!match) throw fail();
  const milliseconds = Date.parse(`${match[1]}${match[3]}`);
  if (!Number.isFinite(milliseconds)) throw fail();
  return BigInt(milliseconds) * 1_000_000n + BigInt((match[2] ?? '').padEnd(9, '0'));
}

/** Capture only an exact, stable, named regular-file generation. */
export async function captureStableSessionFileGeneration(root: string, binding: SessionBinding): Promise<SessionFileGeneration> {
  try {
    await assertAncestors(root, binding);
    for (let attempt = 0; attempt < 8; attempt++) {
      const fd = await open(binding.sessionFile, constants.O_RDONLY | constants.O_NOFOLLOW);
      try {
        const before = await fd.stat({ bigint: true });
        const after = await fd.stat({ bigint: true });
        const named = await lstat(binding.sessionFile, { bigint: true });
        if (!before.isFile() || before.size === 0n || named.isSymbolicLink()) throw fail();
        const captured = generation(before);
        if (sameGeneration(captured, generation(after)) && sameGeneration(captured, generation(named))) return Object.freeze(captured);
      } finally { await fd.close(); }
    }
    throw fail();
  } catch { throw fail(); }
}
/**
 * Read-only, strict proof for the recovery CLI. Conversation content is
 * transiently decoded for structural validation but is never retained,
 * returned, or output. The proof rejects a changing file and every entry
 * appended at/after preparation. A matching compaction is never an orphan: it
 * must be handled by normal reconciliation.
 */
export async function provePreparedCompactionOrphan(
  root: string,
  binding: SessionBinding,
  eventId: EventId,
  prepared: Extract<CheckpointRecord, { phase: 'prepared' }>,
  checkpointMaxBytes: number
): Promise<SessionFileGeneration> {
  try {
    await assertAncestors(root, binding);
    if (prepared.phase !== 'prepared' || prepared.checkpoint.canonicalChatId !== binding.canonicalChatId) throw fail();
    for (let attempt = 0; attempt < 8; attempt++) {
      const fd = await open(binding.sessionFile, constants.O_RDONLY | constants.O_NOFOLLOW);
      try {
        const before = await fd.stat({ bigint: true });
        if (!before.isFile() || before.size === 0n) throw fail();
        const ids = new Set<string>();
        let headerSeen = false, bindingCount = 0, unsafe = false, invalid = false;
        const consume = (line: string): void => {
          const e = record(JSON.parse(line));
          if (!headerSeen) {
            if (e['type'] !== 'session' || e['version'] !== 3 || e['id'] !== binding.sessionId ||
                typeof e['cwd'] !== 'string' || !validTime(e['timestamp'])) throw fail();
            headerSeen = true; return;
          }
          if (typeof e['type'] !== 'string' || !types.has(e['type']) || typeof e['id'] !== 'string' ||
              !e['id'] || ids.has(e['id']) ||
              (e['parentId'] !== null && (typeof e['parentId'] !== 'string' || !ids.has(e['parentId']))) ||
              !validTime(e['timestamp'])) throw fail();
          ids.add(e['id']);
          const appendedAfterPreparation = Date.parse(e['timestamp'] as string) >= Date.parse(prepared.checkpoint.createdAt);
          const string = (key: string) => typeof e[key] === 'string';
          if (e['type'] === 'message') {
            const message = record(e['message']);
            if (typeof message['role'] !== 'string' || !message['role'] ||
                typeof message['timestamp'] !== 'number' || !Number.isFinite(message['timestamp'])) throw fail();
          } else if (e['type'] === 'compaction') {
            if (!string('summary') || !string('firstKeptEntryId') || !ids.has(e['firstKeptEntryId'] as string) ||
                typeof e['tokensBefore'] !== 'number' || !Number.isFinite(e['tokensBefore'])) throw fail();
            // A compaction written at/after checkpoint preparation is a claim on
            // this attempt. Missing details, another ID, or malformed controller
            // details are ambiguous and must not be treated as "no entry".
            if (controllerDetails(e['details'])) {
              const details = e['details'];
              const checkpointId = checked(OpaqueIdSchema, details['checkpointId'], 'restore_failed');
              const checkpoint = checked(CheckpointV1Schema, details['checkpoint'], 'restore_failed');
              if (details['version'] !== 1 || e['fromHook'] !== true) throw fail();
              const restored = validateCheckpoint(checkpoint, checkpointIdentity(checkpoint), checkpointMaxBytes);
              if (e['summary'] !== renderCheckpointSummary(restored, checkpointId, checkpointIdentity(restored), checkpointMaxBytes)) throw fail();
              if (checkpointId === prepared.checkpointId) {
                // A valid matching claim belongs to normal reconciliation; any
                // malformed/mismatched matching claim throws above. Neither is
                // an orphan that this one-time recovery may abort.
                if (!isDeepStrictEqual(restored, prepared.checkpoint)) throw fail();
                unsafe = true;
              } else if (appendedAfterPreparation) throw fail();
            } else if (appendedAfterPreparation) throw fail();
          } else if (e['type'] === 'branch_summary') {
            if (!string('summary') || !string('fromId') || !ids.has(e['fromId'] as string)) throw fail();
          } else if (e['type'] === 'context_edit') {
            if (!string('targetId') || !ids.has(e['targetId'] as string)) throw fail();
            const replacement = e['replacement'];
            const invalidReplacement = replacement !== null && (
              !replacement || typeof replacement !== 'object' || Array.isArray(replacement) ||
              (typeof (replacement as Record<string, unknown>)['content'] !== 'string' &&
                !Array.isArray((replacement as Record<string, unknown>)['content']))
            );
            if (invalidReplacement) throw fail();
          } else if (e['type'] === 'model_change') {
            if (!string('provider') || !string('modelId')) throw fail();
          } else if (e['type'] === 'thinking_level_change') {
            if (!string('thinkingLevel')) throw fail();
          } else if (e['type'] === 'custom_message') {
            if (!string('customType') || (typeof e['content'] !== 'string' && !Array.isArray(e['content'])) || typeof e['display'] !== 'boolean') throw fail();
          } else if (e['type'] === 'usage') {
            if (!string('kind') || !string('provider') || !string('model') || !e['usage'] || typeof e['usage'] !== 'object') throw fail();
          } else if (e['type'] === 'session_info') {
            if (e['name'] !== undefined && !string('name')) throw fail();
          } else if (e['type'] === 'label') {
            if (!string('targetId') || !ids.has(e['targetId'] as string) || (e['label'] !== undefined && !string('label'))) throw fail();
          }
          // No post-preparation activity of any kind can prove an orphan. A
          // matching compaction above is also unsafe even if its clock is bad.
          if (appendedAfterPreparation) unsafe = true;
          if (e['type'] !== 'custom') return;
          if (typeof e['customType'] !== 'string') throw fail();
          if (e['customType'] === BINDING_TYPE) {
            if (!isDeepStrictEqual(checked(SessionBindingSchema, e['data'], 'restore_failed'), binding)) throw fail();
            bindingCount++;
          } else if (e['customType'] === COMPLETION_TYPE) {
            const marker = checked(CompletionMarkerSchema, e['data'], 'restore_failed');
            if (marker.canonicalChatId !== binding.canonicalChatId) throw fail();
            if (marker.eventId === eventId) unsafe = true;
          } else if (e['customType'] === MUTATION_TYPE) {
            const journal = checked(JournalEnvelopeSchema, e['data'], 'restore_failed');
            if (!isDeepStrictEqual(journal.session, binding) || journal.state.canonicalChatId !== binding.canonicalChatId) throw fail();
            if (journal.state.eventId === eventId) unsafe = true;
          } else if (e['customType'].startsWith('whatsapp.controller.')) {
            throw fail();
          }
        };
        const decoder = new TextDecoder('utf-8', { fatal: true });
        const buffer = Buffer.allocUnsafe(65536);
        let remaining = before.size, pending = '';
        try {
          while (remaining > 0n) {
            const length = Number(remaining < BigInt(buffer.length) ? remaining : BigInt(buffer.length));
            const { bytesRead } = await fd.read(buffer, 0, length, null);
            if (!bytesRead) { invalid = true; break; }
            remaining -= BigInt(bytesRead);
            pending += decoder.decode(buffer.subarray(0, bytesRead), { stream: true });
            if (Buffer.byteLength(pending, 'utf8') > MAX_LINE_BYTES && !pending.includes('\n')) { invalid = true; break; }
            let newline: number;
            while ((newline = pending.indexOf('\n')) >= 0) {
              consume(pending.slice(0, newline)); pending = pending.slice(newline + 1);
            }
          }
          pending += decoder.decode();
          if (pending || !headerSeen || bindingCount !== 1 || unsafe) invalid = true;
        } catch { invalid = true; }
        const after = await fd.stat({ bigint: true });
        const named = await lstat(binding.sessionFile, { bigint: true });
        if (before.size !== after.size || before.mtimeNs !== after.mtimeNs || before.ctimeNs !== after.ctimeNs ||
            after.ino !== named.ino || after.dev !== named.dev || after.size !== named.size ||
            after.mtimeNs !== named.mtimeNs || after.ctimeNs !== named.ctimeNs) continue;
        if (invalid || named.isSymbolicLink()) throw fail();
        const proved = generation(after);
        if (prepared.recovery) {
          if (!sameGeneration(proved, checked(SessionFileGenerationSchema, prepared.recovery.generation, 'restore_failed'))) throw fail();
        } else {
          const createdNs = timestampNs(prepared.checkpoint.createdAt);
          if (after.mtimeNs >= createdNs || after.ctimeNs >= createdNs) throw fail();
        }
        return Object.freeze(proved);
      } finally { await fd.close(); }
    }
    throw fail();
  } catch { throw fail(); }
}
