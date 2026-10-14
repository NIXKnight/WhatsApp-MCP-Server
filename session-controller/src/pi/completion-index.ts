import { constants } from 'node:fs';
import { open, lstat, mkdir } from 'node:fs/promises';
import { dirname, resolve, relative, isAbsolute } from 'node:path';
import { isDeepStrictEqual } from 'node:util';
import { TextDecoder } from 'node:util';
import {
  checked, ControllerError, SessionBindingSchema, CompletionMarkerSchema, CompletionProofSchema,
  type AdmissionPolicy, type CanonicalChatId, type CompletionIndexPort, type CompletionProof, type SessionBinding
} from '../domain.js';
export const BINDING_TYPE = 'whatsapp.controller.binding.v1';
export const COMPLETION_TYPE = 'whatsapp.controller.completed.v1';
export interface MappingPort {
  /** ALL configured canonical chats, including undefined for never-bound chats.
   * Use an independent read connection, NEVER the repository's serialized
   * writer: intake holds that writer while consulting this index. */
  list(): Promise<readonly { chat: CanonicalChatId; binding?: SessionBinding }[]>;
}
export interface CompletionIndex extends CompletionIndexPort {
  exclusive<T>(job: () => Promise<T>): Promise<T>;
  evidence(binding: SessionBinding): Promise<readonly CompletionProof[]>;
  /** A failed append/fsync/compact is never downgraded to a cache miss. */
  invalidate(binding: SessionBinding): void;
  /** Called inside exclusive file-only intervals. In-flight evidence yields
   * unavailable, never a false cache miss or an un-fsynced completion proof. */
  reserve(binding: SessionBinding): void;
  publish(binding: SessionBinding): void;
}
const fail = () => new ControllerError('session_failed');
export function assertSessionPath(root: string, binding: SessionBinding): void {
  checked(SessionBindingSchema, binding, 'session_failed');
  const rel = relative(resolve(root), binding.sessionFile);
  if (!isAbsolute(binding.sessionFile) || !rel || rel.startsWith('..') || isAbsolute(rel) || !binding.sessionFile.endsWith('.jsonl')) throw fail();
}
/** Owned dedicated session tree: reject symlinks, sync each newly-created entry
 * in its parent directory, including the root's own creation. */
export async function durableDirectory(path: string, current: () => Promise<void>): Promise<void> {
  const target = resolve(path), parent = dirname(target);
  try { const s = await lstat(target); if (!s.isDirectory() || s.isSymbolicLink()) throw fail(); return; }
  catch (error) { if ((error as NodeJS.ErrnoException).code !== 'ENOENT') throw fail(); }
  if (parent === target) throw fail();
  await durableDirectory(parent, current);
  await current();
  try { await mkdir(target, { mode: 0o700 }); }
  catch (error) { if ((error as NodeJS.ErrnoException).code !== 'EEXIST') throw fail(); }
  const s = await lstat(target); if (!s.isDirectory() || s.isSymbolicLink()) throw fail();
  await current(); const fd = await open(parent, constants.O_RDONLY | constants.O_DIRECTORY | constants.O_NOFOLLOW);
  try { await fd.sync(); } finally { await fd.close(); }
}
export async function syncSession(binding: SessionBinding): Promise<void> {
  const fd = await open(binding.sessionFile, constants.O_RDWR | constants.O_NOFOLLOW);
  try { if (!(await fd.stat()).isFile()) throw fail(); await fd.sync(); } finally { await fd.close(); }
}
export async function syncDirectory(path: string): Promise<void> {
  const fd = await open(path, constants.O_RDONLY | constants.O_DIRECTORY | constants.O_NOFOLLOW);
  try { await fd.sync(); } finally { await fd.close(); }
}
function validTime(value: unknown): boolean {
  if (typeof value !== 'string' || !/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d{1,9})?(Z|[+-]\d{2}:\d{2})$/.test(value) || !Number.isFinite(Date.parse(value))) return false;
  const year = Number(value.slice(0, 4)), month = Number(value.slice(5, 7)), day = Number(value.slice(8, 10));
  const leap = year % 4 === 0 && (year % 100 !== 0 || year % 400 === 0);
  const days = [31, leap ? 29 : 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31];
  return month >= 1 && month <= 12 && day >= 1 && day <= (days[month - 1] ?? 0) && Number(value.slice(11, 13)) < 24 && Number(value.slice(14, 16)) < 60 && Number(value.slice(17, 19)) < 60;
}
const types = new Set(['message', 'model_change', 'thinking_level_change', 'compaction', 'context_edit', 'branch_summary', 'custom', 'custom_message', 'label', 'session_info', 'usage']);
/** Strict UTF-8/JSONL, tree and controller-evidence reader. No SDK permissive
 * parser: missing/truncated/legacy controller evidence fails closed. A changing
 * Pi append/compaction snapshot is retried, not declared corruption.
 * Controller callers run inside index.exclusive; direct callers must arrange
 * an equivalent file-only interval (never acquire Redis ownership from it). */
export async function readEvidence(root: string, binding: SessionBinding,
  durabilityFailed: () => void = () => {}): Promise<readonly CompletionProof[]> {
  try {
    assertSessionPath(root, binding);
    // Check every ancestor inside the dedicated root; O_NOFOLLOW covers leaf.
    let path = dirname(binding.sessionFile);
    while (true) {
      const s = await lstat(path); if (!s.isDirectory() || s.isSymbolicLink()) throw fail();
      if (path === resolve(root)) break;
      const next = dirname(path); if (next === path) throw fail(); path = next;
    }
    for (let attempt = 0; attempt < 8; attempt++) {
      const fd = await open(binding.sessionFile, constants.O_RDWR | constants.O_NOFOLLOW);
      try {
        const before = await fd.stat({ bigint: true }); if (!before.isFile() || before.size === 0n) throw fail();
        const ids = new Set<string>(), events = new Set<string>(), sequences = new Set<number>();
        const proofs: CompletionProof[] = []; let bound = false, headerSeen = false;
        const consume = (line: string) => {
          const raw: unknown = JSON.parse(line);
          if (!raw || typeof raw !== 'object' || Array.isArray(raw)) throw fail();
          const e = raw as Record<string, unknown>;
          if (!headerSeen) {
            if (e['type'] !== 'session' || e['version'] !== 3 || e['id'] !== binding.sessionId || typeof e['cwd'] !== 'string' || !validTime(e['timestamp'])) throw fail();
            headerSeen = true; return;
          }
          if (typeof e['type'] !== 'string' || !types.has(e['type']) || typeof e['id'] !== 'string' || !e['id'] || ids.has(e['id']) ||
              (e['parentId'] !== null && (typeof e['parentId'] !== 'string' || !ids.has(e['parentId']))) || !validTime(e['timestamp'])) throw fail();
          ids.add(e['id']);
          const string = (key: string) => typeof e[key] === 'string';
          if (e['type'] === 'message') {
            const message = e['message'];
            if (!message || typeof message !== 'object' || Array.isArray(message)) throw fail();
            const m = message as Record<string, unknown>;
            // AgentMessage is extensible; native summary roles have summary, not content.
            if (typeof m['role'] !== 'string' || !m['role'] || typeof m['timestamp'] !== 'number' || !Number.isFinite(m['timestamp'])) throw fail();
          } else if (e['type'] === 'compaction') {
            if (!string('summary') || !string('firstKeptEntryId') || !ids.has(e['firstKeptEntryId'] as string) || typeof e['tokensBefore'] !== 'number') throw fail();
          } else if (e['type'] === 'branch_summary') {
            if (!string('summary') || !string('fromId') || !ids.has(e['fromId'] as string)) throw fail();
          } else if (e['type'] === 'context_edit') {
            const replacement = e['replacement'];
            if (!string('targetId') || !ids.has(e['targetId'] as string) ||
                (replacement !== null && (!replacement || typeof replacement !== 'object' || Array.isArray(replacement) ||
                  (typeof (replacement as Record<string, unknown>)['content'] !== 'string' && !Array.isArray((replacement as Record<string, unknown>)['content']))))) throw fail();
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
          if (e['type'] !== 'custom') return;
          if (typeof e['customType'] !== 'string') throw fail();
          if (e['customType'] === BINDING_TYPE) {
            if (bound || !isDeepStrictEqual(checked(SessionBindingSchema, e['data'], 'session_failed'), binding)) throw fail();
            bound = true;
          } else if (e['customType'] === COMPLETION_TYPE) {
            const marker = checked(CompletionMarkerSchema, e['data'], 'session_failed');
            if (!bound || marker.canonicalChatId !== binding.canonicalChatId || events.has(marker.eventId) || sequences.has(marker.chatSequence) || !validTime(marker.completedAt)) throw fail();
            events.add(marker.eventId); sequences.add(marker.chatSequence);
            proofs.push(checked(CompletionProofSchema, { marker, entryId: e['id'], session: binding }, 'session_failed'));
          } else if (e['customType'].startsWith('whatsapp.controller.completed')) throw fail();
        };
        let pending = '', invalid = false;
        const decoder = new TextDecoder('utf-8', { fatal: true });
        const buffer = Buffer.allocUnsafe(65536);
        let remaining = before.size;
        try {
          // Retain at most one JSONL entry plus metadata/proofs, never raw history.
          while (remaining > 0n) {
            const length = Number(remaining < BigInt(buffer.length) ? remaining : BigInt(buffer.length));
            const { bytesRead } = await fd.read(buffer, 0, length, null);
            if (!bytesRead) { invalid = true; break; }
            remaining -= BigInt(bytesRead);
            pending += decoder.decode(buffer.subarray(0, bytesRead), { stream: true });
            let newline: number;
            while ((newline = pending.indexOf('\n')) >= 0) {
              consume(pending.slice(0, newline)); pending = pending.slice(newline + 1);
            }
          }
          pending += decoder.decode();
          if (pending || !headerSeen || !bound) invalid = true;
        } catch { invalid = true; }
        const after = await fd.stat({ bigint: true });
        const named = await lstat(binding.sessionFile, { bigint: true });
        if (before.size !== after.size || before.mtimeNs !== after.mtimeNs || after.ino !== named.ino || after.dev !== named.dev || after.size !== named.size || after.mtimeNs !== named.mtimeNs) continue;
        if (invalid) throw fail();
        // Restart can expose a complete append that never reached fsync.
        // Validate first, then establish file + parent durability on this exact
        // descriptor before returning ANY recovered proof (including empty scans).
        // Index callers hold exclusive; reserved live appends remain unavailable.
        // No Redis ownership gate is awaited in this file-only interval.
        try { await fd.sync(); await syncDirectory(dirname(binding.sessionFile)); }
        catch {
          // Synchronous status publication inside the existing file-only lock:
          // no journal/ownership await and no lock re-entry. A later successful
          // sync cannot rehabilitate this index instance after durability loss.
          durabilityFailed(); throw fail();
        }
        const durable = await fd.stat({ bigint: true });
        const durableName = await lstat(binding.sessionFile, { bigint: true });
        if (after.size !== durable.size || after.mtimeNs !== durable.mtimeNs ||
            durable.ino !== durableName.ino || durable.dev !== durableName.dev ||
            durable.size !== durableName.size || durable.mtimeNs !== durableName.mtimeNs || durableName.isSymbolicLink()) continue;
        return proofs;
      } finally { await fd.close(); }
    }
    throw fail();
  } catch { throw fail(); }
}
export function createCompletionIndex(root: string, policy: AdmissionPolicy, mappings: MappingPort,
  unresolved: (actualChat: string, messageId: string) => Promise<boolean>): CompletionIndex {
  const operatorJids = [...policy.operatorJids];
  const canonical = [...policy.groups, operatorJids[0]].filter((c): c is string => c !== undefined);
  type FileIdentity = { dev: bigint; ino: bigint; size: bigint; mtimeNs: bigint };
  const sameIdentity = (a: FileIdentity, b: FileIdentity) => a.dev === b.dev && a.ino === b.ino && a.size === b.size && a.mtimeNs === b.mtimeNs;
  const identity = async (binding: SessionBinding): Promise<FileIdentity> => {
    assertSessionPath(root, binding);
    const value = await lstat(binding.sessionFile, { bigint: true });
    if (!value.isFile() || value.isSymbolicLink()) throw fail();
    return { dev: value.dev, ino: value.ino, size: value.size, mtimeNs: value.mtimeNs };
  };
  const poisoned = new Set<string>(), pending = new Set<string>();
  const cache = new Map<string, { identity: FileIdentity; proofs: readonly CompletionProof[] }>();
  const poison = (binding: SessionBinding) => {
    poisoned.add(binding.sessionFile); pending.delete(binding.sessionFile); cache.delete(binding.sessionFile);
  };
  const evidence = async (binding: SessionBinding): Promise<readonly CompletionProof[]> => {
    if (poisoned.has(binding.sessionFile)) throw fail();
    try {
      // Callers hold the file-only interval. Two matching named-file samples
      // give cache hits a linearization point; reservations remove the entry
      // before host appends can publish new evidence.
      const before = await identity(binding);
      const cached = cache.get(binding.sessionFile);
      if (cached && sameIdentity(cached.identity, before)) {
        const after = await identity(binding);
        if (sameIdentity(before, after)) return cached.proofs;
      }
      const proofs = await readEvidence(root, binding, () => { poison(binding); });
      const after = await identity(binding);
      // readEvidence already retries until its descriptor and named file agree.
      // If the outer identity changed, its result is valid at that completed
      // scan's linearization point but must not be cached under a later append.
      if (!sameIdentity(before, after)) return proofs;
      cache.set(binding.sessionFile, { identity: after, proofs });
      return proofs;
    } catch { poison(binding); throw fail(); }
  };
  if (!canonical.length || new Set(canonical).size !== canonical.length) throw new ControllerError('config_invalid');
  let tail: Promise<unknown> = Promise.resolve();
  const exclusive = <T>(job: () => Promise<T>): Promise<T> => {
    const task = tail.then(job); tail = task.catch(() => {}); return task;
  };
  const snapshot = async () => {
    const rows = structuredClone(await mappings.list());
    if (rows.length !== canonical.length || new Set(rows.map(r => r.chat)).size !== canonical.length || rows.some(r => !canonical.includes(r.chat))) throw fail();
    const seen = new Set<string>();
    for (const row of rows) if (row.binding) {
      assertSessionPath(root, row.binding);
      if (row.binding.canonicalChatId !== row.chat || seen.has(row.binding.sessionFile)) throw fail();
      seen.add(row.binding.sessionFile);
    }
    return rows;
  };
  const rebuild = async (rows: Awaited<ReturnType<typeof snapshot>>) => {
    const all: CompletionProof[] = [], ids = new Set<string>();
    for (const row of rows) if (row.binding) {
      if (pending.has(row.binding.sessionFile)) throw fail();
      for (const proof of await evidence(row.binding)) {
        if (ids.has(proof.marker.eventId)) throw fail(); ids.add(proof.marker.eventId); all.push(proof);
      }
    }
    return all;
  };
  return {
    exclusive, evidence,
    invalidate(binding) { poison(binding); },
    reserve(binding) {
      if (poisoned.has(binding.sessionFile) || pending.has(binding.sessionFile)) throw fail();
      cache.delete(binding.sessionFile); pending.add(binding.sessionFile);
    },
    publish(binding) { if (poisoned.has(binding.sessionFile) || !pending.delete(binding.sessionFile)) throw fail(); },
    async findEvent(chat, id) {
      try {
        if (!canonical.includes(chat)) throw fail();
        const rows = await snapshot();
        return await exclusive(async () => (await rebuild(rows)).find(p => p.marker.canonicalChatId === chat && p.marker.eventId === id));
      } catch { throw fail(); }
    },
    async classifyOwn(chat, actualChat, messageId) {
      try {
        const route = operatorJids.includes(actualChat) ? operatorJids[0] : actualChat;
        if (route !== chat || !canonical.includes(chat) || !messageId) throw fail();
        // Journal lookup outside index lock; uncertainty always takes priority.
        if (await unresolved(actualChat, messageId)) return 'uncertain';
        const rows = await snapshot();
        const found = await exclusive(async () => (await rebuild(rows)).some(p => p.marker.confirmedSends.some(s => s.destination === actualChat && s.messageId === messageId)));
        if (await unresolved(actualChat, messageId)) return 'uncertain';
        return found ? 'confirmed' : 'manual';
      } catch { throw fail(); }
    }
  };
}
