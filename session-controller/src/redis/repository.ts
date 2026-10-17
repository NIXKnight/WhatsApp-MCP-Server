import { createHash, timingSafeEqual } from 'node:crypto';
import { isDeepStrictEqual } from 'node:util';
import { Type } from 'typebox';
import {
  checked, ControllerError, OpaqueIdSchema, StreamIdSchema, FenceSchema, OwnershipSchema, ChatStateSchema, NotificationV2Schema,
  SessionBindingSchema, CompletionProofSchema, MutationAttemptSchema, MutationStateSchema,
  CheckpointRecordSchema, CheckpointRecoveryBindingV1Schema, SessionFileGenerationSchema, PauseReasonSchema, MessageIdSchema, EventFingerprintSchema, ChatJidSchema, fingerprintEvent, systemClock,
  type AdmissionPolicy, type CanonicalChatId, type CompletionIndexPort,
  type WorkRepository, type Fence, type ChatState, type ClaimedWork, type SessionBinding,
  type Ownership, type PauseReason, type CompletionProof, type MutationAttempt, type MutationState,
  type MutationJournalPort, type CheckpointPort, type CheckpointRecord, type ClaimResult,
  type Acceptance, type Clock, type SessionFileGeneration, type NotificationV2
} from '../domain.js';
import { parsePayload } from '../intake/payload.js';
import { strings, type RedisConnections } from './client.js';
import { configuredChats, keys, scriptKeys, GROUP } from './keys.js';
import { SCRIPT } from './scripts.js';
const duration = Type.Integer({ minimum: 1, maximum: 3600000 });
const recoveryCount = Type.Integer({ minimum: 1, maximum: 128 });
const recoveryDeletedCount = Type.Integer({ minimum: 0, maximum: 1280 });
const absoluteMilliseconds = Type.Integer({ minimum: 0, maximum: Number.MAX_SAFE_INTEGER });
const positiveAbsoluteMilliseconds = Type.Integer({ minimum: 1, maximum: Number.MAX_SAFE_INTEGER });
const entryId = Type.String({ minLength: 1, maxLength: 128 });
const legacyIntent = Type.String({ pattern: '^sha256:v1:[0-9a-f]{64}$', minLength: 74, maxLength: 74 });
const JournalEnvelopeSchema = Type.Object({ version: Type.Literal(1), eventFingerprint: EventFingerprintSchema,
  session: SessionBindingSchema, state: MutationStateSchema }, { additionalProperties: false });
function journalState(raw: string, chat: CanonicalChatId): MutationState {
  const value = json(raw);
  if (value && typeof value === 'object' && 'state' in value) return journalRecord(raw, chat).state;
  return checked(MutationStateSchema, value, 'mutation_uncertain');
}
function journalRecord(raw: string, chat: CanonicalChatId) {
  const r = checked(JournalEnvelopeSchema, json(raw), 'mutation_uncertain');
  if (r.session.canonicalChatId !== chat || r.state.canonicalChatId !== chat) throw new ControllerError('mutation_uncertain');
  return r;
}
function json(raw: string): unknown {
  try { return JSON.parse(raw); } catch { throw new ControllerError('store_unavailable'); }
}
function canonicalJson(value: unknown): string {
  if (value === null || typeof value === 'string' || typeof value === 'number' || typeof value === 'boolean') return JSON.stringify(value);
  if (Array.isArray(value)) return '[' + value.map(canonicalJson).join(',') + ']';
  if (!value || typeof value !== 'object') throw new ControllerError('checkpoint_invalid');
  return '{' + Object.entries(value).sort(([a], [b]) => a.localeCompare(b))
    .map(([key, child]) => JSON.stringify(key) + ':' + canonicalJson(child)).join(',') + '}';
}
function legacyRepairDigest(event: NotificationV2, streamId: string, session: SessionBinding,
  checkpoint: Extract<CheckpointRecord, { phase: 'prepared' }>, generation: SessionFileGeneration): string {
  const exactGeneration = checked(SessionFileGenerationSchema, generation, 'restore_failed');
  const exactEvent = checked(NotificationV2Schema, event, 'restore_failed');
  const material = canonicalJson({ version: 1, domain: 'sc:legacy-compaction-repair:v3',
    event: { value: exactEvent, eventFingerprint: fingerprintEvent(exactEvent), streamId },
    session: binding(session), checkpoint, generation: exactGeneration });
  return 'sha256:v1:' + createHash('sha256').update(material, 'utf8').digest('hex');
}
function binding(value: SessionBinding): SessionBinding {
  const b = checked(SessionBindingSchema, value, 'session_failed');
  return { canonicalChatId: b.canonicalChatId, sessionId: b.sessionId, sessionFile: b.sessionFile };
}
function verifiedProof(value: unknown, chat: CanonicalChatId, id: string): CompletionProof {
  const p = checked(CompletionProofSchema, value, 'session_failed');
  if (p.marker.canonicalChatId !== chat || p.session.canonicalChatId !== chat || p.marker.eventId !== id) throw new ControllerError('session_failed');
  return p;
}
export type PelRecovery = { status: 'ok'; next: string; claimed: number; deleted: number } | { status: 'lost' | 'busy' | 'paused' };
export interface CompactionRepairClaim {
  readonly work: ClaimedWork;
  readonly binding: SessionBinding;
  readonly checkpoint: Extract<CheckpointRecord, { phase: 'prepared' }>;
  /** Present only for an explicitly authorized legacy repair. Never log it. */
  readonly legacyIntentDigest?: string;
}
export function legacyCompactionRepairDigest(claim: CompactionRepairClaim, generation: SessionFileGeneration): string {
  if (claim.checkpoint.recovery) throw new ControllerError('checkpoint_invalid');
  return legacyRepairDigest(claim.work.event, claim.work.streamId, claim.binding, claim.checkpoint, generation);
}
function samePrivateDigest(left: string, right: string): boolean {
  const a = Buffer.from(left, 'utf8'), b = Buffer.from(right, 'utf8');
  return a.length === b.length && timingSafeEqual(a, b);
}
export type CompactionRepairClaimResult =
  | { status: 'claimed'; claim: CompactionRepairClaim }
  | { status: 'ineligible' | 'intent_required' | 'unsafe' | 'busy' | 'lost' };
export interface RedisRepository extends WorkRepository {
  recoverPending(chat: CanonicalChatId, app: Fence, consumer: string, start: string, count: number): Promise<PelRecovery>;
  /** Acquire the exact repair fence for one eligible unbound legacy tuple.
   * The caller must prove its file and explicitly release the fence. */
  createCompactionRepairIntent(chat: CanonicalChatId, app: Fence, owner: string, leaseMs: number): Promise<CompactionRepairClaimResult>;
  /** Recovery-only lease acquisition. It never claims/acks/deletes a stream row.
   * An unbound legacy record additionally requires a private expected-state digest. */
  claimCompactionRepair(chat: CanonicalChatId, app: Fence, owner: string, leaseMs: number, legacyIntentDigest?: string): Promise<CompactionRepairClaimResult>;
  /** Release only the exact repair chat fence; prepared/pause state is retained. */
  releaseCompactionRepair(claim: CompactionRepairClaim): Promise<boolean>;
  /** Atomically re-proves Redis-side orphan conditions and every durable binding,
   * marks prepared aborted, clears only compaction_failed, and releases repair. */
  abortCompactionRepair(claim: CompactionRepairClaim, generation: SessionFileGeneration): Promise<boolean>;
  checkWork(work: ClaimedWork): Promise<boolean>;
  turnState(work: ClaimedWork): Promise<'fresh' | 'started' | 'lost'>;
  beginTurn(work: ClaimedWork): Promise<boolean>;
  readonly journal: MutationJournalPort;
  /** Native Pi path requires exact mapped binding and immutable fingerprint. */
  readonly nativeJournal: MutationJournalPort;
  /** Active journals across every originating session. Unresolved destination
   * attempts take precedence even over a confirmed ID in another journal. */
  ownMutation(actual: string, messageId: string): Promise<'confirmed' | 'uncertain' | 'none'>;
  readonly checkpoints: CheckpointPort;
  readonly chats: readonly CanonicalChatId[];
  /** Wake hint only; neither PEL delivery nor this method grants execution ownership. */
  waitForWork(chat: CanonicalChatId, consumer: string, blockMs: number): Promise<void>;
}
export interface SessionMapping { readonly chat: CanonicalChatId; readonly binding?: SessionBinding }
/** Read every configured mapping without entering the serialized writer. This
 * port is intentionally limited to nonblocking HGET commands on the dedicated
 * mapping client so completion lookup can run while intake owns durability. */
export async function readSessionMappings(
  connections: Pick<RedisConnections, 'mapping'>,
  policy: AdmissionPolicy
): Promise<readonly SessionMapping[]> {
  const chats = configuredChats({ groups: [...policy.groups], operatorJids: [...policy.operatorJids] });
  return Promise.all(chats.map(async chat => {
    let raw: unknown;
    try { raw = await connections.mapping.sendCommand(['HGET', keys(chat).state, 'session']); }
    catch { throw new ControllerError('store_unavailable'); }
    if (raw === null) return { chat };
    if (typeof raw !== 'string') throw new ControllerError('session_failed');
    let value: unknown;
    try { value = JSON.parse(raw); } catch { throw new ControllerError('session_failed'); }
    const parsed = binding(checked(SessionBindingSchema, value, 'session_failed'));
    if (parsed.canonicalChatId !== chat) throw new ControllerError('session_failed');
    return { chat, binding: parsed };
  }));
}
export async function createRepository(connections: RedisConnections, policy: AdmissionPolicy, completions: CompletionIndexPort,
  clock: Clock = systemClock, options: { initialize?: boolean } = {}): Promise<RedisRepository> {
  const admission = { groups: [...policy.groups], operatorJids: [...policy.operatorJids] };
  const chats = configuredChats(admission);
  const allowed = (chat: CanonicalChatId) => { if (!chats.includes(chat)) throw new ControllerError('policy_denied'); };
  const evalScript = async (chat: CanonicalChatId, args: readonly string[]) => {
    allowed(chat); const kk = scriptKeys(chat);
    return strings(await connections.writer.sendCommand(['EVAL', SCRIPT, String(kk.length), ...kk, ...args]));
  };
  const write = (chat: CanonicalChatId, args: readonly string[]) => connections.durable(() => evalScript(chat, args));
  const read = (chat: CanonicalChatId, args: readonly string[]) => connections.serial(() => evalScript(chat, args));
  const first = chats[0]; if (first === undefined) throw new ControllerError('config_invalid');
  if (options.initialize !== false) {
    for (const chat of chats) if ((await write(chat, ['init', chat, GROUP]))[0] !== 'ok') throw new ControllerError('store_unavailable');
  }
  const owner = (v: string) => checked(OpaqueIdSchema, v, 'lease_lost');
  const ms = (v: number) => String(checked(duration, v, 'config_invalid'));
  const argsFor = (work: ClaimedWork): string[] => {
    checked(OwnershipSchema, work.ownership, 'lease_lost'); allowed(work.event.canonical_chat_jid);
    const e = parsePayload(Buffer.from(JSON.stringify(work.event)), admission);
    checked(StreamIdSchema, work.streamId, 'lease_lost');
    const o = work.ownership;
    return [o.app.owner, o.app.epoch, o.chat.owner, o.chat.epoch, e.event_id, work.streamId, fingerprintEvent(e)];
  };
  const ownedWrite = async (work: ClaimedWork, op: string, rest: readonly string[]): Promise<boolean> => (await write(work.event.canonical_chat_jid, [op, ...argsFor(work), ...rest]))[0] === 'ok';
  const acquireResult = (result: string[], id: string): Fence | undefined => {
    if (result[0] === 'busy' || result[0] === 'lost') return undefined;
    return checked(FenceSchema, { owner: id, epoch: result[0], expiresAtMs: Number(result[1]) }, 'store_unavailable');
  };
  const repository: RedisRepository = {
    chats,
    async accept(input): Promise<Acceptance> {
      try {
        // Own a writer interval from snapshot through Pi lookup, Lua and WAITAOF.
        // Across processes completion increments chat AND global identity revisions; stale lookups retry.
        const event = parsePayload(Buffer.from(JSON.stringify(input)), admission);
        const chat = event.canonical_chat_jid, fp = fingerprintEvent(event); allowed(chat);
        return await connections.durable(async () => {
          for (let attempt = 0; attempt < 8; attempt++) {
            const snapshot = await evalScript(chat, ['snapshot']);
            let completed = false;
            for (const candidate of chats) {
              const found = await completions.findEvent(candidate, event.event_id);
              if (found === undefined) continue;
              const proof = verifiedProof(found, candidate, event.event_id);
              if (candidate !== chat || proof.marker.chatSequence !== event.chat_sequence || proof.marker.eventFingerprint !== fp) return { status: 'conflict' };
              const state = await evalScript(chat, ['read']);
              if (!state[1] || !isDeepStrictEqual(binding(proof.session), binding(checked(SessionBindingSchema, json(state[1]), 'session_failed')))) throw new ControllerError('session_failed');
              completed = true;
            }
            const result = await evalScript(chat, ['accept', snapshot[0] ?? '', event.event_id, fp, String(event.chat_sequence), JSON.stringify(event), completed ? 'yes' : 'no', chat, snapshot[2] ?? '']);
            if (result[0] === 'retry') continue;
            if (result[0] === 'accepted' || result[0] === 'duplicate' || result[0] === 'conflict') return { status: result[0] };
            throw new ControllerError('store_unavailable');
          }
          throw new ControllerError('store_unavailable');
        });
      } catch { return { status: 'unavailable' }; }
    },
    async acquireApp(id, leaseMs) { return acquireResult(await write(first, ['app-acquire', owner(id), ms(leaseMs)]), id); },
    async renewApp(fence, leaseMs) {
      checked(FenceSchema, fence, 'lease_lost');
      return acquireResult(await write(first, ['app-renew', fence.owner, fence.epoch, ms(leaseMs)]), fence.owner);
    },
    async releaseApp(fence) {
      checked(FenceSchema, fence, 'lease_lost'); return (await write(first, ['app-release', fence.owner, fence.epoch]))[0] === 'ok';
    },
    async readChat(chat): Promise<ChatState> {
      const r = await read(chat, ['read']);
      return checked(ChatStateSchema, { canonicalChatId: chat, lastCompletedSequence: Number(r[0]),
        ...(r[1] ? { session: checked(SessionBindingSchema, json(r[1]), 'session_failed') } : {}),
        ...(r[2] ? { pause: r[2] } : {}), ...(r[3] ? { gapSinceMs: Number(r[3]) } : {}) }, 'store_unavailable');
    },
    bindSession(work, session) {
      const b = binding(session); if (b.canonicalChatId !== work.event.canonical_chat_jid) throw new ControllerError('session_failed');
      return ownedWrite(work, 'bind', [JSON.stringify(b)]);
    },
    async claimNext(chat, app, id, leaseMs, gapTimeoutMs): Promise<ClaimResult> {
      checked(FenceSchema, app, 'lease_lost');
      const r = await write(chat, ['claim', app.owner, app.epoch, owner(id), ms(leaseMs), ms(gapTimeoutMs), GROUP]);
      if (r[0] === 'empty' || r[0] === 'waiting' || r[0] === 'paused' || r[0] === 'lost') return { status: r[0] };
      if (r[0] !== 'claimed' || !r[1] || !r[2]) throw new ControllerError('store_unavailable');
      const event = parsePayload(Buffer.from(r[2]), admission);
      if (event.canonical_chat_jid !== chat) throw new ControllerError('store_unavailable');
      const ownership = checked(OwnershipSchema, { app, chat: { owner: id, epoch: r[3], expiresAtMs: Number(r[4]) } }, 'store_unavailable');
      return { status: 'claimed', work: { streamId: r[1], event, ownership } };
    },
    async renew(work, leaseMs): Promise<Ownership | undefined> {
      const r = await write(work.event.canonical_chat_jid, ['renew', ...argsFor(work), ms(leaseMs)]);
      if (r[0] === 'lost') return undefined;
      return checked(OwnershipSchema, { app: work.ownership.app, chat: { owner: work.ownership.chat.owner, epoch: r[0], expiresAtMs: Number(r[1]) } }, 'store_unavailable');
    },
    pause(work, reason: PauseReason) { return ownedWrite(work, 'pause', [checked(PauseReasonSchema, reason, 'lease_lost')]); },
    async complete(work, value): Promise<boolean> {
      const event = work.event, proof = verifiedProof(value, event.canonical_chat_jid, event.event_id);
      if (proof.marker.chatSequence !== event.chat_sequence || proof.marker.eventFingerprint !== fingerprintEvent(event)) return false;
      return connections.durable(async () => {
        const durable = await completions.findEvent(event.canonical_chat_jid, event.event_id);
        if (durable === undefined || !isDeepStrictEqual(verifiedProof(durable, event.canonical_chat_jid, event.event_id), proof)) return false;
        return (await evalScript(event.canonical_chat_jid, ['complete', ...argsFor(work), JSON.stringify(binding(proof.session)), GROUP, JSON.stringify(proof.marker.confirmedSends)]))[0] === 'ok';
      });
    },
    journal: {
      async begin(work, value: MutationAttempt) {
        const a = checked(MutationAttemptSchema, value, 'mutation_uncertain');
        if (a.eventId !== work.event.event_id || a.canonicalChatId !== work.event.canonical_chat_jid || !isDeepStrictEqual(a.ownership, work.ownership)) return false;
        return ownedWrite(work, 'journal-begin', [a.attemptId, JSON.stringify(a)]);
      },
      confirm(work, attemptId, messageId) {
        const id = checked(OpaqueIdSchema, attemptId, 'mutation_uncertain');
        const message = checked(MessageIdSchema, messageId, 'mutation_uncertain');
        if (Buffer.byteLength(message) > 512) throw new ControllerError('mutation_uncertain');
        const time = clock.now(); if (!Number.isSafeInteger(time) || time < 0 || time > 8640000000000000) throw new ControllerError('mutation_uncertain');
        return ownedWrite(work, 'journal-transition', [id, 'confirmed', message, new Date(time).toISOString()]);
      },
      uncertain(work, attemptId) { return ownedWrite(work, 'journal-transition', [checked(OpaqueIdSchema, attemptId, 'mutation_uncertain'), 'uncertain', '', '']); },
      async list(chat): Promise<readonly MutationState[]> { return (await read(chat, ['journal-list'])).map(raw => journalState(raw, chat)); }
    },
    nativeJournal: {
      async begin(work, value: MutationAttempt) {
        const a = checked(MutationAttemptSchema, value, 'mutation_uncertain');
        if (a.eventId !== work.event.event_id || a.canonicalChatId !== work.event.canonical_chat_jid || !isDeepStrictEqual(a.ownership, work.ownership)) return false;
        const chat = await repository.readChat(work.event.canonical_chat_jid);
        if (!chat.session) return false;
        const session = binding(chat.session);
        const record = checked(JournalEnvelopeSchema, { version: 1, eventFingerprint: fingerprintEvent(work.event), session, state: a }, 'mutation_uncertain');
        return ownedWrite(work, 'native-journal-begin', [a.attemptId, JSON.stringify(record), JSON.stringify(session)]);
      },
      confirm(work, attemptId, messageId) {
        const id = checked(OpaqueIdSchema, attemptId, 'mutation_uncertain');
        const message = checked(MessageIdSchema, messageId, 'mutation_uncertain');
        if (Buffer.byteLength(message) > 512) throw new ControllerError('mutation_uncertain');
        const time = clock.now(); if (!Number.isSafeInteger(time) || time < 0 || time > 8640000000000000) throw new ControllerError('mutation_uncertain');
        return ownedWrite(work, 'journal-transition', [id, 'confirmed', message, new Date(time).toISOString()]);
      },
      uncertain(work, attemptId) { return ownedWrite(work, 'journal-transition', [checked(OpaqueIdSchema, attemptId, 'mutation_uncertain'), 'uncertain', '', '']); },
      async list(chat): Promise<readonly MutationState[]> {
        return (await read(chat, ['journal-list'])).map(raw => journalRecord(raw, chat).state);
      }
    },
    async ownMutation(actual, messageId) {
      checked(ChatJidSchema, actual, 'mutation_uncertain'); checked(MessageIdSchema, messageId, 'mutation_uncertain');
      // One serialized writer interval across ALL chats, not the index's file
      // interval. No admission check on actual destination; it can be external.
      return connections.serial(async () => {
        let confirmed = false;
        for (const chat of chats) {
          const state = await evalScript(chat, ['read']);
          for (const raw of await evalScript(chat, ['journal-list'])) {
            const value = json(raw);
            if (!value || typeof value !== 'object' || !('state' in value)) {
              if (journalState(raw, chat).destination === actual) return 'uncertain';
              continue;
            }
            const record = journalRecord(raw, chat), a = record.state;
            if (!state[1] || !isDeepStrictEqual(record.session, checked(SessionBindingSchema, json(state[1]), 'session_failed'))) throw new ControllerError('mutation_uncertain');
            if (a.destination !== actual) continue;
            if (a.status !== 'confirmed') return 'uncertain';
            if (a.messageId === messageId) confirmed = true;
          }
        }
        return confirmed ? 'confirmed' : 'none';
      });
    },
    checkpoints: {
      async prepare(work, value: CheckpointRecord) {
        const r = checked(CheckpointRecordSchema, value, 'checkpoint_invalid');
        if (r.phase !== 'prepared' || !r.recovery || r.checkpoint.canonicalChatId !== work.event.canonical_chat_jid ||
            r.checkpoint.lastCompletedSequence !== work.event.chat_sequence - 1) return false;
        const recovery = checked(CheckpointRecoveryBindingV1Schema, r.recovery, 'checkpoint_invalid');
        if (recovery.eventId !== work.event.event_id || recovery.eventFingerprint !== fingerprintEvent(work.event) ||
            recovery.streamId !== work.streamId || recovery.session.canonicalChatId !== work.event.canonical_chat_jid) return false;
        return ownedWrite(work, 'checkpoint-prepare', [JSON.stringify(r), JSON.stringify(binding(recovery.session))]);
      },
      async transition(work, checkpointId, phase, compactionEntryId) {
        const id = checked(OpaqueIdSchema, checkpointId, 'checkpoint_invalid');
        if (phase !== 'compacted' && phase !== 'restored') throw new ControllerError('checkpoint_invalid');
        const entry = checked(entryId, compactionEntryId, 'checkpoint_invalid');
        return connections.durable(async () => {
          const [raw] = await evalScript(work.event.canonical_chat_jid, ['checkpoint-latest']);
          if (!raw) return false;
          const current = checked(CheckpointRecordSchema, json(raw), 'checkpoint_invalid');
          const next = checked(CheckpointRecordSchema, { ...current, phase, compactionEntryId: entry }, 'checkpoint_invalid');
          return (await evalScript(work.event.canonical_chat_jid, ['checkpoint-transition', ...argsFor(work), id, phase, entry, raw, JSON.stringify(next)]))[0] === 'ok';
        });
      },
      async latest(chat) {
        const r = await read(chat, ['checkpoint-latest']); return r[0] ? checked(CheckpointRecordSchema, json(r[0]), 'checkpoint_invalid') : undefined;
      }
    },
    async recoverPending(chat, app, consumer, start, count) {
      checked(FenceSchema, app, 'lease_lost'); checked(StreamIdSchema, start, 'store_unavailable');
      checked(recoveryCount, count, 'config_invalid');
      const r = await write(chat, ['recover-pel', app.owner, app.epoch, owner(consumer), start, String(count), GROUP]);
      if (r[0] === 'lost' || r[0] === 'busy' || r[0] === 'paused') return { status: r[0] };
      if (r[0] !== 'ok') throw new ControllerError('store_unavailable');
      const next = checked(StreamIdSchema, r[1], 'store_unavailable');
      const claimed = checked(Type.Integer({ minimum: 0, maximum: 128 }), Number(r[2]), 'store_unavailable');
      const deleted = checked(recoveryDeletedCount, Number(r[3]), 'store_unavailable');
      return { status: 'ok', next, claimed, deleted };
    },
    async createCompactionRepairIntent(chat, app, id, leaseMs) {
      checked(FenceSchema, app, 'lease_lost');
      const r = await write(chat, ['repair-intent', app.owner, app.epoch, owner(id), ms(leaseMs), GROUP, chat]);
      if (r[0] === 'ineligible' || r[0] === 'unsafe' || r[0] === 'busy' || r[0] === 'lost') return { status: r[0] };
      if (r[0] !== 'claimed' || !r[1] || !r[2] || !r[3] || !r[4] || !r[5] || !r[6]) throw new ControllerError('store_unavailable');
      try {
        const event = parsePayload(Buffer.from(r[2]), admission);
        const parsedBinding = binding(checked(SessionBindingSchema, json(r[3]), 'session_failed'));
        const prepared = checked(CheckpointRecordSchema, json(r[4]), 'checkpoint_invalid');
        if (event.canonical_chat_jid !== chat || parsedBinding.canonicalChatId !== chat || prepared.phase !== 'prepared' ||
            prepared.recovery || prepared.checkpoint.canonicalChatId !== chat ||
            prepared.checkpoint.lastCompletedSequence !== event.chat_sequence - 1) throw new ControllerError('checkpoint_invalid');
        const work = { streamId: checked(StreamIdSchema, r[1], 'store_unavailable'), event, ownership: checked(OwnershipSchema, {
          app, chat: { owner: id, epoch: r[5], expiresAtMs: Number(r[6]) }
        }, 'store_unavailable') };
        return { status: 'claimed', claim: { work, binding: parsedBinding, checkpoint: prepared } };
      } catch {
        try { await write(chat, ['repair-release-fence', app.owner, app.epoch, id, r[5]]); } catch {}
        throw new ControllerError('checkpoint_invalid');
      }
    },
    async claimCompactionRepair(chat, app, id, leaseMs, legacyIntentDigest) {
      checked(FenceSchema, app, 'lease_lost');
      const intent = legacyIntentDigest === undefined ? '' : checked(legacyIntent, legacyIntentDigest, 'config_invalid');
      const r = await write(chat, ['repair-claim', app.owner, app.epoch, owner(id), ms(leaseMs), GROUP, chat, intent]);
      if (r[0] === 'ineligible' || r[0] === 'intent_required' || r[0] === 'unsafe' || r[0] === 'busy' || r[0] === 'lost') return { status: r[0] };
      if (r[0] !== 'claimed' || !r[1] || !r[2] || !r[3] || !r[4] || !r[5] || !r[6]) throw new ControllerError('store_unavailable');
      try {
        const event = parsePayload(Buffer.from(r[2]), admission);
        if (event.canonical_chat_jid !== chat) throw new ControllerError('store_unavailable');
        const parsedBinding = binding(checked(SessionBindingSchema, json(r[3]), 'session_failed'));
        if (parsedBinding.canonicalChatId !== chat) throw new ControllerError('session_failed');
        const checkpoint = checked(CheckpointRecordSchema, json(r[4]), 'checkpoint_invalid');
        if (checkpoint.phase !== 'prepared' || checkpoint.checkpoint.canonicalChatId !== chat ||
            checkpoint.checkpoint.lastCompletedSequence !== event.chat_sequence - 1) throw new ControllerError('checkpoint_invalid');
        const work = { streamId: r[1], event, ownership: checked(OwnershipSchema, {
          app, chat: { owner: id, epoch: r[5], expiresAtMs: Number(r[6]) }
        }, 'store_unavailable') };
        if (checkpoint.recovery) {
          const recovery = checked(CheckpointRecoveryBindingV1Schema, checkpoint.recovery, 'checkpoint_invalid');
          if (recovery.eventId !== event.event_id || recovery.eventFingerprint !== fingerprintEvent(event) ||
              recovery.streamId !== work.streamId || !isDeepStrictEqual(recovery.session, parsedBinding)) {
            throw new ControllerError('checkpoint_invalid');
          }
        } else if (!intent) {
          throw new ControllerError('checkpoint_invalid');
        }
        const claim: CompactionRepairClaim = {
          work, binding: parsedBinding, checkpoint,
          ...(checkpoint.recovery ? {} : { legacyIntentDigest: intent })
        };
        // The one-shot manifest is stored on this exact repair fence. Its
        // closed tuple+file-generation digest is checked only after strict
        // filesystem proof, immediately before abort.
        return { status: 'claimed', claim };
      } catch {
        // Lua already granted a repair fence. Even malformed/substituted reply
        // evidence must not strand ownership while the prepared pause remains.
        try { await write(chat, ['repair-release-fence', app.owner, app.epoch, id, r[5]]); } catch {}
        throw new ControllerError('checkpoint_invalid');
      }
    },
    async releaseCompactionRepair(claim) {
      const { work, binding: expectedBinding, checkpoint: prepared } = claim;
      checked(SessionBindingSchema, expectedBinding, 'session_failed');
      checked(CheckpointRecordSchema, prepared, 'checkpoint_invalid');
      if (prepared.phase !== 'prepared') return false;
      return (await write(work.event.canonical_chat_jid, ['repair-release', ...argsFor(work),
        JSON.stringify(binding(expectedBinding))]))[0] === 'ok';
    },
    async abortCompactionRepair(claim, fileGeneration) {
      const { work, binding: expectedBinding, checkpoint: prepared } = claim;
      checked(SessionBindingSchema, expectedBinding, 'session_failed');
      checked(CheckpointRecordSchema, prepared, 'checkpoint_invalid');
      const provedGeneration = checked(SessionFileGenerationSchema, fileGeneration, 'restore_failed');
      if (prepared.phase !== 'prepared' || expectedBinding.canonicalChatId !== work.event.canonical_chat_jid) return false;
      if (prepared.recovery && (prepared.recovery.eventId !== work.event.event_id ||
          prepared.recovery.eventFingerprint !== fingerprintEvent(work.event) ||
          prepared.recovery.streamId !== work.streamId || !isDeepStrictEqual(prepared.recovery.session, expectedBinding) ||
          !isDeepStrictEqual(prepared.recovery.generation, provedGeneration))) return false;
      const intent = claim.legacyIntentDigest === undefined ? '' : checked(legacyIntent, claim.legacyIntentDigest, 'config_invalid');
      if (!prepared.recovery && (!intent || !samePrivateDigest(legacyRepairDigest(work.event, work.streamId,
          expectedBinding, prepared, provedGeneration), intent))) return false;
      return connections.durable(async () => {
        const [raw] = await evalScript(work.event.canonical_chat_jid, ['checkpoint-latest']);
        if (!raw) return false;
        const current = checked(CheckpointRecordSchema, json(raw), 'checkpoint_invalid');
        if (current.phase !== 'prepared' || !isDeepStrictEqual(current, prepared)) return false;
        const next = checked(CheckpointRecordSchema, { ...current, phase: 'aborted' }, 'checkpoint_invalid');
        return (await evalScript(work.event.canonical_chat_jid, ['checkpoint-abort', ...argsFor(work),
          JSON.stringify(binding(expectedBinding)), current.checkpointId, raw, JSON.stringify(next), intent, GROUP]))[0] === 'ok';
      });
    },
    async checkWork(work) {
      const r = await write(work.event.canonical_chat_jid, ['check-work', ...argsFor(work)]);
      if (r[0] !== 'ok') return false;
      const appUntil = checked(positiveAbsoluteMilliseconds, Number(r[1]), 'store_unavailable');
      const chatUntil = checked(positiveAbsoluteMilliseconds, Number(r[2]), 'store_unavailable');
      // A delayed durability barrier must not turn an expired reply into authority.
      const time = checked(absoluteMilliseconds, clock.now(), 'store_unavailable');
      return time < appUntil && time < chatUntil;
    },
    async turnState(work) {
      const r = await read(work.event.canonical_chat_jid, ['turn-state', ...argsFor(work)]);
      if (r[0] === 'lost') return 'lost';
      if (r[0] !== 'ok') throw new ControllerError('store_unavailable');
      if (!r[1]) return 'fresh';
      if (r[1] !== work.event.event_id) throw new ControllerError('session_failed');
      return 'started';
    },
    beginTurn(work) { return ownedWrite(work, 'begin-turn', []); },
    async waitForWork(chat, consumer, blockMs) {
      allowed(chat); owner(consumer); checked(Type.Integer({ minimum: 1, maximum: 5000 }), blockMs, 'config_invalid');
      try { await connections.reader.sendCommand(['XREADGROUP', 'GROUP', GROUP, consumer, 'COUNT', '1', 'BLOCK', String(blockMs), 'STREAMS', keys(chat).stream, '>']); }
      catch { throw new ControllerError('store_unavailable'); }
    },
    close: () => connections.close()
  };
  return repository;
}
