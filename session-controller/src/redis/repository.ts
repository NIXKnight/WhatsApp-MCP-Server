import { isDeepStrictEqual } from 'node:util';
import { Type } from 'typebox';
import {
  checked, ControllerError, OpaqueIdSchema, StreamIdSchema, FenceSchema, OwnershipSchema, ChatStateSchema,
  SessionBindingSchema, CompletionProofSchema, MutationAttemptSchema, MutationStateSchema,
  CheckpointRecordSchema, PauseReasonSchema, MessageIdSchema, EventFingerprintSchema, ChatJidSchema, fingerprintEvent, systemClock,
  type AdmissionPolicy, type CanonicalChatId, type CompletionIndexPort,
  type WorkRepository, type Fence, type ChatState, type ClaimedWork, type SessionBinding,
  type Ownership, type PauseReason, type CompletionProof, type MutationAttempt, type MutationState,
  type MutationJournalPort, type CheckpointPort, type CheckpointRecord, type ClaimResult,
  type Acceptance, type Clock
} from '../domain.js';
import { parsePayload } from '../intake/payload.js';
import { strings, type RedisConnections } from './client.js';
import { configuredChats, keys, scriptKeys, GROUP } from './keys.js';
import { SCRIPT } from './scripts.js';
const duration = Type.Integer({ minimum: 1, maximum: 3600000 });
const entryId = Type.String({ minLength: 1, maxLength: 128 });
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
export interface RedisRepository extends WorkRepository {
  recoverPending(chat: CanonicalChatId, app: Fence, consumer: string, start: string, count: number): Promise<PelRecovery>;
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
export async function createRepository(connections: RedisConnections, policy: AdmissionPolicy, completions: CompletionIndexPort, clock: Clock = systemClock): Promise<RedisRepository> {
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
  for (const chat of chats) if ((await write(chat, ['init', chat, GROUP]))[0] !== 'ok') throw new ControllerError('store_unavailable');
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
      prepare(work, value: CheckpointRecord) {
        const r = checked(CheckpointRecordSchema, value, 'checkpoint_invalid');
        if (r.phase !== 'prepared' || r.checkpoint.canonicalChatId !== work.event.canonical_chat_jid || r.checkpoint.lastCompletedSequence !== work.event.chat_sequence - 1) return Promise.resolve(false);
        return ownedWrite(work, 'checkpoint-prepare', [JSON.stringify(r)]);
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
      checked(Type.Integer({ minimum: 1, maximum: 128 }), count, 'config_invalid');
      const r = await write(chat, ['recover-pel', app.owner, app.epoch, owner(consumer), start, String(count), GROUP]);
      if (r[0] === 'lost' || r[0] === 'busy' || r[0] === 'paused') return { status: r[0] };
      if (r[0] !== 'ok') throw new ControllerError('store_unavailable');
      const next = checked(StreamIdSchema, r[1], 'store_unavailable');
      const claimed = checked(Type.Integer({ minimum: 0, maximum: 128 }), Number(r[2]), 'store_unavailable');
      const deleted = checked(Type.Integer({ minimum: 0, maximum: 1280 }), Number(r[3]), 'store_unavailable');
      return { status: 'ok', next, claimed, deleted };
    },
    async checkWork(work) {
      const r = await write(work.event.canonical_chat_jid, ['check-work', ...argsFor(work)]);
      if (r[0] !== 'ok') return false;
      const appUntil = checked(Type.Integer({ minimum: 1, maximum: Number.MAX_SAFE_INTEGER }), Number(r[1]), 'store_unavailable');
      const chatUntil = checked(Type.Integer({ minimum: 1, maximum: Number.MAX_SAFE_INTEGER }), Number(r[2]), 'store_unavailable');
      // A delayed durability barrier must not turn an expired reply into authority.
      const time = checked(Type.Integer({ minimum: 0, maximum: Number.MAX_SAFE_INTEGER }), clock.now(), 'store_unavailable');
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
