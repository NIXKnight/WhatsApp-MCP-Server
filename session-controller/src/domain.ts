import { createHash } from 'node:crypto';
import { Type, type Static, type TSchema, type TLiteral } from 'typebox';
import Value from 'typebox/value';

const closed = { additionalProperties: false } as const;
const literalSet = <const T extends [string, ...string[]]>(...values: T) => Type.Union(values.map(v => Type.Literal(v)) as { [K in keyof T]: TLiteral<T[K]> });
export const OpaqueIdSchema = Type.String({ pattern: '^[0-9a-f]{32}$' });
export const GroupJidSchema = Type.String({ pattern: '^\\d+@g\\.us$', maxLength: 128 });
export const PrivateJidSchema = Type.String({ pattern: '^\\d+@(s\\.whatsapp\\.net|lid)$', maxLength: 128 });
export const ParticipantJidSchema = Type.String({ pattern: '^\\d+@(s\\.whatsapp\\.net|lid|bot|hosted|hosted\\.lid)$', maxLength: 128 });
export const ChatJidSchema = Type.Union([GroupJidSchema, PrivateJidSchema]);
export const SequenceSchema = Type.Integer({ minimum: 1, maximum: Number.MAX_SAFE_INTEGER });
export const CounterSchema = Type.Integer({ minimum: 0, maximum: Number.MAX_SAFE_INTEGER });
export const MessageIdSchema = Type.String({ minLength: 1, maxLength: 512 });
export const TimestampSchema = Type.String({ pattern: '^\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}(\\.\\d{1,9})?(Z|[+-]\\d{2}:\\d{2})$', maxLength: 40 });
export const VersionSchema = Type.String({ pattern: '^[A-Za-z0-9._-]{1,64}$' });
export const StreamIdSchema = Type.String({ pattern: '^(0|[1-9]\\d*)-(0|[1-9]\\d*)$', maxLength: 64 });
export const ErrorClassSchema = literalSet('config_invalid', 'signature_invalid', 'payload_invalid', 'policy_denied', 'body_too_large', 'body_invalid', 'request_timeout', 'store_unavailable', 'identity_conflict', 'not_ready', 'intake_closed', 'internal_failed', 'listen_failed', 'lease_lost', 'sequence_gap', 'session_failed', 'mutation_uncertain', 'usage_unknown', 'preparation_failed', 'checkpoint_invalid', 'compaction_failed', 'restore_failed');
export type ErrorClass = Static<typeof ErrorClassSchema>;
export class ControllerError extends Error {
  constructor(readonly code: ErrorClass) { super(code); this.name = 'ControllerError'; }
}
export function checked<S extends TSchema>(schema: S, value: unknown, code: ErrorClass): Static<S> {
  if (!Value.Check(schema, value)) throw new ControllerError(code);
  return value as Static<S>;
}

declare const brand: unique symbol;
export type EventId = string & { readonly [brand]: 'EventId' };
export type CanonicalChatId = string & { readonly [brand]: 'CanonicalChatId' };
export const eventId = (v: unknown): EventId => checked(OpaqueIdSchema, v, 'payload_invalid') as EventId;
export const canonicalChatId = (v: unknown): CanonicalChatId => checked(ChatJidSchema, v, 'payload_invalid') as CanonicalChatId;

const eventFields = {
  schema_version: Type.Literal(2), event_type: Type.Literal('whatsapp.message.received'),
  event_id: OpaqueIdSchema, message_id: MessageIdSchema,
  event_timestamp: TimestampSchema, received_at: TimestampSchema,
  chat_sequence: SequenceSchema, sender_jid: ParticipantJidSchema,
  sender_alt_jid: Type.Optional(ParticipantJidSchema), is_from_me: Type.Boolean()
};
export const NotificationV2Schema = Type.Union([
  Type.Object({ ...eventFields, chat_kind: Type.Literal('group'), chat_jid: GroupJidSchema, canonical_chat_jid: GroupJidSchema }, closed),
  Type.Object({ ...eventFields, chat_kind: Type.Literal('operator_private'), chat_jid: PrivateJidSchema, canonical_chat_jid: PrivateJidSchema }, closed)
]);
export type NotificationWire = Static<typeof NotificationV2Schema>;
export type NotificationV2 = NotificationWire & { event_id: EventId; canonical_chat_jid: CanonicalChatId };
export const EventFingerprintSchema = Type.String({ pattern: '^sha256:v1:[0-9a-f]{64}$', minLength: 74, maxLength: 74 });
export type EventFingerprint = Static<typeof EventFingerprintSchema>;
/** Immutable v2 metadata only: fixed field order, exact timestamp/alias strings,
 * explicit null for absent sender_alt_jid, and domain-separated SHA-256.
 * JSON key order/whitespace do not matter; no raw event/content is persisted.
 * This is comparison evidence, not authentication or admission. Adapters must
 * use parsePayload/admission first; new wire fields require a new hash version.
 */
export function fingerprintEvent(event: NotificationWire): EventFingerprint {
  const p = checked(NotificationV2Schema, event, 'payload_invalid');
  const canonical = JSON.stringify([
    p.schema_version, p.event_type, p.event_id, p.message_id,
    p.event_timestamp, p.received_at, p.chat_sequence, p.sender_jid,
    p.sender_alt_jid ?? null, p.is_from_me, p.chat_kind,
    p.chat_jid, p.canonical_chat_jid
  ]);
  return 'sha256:v1:' + createHash('sha256').update('sc:immutable-event:v1\0', 'utf8').update(canonical, 'utf8').digest('hex');
}
export interface AdmissionPolicy { readonly groups: readonly string[]; readonly operatorJids: readonly string[] }
export interface Clock { now(): number }
export interface IdSource { next(): string }
export interface WaitPort { wait(ms: number, signal: AbortSignal): Promise<void> }
export const systemClock: Clock = { now: () => Date.now() };

export const AcceptanceSchema = Type.Object({ status: literalSet('accepted', 'duplicate', 'conflict', 'unavailable') }, closed);
export type Acceptance = Static<typeof AcceptanceSchema>;
export interface IntakeStore {
  /** accepted/duplicate require atomic admission/dedupe AND WAITAOF >= [1,0]
   * on the same, non-replayed connection. Completed dedupe consults durable Pi
   * markers, comparing fingerprintEvent(event) with marker.eventFingerprint
   * even after Redis event deletion. Missing/malformed durable evidence fails
   * closed, never duplicate. Conflicting identity/sequence reuse or ANY changed
   * v2 field never returns duplicate. Slice 4 implements these dedupe checks.
   * A lost response may be retried with the same immutable event identity.
   */
  accept(event: NotificationV2): Promise<Acceptance>;
}
export interface ReadinessPort { isReady(): boolean }
export interface IntakeLifecycle { stopIntake(): void; close(): Promise<void> }
export interface DrainPort { stopScheduling(): void; drain(): Promise<void> }

export const PauseReasonSchema = literalSet('lease_lost', 'sequence_gap', 'session_failed', 'mutation_uncertain', 'usage_unknown', 'preparation_failed', 'checkpoint_invalid', 'compaction_failed', 'restore_failed');
export type PauseReason = Static<typeof PauseReasonSchema>;
// Epochs are decimal strings: Redis counters must not round through JS/Lua floats.
export const FenceSchema = Type.Object({ owner: OpaqueIdSchema, epoch: Type.String({ pattern: '^[1-9]\\d*$', maxLength: 32 }), expiresAtMs: CounterSchema }, closed);
export type Fence = Static<typeof FenceSchema>;
export const OwnershipSchema = Type.Object({ app: FenceSchema, chat: FenceSchema }, closed);
export type Ownership = Static<typeof OwnershipSchema>;
export const SessionBindingSchema = Type.Object({ canonicalChatId: ChatJidSchema, sessionId: Type.String({ minLength: 1, maxLength: 128 }), sessionFile: Type.String({ minLength: 1, maxLength: 4096 }) }, closed);
export type SessionBinding = Static<typeof SessionBindingSchema>;
export const ChatStateSchema = Type.Object({ canonicalChatId: ChatJidSchema, lastCompletedSequence: CounterSchema, session: Type.Optional(SessionBindingSchema), pause: Type.Optional(PauseReasonSchema), gapSinceMs: Type.Optional(CounterSchema) }, closed);
export type ChatState = Static<typeof ChatStateSchema>;
const workFields = { streamId: StreamIdSchema, event: NotificationV2Schema };
export const WorkStateSchema = Type.Union([
  Type.Object({ ...workFields, phase: Type.Literal('waiting') }, closed),
  Type.Object({ ...workFields, phase: literalSet('leased', 'compacting'), ownership: OwnershipSchema }, closed),
  Type.Object({ ...workFields, phase: Type.Literal('paused'), reason: PauseReasonSchema }, closed),
  Type.Object({ ...workFields, phase: Type.Literal('uncertain'), reason: Type.Literal('mutation_uncertain') }, closed)
]);
export type WorkState = Static<typeof WorkStateSchema>;
export interface ClaimedWork { readonly streamId: string; readonly event: NotificationV2; readonly ownership: Ownership }
export type ClaimResult = { status: 'claimed'; work: ClaimedWork } | { status: 'empty' | 'waiting' | 'paused' | 'lost' };

export const CompletionMarkerSchema = Type.Object({ version: Type.Literal(1), eventId: OpaqueIdSchema, eventFingerprint: EventFingerprintSchema, canonicalChatId: ChatJidSchema, chatSequence: SequenceSchema, completion: literalSet('turn', 'context_only'), completedAt: TimestampSchema, confirmedSends: Type.Array(Type.Object({ destination: ChatJidSchema, messageId: MessageIdSchema }, closed), { uniqueItems: true }) }, closed);
export type CompletionMarker = Static<typeof CompletionMarkerSchema>;
export const CompletionProofSchema = Type.Object({ marker: CompletionMarkerSchema, entryId: Type.String({ minLength: 1, maxLength: 128 }), session: SessionBindingSchema }, closed);
export type CompletionProof = Static<typeof CompletionProofSchema>;
export interface CompletionIndexPort {
  /** Return strict durable evidence, including the original immutable-event
   * fingerprint, never reconstruct it from a replay. Invalid legacy markers
   * cannot prove completion; report session_failed rather than cache-miss.
   */
  findEvent(chat: CanonicalChatId, id: EventId): Promise<CompletionProof | undefined>;
  /** Rebuild the destination+message-ID index from all configured session
   * markers, not only the originating session. Operator aliases canonicalize
   * for routing, but actual destination remains part of correlation identity.
   * An unresolved journal attempt for that destination takes precedence over
   * guessing manual origin. This does not authorize or rewrite destinations.
   */
  classifyOwn(chat: CanonicalChatId, actualChat: string, messageId: string): Promise<'confirmed' | 'manual' | 'uncertain'>;
}

export const MutationToolSchema = literalSet('send_message', 'send_group_message', 'send_auto_message', 'send_media', 'send_reaction', 'edit_message', 'revoke_message');
const mutationFields = { version: Type.Literal(1), attemptId: OpaqueIdSchema, eventId: OpaqueIdSchema, canonicalChatId: ChatJidSchema, toolCallId: Type.String({ minLength: 1, maxLength: 512 }), tool: MutationToolSchema, destination: ChatJidSchema, startedAt: TimestampSchema, ownership: OwnershipSchema };
export const MutationAttemptSchema = Type.Object({ ...mutationFields, status: Type.Literal('attempting') }, closed);
export type MutationAttempt = Static<typeof MutationAttemptSchema>;
export const MutationStateSchema = Type.Union([
  MutationAttemptSchema,
  Type.Object({ ...mutationFields, status: Type.Literal('confirmed'), messageId: MessageIdSchema, finishedAt: TimestampSchema }, closed),
  Type.Object({ ...mutationFields, status: Type.Literal('uncertain'), reason: Type.Literal('mutation_uncertain') }, closed)
]);
export type MutationState = Static<typeof MutationStateSchema>;
export interface MutationJournalPort {
  /** Persist/fsync before calling the tool; false blocks execution. No arguments/results stored. */
  begin(work: ClaimedWork, attempt: MutationAttempt): Promise<boolean>;
  confirm(work: ClaimedWork, attemptId: string, messageId: string): Promise<boolean>;
  uncertain(work: ClaimedWork, attemptId: string): Promise<boolean>;
  list(chat: CanonicalChatId): Promise<readonly MutationState[]>;
}

const note = Type.String({ minLength: 1, maxLength: 2048 });
const notes = () => Type.Array(note, { maxItems: 64 });
export const CheckpointV1Schema = Type.Object({
  version: Type.Literal(1), canonicalChatId: ChatJidSchema, lastCompletedSequence: CounterSchema,
  policyVersion: VersionSchema, templateVersion: VersionSchema, createdAt: TimestampSchema,
  goals: notes(), decisions: notes(), constraints: notes(), verifiedFacts: notes(),
  pendingWork: notes(), approvalBoundaries: notes(), nextSteps: notes(),
  confirmedToolOutcomes: Type.Array(Type.Object({ attemptId: OpaqueIdSchema, tool: MutationToolSchema, destination: ChatJidSchema, messageId: MessageIdSchema }, closed), { maxItems: 256 })
}, closed);
export type CheckpointV1 = Static<typeof CheckpointV1Schema>;
const checkpointRecordFields = { checkpointId: OpaqueIdSchema, checkpoint: CheckpointV1Schema };
export const CheckpointRecordSchema = Type.Union([
  Type.Object({ ...checkpointRecordFields, phase: Type.Literal('prepared') }, closed),
  Type.Object({ ...checkpointRecordFields, phase: literalSet('compacted', 'restored'), compactionEntryId: Type.String({ minLength: 1, maxLength: 128 }) }, closed)
]);
export type CheckpointRecord = Static<typeof CheckpointRecordSchema>;
export interface CheckpointPort {
  /** Every write is fenced and durable; Slice 8 adds identity/version/secret/size validation. */
  prepare(work: ClaimedWork, record: CheckpointRecord): Promise<boolean>;
  transition(work: ClaimedWork, checkpointId: string, phase: 'compacted' | 'restored', compactionEntryId: string): Promise<boolean>;
  latest(chat: CanonicalChatId): Promise<CheckpointRecord | undefined>;
}
export interface WorkRepository extends IntakeStore {
  acquireApp(owner: string, leaseMs: number): Promise<Fence | undefined>;
  renewApp(fence: Fence, leaseMs: number): Promise<Fence | undefined>;
  releaseApp(fence: Fence): Promise<boolean>;
  readChat(chat: CanonicalChatId): Promise<ChatState>;
  bindSession(work: ClaimedWork, binding: SessionBinding): Promise<boolean>;
  /** Claim only lastCompletedSequence+1, recovering PEL before new reads.
   * A gap waits, then persists a chat-local pause; no sequence is skipped.
   */
  claimNext(chat: CanonicalChatId, app: Fence, owner: string, leaseMs: number, gapTimeoutMs: number): Promise<ClaimResult>;
  renew(work: ClaimedWork, leaseMs: number): Promise<Ownership | undefined>;
  pause(work: ClaimedWork, reason: PauseReason): Promise<boolean>;
  /** Only after a validated, fsynced non-context Pi marker whose identity,
   * sequence and eventFingerprint match work.event via fingerprintEvent:
   * advance the chat cursor, XACK/XDEL, remove event/tool state. Keep mapping/
   * sequence/checkpoint. All mutations check both current, unexpired app AND
   * chat fences; mismatched proof cannot complete work.
   */
  complete(work: ClaimedWork, proof: CompletionProof): Promise<boolean>;
  close(): Promise<void>;
}
export interface UsageSnapshot { readonly tokens: number | null; readonly contextWindow: number; readonly percent: number | null }
export interface PiSessionPort {
  readonly binding: SessionBinding;
  /** True only while the active branch has no conversational, compaction,
   * completion, or mutation entries and this host has not started a prompt.
   * The controller binding entry itself does not make a session non-fresh. */
  isFresh(): boolean;
  /** Resolve only after agent_settled, including extension-triggered follow-ups. */
  prompt(event: NotificationV2, instructions: string): Promise<void>;
  usage(): UsageSnapshot | undefined;
  /** Disable automatic compaction; invoke manually only on a settled session. */
  compact(instructions: string, checkpoint: CheckpointV1): Promise<{ entryId: string; checkpoint: CheckpointV1 }>;
  /** Append strict custom entry with fingerprintEvent(original accepted event),
   * flush/fsync session file before returning proof. Retain only its digest,
   * not a raw event/content copy. Slice 6 supplies this durable adapter.
   */
  appendCompletion(marker: CompletionMarker): Promise<CompletionProof>;
  dispose(): Promise<void>;
}
export interface PiHostPort extends CompletionIndexPort {
  /** Exact mapped file only, never continueRecent(); validate chat identity.
   * A newly created binding must be fenced/durable before the first prompt.
   * Resource loading retains all operator-configured Pi capabilities and adds
   * the existing WhatsApp stdio MCP server, with extensions explicitly bound.
   */
  open(work: ClaimedWork, binding?: SessionBinding): Promise<PiSessionPort>;
  close(): Promise<void>;
}
export const DiagnosticClassSchema = Type.Union([ErrorClassSchema, literalSet('intake_accepted', 'intake_duplicate', 'ready', 'stopped')]);
export type DiagnosticClass = Static<typeof DiagnosticClassSchema>;
export interface DiagnosticInput { readonly class: DiagnosticClass; readonly eventId?: EventId; readonly chat?: CanonicalChatId; readonly count?: number; readonly httpStatus?: number }
export interface DiagnosticPort { emit(input: DiagnosticInput): void }
