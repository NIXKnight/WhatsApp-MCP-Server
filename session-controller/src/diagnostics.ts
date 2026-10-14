import { createHash } from 'node:crypto';
import Value from 'typebox/value';
import { DiagnosticClassSchema, OpaqueIdSchema, ChatJidSchema, systemClock, type Clock, type DiagnosticPort } from './domain.js';
export function chatReference(chat: string): string {
  if (!Value.Check(ChatJidSchema, chat)) return 'invalid';
  return createHash('sha256').update('sc:v1:chat:').update(chat, 'utf8').digest('hex');
}
export function createDiagnostics(write: (line: string) => void, clock: Clock = systemClock): DiagnosticPort {
  return { emit(input) {
    if (!Value.Check(DiagnosticClassSchema, input.class)) return;
    const now = clock.now();
    const record: Record<string, string | number> = { class: input.class, timestamp: new Date(Number.isFinite(now) && Math.abs(now) <= 8640000000000000 ? now : 0).toISOString() };
    if (Value.Check(OpaqueIdSchema, input.eventId)) record['event_id'] = input.eventId as string;
    if (Value.Check(ChatJidSchema, input.chat)) record['chat_ref'] = chatReference(input.chat as string);
    if (input.count !== undefined && Number.isSafeInteger(input.count) && input.count >= 0) record['count'] = Math.min(input.count, 1000000000);
    if (input.httpStatus !== undefined && Number.isInteger(input.httpStatus) && input.httpStatus >= 100 && input.httpStatus <= 599) record['http_status'] = input.httpStatus;
    try { write(JSON.stringify(record) + '\n'); } catch { /* Diagnostics cannot change work disposition. */ }
  } };
}
