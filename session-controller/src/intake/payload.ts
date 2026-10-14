import { TextDecoder } from 'node:util';
import { checked, ControllerError, NotificationV2Schema, eventId, canonicalChatId, type AdmissionPolicy, type NotificationV2 } from '../domain.js';
export function validTimestamp(value: string): boolean {
  const m = /^(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2}):(\d{2})(?:\.\d{1,9})?(Z|[+-]\d{2}:\d{2})$/.exec(value);
  if (!m) return false;
  const year = Number(m[1]), month = Number(m[2]), day = Number(m[3]);
  const leap = year % 4 === 0 && (year % 100 !== 0 || year % 400 === 0);
  const days = [31, leap ? 29 : 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31];
  const zone = m[7] as string;
  return year > 0 && month >= 1 && month <= 12 && day >= 1 && day <= (days[month - 1] ?? 0) && Number(m[4]) < 24 && Number(m[5]) < 60 && Number(m[6]) < 60 && (zone === 'Z' || (Number(zone.slice(1, 3)) < 24 && Number(zone.slice(4)) < 60));
}
export function parsePayload(raw: Buffer, policy: AdmissionPolicy): NotificationV2 {
  let value: unknown;
  try { value = JSON.parse(new TextDecoder('utf-8', { fatal: true }).decode(raw)); } catch { throw new ControllerError('payload_invalid'); }
  const p = checked(NotificationV2Schema, value, 'payload_invalid');
  if (!validTimestamp(p.event_timestamp) || !validTimestamp(p.received_at) || Buffer.byteLength(p.message_id, 'utf8') > 512) throw new ControllerError('payload_invalid');
  if (p.chat_kind === 'group') {
    if (p.chat_jid !== p.canonical_chat_jid) throw new ControllerError('payload_invalid');
    if (!policy.groups.includes(p.chat_jid)) throw new ControllerError('policy_denied');
  } else {
    if (!policy.operatorJids.includes(p.chat_jid) || p.canonical_chat_jid !== policy.operatorJids[0]) throw new ControllerError('policy_denied');
    // Own-account operator replies may have the account's non-operator sender.
    if (!p.is_from_me && (!policy.operatorJids.includes(p.sender_jid) || (p.sender_alt_jid !== undefined && !policy.operatorJids.includes(p.sender_alt_jid)))) throw new ControllerError('policy_denied');
  }
  return { ...p, event_id: eventId(p.event_id), canonical_chat_jid: canonicalChatId(p.canonical_chat_jid) };
}
