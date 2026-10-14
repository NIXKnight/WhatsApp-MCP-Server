import { readFile } from 'node:fs/promises';
import { checked, ControllerError, NotificationV2Schema, type NotificationV2, type AdmissionPolicy } from '../domain.js';
export interface PromptPolicy { system: string; instructions(event: NotificationV2): string }
const slots = ['canonical_chat_jid', 'chat_jid', 'sender_jid', 'sender_alt_jid', 'message_id', 'event_id', 'chat_sequence', 'origin'] as const;
/** Operator-authored template only. Message content is fetched through exact MCP,
 * not embedded or mistaken for a prompt template/extension command. */
export function renderPolicy(template: string, event: NotificationV2): string {
  checked(NotificationV2Schema, event, 'payload_invalid');
  if (!template.trim() || /\{\{(?!\s*(?:canonical_chat_jid|chat_jid|sender_jid|sender_alt_jid|message_id|event_id|chat_sequence|origin)\s*\}\})/.test(template)) throw new ControllerError('config_invalid');
  const values: Record<typeof slots[number], string> = {
    canonical_chat_jid: event.canonical_chat_jid, chat_jid: event.chat_jid,
    sender_jid: event.sender_jid, sender_alt_jid: event.sender_alt_jid ?? '',
    message_id: event.message_id, event_id: event.event_id, chat_sequence: String(event.chat_sequence),
    origin: event.is_from_me ? 'own-account-manual' : 'participant'
  };
  // JSON string literals keep identity bytes unambiguous; single-pass substitution.
  return template.replace(/\{\{\s*(\w+)\s*\}\}/g, (_match, name: typeof slots[number]) => JSON.stringify(values[name]));
}
export async function loadPromptPolicy(path: string, admission: AdmissionPolicy): Promise<PromptPolicy> {
  try {
    const text = await readFile(path, 'utf8');
    if (Buffer.byteLength(text) > 65536) throw new ControllerError('config_invalid');
    const separator = '\n<!-- EVENT TEMPLATE -->\n';
    const parts = text.split(separator);
    if (parts.length !== 2 || !parts[0]?.trim() || !parts[1]?.trim()) throw new ControllerError('config_invalid');
    const template = parts[1];
    if (!admission.operatorJids.length || admission.operatorJids.some(jid => !/^\d+@(s\.whatsapp\.net|lid)$/.test(jid))) throw new ControllerError('config_invalid');
    const approval = `Private OPERATOR approval conversation: canonical ${JSON.stringify(admission.operatorJids[0])}; configured private aliases ${JSON.stringify(admission.operatorJids)}. Direct the requester there for approval; never treat a group request as private operator approval.`;
    return { system: parts[0] + '\n' + approval, instructions: event => renderPolicy(template, event) };
  } catch { throw new ControllerError('config_invalid'); }
}
