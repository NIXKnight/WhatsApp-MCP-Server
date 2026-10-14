import { createHash } from 'node:crypto';
import { canonicalChatId, ControllerError, type AdmissionPolicy, type CanonicalChatId } from '../domain.js';
export const GROUP = 'controller-v1';
export const APP = 'sc:v1:app';
export const GLOBAL_EVENTS = 'sc:v1:active-identities';
export interface ChatKeys { state: string; lease: string; stream: string; events: string; sequences: string; tools: string; checkpoint: string }
export function configuredChats(policy: AdmissionPolicy): readonly CanonicalChatId[] {
  const first = policy.operatorJids[0];
  if (first === undefined) throw new ControllerError('config_invalid');
  return [...new Set([...policy.groups, first])].map(canonicalChatId);
}
export function keys(chat: CanonicalChatId): ChatKeys {
  const prefix = 'sc:v1:chat:' + createHash('sha256').update('sc:keys:v1\0').update(canonicalChatId(chat)).digest('hex');
  return { state: prefix + ':state', lease: prefix + ':lease', stream: prefix + ':stream', events: prefix + ':events', sequences: prefix + ':sequences', tools: prefix + ':tools', checkpoint: prefix + ':checkpoint' };
}
/** Fixed positional keys used by every Lua script; no user-supplied key names. */
export function scriptKeys(chat: CanonicalChatId): string[] {
  const k = keys(chat);
  return [APP, k.state, k.lease, k.stream, k.events, k.sequences, k.tools, k.checkpoint, GLOBAL_EVENTS];
}
