import type { ExtensionFactory, ToolCallEvent, ToolResultEvent, ToolInfo } from '@earendil-works/pi-coding-agent';
import { randomBytes } from 'node:crypto';
import { checked, ControllerError, ChatJidSchema, MessageIdSchema, MutationAttemptSchema, fingerprintEvent,
  type SessionBinding, type MutationState, type MutationAttempt, type CompletionMarker, type CanonicalChatId, type Clock, systemClock } from '../domain.js';
import type { TurnContext } from '../scheduler/chat-worker.js';
import type { RedisRepository } from '../redis/repository.js';
import { ADAPTER_WHATSAPP_SERVER, MCP_ADAPTER_SOURCE_PATH, WHATSAPP_SERVER } from './mcp-loader.js';
export const MUTATION_TYPE = 'whatsapp.controller.mutation.v1';
const fields = { send_message: 'to', send_group_message: 'jid', send_auto_message: 'jid', send_media: 'to',
  send_reaction: 'chat_jid', edit_message: 'chat_jid', revoke_message: 'chat_jid' } as const;
type Tool = keyof typeof fields;
const toolOf = (name: string): Tool | undefined => Object.keys(fields).find(t => name === `mcp__${WHATSAPP_SERVER}__${t}`) as Tool | undefined;
const fail = () => new ControllerError('mutation_uncertain');
const promptFailureCategory = (error: unknown): string => {
  if (error instanceof ControllerError) return error.code;
  if (error instanceof Error && error.name === 'AbortError') return 'aborted';
  const status = typeof error === 'object' && error !== null && 'status' in error ? Number(error.status) : undefined;
  if (status === 401 || status === 403) return 'authentication';
  if (status === 429) return 'rate_limit';
  if (status === 400 || status === 413) return 'request_rejected';
  if (error instanceof TypeError) return 'transport';
  const message = error instanceof Error ? error.message.toLowerCase() : '';
  if (/hindsight.*(?:401|403|unauthorized|forbidden)|(?:401|403|unauthorized|forbidden).*hindsight/.test(message)) return 'memory_authentication';
  if (/mcp.*(?:oauth|needs.auth|unauthorized|forbidden)|(?:oauth|needs.auth|unauthorized|forbidden).*mcp/.test(message)) return 'mcp_authentication';
  if (/provider.*not configured|api.?key|credential.*provider|(?:401|403|unauthorized|forbidden).*(?:provider|model)/.test(message)) return 'model_authentication';
  if (/approval|permission|authoriz/.test(message)) return 'approval_required';
  if (/no model|unknown model|model.*(?:missing|unavailable|not found)/.test(message)) return 'model_unavailable';
  if (/context|token limit|too many tokens|maximum length|request too large/.test(message)) return 'context_limit';
  if (/tool.*schema|invalid.*tool|function.*schema|tool definition/.test(message)) return 'tool_schema';
  if (/mcp|stdio|server.*connect|protocol.*error/.test(message)) return 'mcp_runtime';
  if (/hindsight|memory|reflect|retain/.test(message)) return 'memory_runtime';
  if (/extension|hook|handler/.test(message)) return 'extension_runtime';
  if (/approval.*required|approval.*denied|permission/.test(message)) return 'approval_required';
  if (/fetch|network|socket|econn|timeout|timed out/.test(message)) return 'transport';
  return 'unknown';
};
const reportPromptFailure = (error: unknown): void => {
  try { console.error(JSON.stringify({ level: 'ERROR', component: 'journal', event: 'prompt_failed', stage: 'wrapped_prompt', category: promptFailureCategory(error) })); } catch {}
};
const object = (v: unknown): Record<string, unknown> | undefined => v !== null && typeof v === 'object' && !Array.isArray(v) ? v as Record<string, unknown> : undefined;
const responseKeys: Record<Tool, readonly ['message_id', 'success']> = {
  send_message: ['message_id', 'success'], send_group_message: ['message_id', 'success'], send_auto_message: ['message_id', 'success'],
  send_media: ['message_id', 'success'], send_reaction: ['message_id', 'success'], edit_message: ['message_id', 'success'], revoke_message: ['message_id', 'success']
};
const exactKeys = (value: Record<string, unknown>, expected: readonly string[]) => {
  const actual = Object.keys(value).sort(); return actual.length === expected.length && actual.every((key, index) => key === expected[index]);
};
const withinDepth = (value: unknown, maximum: number): boolean => {
  const pending: Array<{ value: unknown; depth: number }> = [{ value, depth: 0 }];
  while (pending.length) {
    const next = pending.pop()!; if (!next.value || typeof next.value !== 'object') continue;
    const depth = next.depth + 1; if (depth > maximum) return false;
    for (const child of Array.isArray(next.value) ? next.value : Object.values(next.value)) pending.push({ value: child, depth });
  }
  return true;
};
/** Mirror existing bridge normaliseJID exactly for send/auto/media; no admission,
 * canonical alias substitution, argument mutation or destination authorization. */
export function mutationDestination(tool: Tool, input: Record<string, unknown>): string {
  const value = input[fields[tool]]; if (typeof value !== 'string') throw fail();
  const canNormalize = tool === 'send_message' || tool === 'send_auto_message' || tool === 'send_media';
  const digits = value.replace(/[^0-9]/g, '');
  const actual = canNormalize && !value.includes('@') && digits ? digits + '@s.whatsapp.net' : value;
  return checked(ChatJidSchema, actual, 'mutation_uncertain');
}
/** FastMCP 3.2 wraps every annotated `-> str` result as
 * structuredContent.result. The adapter's bounded direct-tool details retain
 * the untruncated CallToolResult as details.mcpResult for these small mutation
 * responses; model-facing content alone is never confirmation evidence. */
export function confirmedMessage(event: ToolResultEvent, tool: Tool): string | undefined {
  const details = object(event.details), envelope = object(details?.['mcpResult']);
  if (event.isError || details?.['server'] !== ADAPTER_WHATSAPP_SERVER || details['tool'] !== tool ||
      details['error'] !== undefined || details['outputGuard'] !== undefined || !envelope) return undefined;
  const allowedEnvelopeKeys = new Set(['content', 'structuredContent', 'isError', '_meta']);
  if (Object.keys(envelope).some(key => !allowedEnvelopeKeys.has(key)) ||
      (envelope['isError'] !== undefined && envelope['isError'] !== false)) return undefined;
  const content = envelope['content']; if (!Array.isArray(content) || content.length !== 1) return undefined;
  const block = object(content[0]), structured = object(envelope['structuredContent']);
  if (!block || !exactKeys(block, ['text', 'type']) || block['type'] !== 'text' || typeof block['text'] !== 'string' ||
      !structured || !exactKeys(structured, ['result']) || typeof structured['result'] !== 'string' || block['text'] !== structured['result']) return undefined;
  // Model-facing rendering may add blocks independently of the retained raw
  // envelope. Its first block must still be the exact FastMCP result string.
  const visible = event.content;
  if (!Array.isArray(visible) || visible.length < 1) return undefined;
  const visibleBlock = object(visible[0]);
  if (!visibleBlock || !exactKeys(visibleBlock, ['text', 'type']) || visibleBlock['type'] !== 'text' ||
      visibleBlock['text'] !== block['text']) return undefined;
  const raw = structured['result'];
  if (Buffer.byteLength(raw) < 2 || Buffer.byteLength(raw) > 4096) return undefined;
  let parsed: unknown; try { parsed = JSON.parse(raw); } catch { return undefined; }
  const payload = object(parsed);
  // The seven named Go endpoints all emit success/message_id. Locked text-send
  // SendResponse forbids extras; apply an explicit identical contract per tool.
  if (!payload || !withinDepth(payload, 1) || !exactKeys(payload, responseKeys[tool]) || payload['success'] !== true) return undefined;
  let id: string; try { id = checked(MessageIdSchema, payload['message_id'], 'mutation_uncertain'); } catch { return undefined; }
  if (!id.trim() || Buffer.byteLength(id) > 512) return undefined;
  return id;
}
function freeze(value: unknown): void {
  if (!value || typeof value !== 'object' || Object.isFrozen(value)) return;
  for (const child of Object.values(value)) freeze(child); Object.freeze(value);
}
export interface JournalRecord { version: 1; eventFingerprint: string; session: SessionBinding; state: MutationState }
export interface JournalBinding {
  session: SessionBinding;
  /** Fenced manager.appendCustomEntry + descriptor fsync; rejects fixed errors. */
  record(value: JournalRecord): Promise<void>;
}
export interface JournalAttachment {
  extension: ExtensionFactory;
  bind(binding: JournalBinding): void;
  attach(binding: SessionBinding, context: TurnContext): Promise<void>;
  run(binding: SessionBinding, prompt: () => Promise<void>): Promise<void>;
  confirmed(context: TurnContext): Promise<CompletionMarker['confirmedSends']>;
  classifyOwn(chat: CanonicalChatId, actual: string, id: string,
    fallback: () => Promise<'confirmed' | 'manual' | 'uncertain'>): Promise<'confirmed' | 'manual' | 'uncertain'>;
  dispose(binding: SessionBinding): Promise<void>;
}
interface Live { binding: JournalBinding; context?: TurnContext; running: boolean; poisoned: boolean; registered: boolean;
  calls: Map<string, MutationAttempt>; tail: Promise<void> }
export function createToolJournal(ports: Pick<RedisRepository, 'nativeJournal' | 'ownMutation'>,
  options: { clock?: Clock; id?: () => string } = {}): JournalAttachment {
  const repository = { journal: ports.nativeJournal, ownMutation: ports.ownMutation.bind(ports) };
  const live = new Map<string, Live>(), clock = options.clock ?? systemClock;
  const id = options.id ?? (() => randomBytes(16).toString('hex'));
  const get = (binding: SessionBinding): Live => {
    const l = live.get(binding.sessionId);
    if (!l || JSON.stringify(l.binding.session) !== JSON.stringify(binding)) throw fail(); return l;
  };
  const gate = async (l: Live): Promise<TurnContext> => {
    if (!l.context || !l.running || l.poisoned) throw fail(); await l.context.current();
    if (l.poisoned) throw fail(); return l.context;
  };
  const enqueue = <T>(l: Live, action: () => Promise<T>): Promise<T> => {
    const task = l.tail.then(action); l.tail = task.then(() => {}, () => { l.poisoned = true; }); return task;
  };
  const record = async (l: Live, state: MutationState) => {
    const context = await gate(l);
    await l.binding.record({ version: 1, session: l.binding.session, eventFingerprint: fingerprintEvent(context.work().event), state });
    await gate(l);
  };
  const uncertain = async (l: Live, attempt: MutationAttempt) => {
    // If fenced out, leave durable attempting in place: ownMutation treats it
    // UNCERTAIN, and the next app-fenced claim converts it without executing.
    const context = await gate(l);
    if (!await repository.journal.uncertain(context.work(), attempt.attemptId)) throw fail();
    await record(l, { ...attempt, status: 'uncertain', reason: 'mutation_uncertain' }); l.poisoned = true;
  };
  const extension: ExtensionFactory = pi => {
    pi.on('session_start', async (_event, ctx) => {
      const l = live.get(ctx.sessionManager.getSessionId()); if (l) l.registered = true;
    });
    const resolve = (sessionId: string) => { const l = live.get(sessionId); if (!l) throw fail(); return l; };
    const verifySource = (event: ToolCallEvent) => {
      const matching = pi.getAllTools().filter(t => t.name === event.toolName);
      if (matching.length !== 1) throw fail(); const info: ToolInfo = matching[0]!;
      // Direct tools inherit the named programmatic adapter factory's public
      // source metadata. Namespace metadata is optional for eager direct tools.
      if (info.sourceInfo.path !== MCP_ADAPTER_SOURCE_PATH || info.sourceInfo.source !== 'inline') throw fail();
    };
    pi.on('tool_call', async (event, ctx) => {
      const tool = toolOf(event.toolName); if (!tool) return undefined;
      let l: Live | undefined;
      try {
        l = resolve(ctx.sessionManager.getSessionId()); const target = l;
        await enqueue(target, async () => {
          const context = await gate(target); verifySource(event);
          if (target.calls.has(event.toolCallId)) throw fail();
          const destination = mutationDestination(tool, event.input);
          const work = context.work();
          const attempt = checked(MutationAttemptSchema, { version: 1, attemptId: id(), eventId: work.event.event_id,
            canonicalChatId: work.event.canonical_chat_jid, toolCallId: event.toolCallId, tool, destination,
            startedAt: new Date(clock.now()).toISOString(), ownership: work.ownership, status: 'attempting' }, 'mutation_uncertain');
          await context.current();
          // Renew may replace ownership; retain the exact snapshot actually written.
          const exact = { ...attempt, ownership: context.work().ownership };
          if (!await repository.journal.begin(context.work(), exact)) throw fail();
          target.calls.set(event.toolCallId, exact);
          await record(target, exact); await gate(target);
          // Seal the normalized destination's original input, without rewriting it.
          // Later handlers may read it; attempted mutation throws and blocks Pi's
          // before-call pipeline, leaving a conservative unresolved attempt.
          freeze(event.input); Object.defineProperty(event, 'input', { value: event.input, writable: false, configurable: false });
          Object.defineProperty(event, 'toolName', { value: event.toolName, writable: false, configurable: false });
          Object.defineProperty(event, 'toolCallId', { value: event.toolCallId, writable: false, configurable: false });
        });
        return undefined;
      } catch { if (l) l.poisoned = true; ctx.abort(); return { block: true, terminate: true, reason: 'mutation_uncertain' }; }
    });
    pi.on('tool_result', async (event, ctx) => {
      const tool = toolOf(event.toolName); if (!tool) return undefined;
      let l: Live | undefined;
      try {
        l = resolve(ctx.sessionManager.getSessionId()); const target = l;
        await enqueue(target, async () => {
          const context = await gate(target), attempt = target.calls.get(event.toolCallId);
          if (!attempt || attempt.tool !== tool || mutationDestination(tool, event.input) !== attempt.destination) throw fail();
          const messageId = confirmedMessage(event, tool);
          if (!messageId) { await uncertain(target, attempt); throw fail(); }
          const state = { ...attempt, status: 'confirmed' as const, messageId, finishedAt: new Date(clock.now()).toISOString() };
          // Session correlation metadata is durable before Redis confirmation;
          // lost confirm leaves attempting (uncertain), never guessed success.
          await record(target, state); await context.current();
          if (!await repository.journal.confirm(context.work(), attempt.attemptId, messageId)) throw fail();
          await gate(target); target.calls.delete(event.toolCallId);
        });
        return undefined;
      } catch {
        if (l) l.poisoned = true; ctx.abort();
        // Result-hook exceptions are swallowed by SDK runner: return a fixed
        // error AND poison run(), rather than throw and allow completion.
        return { isError: true, content: [{ type: 'text' as const, text: 'mutation_uncertain' }], structuredContent: { content: [], isError: true } };
      }
    });
    pi.on('before_provider_request', async (_event, ctx) => {
      const l = live.get(ctx.sessionManager.getSessionId()); if (l?.poisoned) ctx.abort();
    });
  };
  return {
    extension,
    bind(binding) { if (live.has(binding.session.sessionId)) throw fail(); live.set(binding.session.sessionId, { binding, running: false, poisoned: false, registered: false, calls: new Map(), tail: Promise.resolve() }); },
    async attach(binding, context) {
      const l = get(binding); await context.current();
      if (!l.registered || l.context || l.poisoned || context.work().event.canonical_chat_jid !== binding.canonicalChatId) throw fail(); l.context = context;
    },
    async run(binding, prompt) {
      const l = get(binding); if (!l.context || l.running || l.poisoned) throw fail();
      l.running = true;
      let promptFailure: unknown, promptFailed = false, classifiedPromptFailure = false;
      try {
        await gate(l);
        const event = l.context.work().event;
        const prior = await repository.journal.list(event.canonical_chat_jid);
        // Any durable record for this event predates this run and blocks replay,
        // including a confirmed send whose prompt later failed.
        if (prior.some(a => a.eventId === event.event_id)) throw fail();
        try { await prompt(); } catch (error) { promptFailed = true; promptFailure = error; }
        await l.tail;
        const after = await repository.journal.list(event.canonical_chat_jid);
        const current = after.filter(a => a.eventId === event.event_id);
        const active = l.calls.size > 0 || current.some(a => a.status === 'attempting' || a.status === 'uncertain');
        if (active) {
          // A call may have reached the external service. Convert one live
          // attempt when still fenced; all remaining attempting evidence is
          // already conservatively unresolved and prevents replay.
          for (const a of l.calls.values()) { await uncertain(l, a); break; }
          throw fail();
        }
        if (promptFailed) {
          classifiedPromptFailure = true;
          reportPromptFailure(promptFailure);
          // A confirmed send is retained, but failure after that side effect is
          // a session failure rather than an unknown mutation outcome.
          if (current.some(a => a.status === 'confirmed')) throw new ControllerError('session_failed');
          if (promptFailure instanceof ControllerError) throw promptFailure;
          throw new ControllerError('session_failed');
        }
        await gate(l);
        if (after.some(a => a.eventId !== event.event_id || a.status !== 'confirmed')) throw fail();
      } catch (error) {
        l.poisoned = true;
        if (classifiedPromptFailure && error instanceof ControllerError) throw error;
        throw fail();
      } finally { await l.tail; l.running = false; }
    },
    async confirmed(context) {
      await context.current();
      const records = await repository.journal.list(context.work().event.canonical_chat_jid);
      if (records.some(a => a.eventId !== context.work().event.event_id || a.status !== 'confirmed')) throw fail();
      return [...new Map(records.map(a => {
        if (a.status !== 'confirmed') throw fail(); const value = { destination: a.destination, messageId: a.messageId };
        return [JSON.stringify(value), value] as const;
      })).values()];
    },
    async classifyOwn(_chat, actual, messageId, fallback) {
      const first = await repository.ownMutation(actual, messageId); if (first === 'uncertain') return first;
      const indexed = await fallback(); if (indexed === 'uncertain') return indexed;
      const last = await repository.ownMutation(actual, messageId); if (last === 'uncertain') return last;
      if (last === 'confirmed' || indexed === 'confirmed') return 'confirmed';
      if (first === 'confirmed') {
        const settled = await fallback(); return settled === 'confirmed' ? 'confirmed' : 'uncertain';
      }
      return indexed;
    },
    async dispose(binding) { const l = get(binding); await l.tail; if (l.running) throw fail(); live.delete(binding.sessionId); }
  };
}
