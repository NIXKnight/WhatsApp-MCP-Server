import { once } from 'node:events';
import { describe, it, expect, vi } from 'vitest';
import type { ExtensionAPI, ExtensionContext, CustomToolCallEvent, ToolResultEvent, ToolInfo } from '@earendil-works/pi-coding-agent';
import { canonicalChatId, eventId, fingerprintEvent, ControllerError, type ClaimedWork, type MutationState, type MutationAttempt, type SessionBinding, type PiSessionPort, type CompletionMarker } from '../../src/domain.js';
import { createToolJournal, confirmedMessage, mutationDestination, type JournalRecord } from '../../src/pi/tool-journal-extension.js';
import { ADAPTER_WHATSAPP_SERVER, MCP_ADAPTER_SOURCE_PATH, WHATSAPP_SERVER } from '../../src/pi/mcp-loader.js';
import { runChatWorker, type TurnContext, type WorkerOptions, type WorkerRepository } from '../../src/scheduler/chat-worker.js';
import { createRepository } from '../../src/redis/repository.js';
import type { RedisConnections } from '../../src/redis/client.js';
const chat = canonicalChatId('100@g.us'), other = canonicalChatId('200@g.us');
const binding: SessionBinding = { canonicalChatId: chat, sessionId: 'SYNTHETIC_SESSION', sessionFile: '/tmp/SYNTHETIC.jsonl' };
const fence = { owner: 'a'.repeat(32), epoch: '1', expiresAtMs: 2000000000000 };
const work: ClaimedWork = { streamId: '1-0', ownership: { app: fence, chat: { ...fence, owner: 'b'.repeat(32) } },
  event: { schema_version: 2, event_type: 'whatsapp.message.received', event_id: eventId('1'.repeat(32)), message_id: 'SYNTHETIC_INPUT',
    event_timestamp: '2033-05-18T03:33:20Z', received_at: '2033-05-18T03:33:21Z', chat_sequence: 1, chat_kind: 'group',
    chat_jid: chat, canonical_chat_jid: chat, sender_jid: '300@lid', is_from_me: false } };
const context: TurnContext = { work: () => work, current: async () => {} };
function gate() { let resolve!: () => void; const promise = new Promise<void>(r => { resolve = r; }); return { promise, resolve }; }
function call(tool = 'send_message', id = 'SYNTHETIC_CALL', input: Record<string, unknown> = { to: other, text: 'SYNTHETIC_BODY' }): CustomToolCallEvent {
  return { type: 'tool_call', toolName: `mcp__${WHATSAPP_SERVER}__${tool}`, toolCallId: id, input, parentToolCallId: 'SYNTHETIC_CODEMODE' };
}
/** Generated from FastMCP 3.2 Tool.convert_result for annotated `-> str`.
 * The adapter preserves this small raw CallToolResult in bounded details. */
function result(e = call(), payload: unknown = { success: true, message_id: 'SYNTHETIC_SENT' }): ToolResultEvent {
  const raw = JSON.stringify(payload); if (raw === undefined) throw new Error('synthetic_fixture_invalid');
  const fastMcp = { content: [{ type: 'text' as const, text: raw }], structuredContent: { result: raw },
    _meta: { fastmcp: { wrap_result: true } } };
  const { _meta: _removedByPi, ...piEnvelope } = fastMcp;
  return { type: 'tool_result', toolName: e.toolName, toolCallId: e.toolCallId, parentToolCallId: e.parentToolCallId!, input: e.input,
    content: fastMcp.content, details: { server: ADAPTER_WHATSAPP_SERVER, tool: e.toolName.split('__').at(-1), mcpResult: fastMcp }, isError: false,
    structuredContent: piEnvelope };
}
function errorResult(e = call()): ToolResultEvent {
  const callToolResult = { content: [{ type: 'text' as const, text: 'SYNTHETIC_FIXED_ERROR' }], isError: true };
  return { type: 'tool_result', toolName: e.toolName, toolCallId: e.toolCallId, parentToolCallId: e.parentToolCallId!, input: e.input,
    content: callToolResult.content, details: { error: 'tool_error', server: ADAPTER_WHATSAPP_SERVER, mcpResult: callToolResult }, isError: true,
    structuredContent: callToolResult };
}
async function fixture() {
  const states = new Map<string, MutationState>(), records: JournalRecord[] = [], effects: string[] = [];
  const handlers: Record<string, (e: any, ctx: any) => Promise<any>> = {};
  let serial = 0, own: 'confirmed' | 'uncertain' | 'none' = 'none';
  const nativeJournal = {
    begin: vi.fn(async (_w: ClaimedWork, a: MutationAttempt) => { effects.push('begin'); states.set(a.attemptId, a); return true; }),
    confirm: vi.fn(async (_w: ClaimedWork, id: string, messageId: string) => { effects.push('confirm'); const a = states.get(id)!; if (a.status !== 'attempting') return false;
      states.set(id, { ...a, status: 'confirmed', messageId, finishedAt: '2033-05-18T03:33:22Z' }); return true; }),
    uncertain: vi.fn(async (_w: ClaimedWork, id: string) => { effects.push('uncertain'); const a = states.get(id)!;
      states.set(id, { ...a, status: 'uncertain', reason: 'mutation_uncertain' }); return true; }),
    list: vi.fn(async () => [...states.values()])
  };
  const ownMutation = vi.fn(async (): Promise<typeof own> => own);
  const journal = createToolJournal({ nativeJournal, ownMutation }, { clock: { now: () => 2000000000000 }, id: () => (++serial).toString(16).padStart(32, '0') });
  const info: ToolInfo = { name: call().toolName, description: '', parameters: { type: 'object' }, exposure: 'direct',
    sourceInfo: { path: MCP_ADAPTER_SOURCE_PATH, source: 'inline', scope: 'temporary', origin: 'top-level' } };
  const pi = { on: (type: string, handler: any) => { handlers[type] = handler; return () => {}; }, getAllTools: () => [info] } as unknown as ExtensionAPI;
  await journal.extension(pi);
  const ctx = { sessionManager: { getSessionId: () => binding.sessionId }, abort: vi.fn() } as unknown as ExtensionContext;
  const record = vi.fn(async (r: JournalRecord) => { effects.push(r.state.status + ':fsync'); records.push(structuredClone(r)); });
  journal.bind({ session: binding, record }); await handlers['session_start']!({ type: 'session_start' }, ctx); await journal.attach(binding, context);
  return { journal, ctx, nativeJournal, ownMutation, states, records, effects, record, info, handlers,
    setOwn: (v: typeof own) => { own = v; }, invokeCall: (e = call()) => handlers['tool_call']!(e, ctx), invokeResult: (e = result()) => handlers['tool_result']!(e, ctx) };
}
describe('native-shaped durable mutation hooks', () => {
  it('awaits attempting plus fsync before execution, and confirmed fsync before Redis confirmation', async () => {
    const f = await fixture(), before = gate(); f.nativeJournal.begin.mockImplementationOnce(async (_w, a) => { await before.promise; f.states.set(a.attemptId, a); f.effects.push('begin'); return true; });
    const e = call(); let executed = false;
    const operation = f.journal.run(binding, async () => { expect(await f.invokeCall(e)).toBeUndefined(); executed = true; f.effects.push('execute'); await f.invokeResult(result(e)); });
    await new Promise(r => setTimeout(r, 0)); expect(executed).toBe(false); before.resolve(); await operation;
    expect(f.effects).toEqual(['begin', 'attempting:fsync', 'execute', 'confirmed:fsync', 'confirm']);
    expect(f.records[0]?.state.toolCallId).toBe(e.toolCallId);
    expect(f.records[0]?.eventFingerprint).toBe(fingerprintEvent(work.event)); expect(f.records[0]?.session).toEqual(binding);
    expect(JSON.stringify(f.records)).not.toContain('SYNTHETIC_BODY');
    expect(await f.journal.confirmed(context)).toEqual([{ destination: other, messageId: 'SYNTHETIC_SENT' }]);
  });
  it('seals inputs without rewriting destination or imposing allowed-chat policy', async () => {
    const f = await fixture(), e = call('send_message', 'SYNTHETIC_CALL', { to: '+9 99', text: 'SYNTHETIC_BODY', mentions: [] });
    await f.journal.run(binding, async () => { await f.invokeCall(e); expect(e.input['to']).toBe('+9 99'); expect(Object.isFrozen(e.input)).toBe(true);
      expect(() => { e.input['to'] = chat; }).toThrow(); await f.invokeResult(result(e)); });
    expect(f.records[0]?.state.destination).toBe('999@s.whatsapp.net');
  });
  it('refuses attachment when its factory was not installed before SDK session_start', async () => {
    const f = await fixture(); const missing = createToolJournal({ nativeJournal: f.nativeJournal, ownMutation: async () => 'none' });
    missing.bind({ session: binding, record: f.record });
    await expect(missing.attach(binding, context)).rejects.toThrow('mutation_uncertain');
    expect(f.nativeJournal.begin).not.toHaveBeenCalled();
  });
  it('requires active attached running work before journal or tool execution', async () => {
    const f = await fixture(); expect((await f.invokeCall()).block).toBe(true); expect(f.nativeJournal.begin).not.toHaveBeenCalled();
  });
  it('blocks metadata shadowing and retains configured unrelated capabilities', async () => {
    const f = await fixture(); f.info.sourceInfo.path = '<inline:shadow>';
    expect(await f.invokeCall({ ...call(), toolName: 'unrelated_configured_tool' })).toBeUndefined();
    await expect(f.journal.run(binding, async () => { expect((await f.invokeCall()).block).toBe(true); })).rejects.toThrow('mutation_uncertain');
    expect(f.nativeJournal.begin).not.toHaveBeenCalled();
  });
  it('does not invoke execution when the durable begin barrier fails', async () => {
    const f = await fixture(); f.nativeJournal.begin.mockRejectedValueOnce(new Error('SYNTHETIC_FAILURE'));
    await expect(f.journal.run(binding, async () => { expect((await f.invokeCall()).block).toBe(true); })).rejects.toThrow('mutation_uncertain');
    expect(f.record).not.toHaveBeenCalled(); expect(f.ctx.abort).toHaveBeenCalled();
  });
  it('awaits result fsync; a failed barrier cannot confirm or allow completion despite SDK swallowing errors', async () => {
    const f = await fixture(); f.record.mockImplementationOnce(async () => {}).mockRejectedValueOnce(new Error('SYNTHETIC_FAILURE'));
    await expect(f.journal.run(binding, async () => { const e = call(); await f.invokeCall(e); expect((await f.invokeResult(result(e))).isError).toBe(true); })).rejects.toThrow('mutation_uncertain');
    expect(f.nativeJournal.confirm).not.toHaveBeenCalled(); expect([...f.states.values()][0]?.status).toBe('attempting');
    await expect(f.journal.run(binding, async () => {})).rejects.toThrow('mutation_uncertain');
  });
  it('production-equivalent FastMCP/Pi errors may have sent; they are uncertain and never retried', async () => {
    const f = await fixture(); await expect(f.journal.run(binding, async () => { const e = call(); await f.invokeCall(e);
      await f.invokeResult(errorResult(e)); })).rejects.toThrow('mutation_uncertain');
    expect(f.nativeJournal.confirm).not.toHaveBeenCalled(); expect([...f.states.values()][0]?.status).toBe('uncertain');
    expect(f.nativeJournal.begin).toHaveBeenCalledTimes(1);
    await expect(f.journal.run(binding, async () => { throw new Error('must_not_retry'); })).rejects.toThrow('mutation_uncertain');
    expect(f.nativeJournal.begin).toHaveBeenCalledTimes(1);
  });
  it('incomplete result delivery is uncertain and has no automatic replay', async () => {
    const f = await fixture(); await expect(f.journal.run(binding, async () => { await f.invokeCall(); })).rejects.toThrow('mutation_uncertain');
    expect([...f.states.values()][0]?.status).toBe('uncertain'); expect(f.nativeJournal.begin).toHaveBeenCalledTimes(1);
  });
  it('fence loss after attempting leaves effective uncertainty, no stale confirmation', async () => {
    const f = await fixture(); let lost = false;
    // The fixture attached the stable context; replace its current function for this test only.
    const prior = context.current; context.current = async () => { if (lost) throw new Error('lease_lost'); };
    try { await expect(f.journal.run(binding, async () => { const e = call(); await f.invokeCall(e); lost = true; await f.invokeResult(result(e)); })).rejects.toThrow('mutation_uncertain'); }
    finally { context.current = prior; }
    expect(f.nativeJournal.confirm).not.toHaveBeenCalled(); expect([...f.states.values()][0]?.status).toBe('attempting');
  });
  it('nested batch IDs correlate individually even when results arrive in reverse order', async () => {
    const f = await fixture(), a = call('send_message', 'SYNTHETIC_CODEMODE/1'), b = call('send_message', 'SYNTHETIC_CODEMODE/2');
    await f.journal.run(binding, async () => { await Promise.all([f.invokeCall(a), f.invokeCall(b)]);
      await Promise.all([f.invokeResult(result(b, { success: true, message_id: 'SYNTHETIC_B' })), f.invokeResult(result(a, { success: true, message_id: 'SYNTHETIC_A' }))]); });
    expect(f.nativeJournal.confirm).toHaveBeenCalledTimes(2); expect(await f.journal.confirmed(context)).toHaveLength(2);
  });
  it('unknown results, duplicate call IDs and mismatched destinations cannot confirm', async () => {
    const f = await fixture(); await expect(f.journal.run(binding, async () => { await f.invokeCall(); expect((await f.invokeCall()).block).toBe(true); })).rejects.toThrow('mutation_uncertain');
    expect(f.nativeJournal.begin).toHaveBeenCalledTimes(1); expect(f.nativeJournal.confirm).not.toHaveBeenCalled();
    const g = await fixture(); await expect(g.journal.run(binding, async () => { const e = call(); await g.invokeCall(e); const r = result(e); r.input = { to: chat }; await g.invokeResult(r); })).rejects.toThrow('mutation_uncertain');
    expect(g.nativeJournal.confirm).not.toHaveBeenCalled();
  });
  it('active all-session correlation overrides manual guessing; unresolved destination precedence wins', async () => {
    const f = await fixture(), fallback = vi.fn(async () => 'manual' as const);
    f.setOwn('confirmed'); expect(await f.journal.classifyOwn(chat, other, 'SYNTHETIC_SENT', fallback)).toBe('confirmed');
    f.setOwn('uncertain'); expect(await f.journal.classifyOwn(chat, other, 'SYNTHETIC_SENT', fallback)).toBe('uncertain');
    f.setOwn('none'); expect(await f.journal.classifyOwn(chat, other, 'SYNTHETIC_MANUAL', fallback)).toBe('manual');
  });
  it('bridges confirmed journal cleanup only after durable completion proof is visible', async () => {
    const proven = await fixture();
    proven.ownMutation.mockResolvedValueOnce('confirmed').mockResolvedValueOnce('none');
    const durable = vi.fn(async (): Promise<'confirmed' | 'manual' | 'uncertain'> => 'manual')
      .mockResolvedValueOnce('manual').mockResolvedValueOnce('confirmed');
    expect(await proven.journal.classifyOwn(chat, other, 'SYNTHETIC_HANDOFF', durable)).toBe('confirmed');
    expect(durable).toHaveBeenCalledTimes(2);

    const missing = await fixture();
    missing.ownMutation.mockResolvedValueOnce('confirmed').mockResolvedValueOnce('none');
    const absent = vi.fn(async (): Promise<'confirmed' | 'manual' | 'uncertain'> => 'manual');
    expect(await missing.journal.classifyOwn(chat, other, 'SYNTHETIC_HANDOFF', absent)).toBe('uncertain');
    expect(absent).toHaveBeenCalledTimes(2);

    const unresolved = await fixture();
    unresolved.ownMutation.mockResolvedValueOnce('none').mockResolvedValueOnce('confirmed');
    const uncertain = vi.fn(async (): Promise<'confirmed' | 'manual' | 'uncertain'> => 'uncertain');
    expect(await unresolved.journal.classifyOwn(chat, other, 'SYNTHETIC_UNCERTAIN', uncertain)).toBe('uncertain');
    expect(unresolved.ownMutation).toHaveBeenCalledTimes(1);

    const late = await fixture();
    late.ownMutation.mockResolvedValueOnce('none').mockResolvedValueOnce('uncertain');
    const indexedConfirmed = vi.fn(async (): Promise<'confirmed' | 'manual' | 'uncertain'> => 'confirmed');
    expect(await late.journal.classifyOwn(chat, other, 'SYNTHETIC_LATE_UNCERTAIN', indexedConfirmed)).toBe('uncertain');
    expect(late.ownMutation).toHaveBeenCalledTimes(2);
  });
});
describe('locked seven tools and native response envelope', () => {
  it.each([
    ['send_message', { to: '+9 99' }, '999@s.whatsapp.net'], ['send_group_message', { jid: other }, other],
    ['send_auto_message', { jid: '999' }, '999@s.whatsapp.net'], ['send_media', { to: '999@lid' }, '999@lid'],
    ['send_reaction', { chat_jid: other }, other], ['edit_message', { chat_jid: other }, other], ['revoke_message', { chat_jid: other }, other]
  ] as const)('journals %s using its actual parameter field and qualified name', async (tool, input, actual) => {
    const f = await fixture(), e = call(tool, 'SYNTHETIC_CALL', { ...input }); f.info.name = e.toolName;
    expect(mutationDestination(tool, e.input)).toBe(actual);
    await f.journal.run(binding, async () => { await f.invokeCall(e); await f.invokeResult(result(e)); });
    expect(f.records[0]?.state.tool).toBe(tool); expect(f.records[0]?.state.destination).toBe(actual);
  });
  it.each([
    {}, { success: false, message_id: 'SYNTHETIC_SENT' }, { success: true }, { success: true, message_id: '' },
    { success: true, message_id: 1 }, { success: true, message_id: 'SYNTHETIC_SENT', extra: true },
    { id: 'SYNTHETIC_SENT', timestamp: '2033-05-18T03:33:22Z' }, { success: true, message_id: { nested: 'SYNTHETIC_SENT' } }
  ])('does not infer explicit success from invalid locked response %j', payload => { expect(confirmedMessage(result(call(), payload), 'send_message')).toBeUndefined(); });
  it('accepts only the bounded adapter raw result and rejects direct/multiple/nonJSON/mixed/guarded/oversized forms', () => {
    const raw = (event: ToolResultEvent) => (event.details as any).mcpResult;
    const valid = result(); expect(confirmedMessage(valid, 'send_message')).toBe('SYNTHETIC_SENT');
    const direct = result(); raw(direct).structuredContent = { success: true, message_id: 'SYNTHETIC_SENT' };
    expect(confirmedMessage(direct, 'send_message')).toBeUndefined();
    const multiple = result(); raw(multiple).content.push({ type: 'text', text: JSON.stringify({ success: true, message_id: 'SYNTHETIC_SENT' }) });
    expect(confirmedMessage(multiple, 'send_message')).toBeUndefined();
    const nonJson = result(); raw(nonJson).structuredContent.result = 'SYNTHETIC_NONJSON';
    raw(nonJson).content[0].text = 'SYNTHETIC_NONJSON'; (nonJson.content[0] as any).text = 'SYNTHETIC_NONJSON';
    expect(confirmedMessage(nonJson, 'send_message')).toBeUndefined();
    const mixed = result(); (mixed.content[0] as any).text = JSON.stringify({ success: true, message_id: 'SYNTHETIC_OTHER' });
    expect(confirmedMessage(mixed, 'send_message')).toBeUndefined();
    const guarded = result(); (guarded.details as any).outputGuard = { truncated: true };
    expect(confirmedMessage(guarded, 'send_message')).toBeUndefined();
    const oversized = result(call(), { success: true, message_id: 'x'.repeat(4096) }); expect(confirmedMessage(oversized, 'send_message')).toBeUndefined();
    const failed = errorResult(); expect(confirmedMessage(failed, 'send_message')).toBeUndefined();
    const absent = result(); delete (absent.details as any).mcpResult; expect(confirmedMessage(absent, 'send_message')).toBeUndefined();
  });
});

describe('repository native metadata composition (fake command port, not Redis execution)', () => {
  it('fences native begin with exact mapped binding/fingerprint and keeps old narrow journal API', async () => {
    const commands: string[][] = []; const chats = [chat, other, canonicalChatId('300@lid')];
    const envelope = { version: 1, eventFingerprint: fingerprintEvent(work.event), session: binding, state: { version: 1, attemptId: 'c'.repeat(32), eventId: work.event.event_id,
      canonicalChatId: chat, toolCallId: 'SYNTHETIC_CALL', tool: 'send_message', destination: other, startedAt: '2033-05-18T03:33:20Z', ownership: work.ownership, status: 'attempting' } };
    let listing = false;
    const sendCommand = async (args: readonly string[]) => { commands.push([...args]); const op = args[12];
      if (op === 'read') return ['0', JSON.stringify(binding), '', '']; if (op === 'journal-list') return listing ? [JSON.stringify(envelope)] : []; return ['ok']; };
    const redisPort = { sendCommand };
    const c: RedisConnections = { writer: redisPort, reader: redisPort, mapping: redisPort, isReady: () => true, close: async () => {}, serial: action => action(), durable: action => action() };
    const repository = await createRepository(c, { groups: [chat, other], operatorJids: [chats[2]!] }, { findEvent: async () => undefined, classifyOwn: async () => 'manual' });
    expect(await repository.nativeJournal.begin(work, envelope.state as MutationAttempt)).toBe(true);
    const command = commands.find(a => a[12] === 'native-journal-begin')!;
    expect(JSON.parse(command[21]!)).toEqual(envelope); expect(JSON.parse(command[22]!)).toEqual(binding);
    listing = true;
    // Only origin chat returns records; other configured bindings remain separate.
    c.writer.sendCommand = async args => args[12] === 'read' ? ['0', JSON.stringify(binding), '', ''] : args[12] === 'journal-list' ? [JSON.stringify(envelope)] : ['ok'];
    // Early unresolved destination return from the origin precedes any other binding.
    expect(await repository.ownMutation(other, 'SYNTHETIC_ANY')).toBe('uncertain');
    expect(repository.journal).toBeDefined();
  });
});

describe('direct worker own-message routing', () => {
  it.each([
    { name: 'affected participant destination pauses before host open', fromMe: false, mutation: 'uncertain', own: 'manual', open: false, prompt: false, begin: false, completion: undefined },
    { name: 'unaffected participant opens, prompts and completes', fromMe: false, mutation: 'none', own: 'manual', open: true, prompt: true, begin: true, completion: 'turn' },
    { name: 'manual own message prompts and writes a turn marker', fromMe: true, mutation: 'none', own: 'manual', open: true, prompt: true, begin: true, completion: 'turn' },
    { name: 'confirmed own message skips begin and prompt and writes context-only', fromMe: true, mutation: 'none', own: 'confirmed', open: true, prompt: false, begin: false, completion: 'context_only' }
  ] as const)('$name', async ({ fromMe, mutation, own, open, prompt, begin, completion }) => {
    const effects: string[] = [];
    let appended: CompletionMarker | undefined;
    const currentWork: ClaimedWork = { ...work, event: { ...work.event, is_from_me: fromMe,
      message_id: fromMe ? 'SYNTHETIC_OWN_INPUT' : 'SYNTHETIC_PARTICIPANT_INPUT' } };
    const workerBinding: SessionBinding = { canonicalChatId: chat, sessionId: 'SYNTHETIC_WORKER_SESSION', sessionFile: '/tmp/SYNTHETIC_WORKER.jsonl' };
    const session: PiSessionPort = {
      binding: workerBinding,
      isFresh: () => true,
      async prompt() { effects.push('prompt'); },
      usage: () => undefined,
      async compact() { throw new ControllerError('compaction_failed'); },
      async appendCompletion(marker) { effects.push('marker'); appended = marker;
        return { marker, entryId: 'SYNTHETIC_WORKER_ENTRY', session: workerBinding }; },
      async dispose() { effects.push('dispose'); }
    };
    const repository: WorkerRepository = {
      async renew(value) { return value.ownership; },
      async checkWork() { return true; },
      async readChat() { return { canonicalChatId: chat, lastCompletedSequence: 0 }; },
      async bindSession() { effects.push('bind'); return true; },
      async turnState() { effects.push('turn-state'); return 'fresh'; },
      async beginTurn() { effects.push('begin'); return true; },
      async pause(_value, reason) { effects.push(`pause:${reason}`); return true; },
      async complete() { effects.push('complete'); return true; },
      async ownMutation() { effects.push(`mutation:${mutation}`); return mutation; }
    };
    const wait: WorkerOptions['wait'] = { async wait(_ms, signal) {
      await new Promise<void>(resolve => {
        if (signal.aborted) { resolve(); return; }
        signal.addEventListener('abort', () => resolve(), { once: true });
      });
    } };
    const options: WorkerOptions = {
      leaseMs: 1000, renewMs: 1000, appCurrent: () => true,
      instructions: () => 'SYNTHETIC_WORKER_PROMPT', wait,
      diagnostics: { emit: event => { effects.push(`diagnostic:${event.class}`); } },
      clock: { now: () => 2000000000000 },
      host: {
        async findEvent() { return undefined; },
        async classifyOwn() { effects.push(`own:${own}`); return own; },
        async open() { effects.push('open'); return session; },
        async attach() { effects.push('host-attach'); },
        async close() {},
      },
      turns: {
        async attach() { effects.push('turn-attach'); },
        async prepare() { effects.push('prepare'); },
        async run(_session, _context, invoke) { effects.push('run'); await invoke(); },
        async confirmed() { return []; }
      }
    };
    await runChatWorker(repository, currentWork, options);
    expect(effects.includes('open')).toBe(open);
    expect(effects.includes('prompt')).toBe(prompt);
    expect(effects.includes('begin')).toBe(begin);
    if (!open) {
      expect(effects).toContain('pause:mutation_uncertain');
      expect(effects).not.toContain('turn-state');
      expect(effects).not.toContain('complete');
      expect(appended).toBeUndefined();
    } else {
      expect(effects).toContain('complete');
      expect(effects.indexOf('open')).toBeLessThan(effects.indexOf('complete'));
      if (prompt) expect(effects.indexOf('prompt')).toBeLessThan(effects.indexOf('complete'));
      expect(appended?.completion).toBe(completion);
    }
  });
});

// Optional integration ONLY launches an owned ephemeral Redis binary; it never
// attaches to a supplied Redis URL or touches another process/namespace.
const ownedBinary = process.env['SLICE7_OWNED_REDIS_BINARY'];
(ownedBinary ? describe : describe.skip)('owned ephemeral Redis native journal', () => {
  it('recovers incomplete attempts as uncertain, rejects stale fences, and another chat progresses', async () => {
    const { spawn } = await import('node:child_process');
    const { mkdtemp, rm } = await import('node:fs/promises'); const { createServer } = await import('node:net');
    const { isAbsolute } = await import('node:path'); const { connectRedis, requireBaseline } = await import('../../src/redis/client.js');
    const { createClient } = await import('redis'); const { keys } = await import('../../src/redis/keys.js');
    if (!ownedBinary || !isAbsolute(ownedBinary)) throw new Error('synthetic_binary_required');
    const directory = await mkdtemp('/tmp/whatsapp-slice7-owned-');
    const socket = createServer(); socket.listen(0, '127.0.0.1'); await once(socket, 'listening');
    const address = socket.address(); if (!address || typeof address === 'string') throw new Error('synthetic_port_failed');
    const port = address.port; await new Promise<void>(resolve => socket.close(() => resolve()));
    const child = spawn(ownedBinary, ['--bind', '127.0.0.1', '--port', String(port), '--dir', directory, '--appendonly', 'yes', '--appendfsync', 'always', '--maxmemory-policy', 'noeviction', '--save', '', '--daemonize', 'no'], { stdio: 'ignore', env: { PATH: '/usr/bin:/bin' } });
    child.on('error', () => {});
    const createProbe = () => {
      const probe = createClient({ url: `redis://127.0.0.1:${port}/0`, disableOfflineQueue: true, socket: { reconnectStrategy: false, connectTimeout: 100 } });
      probe.on('error', () => {}); return probe;
    };
    let admin: ReturnType<typeof createProbe> | undefined;
    let c: RedisConnections | undefined;
    try {
      for (let n = 0; n < 200; n++) {
        const probe = createProbe();
        try { await probe.connect(); await requireBaseline(probe); admin = probe; break; }
        catch { try { probe.destroy(); } catch { /* failed probe only */ } await new Promise(r => setTimeout(r, 10)); }
      }
      if (!admin) throw new Error('synthetic_start_failed');
      c = await connectRedis({ url: `redis://127.0.0.1:${port}/0`, waitAofMs: 1000, blockMs: 100 });
      const policy = { groups: [chat, other], operatorJids: ['300@lid'] };
      const repo = await createRepository(c, policy, { findEvent: async () => undefined, classifyOwn: async () => 'manual' });
      expect((await repo.accept(work.event)).status).toBe('accepted');
      const app = (await repo.acquireApp('a'.repeat(32), 300000))!;
      const claim = await repo.claimNext(chat, app, 'b'.repeat(32), 300000, 1000); if (claim.status !== 'claimed') throw new Error('synthetic_claim_failed');
      const w = claim.work; await repo.bindSession(w, binding); await repo.beginTurn(w);
      const a: MutationAttempt = { version: 1, attemptId: 'c'.repeat(32), eventId: w.event.event_id, canonicalChatId: chat,
        toolCallId: 'SYNTHETIC_NESTED/1', tool: 'send_message', destination: other, startedAt: '2033-05-18T03:33:20Z', ownership: w.ownership, status: 'attempting' };
      expect(await repo.nativeJournal.begin(w, a)).toBe(true);
      expect(await repo.nativeJournal.begin(w, { ...a, attemptId: 'd'.repeat(32) })).toBe(false);
      expect(await repo.ownMutation(other, 'SYNTHETIC_CALLBACK')).toBe('uncertain');
      expect(await repo.nativeJournal.confirm(w, a.attemptId, 'SYNTHETIC_SENT')).toBe(true);
      expect(await repo.ownMutation(other, 'SYNTHETIC_SENT')).toBe('confirmed');
      const unresolved = { ...a, attemptId: 'f'.repeat(32), toolCallId: 'SYNTHETIC_NESTED/2' };
      expect(await repo.nativeJournal.begin(w, unresolved)).toBe(true);
      expect(await repo.ownMutation(other, 'SYNTHETIC_SENT')).toBe('uncertain');
      await admin.hSet(keys(chat).lease, 'until', '1');
      expect(await repo.nativeJournal.confirm(w, unresolved.attemptId, 'SYNTHETIC_STALE_RESULT')).toBe(false);
      expect((await repo.claimNext(chat, app, 'b'.repeat(32), 300000, 1000)).status).toBe('paused');
      expect((await repo.nativeJournal.list(chat)).find(r => r.attemptId === unresolved.attemptId)?.status).toBe('uncertain');
      expect((await repo.readChat(chat)).pause).toBe('mutation_uncertain');
      const second = { ...work.event, canonical_chat_jid: other, chat_jid: other, event_id: eventId('2'.repeat(32)) };
      expect((await repo.accept(second)).status).toBe('accepted');
      expect((await repo.claimNext(other, app, 'e'.repeat(32), 300000, 1000)).status).toBe('claimed');
    } finally {
      await c?.close(); if (admin?.isOpen) admin.destroy();
      if (child.exitCode === null && child.signalCode === null) {
        const stopped = once(child, 'exit').then(() => undefined);
        const bound = () => new Promise<void>(resolve => setTimeout(resolve, 1000));
        child.kill('SIGTERM'); await Promise.race([stopped, bound()]);
        if (child.exitCode === null && child.signalCode === null) { child.kill('SIGKILL'); await Promise.race([stopped, bound()]); }
      }
      await rm(directory, { recursive: true, force: true });
    }
  }, 30000);
});

describe('session host journal attachment', () => {
  it('binds before SDK startup, attaches before prompt, awaits durable hooks, and publishes only matching completion', async () => {
    const { mkdtemp, rm } = await import('node:fs/promises'); const { appendFileSync, existsSync } = await import('node:fs');
    const { join } = await import('node:path'); const { createSessionHost } = await import('../../src/pi/session-host.js');
    const { createCompletionIndex } = await import('../../src/pi/completion-index.js');
    type Sdk = import('../../src/pi/mcp-loader.js').SdkPort;
    type Raw = import('../../src/pi/mcp-loader.js').SdkSession;
    type Manager = import('../../src/pi/mcp-loader.js').SdkManager;
    const root = await mkdtemp('/tmp/whatsapp-slice7-host-');
    const f = await fixture(); await f.journal.dispose(binding);
    const rows: { chat: typeof chat; binding?: SessionBinding }[] = [chat, other, canonicalChatId('300@lid')].map(chat => ({ chat }));
    const index = createCompletionIndex(root, { groups: [chat, other], operatorJids: ['300@lid'] }, { list: async () => rows }, async () => false);
    let manager: Manager; let entry = 0; let parent: string | null = null;
    const listeners = new Set<(e: import('@earendil-works/pi-coding-agent').AgentSessionEvent) => void>();
    let idle = true; let rawRuntime: Raw | undefined;
    const sdk: Sdk = {
      create(_cwd, directory) {
        const file = join(directory, 'SYNTHETIC.jsonl'); const entries: unknown[] = [];
        manager = { getSessionId: () => binding.sessionId, getSessionFile: () => file,
          getHeader: () => ({ type: 'session', version: 3, id: binding.sessionId, cwd: '/tmp', timestamp: '2033-05-18T03:33:20Z' }), getEntries: () => entries,
          appendCustomEntry(customType, data) {
            const id = 'SYNTHETIC_ENTRY_' + (++entry); const e = { type: 'custom', id, parentId: parent, timestamp: '2033-05-18T03:33:21Z', customType, data };
            parent = id; entries.push(e); if (existsSync(file)) appendFileSync(file, JSON.stringify(e) + '\n'); return id;
          } }; return manager;
      },
      open: () => manager,
      ready: async (_raw, current) => { await current(); },
      async connect(_manager, current) {
        await current();
        const raw: Raw = {
          get isIdle() { return idle; },
          bindExtensions: async () => { await f.handlers['session_start']!({ type: 'session_start' }, f.ctx); },
          getAllTools: () => [f.info], getActiveToolNames: () => [f.info.name],
          getCallableToolNames: () => [f.info.name], getToolDefinition: () => undefined,
          extensionRunner: {} as never, setAutoCompactionEnabled: () => {},
          subscribe: cb => { listeners.add(cb); return () => { listeners.delete(cb); }; },
          prompt: async () => {
            idle = false; const e = call(); expect(await f.invokeCall(e)).toBeUndefined(); await f.invokeResult(result(e)); idle = true;
            for (const cb of listeners) cb({ type: 'agent_settled', aborted: false } as import('@earendil-works/pi-coding-agent').AgentSessionEvent);
          }, waitForIdle: async () => { expect(idle).toBe(true); }, dispose: () => {},
          getContextUsage: () => undefined, compact: async () => { throw new Error('SYNTHETIC_UNUSED'); }
        }; rawRuntime = raw; return raw;
      }
    };
    const host = createSessionHost({ cwd: '/tmp', root, sdk, index, journal: f.journal, current: context.current,
      bind: async (_work, mapped) => { rows[0]!.binding = mapped; return true; } });
    try {
      const session = await host.open(work); await host.attach(session, context); await session.prompt(work.event, 'SYNTHETIC_PROMPT');
      const confirmedSends = await f.journal.confirmed(context);
      const proof = await session.appendCompletion({ version: 1, eventId: work.event.event_id, canonicalChatId: chat, eventFingerprint: fingerprintEvent(work.event),
        chatSequence: 1, completion: 'turn', completedAt: '2033-05-18T03:33:22Z', confirmedSends });
      expect(proof.session).toEqual(session.binding); expect(await host.findEvent(chat, work.event.event_id)).toEqual(proof);
      expect(await host.classifyOwn(other, other, 'SYNTHETIC_SENT')).toBe('confirmed');
      expect(f.nativeJournal.confirm).toHaveBeenCalledTimes(1); await session.dispose(); await session.dispose();

      const runtimeFailureSession = await host.open(work, session.binding);
      if (!rawRuntime) throw new ControllerError('session_failed');
      const runtimeDispose = vi.spyOn(rawRuntime, 'dispose').mockImplementationOnce(() => { throw new Error('synthetic_cleanup_failure'); });
      await expect(runtimeFailureSession.dispose()).rejects.toMatchObject({ code: 'session_failed' });
      await expect(runtimeFailureSession.dispose()).rejects.toMatchObject({ code: 'session_failed' });
      await expect(host.attach(runtimeFailureSession, context)).rejects.toMatchObject({ code: 'session_failed' });
      expect(runtimeDispose).toHaveBeenCalledTimes(1); runtimeDispose.mockRestore();
      const afterRuntimeFailure = await host.open(work, session.binding); await afterRuntimeFailure.dispose();

      const journalFailureSession = await host.open(work, session.binding);
      const journalDispose = vi.spyOn(f.journal, 'dispose').mockRejectedValueOnce(new Error('synthetic_cleanup_failure'));
      await expect(journalFailureSession.dispose()).rejects.toMatchObject({ code: 'session_failed' });
      await expect(journalFailureSession.dispose()).rejects.toMatchObject({ code: 'session_failed' });
      await expect(host.attach(journalFailureSession, context)).rejects.toMatchObject({ code: 'session_failed' });
      expect(journalDispose).toHaveBeenCalledTimes(1); journalDispose.mockRestore();
      await expect(host.open(work, session.binding)).rejects.toMatchObject({ code: 'mutation_uncertain' });
      await f.journal.dispose(journalFailureSession.binding);
      const afterJournalFailure = await host.open(work, session.binding); await afterJournalFailure.dispose();

      await host.close();
      const unjournaled = createSessionHost({ cwd: '/tmp', root, sdk, index, current: context.current, bind: async () => true });
      await expect(unjournaled.open(work, session.binding)).rejects.toThrow('session_failed');
      await unjournaled.close();
    } finally { await rm(root, { recursive: true, force: true }); }
  });
});
