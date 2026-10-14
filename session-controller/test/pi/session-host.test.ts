import { describe, it, expect, vi } from 'vitest';
import { mkdtemp, readFile, writeFile, rm, symlink, open, rename } from 'node:fs/promises';
import { appendFileSync, readFileSync } from 'node:fs';
import { join } from 'node:path';
import { tmpdir } from 'node:os';
import { randomUUID } from 'node:crypto';
import {
  canonicalChatId, eventId, fingerprintEvent, ControllerError,
  type ClaimedWork, type CompletionMarker, type SessionBinding, type CheckpointV1
} from '../../src/domain.js';
import { createCompletionIndex, readEvidence, COMPLETION_TYPE, type MappingPort } from '../../src/pi/completion-index.js';
import { createSessionHost } from '../../src/pi/session-host.js';
import { renderPolicy, loadPromptPolicy } from '../../src/pi/prompt-policy.js';
import type { SdkPort, SdkManager, SdkSession } from '../../src/pi/mcp-loader.js';
import type { TurnContext } from '../../src/scheduler/chat-worker.js';
import type { AgentSessionEvent, SessionEntry } from '@earendil-works/pi-coding-agent';
const group = canonicalChatId('100@g.us'), other = canonicalChatId('200@g.us'), operator = canonicalChatId('300@s.whatsapp.net');
const alias = '400@lid';
const policy = { groups: [group, other], operatorJids: [operator, alias] };
function gate() { let resolve!: () => void; const promise = new Promise<void>(r => { resolve = r; }); return { promise, resolve }; }
function work(chat = group, number = 1): ClaimedWork {
  const fence = { owner: 'a'.repeat(32), epoch: '1', expiresAtMs: 2000000000000 };
  return { streamId: `${number}-0`, ownership: { app: fence, chat: { ...fence, owner: 'b'.repeat(32) } },
    event: { schema_version: 2, event_type: 'whatsapp.message.received', event_id: eventId(number.toString(16).padStart(32, '0')),
      message_id: `SYNTHETIC_${number}`, event_timestamp: '2033-05-18T03:33:20Z', received_at: '2033-05-18T03:33:21Z',
      chat_sequence: number, chat_kind: chat.endsWith('@g.us') ? 'group' : 'operator_private', chat_jid: chat,
      canonical_chat_jid: chat, sender_jid: '500@lid', is_from_me: false } as ClaimedWork['event'] };
}
function marker(w: ClaimedWork): CompletionMarker {
  return { version: 1, eventId: w.event.event_id, eventFingerprint: fingerprintEvent(w.event), canonicalChatId: w.event.canonical_chat_jid,
    chatSequence: w.event.chat_sequence, completion: 'turn', completedAt: '2033-05-18T03:33:22Z', confirmedSends: [] };
}
function manager(file: string, header: Record<string, unknown>, entries: Record<string, unknown>[]): SdkManager {
  return {
    getSessionId: () => header['id'] as string, getSessionFile: () => file,
    getHeader: () => header, getEntries: () => entries,
    appendCustomEntry(type, data) {
      const entry = { type: 'custom', id: randomUUID(), parentId: entries.at(-1)?.['id'] ?? null, timestamp: '2033-05-18T03:33:21Z', customType: type, data };
      entries.push(entry);
      // Fake exact SDK 1.1.0 delay: setup-only create is in memory; open appends.
      try { readFileSync(file); appendFileSync(file, JSON.stringify(entry) + '\n'); }
      catch (error) { if ((error as NodeJS.ErrnoException).code !== 'ENOENT') throw error; }
      return entry.id;
    }
  };
}
async function fixture() {
  const root = await mkdtemp(join(tmpdir(), 'whatsapp-slice6-isolated-'));
  const rows: { chat: ReturnType<typeof canonicalChatId>; binding?: SessionBinding }[] = [group, other, operator].map(chat => ({ chat }));
  const mappings: MappingPort = { list: async () => rows.map(r => ({ ...r })) };
  let lost = false, uncertain = false;
  const effects: string[] = [], opened: string[] = [];
  const listeners = new Set<(event: AgentSessionEvent) => void>();
  let followup = gate(), prompted = gate();
  let idle = true, aborted = false, omitSettled = false, appendFailure = false, markerWritten = false;
  let markerHold: ReturnType<typeof gate> | undefined, written: ReturnType<typeof gate> | undefined;
  const current = async () => { effects.push('gate'); if (lost) throw new ControllerError('lease_lost'); if (markerWritten) await markerHold?.promise; };
  const runtime: SdkSession = {
    get isIdle() { return idle; }, setAutoCompactionEnabled: enabled => { expect(enabled).toBe(false); effects.push('auto-off'); },
    bindExtensions: async () => { await current(); effects.push('extensions-bound'); },
    prompt: async (_text, opts) => {
      await current(); expect(opts?.expandPromptTemplates).toBe(false); effects.push('prompt'); idle = false;
      for (const listener of listeners) listener({ type: 'agent_end', messages: [], willRetry: false });
      prompted.resolve(); await followup.promise;
      effects.push('extension-followup'); await current(); idle = true;
      if (!omitSettled) for (const listener of listeners) listener({ type: 'agent_settled', aborted });
    },
    subscribe: listener => { listeners.add(listener); return () => { listeners.delete(listener); }; },
    waitForIdle: async () => { if (!idle) await followup.promise; },
    dispose: () => { effects.push('disposed'); },
    getContextUsage: () => ({ tokens: 12, contextWindow: 1000, percent: 1.2 }), getAllTools: () => [],
    compact: async () => { throw new Error('SYNTHETIC_DO_NOT_CALL_NATIVE_COMPACT'); }
  };
  const sdk: SdkPort = {
    create: (cwd, dir) => { effects.push('create'); const id = randomUUID(); return manager(join(dir, `${id}.jsonl`), { type: 'session', version: 3, id, cwd, timestamp: '2033-05-18T03:33:20Z' }, []); },
    open: file => {
      effects.push('open'); opened.push(file);
      const entries = readFileSync(file, 'utf8').trimEnd().split('\n').map(line => JSON.parse(line) as Record<string, unknown>);
      const base = manager(file, entries.shift()!, entries);
      return { ...base, appendCustomEntry: (type, data) => {
        const id = base.appendCustomEntry(type, data);
        if (type === COMPLETION_TYPE) { markerWritten = true; written?.resolve(); }
        if (appendFailure && type === COMPLETION_TYPE) throw new Error('SYNTHETIC_POST_APPEND_FAILURE');
        return id;
      } };
    },
    connect: async (_manager, check) => { await check(); expect(rows.some(r => r.binding)).toBe(true); effects.push('connect'); return runtime; },
    ready: async (_session, check) => { await check(); expect(effects).toContain('extensions-bound'); effects.push('mcp-ready'); }
  };
  const index = createCompletionIndex(root, policy, mappings, async () => uncertain);
  let compactGate = gate();
  const host = createSessionHost({ cwd: '/tmp/synthetic-workspace', root, sdk, index, current,
    bind: async (w, binding) => { await current(); effects.push('durable-bind'); const row = rows.find(r => r.chat === w.event.canonical_chat_jid)!; if (row.binding && JSON.stringify(row.binding) !== JSON.stringify(binding)) return false; row.binding = binding; return true; },
    compact: async (_raw, context, _instructions, checkpoint) => {
      await context.current(); effects.push('compact-start'); await compactGate.promise; await context.current();
      const b = rows.find(r => r.chat === context.work().event.canonical_chat_jid)!.binding!;
      const lines = JSON.parse((await readFile(b.sessionFile, 'utf8')).trimEnd().split('\n').at(-1)!) as { id: string };
      appendFileSync(b.sessionFile, JSON.stringify({ type: 'compaction', id: 'SYNTHETIC_COMPACT', parentId: lines.id, timestamp: '2033-05-18T03:33:23Z', summary: 'SYNTHETIC_SUMMARY', firstKeptEntryId: 'SYNTHETIC_COMPACT', tokensBefore: 12 }) + '\n');
      return { entryId: 'SYNTHETIC_COMPACT', checkpoint };
    }
  });
  const attach = async (w: ClaimedWork, session: Awaited<ReturnType<typeof host.open>>) => {
    const context: TurnContext = { current, work: () => w }; await host.attach(session, context); return context;
  };
  return { root, rows, host, index, effects, opened, listeners, runtime, sdk, attach,
    prompted: () => prompted.promise, release: () => followup.resolve(), lose: () => { lost = true; }, uncertain: (v: boolean) => { uncertain = v; },
    reset: () => { followup = gate(); prompted = gate(); idle = true; }, abort: () => { aborted = true; }, omit: () => { omitSettled = true; },
    compactRelease: () => compactGate.resolve(), compactReset: () => { compactGate = gate(); }, failMarker: () => { appendFailure = true; },
    holdMarker: () => { markerHold = gate(); written = gate(); return { written: written.promise, release: markerHold.resolve }; },
    cleanup: async () => { markerHold?.resolve(); followup.resolve(); compactGate.resolve(); await host.close(); await rm(root, { recursive: true, force: true }); }
  };
}
async function turn(f: Awaited<ReturnType<typeof fixture>>, w: ClaimedWork, binding?: SessionBinding) {
  const session = await f.host.open(w, binding); await f.attach(w, session); f.release(); await session.prompt(w.event, 'SYNTHETIC_INSTRUCTIONS'); return session;
}
describe('isolated Pi boundary and durable noncontext evidence (no native runtime)', () => {
  it('creates exact canonical mapping before startup/prompt; waits through extension followups; cleans listeners and reopens exact file', async () => {
    const f = await fixture();
    try {
      const w = work(), session = await f.host.open(w);
      expect(f.effects.indexOf('durable-bind')).toBeLessThan(f.effects.indexOf('connect'));
      await expect(session.prompt(w.event, 'SYNTHETIC')).rejects.toMatchObject({ code: 'session_failed' });
      await f.attach(w, session);
      let finished = false; const task = session.prompt(w.event, 'SYNTHETIC').then(() => { finished = true; });
      await f.prompted(); expect(finished).toBe(false); expect(f.listeners.size).toBe(1);
      await expect(session.appendCompletion(marker(w))).rejects.toMatchObject({ code: 'session_failed' });
      f.release(); await task; expect(f.listeners.size).toBe(0);
      const proof = await session.appendCompletion(marker(w));
      expect(await f.index.findEvent(group, w.event.event_id)).toEqual(proof);
      const raw = (await readFile(session.binding.sessionFile, 'utf8')).trimEnd().split('\n').map(line => JSON.parse(line) as { type: string; customType?: string });
      expect(raw.at(-1)).toMatchObject({ type: 'custom', customType: COMPLETION_TYPE }); expect(raw.some(e => e.type === 'custom_message')).toBe(false);
      await session.dispose(); await session.dispose(); f.reset();
      const restored = await f.host.open(work(group, 2), session.binding);
      expect(f.opened.at(-1)).toBe(session.binding.sessionFile); expect(f.effects.filter(e => e === 'create')).toHaveLength(1);
      const next = work(group, 2); await f.attach(next, restored);
      const nextTurn = restored.prompt(next.event, 'SYNTHETIC'); await f.prompted();
      let disposed = false; const disposing = restored.dispose().then(() => { disposed = true; });
      await new Promise(r => setImmediate(r)); expect(disposed).toBe(false);
      await expect(restored.prompt(next.event, 'SYNTHETIC')).rejects.toMatchObject({ code: 'session_failed' });
      f.release(); await nextTurn; await disposing;
      await expect(session.prompt(w.event, 'SYNTHETIC')).rejects.toMatchObject({ code: 'session_failed' });
    } finally { await f.cleanup(); }
  });
  it('requires original fingerprint and strict durable evidence; malformed, missing and legacy files fail closed', async () => {
    const f = await fixture();
    try {
      const w = work(), session = await turn(f, w);
      await expect(session.appendCompletion({ ...marker(w), eventFingerprint: 'sha256:v1:' + '0'.repeat(64) })).rejects.toMatchObject({ code: 'session_failed' });
      const proof = await session.appendCompletion(marker(w)); expect(await session.appendCompletion(marker(w))).toEqual(proof);
      await expect(session.appendCompletion({ ...marker(w), completedAt: '2033-05-18T03:33:24Z' })).rejects.toMatchObject({ code: 'session_failed' });
      const file = session.binding.sessionFile, saved = await readFile(file);
      const freshIndex = () => createCompletionIndex(f.root, policy, { list: async () => f.rows }, async () => false);
      const lines = saved.toString('utf8').trimEnd().split('\n');
      const header = JSON.parse(lines[0]!) as Record<string, unknown>;
      const withHeader = (patch: Record<string, unknown>) => Buffer.from([
        JSON.stringify({ ...header, ...patch }), ...lines.slice(1)
      ].join('\n') + '\n');
      const cases = [
        Buffer.from(saved.toString().replace(/sha256:v1:[0-9a-f]{64}/, 'legacy')),
        saved.subarray(0, saved.length - 1), Buffer.concat([saved, Buffer.from('{BROKEN}\n')]),
        Buffer.concat([saved, Buffer.from([0xff, 0x0a])]), withHeader({ version: 2 }), withHeader({ id: randomUUID() })
      ];
      for (const bytes of cases) {
        await writeFile(file, bytes);
        await expect(freshIndex().findEvent(group, w.event.event_id)).rejects.toMatchObject({ code: 'session_failed' });
      }
      await writeFile(file, saved); expect(await readEvidence(f.root, session.binding)).toEqual([proof]);
      await session.dispose(); const openedBeforeMissing = f.opened.length; await rm(file);
      await expect(f.host.open(work(group, 2), session.binding)).rejects.toMatchObject({ code: 'session_failed' });
      expect(f.opened).toHaveLength(openedBeforeMissing);
      await expect(freshIndex().findEvent(other, eventId('9'.repeat(32)))).rejects.toMatchObject({ code: 'session_failed' });
    } finally { await f.cleanup(); }
  });
  it('rebuilds correlation across every mapped origin, preserving exact alias destination+ID and uncertainty priority', async () => {
    const f = await fixture();
    try {
      const w = work(), a = await turn(f, w);
      const m = { ...marker(w), confirmedSends: [{ destination: alias, messageId: 'SYNTHETIC_SENT' }, { destination: other, messageId: 'SYNTHETIC_SENT' }] };
      const proof = await a.appendCompletion(m); await a.dispose();
      expect(await f.index.classifyOwn(operator, alias, 'SYNTHETIC_SENT')).toBe('confirmed');
      expect(await f.index.classifyOwn(operator, operator, 'SYNTHETIC_SENT')).toBe('manual');
      expect(await f.index.classifyOwn(other, other, 'SYNTHETIC_SENT')).toBe('confirmed');
      expect(await f.index.classifyOwn(other, other, 'SYNTHETIC_OTHER')).toBe('manual');
      f.uncertain(true); expect(await f.index.classifyOwn(operator, alias, 'SYNTHETIC_SENT')).toBe('uncertain'); f.uncertain(false);
      const fresh = createCompletionIndex(f.root, policy, { list: async () => f.rows }, async () => false);
      expect(await fresh.findEvent(group, w.event.event_id)).toEqual(proof);
      const b = await turn(f, work(other, 2)); await b.appendCompletion(marker(work(other, 2))); await b.dispose();
      await rm(b.binding.sessionFile); await symlink(a.binding.sessionFile, b.binding.sessionFile);
      await expect(fresh.findEvent(group, w.event.event_id)).rejects.toMatchObject({ code: 'session_failed' });
      const incomplete = createCompletionIndex(f.root, policy, { list: async () => f.rows.slice(0, 1) }, async () => false);
      await expect(incomplete.findEvent(group, w.event.event_id)).rejects.toMatchObject({ code: 'session_failed' });
    } finally { await f.cleanup(); }
  });
  it('serializes concurrent intake lookups with marker proof and allows read snapshots during injected compaction without writer/index inversion', async () => {
    const f = await fixture();
    try {
      const w = work(), session = await turn(f, w);
      const lookups = Array.from({ length: 12 }, () => f.index.findEvent(group, w.event.event_id));
      const held = f.holdMarker(); const append = session.appendCompletion(marker(w));
      const prior = await Promise.all(lookups); await held.written;
      // The marker is visible on disk but not yet fsynced/published. Intake
      // must fail closed without waiting for the held Redis ownership gate.
      await expect(f.index.findEvent(group, w.event.event_id)).rejects.toMatchObject({ code: 'session_failed' });
      held.release(); const proof = await append;
      expect(prior.every(p => p === undefined || p.marker.eventFingerprint === proof.marker.eventFingerprint)).toBe(true);
      expect(await f.index.findEvent(group, w.event.event_id)).toEqual(proof);
      const checkpoint = { version: 1 } as CheckpointV1; // injected compact port does not claim Slice 8 validation
      const compact = session.compact('SYNTHETIC_COMPACT', checkpoint);
      await new Promise(r => setImmediate(r));
      expect(await f.index.findEvent(group, w.event.event_id)).toEqual(proof);
      f.compactRelease(); await compact; expect(await f.index.findEvent(group, w.event.event_id)).toEqual(proof);
    } finally { await f.cleanup(); }
  });
  it('blocks stale side effects, aborted/consumed turns and overlapping handles; errors remain fixed classes', async () => {
    const f = await fixture();
    try {
      const w = work(), session = await f.host.open(w); await f.attach(w, session);
      await expect(f.host.open(w, session.binding)).rejects.toMatchObject({ code: 'session_failed' });
      f.release(); f.abort(); await expect(session.prompt(w.event, 'SYNTHETIC')).rejects.toMatchObject({ code: 'session_failed' }); expect(f.listeners.size).toBe(0);
      await expect(session.appendCompletion(marker(w))).rejects.toMatchObject({ code: 'session_failed' });
      f.lose(); await expect(session.prompt(w.event, 'SYNTHETIC')).rejects.toMatchObject({ code: 'lease_lost' });
      expect(f.effects.filter(e => e === 'prompt')).toHaveLength(1);
      await session.dispose(); await expect(f.host.open(work(group, 2), session.binding)).rejects.toMatchObject({ code: 'lease_lost' });
    } finally { await f.cleanup(); }
    const g = await fixture();
    try { const w = work(), session = await g.host.open(w); await g.attach(w, session); g.release(); g.omit(); await expect(session.prompt(w.event, 'SYNTHETIC')).rejects.toMatchObject({ code: 'session_failed' }); expect(g.listeners.size).toBe(0); }
    finally { await g.cleanup(); }
    const h = await fixture();
    try {
      const w = work(), session = await turn(h, w); h.failMarker();
      await expect(session.appendCompletion(marker(w))).rejects.toMatchObject({ code: 'session_failed' });
      await expect(h.index.findEvent(group, w.event.event_id)).rejects.toMatchObject({ code: 'session_failed' });
      await expect(h.index.findEvent(other, eventId('9'.repeat(32)))).rejects.toMatchObject({ code: 'session_failed' });
    } finally { await h.cleanup(); }
    const j = await fixture();
    let readySpy: ReturnType<typeof vi.spyOn> | undefined, disposeSpy: ReturnType<typeof vi.spyOn> | undefined;
    try {
      readySpy = vi.spyOn(j.sdk, 'ready').mockRejectedValueOnce(new ControllerError('lease_lost'));
      disposeSpy = vi.spyOn(j.runtime, 'dispose').mockImplementationOnce(() => { throw new Error('SYNTHETIC_DISPOSE_FAILURE'); });
      await expect(j.host.open(work())).rejects.toMatchObject({ code: 'lease_lost' });
      expect(disposeSpy).toHaveBeenCalledTimes(1);
    } finally { readySpy?.mockRestore(); disposeSpy?.mockRestore(); await j.cleanup(); }
  });
  it('accepts declaration-checked Pi 1.1.0 entry variants and optional metadata without weakening controller identities', async () => {
    const f = await fixture();
    try {
      const w = work(), session = await turn(f, w), proof = await session.appendCompletion(marker(w));
      const file = session.binding.sessionFile, saved = await readFile(file, 'utf8');
      let parentId = proof.entryId;
      const native: SessionEntry[] = [];
      const add = (entry: SessionEntry) => { native.push(entry); parentId = entry.id; };
      const base = () => ({ id: `NATIVE_${native.length}`, parentId, timestamp: '2033-05-18T03:33:24Z' });
      add({ ...base(), type: 'message', message: { role: 'user', content: 'SYNTHETIC', timestamp: 2000000000000 } });
      const targetId = parentId;
      add({ ...base(), type: 'context_edit', targetId, replacement: { content: [{ type: 'text', text: 'SYNTHETIC_EDIT' }] } });
      add({ ...base(), type: 'context_edit', targetId, replacement: { content: 'SYNTHETIC_EDIT' } });
      add({ ...base(), type: 'context_edit', targetId, replacement: null });
      add({ ...base(), type: 'session_info' }); // name is OPTIONAL in 1.1.0
      add({ ...base(), type: 'session_info', name: 'SYNTHETIC' });
      add({ ...base(), type: 'label', targetId, label: undefined });
      add({ ...base(), type: 'custom', customType: 'synthetic.extension' }); // data optional
      add({ ...base(), type: 'model_change', provider: 'synthetic', modelId: 'synthetic' });
      add({ ...base(), type: 'thinking_level_change', thinkingLevel: 'off' });
      add({ ...base(), type: 'custom_message', customType: 'synthetic', content: 'SYNTHETIC', display: false });
      add({ ...base(), type: 'usage', kind: 'cache_warm', provider: 'synthetic', model: 'synthetic', usage: {
        input: 0, output: 0, cacheRead: 0, cacheWrite: 0, totalTokens: 0, cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0 }
      } });
      add({ ...base(), type: 'compaction', summary: 'SYNTHETIC', firstKeptEntryId: targetId, tokensBefore: 0 });
      add({ ...base(), type: 'branch_summary', summary: 'SYNTHETIC', fromId: targetId });
      add({ ...base(), type: 'message', message: { role: 'branchSummary', summary: 'SYNTHETIC', fromId: targetId, timestamp: 2000000000000 } });
      add({ ...base(), type: 'message', message: { role: 'compactionSummary', summary: 'SYNTHETIC', tokensBefore: 0, timestamp: 2000000000000 } });
      await writeFile(file, saved + native.map(e => JSON.stringify(e) + '\n').join(''));
      expect(await f.index.findEvent(group, w.event.event_id)).toEqual(proof);
      for (const bad of [
        { type: 'context_edit', targetId, replacement: 'NOT_NATIVE' },
        { type: 'session_info', name: 123 },
        { type: 'custom', customType: COMPLETION_TYPE, data: { ...marker(w), canonicalChatId: other } },
        { type: 'custom', customType: COMPLETION_TYPE + '.legacy', data: marker(w) },
        { type: 'custom', customType: COMPLETION_TYPE, data: marker(w) }
      ]) {
        await writeFile(file, saved + JSON.stringify({ ...base(), ...bad, parentId: proof.entryId }) + '\n');
        const fresh = createCompletionIndex(f.root, policy, { list: async () => f.rows }, async () => false);
        await expect(fresh.findEvent(group, w.event.event_id)).rejects.toMatchObject({ code: 'session_failed' });
      }
    } finally { await f.cleanup(); }
  });
  it('reconstructed index requires file then parent sync before cleanup of an append-visible crash marker', async () => {
    const f = await fixture(); let spy: ReturnType<typeof vi.spyOn> | undefined;
    try {
      const w = work(), session = await turn(f, w);
      const held = f.holdMarker(), append = session.appendCompletion(marker(w)); await held.written;
      const handle = await open(session.binding.sessionFile, 'r+');
      const prototype = Object.getPrototypeOf(handle) as { sync(): Promise<void> };
      const original = prototype.sync; await handle.close();
      const entered = gate(), release = gate(), order: string[] = [];
      spy = vi.spyOn(prototype, 'sync').mockImplementation(async function(this: typeof handle) {
        const kind = (await this.stat()).isFile() ? 'file' : 'parent'; order.push(kind);
        if (kind === 'file') { entered.resolve(); await release.promise; }
        await original.call(this);
      });
      const fresh = createCompletionIndex(f.root, policy, { list: async () => f.rows }, async () => false);
      let cleaned = false;
      const recovered = fresh.findEvent(group, w.event.event_id).then(p => { expect(p).toBeDefined(); cleaned = true; return p; });
      await entered.promise; expect(cleaned).toBe(false); expect(order).toEqual(['file']);
      let concurrentEffect = false; const queued = fresh.exclusive(async () => { concurrentEffect = true; });
      await new Promise(r => setImmediate(r)); expect(concurrentEffect).toBe(false);
      release.resolve(); const proof = await recovered; expect(order).toEqual(['file', 'parent']); expect(cleaned).toBe(true);
      spy.mockRestore(); await queued; expect(concurrentEffect).toBe(true);
      held.release(); expect(await append).toEqual(proof);
      // Atomic injected compaction replaces the named inode during parent sync.
      // Recovery must retry, sync the new file and return only its snapshot.
      let replaced = false; const retried: string[] = [];
      spy = vi.spyOn(prototype, 'sync').mockImplementation(async function(this: typeof handle) {
        const kind = (await this.stat()).isFile() ? 'file' : 'parent'; retried.push(kind);
        if (kind === 'parent' && !replaced) {
          replaced = true; const file = session.binding.sessionFile;
          const temporary = file + '.synthetic-compact';
          const bytes = await readFile(file, 'utf8');
          const compact: SessionEntry = { type: 'compaction', id: 'RECOVERY_COMPACT', parentId: proof!.entryId, timestamp: '2033-05-18T03:33:24Z', summary: 'SYNTHETIC', firstKeptEntryId: proof!.entryId, tokensBefore: 0 };
          await writeFile(temporary, bytes + JSON.stringify(compact) + '\n'); await rename(temporary, file);
        }
        await original.call(this);
      });
      appendFileSync(session.binding.sessionFile, JSON.stringify({ type: 'custom', id: 'SYNTHETIC_CACHE_REFRESH', parentId: proof!.entryId, timestamp: '2033-05-18T03:33:25Z', customType: 'synthetic.cache.refresh' }) + '\n');
      expect(await fresh.findEvent(group, w.event.event_id)).toEqual(proof);
      expect(retried).toEqual(['file', 'parent', 'file', 'parent']);
    } finally { spy?.mockRestore(); await f.cleanup(); }
  });
  it('recovery file or parent fsync failure prevents proof-based cleanup and corrupt/truncated crash markers remain closed', async () => {
    const f = await fixture(); let spy: ReturnType<typeof vi.spyOn> | undefined;
    try {
      const w = work(), session = await turn(f, w);
      const held = f.holdMarker(), append = session.appendCompletion(marker(w)); await held.written;
      const file = session.binding.sessionFile, saved = await readFile(file);
      const handle = await open(file, 'r+'), prototype = Object.getPrototypeOf(handle) as { sync(): Promise<void> };
      const original = prototype.sync; await handle.close();
      for (const failKind of ['file', 'parent']) {
        spy = vi.spyOn(prototype, 'sync').mockImplementation(async function(this: typeof handle) {
          if (((await this.stat()).isFile() ? 'file' : 'parent') === failKind) throw new Error('SYNTHETIC_SYNC_FAILURE');
          await original.call(this);
        });
        const fresh = createCompletionIndex(f.root, policy, { list: async () => f.rows }, async () => false);
        let cleaned = false;
        await expect(fresh.findEvent(group, w.event.event_id).then(() => { cleaned = true; })).rejects.toMatchObject({ code: 'session_failed' });
        expect(cleaned).toBe(false); spy.mockRestore();
      }
      for (const bytes of [saved.subarray(0, saved.length - 1), Buffer.concat([saved, Buffer.from('{BAD}\n')])]) {
        await writeFile(file, bytes);
        const fresh = createCompletionIndex(f.root, policy, { list: async () => f.rows }, async () => false);
        await expect(fresh.findEvent(group, w.event.event_id)).rejects.toMatchObject({ code: 'session_failed' });
      }
      await writeFile(file, saved); held.release(); await append;
    } finally { spy?.mockRestore(); await f.cleanup(); }
  });
  it.each(['file', 'parent'] as const)('poisons same-index lookup, correlation and recording after recovery %s sync failure; new reconstruction can recover', async failKind => {
    const f = await fixture(); let spy: ReturnType<typeof vi.spyOn> | undefined;
    try {
      const w = work(), session = await turn(f, w);
      const m = { ...marker(w), confirmedSends: [{ destination: alias, messageId: 'SYNTHETIC_RECOVERED_SEND' }] };
      const proof = await session.appendCompletion(m);
      const handle = await open(session.binding.sessionFile, 'r+');
      const prototype = Object.getPrototypeOf(handle) as { sync(): Promise<void> };
      const original = prototype.sync; await handle.close();
      spy = vi.spyOn(prototype, 'sync').mockImplementation(async function(this: typeof handle) {
        if (((await this.stat()).isFile() ? 'file' : 'parent') === failKind) throw new Error('SYNTHETIC_SYNC_FAILURE');
        await original.call(this);
      });
      appendFileSync(session.binding.sessionFile, JSON.stringify({ type: 'custom', id: `SYNTHETIC_POISON_${failKind}`, parentId: proof.entryId, timestamp: '2033-05-18T03:33:25Z', customType: 'synthetic.poison' }) + '\n');
      await expect(f.index.findEvent(group, w.event.event_id)).rejects.toMatchObject({ code: 'session_failed' });
      spy.mockRestore();
      // Sync now succeeds, but this exact instance must never return its proof.
      let syncs = 0;
      spy = vi.spyOn(prototype, 'sync').mockImplementation(async function(this: typeof handle) { syncs++; await original.call(this); });
      await expect(f.index.findEvent(group, w.event.event_id)).rejects.toMatchObject({ code: 'session_failed' });
      await expect(f.index.classifyOwn(operator, alias, 'SYNTHETIC_RECOVERED_SEND')).rejects.toMatchObject({ code: 'session_failed' });
      await expect(session.appendCompletion(m)).rejects.toMatchObject({ code: 'session_failed' });
      await expect(f.index.exclusive(() => f.index.evidence(session.binding))).rejects.toMatchObject({ code: 'session_failed' });
      expect(syncs).toBe(0);
      const fresh = createCompletionIndex(f.root, policy, { list: async () => f.rows }, async () => false);
      expect(await fresh.findEvent(group, w.event.event_id)).toEqual(proof);
      expect(syncs).toBe(2);
      expect(await fresh.classifyOwn(operator, alias, 'SYNTHETIC_RECOVERED_SEND')).toBe('confirmed');
    } finally { spy?.mockRestore(); await f.cleanup(); }
  });
  it('caches stable evidence, closes all handles, and substitutes operator templates without weakening policy guidance', async () => {
    const f = await fixture(); let syncSpy: ReturnType<typeof vi.spyOn> | undefined;
    try {
      const w = work(), session = await turn(f, w), proof = await session.appendCompletion(marker(w));
      const handle = await open(session.binding.sessionFile, 'r+');
      const prototype = Object.getPrototypeOf(handle) as { sync(): Promise<void> };
      const original = prototype.sync; await handle.close();
      let syncs = 0;
      syncSpy = vi.spyOn(prototype, 'sync').mockImplementation(async function(this: typeof handle) { syncs++; await original.call(this); });
      const fresh = createCompletionIndex(f.root, policy, { list: async () => f.rows }, async () => false);
      expect(await fresh.findEvent(group, w.event.event_id)).toEqual(proof); expect(syncs).toBe(2);
      expect(await fresh.findEvent(group, w.event.event_id)).toEqual(proof); expect(syncs).toBe(2);
      appendFileSync(session.binding.sessionFile, JSON.stringify({ type: 'custom', id: 'SYNTHETIC_VALID_APPEND', parentId: proof.entryId, timestamp: '2033-05-18T03:33:25Z', customType: 'synthetic.valid' }) + '\n');
      expect(await fresh.findEvent(group, w.event.event_id)).toEqual(proof); expect(syncs).toBe(4);
      appendFileSync(session.binding.sessionFile, '{MALFORMED}\n');
      await expect(fresh.findEvent(group, w.event.event_id)).rejects.toMatchObject({ code: 'session_failed' });
      const failedSyncs = syncs;
      await expect(fresh.findEvent(group, w.event.event_id)).rejects.toMatchObject({ code: 'session_failed' }); expect(syncs).toBe(failedSyncs);
    } finally { syncSpy?.mockRestore(); await f.cleanup(); }

    const g = await fixture(); let disposeSpy: ReturnType<typeof vi.spyOn> | undefined;
    try {
      await g.host.open(work(group, 1)); await g.host.open(work(other, 2));
      disposeSpy = vi.spyOn(g.runtime, 'dispose').mockImplementationOnce(() => { throw new Error('SYNTHETIC_DISPOSE_FAILURE'); }).mockImplementation(() => {});
      await expect(g.host.close()).rejects.toMatchObject({ code: 'session_failed' }); expect(disposeSpy).toHaveBeenCalledTimes(2);
      await expect(g.host.close()).rejects.toMatchObject({ code: 'session_failed' }); expect(disposeSpy).toHaveBeenCalledTimes(2);
      await expect(g.host.open(work(operator, 3))).rejects.toMatchObject({ code: 'session_failed' });
    } finally { disposeSpy?.mockRestore(); await rm(g.root, { recursive: true, force: true }); }

    const w = work(operator); const event = { ...w.event, chat_jid: alias, sender_jid: alias, sender_alt_jid: operator, is_from_me: true, message_id: 'SYNTHETIC_{{origin}}' };
    const output = renderPolicy('{{canonical_chat_jid}} {{chat_jid}} {{sender_jid}} {{sender_alt_jid}} {{message_id}} {{origin}}', event);
    expect(output).toContain(JSON.stringify(operator)); expect(output).toContain(JSON.stringify(alias)); expect(output).toContain('SYNTHETIC_{{origin}}'); expect(output).toContain('own-account-manual');
    expect(() => renderPolicy('{{unknown}}', event)).toThrow('config_invalid');
    const promptPolicy = await loadPromptPolicy(join(import.meta.dirname, '../../config/system-prompt.md'), { groups: [group, other], operatorJids: [operator, alias] });
    expect(promptPolicy.system).toContain('unknown mutation, DENY'); expect(promptPolicy.system).toContain('Direct the requester'); expect(promptPolicy.system).toContain(JSON.stringify(operator)); expect(promptPolicy.system).toContain(JSON.stringify(alias));
    expect(promptPolicy.system).toContain('fields directly, not {message: {...}}'); expect(promptPolicy.system).toContain('top-level content');
    // Locked Slice 2 serializes response.message.model_dump directly.
    const bridgeResponse = { message: { content: 'SYNTHETIC_TEXT', sender: alias, sender_name: 'SYNTHETIC', timestamp: '2033-05-18T03:33:20Z' } };
    const toolOutput = JSON.parse(JSON.stringify(bridgeResponse.message)) as Record<string, unknown>;
    expect(toolOutput['message']).toBeUndefined(); expect(toolOutput['content']).toBe('SYNTHETIC_TEXT'); expect(toolOutput['sender']).toBe(alias);
    await expect(loadPromptPolicy(join(import.meta.dirname, '../../config/system-prompt.md'), { groups: [group], operatorJids: [] })).rejects.toMatchObject({ code: 'config_invalid' }); expect(promptPolicy.system).toContain('not a capability sandbox'); expect(promptPolicy.instructions(event)).toContain('SYNTHETIC_{{origin}}');
  });
});
