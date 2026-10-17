import { appendFileSync, readFileSync, statSync, unlinkSync, writeFileSync } from 'node:fs';
import { mkdtemp, open, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { randomUUID } from 'node:crypto';
import { describe, expect, it, vi } from 'vitest';
import type { CompactionEntry, CompactionResult, ExtensionContext, SessionBeforeCompactEvent, SessionCompactEvent, SessionEntry, SessionStartEvent } from '@earendil-works/pi-coding-agent';
import { canonicalChatId, ControllerError, eventId, fingerprintEvent, type CheckpointRecord, type CheckpointV1, type ClaimedWork, type PiSessionPort } from '../../src/domain.js';
import type { ControllerConfig } from '../../src/config.js';
import { assertSupportedEntries, checkpointIdentity, confirmedHistory, containsSensitive, renderCheckpointSummary, validateCheckpoint } from '../../src/compaction/checkpoint.js';
import { createCompactor } from '../../src/compaction/compactor.js';
import { createCheckpointExtension } from '../../src/compaction/checkpoint-extension.js';
import { BINDING_TYPE, COMPLETION_TYPE, createCompletionIndex } from '../../src/pi/completion-index.js';
import { MUTATION_TYPE } from '../../src/pi/tool-journal-extension.js';
import { createSessionHost } from '../../src/pi/session-host.js';
import { loadExactPrepareCompaction, type SdkManager, type SdkPort, type SdkSession } from '../../src/pi/mcp-loader.js';
import { createJournalTurns, runChatWorker, type TurnContext, type TurnPorts, type WorkerOptions, type WorkerRepository } from '../../src/scheduler/chat-worker.js';

const chat = canonicalChatId('100@g.us'), other = canonicalChatId('200@g.us');
const binding = { canonicalChatId: chat, sessionId: 'SYNTHETIC_SESSION', sessionFile: '/tmp/SYNTHETIC.jsonl' };
const fence = { owner: 'a'.repeat(32), epoch: '1', expiresAtMs: 2000000000000 };
function work(c = chat): ClaimedWork { return { streamId: '1-0', ownership: { app: fence, chat: { ...fence, owner: 'b'.repeat(32) } }, event: { schema_version: 2, event_type: 'whatsapp.message.received', event_id: eventId(c === chat ? '1'.repeat(32) : '2'.repeat(32)), message_id: 'SYNTHETIC', event_timestamp: '2033-05-18T03:33:20Z', received_at: '2033-05-18T03:33:21Z', chat_sequence: 1, chat_kind: 'group', chat_jid: c, canonical_chat_jid: c, sender_jid: '300@lid', is_from_me: false } }; }
const outcome = { attemptId: 'd'.repeat(32), tool: 'send_message' as const, destination: chat, messageId: 'SYNTHETIC_MESSAGE' };
const checkpoint = (outcomes: CheckpointV1['confirmedToolOutcomes'] = [], c = chat): CheckpointV1 => ({ version: 1, canonicalChatId: c, lastCompletedSequence: 0, policyVersion: 'p1', templateVersion: 't1', createdAt: '2033-05-18T03:33:22Z', goals: ['Continue safely'], decisions: ['Use durable state'], constraints: ['No replay'], verifiedFacts: ['Prior turns settled'], pendingWork: ['Handle accepted event'], approvalBoundaries: ['Operator approval stays private'], nextSteps: ['Process accepted event'], confirmedToolOutcomes: outcomes });
const compaction = { safeWorkingWindowTokens: 10000, defaultContextWindowTokens: 10000, headroomPercent: 15, responseReserveTokens: 100, preparationReserveTokens: 100, summaryReserveTokens: 100, checkpointMaxBytes: 65536 } satisfies ControllerConfig['compaction'];
const config = { pi: { policyVersion: 'p1', templateVersion: 't1', preCompactionPath: '/synthetic/pre.md', compactionSummaryPath: '/synthetic/summary.md' }, compaction } as ControllerConfig;
const turn = { work: () => work(), current: async () => {} };
const base = (id: string) => ({ type: 'custom' as const, id, parentId: null, timestamp: '2033-05-18T03:33:22Z' });
function historyEntries(): SessionEntry[] {
  const prior = { ...work().event, event_id: eventId('3'.repeat(32)), chat_sequence: 1 };
  const marker = { version: 1 as const, eventId: prior.event_id, eventFingerprint: fingerprintEvent(prior), canonicalChatId: chat, chatSequence: 1, completion: 'turn' as const, completedAt: '2033-05-18T03:33:22Z', confirmedSends: [{ destination: chat, messageId: outcome.messageId }] };
  const attempt = { version: 1 as const, attemptId: outcome.attemptId, eventId: marker.eventId, canonicalChatId: chat, toolCallId: 'SYNTHETIC_CALL', tool: outcome.tool, destination: chat, startedAt: '2033-05-18T03:33:20Z', ownership: work().ownership };
  return [
    { ...base('1'), customType: BINDING_TYPE, data: binding },
    { type: 'session_info', id: 'native', parentId: '1', timestamp: '2033-05-18T03:33:22Z' },
    { ...base('2'), customType: MUTATION_TYPE, data: { version: 1 as const, eventFingerprint: marker.eventFingerprint, session: binding, state: { ...attempt, status: 'attempting' as const } } },
    { ...base('3'), customType: MUTATION_TYPE, data: { version: 1 as const, eventFingerprint: marker.eventFingerprint, session: binding, state: { ...attempt, status: 'confirmed' as const, messageId: outcome.messageId, finishedAt: '2033-05-18T03:33:21Z' } } },
    { ...base('4'), customType: COMPLETION_TYPE, data: marker }
  ] as SessionEntry[];
}

describe('strict checkpoint and active-branch history', () => {
  it('renders bounded continuity and rejects sensitive or unsupported material', () => {
    const value = checkpoint(), identity = checkpointIdentity(value), checkpointId = 'c'.repeat(32);
    const summary = renderCheckpointSummary(validateCheckpoint(value, identity, 65536), checkpointId, identity, 65536);
    expect(summary).toBe(renderCheckpointSummary(value, checkpointId, identity, 65536));
    expect(summary).toContain('## Approval Boundaries');
    expect(summary).toContain(`- Checkpoint ID: ${checkpointId}`); expect(summary).toContain(`- Created at: ${value.createdAt}`);
    expect(Buffer.byteLength(summary, 'utf8')).toBeLessThanOrEqual(65536);
    expect(() => validateCheckpoint({ ...value, createdAt: '2033-05-18T03:33:23Z' }, identity, 65536)).toThrow('checkpoint_invalid');
    for (const field of ['goals', 'decisions', 'constraints', 'verifiedFacts', 'pendingWork', 'approvalBoundaries', 'nextSteps'] as const) {
      for (const newline of ['\r', '\n']) expect(() => validateCheckpoint({ ...value, [field]: [`Safe${newline}## Injected Metadata`] }, identity, 65536)).toThrow('checkpoint_invalid');
    }
    expect(containsSensitive({ value: 'authorization: Bearer SYNTHETIC_NOT_A_REAL_CREDENTIAL' })).toBe(true);
    expect(() => assertSupportedEntries([{ type: 'future_entry' }])).toThrow('checkpoint_invalid');
  });
  it('retains confirmed outcomes from the exact active branch after Redis cleanup/restart', () => {
    expect(confirmedHistory(historyEntries(), binding, 1, { ...checkpoint([outcome]), lastCompletedSequence: 1 })).toEqual([outcome]);
  });
  it('rejects all six isolated ordering/corruption scenarios', () => {
    for (const kind of [
      'missing-binding', 'completion-first', 'mutation-after-completion', 'duplicate-transition', 'conflicting-transition', 'unsupported-controller'
    ] as const) {
      const entries = historyEntries();
      if (kind === 'missing-binding') entries.shift();
      if (kind === 'completion-first') entries.splice(1, 0, entries.pop()!);
      if (kind === 'mutation-after-completion') entries.push(entries[2]!);
      if (kind === 'duplicate-transition') entries.splice(3, 0, entries[2]!);
      if (kind === 'conflicting-transition') entries.splice(3, 0, { ...entries[3]!, data: { ...(entries[3] as { data: Record<string, unknown> }).data, eventFingerprint: 'sha256:v1:' + '0'.repeat(64) } } as SessionEntry);
      if (kind === 'unsupported-controller') entries.splice(1, 0, { ...base('x'), customType: 'whatsapp.controller.future.v2', data: {} });
      expect.soft(() => confirmedHistory(entries, binding, 1)).toThrow('checkpoint_invalid');
    }
  });
});

describe('exact pinned Pi compactability preflight', () => {
  it('matches deployed post-compaction and sufficient-history branches without model work or mutation', async () => {
    const prepare = await loadExactPrepareCompaction();
    const user = (id: string, parentId: string | null, size: number): SessionEntry => ({
      type: 'message', id, parentId, timestamp: '2033-05-18T03:33:20Z',
      message: { role: 'user', content: 'x'.repeat(size), timestamp: 1 }
    });
    const old = user('OLD', null, 200000);
    const prior = { type: 'compaction', id: 'PRIOR', parentId: 'OLD', timestamp: '2033-05-18T03:33:21Z',
      summary: 'PRIOR_SUMMARY', firstKeptEntryId: 'OLD', tokensBefore: 50000 } satisfies SessionEntry;
    const recent = user('RECENT', 'PRIOR', 1000);
    const settings = { enabled: false, reserveTokens: 16384, keepRecentTokens: 32768 };
    expect(prepare([old, prior, recent], settings)).toBeUndefined();
    const enough = [user('ONE', null, 2000), user('TWO', 'ONE', 2000), user('THREE', 'TWO', 2000)];
    expect(prepare(enough, { ...settings, keepRecentTokens: 100 })).toBeDefined();
  });
});

describe('typed armed Pi 1.1 adapter', () => {
  it('finishes compactability and fallible preparation before one armed manual compaction and persists transitions', async () => {
    const phases: string[] = []; let record: { checkpointId: string; phase: 'prepared' | 'compacted' | 'restored'; checkpoint: CheckpointV1; compactionEntryId?: string } | undefined;
    writeFileSync(binding.sessionFile, '{}\n', { mode: 0o600 });
    const extension = createCheckpointExtension({ checkpoints: {
      prepare: async (_work: ClaimedWork, value: typeof record) => { phases.push('prepared'); record = value; return true; },
      transition: async (_work: ClaimedWork, _id: string, phase: 'compacted' | 'restored', entry: string) => { if (!record) return false; record = { ...record, phase, compactionEntryId: entry }; phases.push(phase); return true; }, latest: async () => record
    } } as never, { ...compaction, sessionDir: '/tmp' });
    const branch = [{ type: 'message' as const, id: 'SYNTHETIC_KEPT', parentId: null, timestamp: '2033-05-18T03:33:20Z', message: { role: 'user' as const, content: 'hello', timestamp: 1 } }];
    const seed = checkpoint(); let modelCheckpoint: CheckpointV1 = { ...seed, createdAt: '2033-05-18T03:33:24Z' };
    let compactable = false, providerCalls = 0, preflightGeneration = 'stable', changeAfterProvider = false;
    let returnedVariant: (value: CompactionResult, entry: CompactionEntry) => unknown = value => value;
    const ctx = { sessionManager: { getSessionId: () => binding.sessionId, getBranch: () => branch, buildSessionProjection: () => ({ entries: [{ sourceEntry: branch[0]!, messages: [branch[0]!.message] }], messages: [branch[0]!.message], thinkingLevel: 'off', model: null }) }, isIdle: () => true, getContextUsage: () => ({ tokens: 9000, contextWindow: 10000, percent: 90 }), model: { id: 'SYNTHETIC_MODEL' }, modelRegistry: { complete: async () => { providerCalls++; if (changeAfterProvider) preflightGeneration = 'changed'; return ({ content: [{ type: 'text' as const, text: JSON.stringify(modelCheckpoint) }], usage: { input: 1, output: 1, cacheRead: 0, cacheWrite: 0, totalTokens: 2, cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0 } } }); } } } as unknown as ExtensionContext;
    extension.events.sessionStart({ type: 'session_start', reason: 'new' } satisfies SessionStartEvent, ctx);
    const before = { type: 'session_before_compact', preparation: { firstKeptEntryId: 'SYNTHETIC_KEPT', messagesToSummarize: [], turnPrefixMessages: [], isSplitTurn: false, tokensBefore: 9000, fileOps: { read: new Set<string>(), written: new Set<string>(), edited: new Set<string>() }, settings: { enabled: false, reserveTokens: 100, keepRecentTokens: 100 } }, branchEntries: branch, customInstructions: 'SYNTHETIC', reason: 'manual', willRetry: false, signal: new AbortController().signal } satisfies SessionBeforeCompactEvent;
    const compact = { compactionPreflight: () => compactable ? preflightGeneration : false, compact: async () => { phases.push('compact-call'); const supplied = extension.events.beforeCompact(before, ctx); const value = supplied?.compaction; if (!value || !value.usage) throw new Error(); const entry = { type: 'compaction', id: 'SYNTHETIC_ENTRY', parentId: null, timestamp: '2033-05-18T03:33:23Z', summary: value.summary, firstKeptEntryId: value.firstKeptEntryId, tokensBefore: value.tokensBefore, usage: value.usage, details: value.details, fromHook: true } satisfies CompactionEntry; extension.events.compact({ type: 'session_compact', compactionEntry: entry, fromExtension: true, reason: 'manual', willRetry: false } satisfies SessionCompactEvent, ctx); return returnedVariant(value, entry) as CompactionResult; } };
    await expect(extension.run(binding.sessionId, binding, compact, turn, seed, 'PREPARE', 'SUMMARY')).rejects.toThrow('compaction_capacity');
    expect(phases).toEqual([]); expect(providerCalls).toBe(0);
    compactable = true;
    await expect(extension.run(binding.sessionId, binding, compact, turn, seed, 'PREPARE', 'SUMMARY')).rejects.toThrow('checkpoint_invalid');
    expect(phases).toEqual([]); expect(providerCalls).toBe(1); modelCheckpoint = seed;
    changeAfterProvider = true; preflightGeneration = 'stable';
    await expect(extension.run(binding.sessionId, binding, compact, turn, seed, 'PREPARE', 'SUMMARY')).rejects.toThrow('compaction_capacity');
    expect(phases).toEqual([]); changeAfterProvider = false; preflightGeneration = 'stable';
    const malformedReturned: Array<(value: CompactionResult, entry: CompactionEntry) => unknown> = [
      value => ({ ...value, details: null }),
      value => ({ ...value, details: [] }),
      value => { const details = value.details as Record<string, unknown>; return { ...value, details: { checkpointId: details['checkpointId'], checkpoint: details['checkpoint'] } }; },
      value => ({ ...value, details: { ...(value.details as Record<string, unknown>), version: 2 } }),
      value => ({ ...value, details: { ...(value.details as Record<string, unknown>), checkpointId: 'f'.repeat(32) } }),
      value => ({ ...value, details: { ...(value.details as Record<string, unknown>), checkpoint: { ...seed, goals: ['RETURNED_MISMATCH'] } } })
    ];
    for (const variant of malformedReturned) {
      phases.length = 0; returnedVariant = variant;
      await expect(extension.run(binding.sessionId, binding, compact, turn, seed, 'PREPARE', 'SUMMARY')).rejects.toThrow('compaction_failed');
      expect(phases).toEqual(['prepared', 'compact-call']);
    }
    phases.length = 0; returnedVariant = value => value;
    const result = await extension.run(binding.sessionId, binding, compact, turn, seed, 'PREPARE', 'SUMMARY');
    const preparedGeneration = (record as CheckpointRecord | undefined)?.recovery?.generation;
    const fileStatus = statSync(binding.sessionFile, { bigint: true });
    expect(preparedGeneration).toEqual({ device: fileStatus.dev.toString(), inode: fileStatus.ino.toString(),
      size: fileStatus.size.toString(), mtimeNs: fileStatus.mtimeNs.toString(), ctimeNs: fileStatus.ctimeNs.toString() });
    expect(phases).toEqual(['prepared', 'compact-call']); await result.compacted(); await result.restored(); expect(phases).toEqual(['prepared', 'compact-call', 'compacted', 'restored']);
    unlinkSync(binding.sessionFile);
  });
});

interface WorkerFixture {
  repository: WorkerRepository; options: WorkerOptions; session: PiSessionPort & { history(): Promise<readonly never[]> };
  effects: string[]; compactCalls(): number; latest(value: unknown): void; recover(value: CheckpointV1): void;
  setFresh(value: boolean): void; setMapped(value: PiSessionPort['binding']): void; setSequence(value: number): void; setNative(value: readonly unknown[]): void;
}
function workerFixture(c = chat, initialUsage: ReturnType<PiSessionPort['usage']> = { tokens: 9000, contextWindow: 10000, percent: 90 },
  promptUsage: ReturnType<PiSessionPort['usage']> = { tokens: 1000, contextWindow: 10000, percent: 10 }, initialFresh = true): WorkerFixture {
  const effects: string[] = []; let paused: string | undefined, usage = initialUsage, fresh = initialFresh;
  let latest: unknown, compacts = 0, began = false, sequence = 0, native: readonly unknown[] = [];
  const b: PiSessionPort['binding'] = { ...binding, canonicalChatId: c, sessionId: `SESSION_${c}` }; let mapped: PiSessionPort['binding'] = b;
  const repository: WorkerRepository & { checkpoints: { latest(): Promise<unknown>; transition(_work: ClaimedWork, id: string, phase: 'compacted' | 'restored', entry: string): Promise<boolean> }; nativeJournal: { list(): Promise<readonly unknown[]> } } = {
    renew: async value => value.ownership, checkWork: async () => true,
    readChat: async () => ({ canonicalChatId: c, lastCompletedSequence: sequence, session: mapped, ...(paused ? { pause: paused as 'usage_unknown' } : {}) }), bindSession: async () => true,
    turnState: async () => began ? 'started' : 'fresh', beginTurn: async () => { effects.push('begin'); began = true; return true; },
    pause: async (_value, reason) => { paused = reason; effects.push(`pause:${reason}`); return true; }, complete: async () => { effects.push('complete'); return true; },
    checkpoints: { latest: async () => latest, transition: async (_value, id, phase, entry) => { const prior = latest as { checkpointId: string; checkpoint: CheckpointV1 } | undefined; if (!prior || prior.checkpointId !== id) return false; latest = { ...prior, phase, compactionEntryId: entry }; effects.push(`transition:${phase}`); return true; } }, nativeJournal: { list: async () => native }
  };
  const session = { binding: b, isFresh: () => fresh, usage: () => usage, history: async () => [], compact: async (_instructions: string, seed: CheckpointV1) => { effects.push('compact'); compacts++; latest = { checkpointId: 'c'.repeat(32), phase: 'restored', checkpoint: seed, compactionEntryId: 'SYNTHETIC_COMPACTION' }; usage = { tokens: null, contextWindow: 10000, percent: null }; effects.push('usage:null'); return { entryId: 'SYNTHETIC_COMPACTION', checkpointId: 'c'.repeat(32), checkpoint: seed }; },
    // Test-only mutation lives inside the prompt implementation and represents
    // usage supplied by the actual settled assistant response; production has
    // no setUsage mechanism and no synthetic prompt.
    prompt: async () => { effects.push('prompt'); fresh = false; usage = promptUsage; effects.push(promptUsage?.tokens === null ? 'prompt-usage:null' : 'prompt-usage:valid'); },
    appendCompletion: async marker => { effects.push('marker'); return { entryId: 'ENTRY', session: b, marker }; }, dispose: async () => { effects.push('dispose'); } } as PiSessionPort & { history(): Promise<readonly never[]> };
  const compactor = createCompactor({ config, repository: repository as never, incoming: () => ({ event: 0, tools: 0, prompt: 0 }), readTemplate: async () => { effects.push('template'); return 'SYNTHETIC'; } });
  const options: WorkerOptions = { leaseMs: 1000, renewMs: 100000, appCurrent: () => true, instructions: () => 'SYNTHETIC', wait: { wait: async (_ms, signal) => new Promise<void>(resolve => signal.addEventListener('abort', () => resolve(), { once: true })) }, diagnostics: { emit: value => effects.push(`diagnostic:${value.class}`) }, host: { findEvent: async () => undefined, classifyOwn: async () => 'manual', open: async () => session, close: async () => {} }, turns: { attach: async () => {}, prepare: compactor.prepare, run: async (_s, _ctx, prompt) => prompt(), afterPrompt: compactor.afterPrompt, confirmed: async () => [] } };
  return { repository, options, session, effects, compactCalls: () => compacts, latest: value => { latest = value; }, recover: value => { Object.assign(session, { reconcileCheckpoint: async () => { effects.push('reconcile'); return { entryId: 'RECOVERED_ENTRY', checkpoint: value }; } }); },
    setFresh: value => { fresh = value; }, setMapped: value => { mapped = value; }, setSequence: value => { sequence = value; }, setNative: value => { native = value; } };
}

describe('locked Slice 7 journal-turn composition compatibility', () => {
  const journal = { confirmed: async () => [] };
  it('keeps the locked two-argument call compile-safe and defaults afterPrompt to an async no-op', async () => {
    const effects: string[] = [];
    const turns: TurnPorts = createJournalTurns(journal, async () => { effects.push('prepare'); });
    await turns.prepare({} as PiSessionPort, turn);
    await turns.afterPrompt?.({} as PiSessionPort, turn, { usageRequiredAfterPrompt: false });
    expect(effects).toEqual(['prepare']);
  });
  it('runs an injected Slice 8 afterPrompt hook only after prepare and the real prompt', async () => {
    const effects: string[] = [];
    const turns = createJournalTurns(journal,
      async () => { effects.push('prepare'); return { usageRequiredAfterPrompt: true }; },
      async () => { effects.push('afterPrompt'); });
    const preparation = await turns.prepare({} as PiSessionPort, turn) ?? { usageRequiredAfterPrompt: false };
    await turns.run({} as PiSessionPort, turn, async () => { effects.push('prompt'); });
    await turns.afterPrompt?.({} as PiSessionPort, turn, preparation);
    expect(effects).toEqual(['prepare', 'prompt', 'afterPrompt']);
  });
});

describe('merged worker pre-compaction and post-prompt accounting', () => {
  it('compacts exactly once, accepts expected null, runs current prompt, then completes from its valid usage', async () => {
    const f = workerFixture(); await runChatWorker(f.repository, work(), f.options);
    expect(f.compactCalls()).toBe(1);
    expect(f.effects).toEqual(expect.arrayContaining(['compact', 'usage:null', 'begin', 'prompt', 'prompt-usage:valid', 'marker', 'complete']));
    expect(f.effects.indexOf('usage:null')).toBeLessThan(f.effects.indexOf('prompt'));
    expect(f.effects.indexOf('prompt-usage:valid')).toBeLessThan(f.effects.indexOf('marker'));
    expect(f.effects).not.toContain('pause:usage_unknown');

    // Separate settlement scenario: the actual response still leaves usage null.
    const missing = workerFixture(chat, { tokens: 9000, contextWindow: 10000, percent: 90 }, { tokens: null, contextWindow: 10000, percent: null });
    await runChatWorker(missing.repository, work(), missing.options);
    expect(missing.effects).toEqual(expect.arrayContaining(['compact', 'usage:null', 'begin', 'prompt', 'prompt-usage:null', 'pause:usage_unknown']));
    expect(missing.effects).not.toContain('marker'); expect(missing.effects).not.toContain('complete');
  });
  it('allows unknown usage only for the exact fresh first turn and requires known usage after its real prompt', async () => {
    const unknown = { tokens: null, contextWindow: 10000, percent: null } as const;
    const allowed = workerFixture(chat, unknown); await runChatWorker(allowed.repository, work(), allowed.options);
    expect(allowed.compactCalls()).toBe(0); expect(allowed.effects).not.toContain('template');
    expect(allowed.effects).toEqual(expect.arrayContaining(['begin', 'prompt', 'prompt-usage:valid', 'marker', 'complete']));

    const confirmedOnly = workerFixture(chat, unknown);
    confirmedOnly.setNative([{ status: 'confirmed', attemptId: outcome.attemptId, tool: outcome.tool, destination: outcome.destination, messageId: outcome.messageId }]);
    await runChatWorker(confirmedOnly.repository, work(), confirmedOnly.options); expect(confirmedOnly.effects).toContain('complete');

    const postPromptUnknown = workerFixture(chat, unknown, unknown); await runChatWorker(postPromptUnknown.repository, work(), postPromptUnknown.options);
    expect(postPromptUnknown.effects).toEqual(expect.arrayContaining(['begin', 'prompt', 'prompt-usage:null', 'pause:usage_unknown']));
    expect(postPromptUnknown.effects).not.toContain('marker'); expect(postPromptUnknown.effects).not.toContain('complete');
  });
  it('rejects unknown usage for non-fresh or checkpointed sessions before begin/prompt', async () => {
    const unknown = { tokens: null, contextWindow: 10000, percent: null } as const;
    const nonFresh = workerFixture(chat, unknown, undefined, false);
    await runChatWorker(nonFresh.repository, work(), nonFresh.options);
    expect(nonFresh.effects).toEqual(expect.arrayContaining(['pause:usage_unknown'])); expect(nonFresh.effects).not.toContain('begin'); expect(nonFresh.effects).not.toContain('prompt');

    const checkpointed = workerFixture(chat, unknown); const value = checkpoint();
    checkpointed.latest({ checkpointId: 'c'.repeat(32), phase: 'restored', checkpoint: value, compactionEntryId: 'RECOVERED_ENTRY' }); checkpointed.recover(value);
    await runChatWorker(checkpointed.repository, work(), checkpointed.options);
    expect(checkpointed.effects).toEqual(expect.arrayContaining(['reconcile', 'pause:usage_unknown'])); expect(checkpointed.effects).not.toContain('begin'); expect(checkpointed.effects).not.toContain('prompt');
  });
  it('rejects fresh unknown usage when durable mapping, sequence, or native journal proof is invalid', async () => {
    const unknown = { tokens: null, contextWindow: 10000, percent: null } as const;
    const mismatches = [
      () => { const f = workerFixture(chat, unknown); f.setMapped({ ...f.session.binding, sessionFile: '/tmp/OTHER.jsonl' }); return f; },
      () => { const f = workerFixture(chat, unknown); f.setSequence(1); return f; },
      () => { const f = workerFixture(chat, unknown); f.setNative([{ status: 'attempting' }]); return f; }
    ];
    for (const make of mismatches) {
      const f = make();
      await expect.soft(f.options.turns.prepare(f.session, turn)).rejects.toMatchObject({ code: 'checkpoint_invalid' });
      expect.soft(f.effects).not.toContain('begin'); expect.soft(f.effects).not.toContain('prompt');
    }
  });
  it('runs isolated pre-prompt failures with exact pauses before begin/prompt', async () => {
    for (const [reason, make] of [
      ['usage_unknown', () => workerFixture(chat, { tokens: 1, contextWindow: 0, percent: 1 }, undefined, false)],
      ['checkpoint_invalid', () => { const f = workerFixture(); Object.assign(f.session, { history: async () => { throw new ControllerError('checkpoint_invalid'); } }); return f; }],
      ['compaction_capacity', () => { const f = workerFixture(); Object.assign(f.session, { compact: async () => { throw new ControllerError('compaction_capacity'); } }); return f; }],
      ['compaction_failed', () => { const f = workerFixture(); Object.assign(f.session, { compact: async () => { throw new ControllerError('compaction_failed'); } }); return f; }],
      ['restore_failed', () => { const f = workerFixture(); f.latest({ checkpointId: 'c'.repeat(32), phase: 'prepared', checkpoint: checkpoint() }); return f; }]
    ] as const) {
      const f = make(); await runChatWorker(f.repository, work(), f.options);
      expect.soft(f.effects.filter(effect => effect.startsWith('pause:'))).toEqual([`pause:${reason}`]);
      expect.soft(f.effects).not.toContain('begin'); expect.soft(f.effects).not.toContain('prompt');
    }
  });
  it('a prepared-only record without exact active-branch evidence pauses without another model/compact/prompt', async () => {
    const f = workerFixture(); f.latest({ checkpointId: 'c'.repeat(32), phase: 'prepared', checkpoint: checkpoint() }); await runChatWorker(f.repository, work(), f.options);
    expect(f.compactCalls()).toBe(0); expect(f.effects).not.toContain('template'); expect(f.effects).not.toContain('begin'); expect(f.effects).not.toContain('prompt'); expect(f.effects).toContain('pause:restore_failed');
  });
  it('reconstructs prepared, compacted, and restored Redis state from exact active-branch evidence without a second compaction', async () => {
    for (const phase of ['prepared', 'compacted', 'restored'] as const) {
      const f = workerFixture(chat, { tokens: 1000, contextWindow: 10000, percent: 10 }); const value = checkpoint();
      f.latest({ checkpointId: 'c'.repeat(32), phase, checkpoint: value, ...(phase === 'prepared' ? {} : { compactionEntryId: 'RECOVERED_ENTRY' }) }); f.recover(value);
      await runChatWorker(f.repository, work(), f.options);
      expect(f.compactCalls()).toBe(0); expect(f.effects).toContain('reconcile'); expect(f.effects).toContain('prompt');
      expect(f.effects.includes('transition:compacted')).toBe(phase === 'prepared');
      expect(f.effects.includes('transition:restored')).toBe(phase !== 'restored');
    }
  });
  it('treats an aborted checkpoint as terminal history and retries through current usage without reconciliation', async () => {
    const f = workerFixture(chat, { tokens: 1000, contextWindow: 10000, percent: 10 });
    f.latest({ checkpointId: 'c'.repeat(32), phase: 'aborted', checkpoint: checkpoint() });
    await runChatWorker(f.repository, work(), f.options);
    expect(f.effects).not.toContain('reconcile');
    expect(f.effects).toContain('prompt');
    expect(f.effects).toContain('complete');
  });
  it('an unrelated chat progresses while the affected chat fails pre-prompt usage', async () => {
    const bad = workerFixture(chat, { tokens: 1, contextWindow: 0, percent: 1 }, undefined, false); const good = workerFixture(other, { tokens: 1000, contextWindow: 10000, percent: 10 });
    await Promise.all([runChatWorker(bad.repository, work(), bad.options), runChatWorker(good.repository, work(other), good.options)]);
    expect(bad.effects).not.toContain('prompt'); expect(good.effects).toContain('complete');
  });
});

describe('session-host durability barrier', () => {
  it('fsyncs file and parent, reopens the exact file, reads checkpoint details, then marks compacted/restored', async () => {
    const root = await mkdtemp(join(tmpdir(), 'slice8-host-')); const rows: Array<{ chat: typeof chat; binding?: typeof binding }> = [{ chat }];
    const freshReadbackScenarios = [
      'zero matching claims', 'duplicate exact-ID active-chain claims', 'omitted fromHook', 'fromHook false',
      'null details', 'array details', 'missing version', 'wrong version', 'wrong checkpoint ID',
      'valid-envelope checkpoint createdAt mismatch', 'exact-summary mismatch'
    ] as const;
    type FreshReadbackScenario = typeof freshReadbackScenarios[number];
    let freshReadback: { scenario: FreshReadbackScenario; checkpointId: string; entryId: string } | undefined;
    const calls: string[] = []; let opens = 0; let connectedManager: (SdkManager & { getBranch(): readonly unknown[] }) | undefined;
    const listeners = new Set<(value: { type: 'agent_settled'; aborted: boolean }) => void>();
    const makeManager = (file: string, header: Record<string, unknown>, entries: Record<string, unknown>[]): SdkManager & { getBranch(): readonly unknown[] } => {
      const activeBranch = () => { const byId = new Map(entries.map(entry => [entry['id'], entry])); const branch: Record<string, unknown>[] = [], seen = new Set<unknown>(); let current = entries.at(-1); while (current && !seen.has(current['id'])) { seen.add(current['id']); branch.push(current); const parent = current['parentId']; current = typeof parent === 'string' ? byId.get(parent) : undefined; } return branch.reverse(); };
      return { getSessionId: () => header['id'] as string, getSessionFile: () => file, getHeader: () => header, getEntries: () => entries, getBranch: activeBranch, appendCustomEntry: (type, data) => { const id = randomUUID(); const entry = { type: 'custom', customType: type, data, id, parentId: entries.at(-1)?.['id'] ?? null, timestamp: '2033-05-18T03:33:20Z' }; entries.push(entry); try { readFileSync(file); appendFileSync(file, JSON.stringify(entry) + '\n'); } catch { /* create is memory-only */ } return id; } };
    };
    const sdk: SdkPort = { create: (cwd, dir) => makeManager(join(dir, 'session.jsonl'), { type: 'session', version: 3, id: 'HOST_SESSION', cwd, timestamp: '2033-05-18T03:33:20Z' }, []), open: file => { opens++; const rows = readFileSync(file, 'utf8').trimEnd().split('\n').map(line => JSON.parse(line) as Record<string, unknown>); return makeManager(file, rows.shift()!, rows); }, connect: async manager => { connectedManager = manager as SdkManager & { getBranch(): readonly unknown[] }; return { isIdle: true, setAutoCompactionEnabled: () => {}, bindExtensions: async () => {}, prompt: async () => { for (const listener of listeners) listener({ type: 'agent_settled', aborted: false }); }, subscribe: listener => { listeners.add(listener as never); return () => {}; }, waitForIdle: async () => {}, dispose: () => {}, getContextUsage: () => ({ tokens: 1, contextWindow: 10000, percent: 1 }), compact: async () => { throw new Error(); }, getAllTools: () => [], getActiveToolNames: () => [], getCallableToolNames: () => [], getToolDefinition: () => undefined, extensionRunner: {} as never } as SdkSession; }, ready: async () => {} };
    const makeIndex = () => createCompletionIndex(root, { groups: [chat], operatorJids: [] }, { list: async () => rows }, async () => false);
    const cp = checkpoint();
    const makeHost = (completionIndex: ReturnType<typeof createCompletionIndex>) => createSessionHost({ cwd: '/tmp', root, sdk, index: completionIndex, current: async () => {}, bind: async (_w, value) => { rows[0]!.binding = value as typeof binding; return true; }, checkpointMaxBytes: 65536, compact: async (_raw, _context, _instructions, value) => {
      calls.push('compact');
      const scenario = freshReadback?.scenario, checkpointId = freshReadback?.checkpointId ?? 'c'.repeat(32), entryId = freshReadback?.entryId ?? 'COMPACT_ENTRY';
      const file = rows[0]!.binding!.sessionFile; const parent = JSON.parse(readFileSync(file, 'utf8').trimEnd().split('\n').at(-1)!) as { id: string };
      const appendClaim = (id: string, details: unknown, fromHook?: boolean, parentId = parent.id, summary = 'INVALID') => appendFileSync(file, JSON.stringify({ type: 'compaction', id, parentId, timestamp: '2033-05-18T03:33:22Z', summary, firstKeptEntryId: parent.id, tokensBefore: 1, ...(fromHook === undefined ? {} : { fromHook }), details }) + '\n');
      const validDetails = { version: 1, checkpointId, checkpoint: value };
      const validSummary = renderCheckpointSummary(value, checkpointId, checkpointIdentity(value), 65536);
      if (scenario === undefined) {
        appendClaim('ABANDONED_CONFLICT', { version: 1, checkpointId, checkpoint: { ...value, goals: ['conflicting abandoned evidence'] } }, true, parent.id, 'ABANDONED');
        appendClaim(entryId, validDetails, true, parent.id, validSummary);
      } else {
        switch (scenario) {
          case 'zero matching claims': break;
          case 'duplicate exact-ID active-chain claims':
            appendClaim(`${entryId}_A`, validDetails, true, parent.id, validSummary);
            appendClaim(entryId, validDetails, true, `${entryId}_A`, validSummary);
            break;
          case 'omitted fromHook': appendClaim(entryId, validDetails, undefined, parent.id, validSummary); break;
          case 'fromHook false': appendClaim(entryId, validDetails, false, parent.id, validSummary); break;
          case 'null details': appendClaim(entryId, null, true); break;
          case 'array details': appendClaim(entryId, [], true); break;
          case 'missing version': appendClaim(entryId, { checkpointId, checkpoint: value }, true); break;
          case 'wrong version': appendClaim(entryId, { ...validDetails, version: 2 }, true); break;
          case 'wrong checkpoint ID': appendClaim(entryId, { ...validDetails, checkpointId: 'f'.repeat(32) }, true); break;
          case 'valid-envelope checkpoint createdAt mismatch': {
            const persisted = { ...value, createdAt: '2033-05-18T03:33:23Z' };
            appendClaim(entryId, { version: 1, checkpointId, checkpoint: persisted }, true, parent.id,
              renderCheckpointSummary(persisted, checkpointId, checkpointIdentity(persisted), 65536));
            break;
          }
          case 'exact-summary mismatch': appendClaim(entryId, validDetails, true, parent.id, `${validSummary}\nMISMATCH`); break;
        }
      }
      return { entryId, checkpointId, checkpoint: value, compacted: async () => { calls.push('compacted'); }, restored: async () => { calls.push('restored'); } };
    } });
    const index = makeIndex(), host = makeHost(index);
    const handle = await open(root, 'r'); const prototype = Object.getPrototypeOf(handle) as { sync(): Promise<void> }; await handle.close(); const original = prototype.sync; const synced: string[] = []; const spy = vi.spyOn(prototype, 'sync').mockImplementation(async function(this: typeof handle) { synced.push((await this.stat()).isFile() ? 'file' : 'parent'); await original.call(this); });
    try {
      const w = work(), session = await host.open(w); const context: TurnContext = { current: async () => {}, work: () => w };
      await host.attach(session, context);
      expect(session.isFresh()).toBe(true); // Binding custom entry is allowed.
      const branchEntries = connectedManager!.getEntries() as unknown as Record<string, unknown>[];
      const parentId = branchEntries.at(-1)?.['id'] as string;
      const forbidden = [
        { type: 'message', id: 'FRESH_MESSAGE', parentId, timestamp: '2033-05-18T03:33:21Z', message: { role: 'user', content: 'SYNTHETIC', timestamp: 1 } },
        { type: 'compaction', id: 'FRESH_COMPACTION', parentId, timestamp: '2033-05-18T03:33:21Z' },
        { type: 'custom', id: 'FRESH_COMPLETION', parentId, timestamp: '2033-05-18T03:33:21Z', customType: COMPLETION_TYPE, data: {} },
        { type: 'custom', id: 'FRESH_MUTATION', parentId, timestamp: '2033-05-18T03:33:21Z', customType: MUTATION_TYPE, data: {} }
      ];
      for (const entry of forbidden) {
        branchEntries.push(entry); expect.soft(session.isFresh(), String(entry['id'])).toBe(false); branchEntries.pop();
        expect.soft(session.isFresh(), `${String(entry['id'])}:removed`).toBe(true);
      }
      synced.length = 0;
      const result = await session.compact('SYNTHETIC', cp); expect(result.checkpoint).toEqual(cp);
      const recovery = session as PiSessionPort & { reconcileCheckpoint(record: { checkpointId: string; phase: 'prepared' | 'restored'; checkpoint: CheckpointV1; compactionEntryId?: string }): Promise<{ entryId: string; checkpoint: CheckpointV1 }> };
      expect(await recovery.reconcileCheckpoint({ checkpointId: 'c'.repeat(32), phase: 'prepared', checkpoint: cp })).toEqual({ entryId: 'COMPACT_ENTRY', checkpoint: cp });
      expect(await recovery.reconcileCheckpoint({ checkpointId: 'c'.repeat(32), phase: 'restored', checkpoint: cp, compactionEntryId: 'COMPACT_ENTRY' })).toEqual({ entryId: 'COMPACT_ENTRY', checkpoint: cp });
      expect(synced).toEqual(['file', 'parent', 'file', 'parent', 'file', 'parent', 'file', 'parent']); expect(opens).toBe(4);
      const file = rows[0]!.binding!.sessionFile, validLifecycleBytes = readFileSync(file), opensBeforeFreshMatrix = opens;
      for (const [scenarioIndex, scenario] of freshReadbackScenarios.entries()) {
        freshReadback = undefined;
        writeFileSync(file, validLifecycleBytes);
        const freshIndex = makeIndex(), freshHost = makeHost(freshIndex);
        let freshSession: PiSessionPort | undefined;
        try {
          freshSession = await freshHost.open(w, rows[0]!.binding);
          await freshHost.attach(freshSession, context);
          freshReadback = { scenario, checkpointId: '0123456789a'[scenarioIndex]!.repeat(32), entryId: `FRESH_${scenarioIndex}` };
          await expect.soft(freshSession.compact('SYNTHETIC', cp)).rejects.toMatchObject({ code: 'compaction_failed' });
        } finally {
          freshReadback = undefined;
          try { await freshSession?.dispose(); }
          finally {
            try { await freshHost.close(); }
            finally { writeFileSync(file, validLifecycleBytes); }
          }
        }
        expect.soft(readFileSync(file)).toEqual(validLifecycleBytes);
      }
      expect(opens).toBe(opensBeforeFreshMatrix + freshReadbackScenarios.length * 2);
      const appendClaim = (entryId: string, details: unknown, fromHook?: boolean, parentId = 'COMPACT_ENTRY', summary = 'INVALID') => appendFileSync(file, JSON.stringify({ type: 'compaction', id: entryId, parentId, timestamp: '2033-05-18T03:33:24Z', summary, firstKeptEntryId: 'COMPACT_ENTRY', tokensBefore: 1, ...(fromHook === undefined ? {} : { fromHook }), details }) + '\n');
      const invalid = [
        ['a'.repeat(32), { version: 1, checkpointId: 'a'.repeat(32), checkpoint: cp }, undefined],
        ['b'.repeat(32), { version: 1, checkpointId: 'b'.repeat(32), checkpoint: cp }, false],
        ['d'.repeat(32), null, true],
        ['e'.repeat(32), [], true],
        ['f'.repeat(32), { checkpointId: 'f'.repeat(32), checkpoint: cp }, true],
        ['1'.repeat(32), { version: 2, checkpointId: '1'.repeat(32), checkpoint: cp }, true],
        ['2'.repeat(32), { version: 1, checkpointId: '3'.repeat(32), checkpoint: cp }, true],
        ['4'.repeat(32), { version: 1, checkpointId: '4'.repeat(32), checkpoint: { ...cp, createdAt: '2033-05-18T03:33:23Z' } }, true],
        ['6'.repeat(32), { version: 1, checkpointId: '6'.repeat(32), checkpoint: cp }, true]
      ] as const;
      for (const [checkpointId, details, fromHook] of invalid) {
        appendClaim(`INVALID_${checkpointId[0]}`, details, fromHook);
        await expect(recovery.reconcileCheckpoint({ checkpointId, phase: 'prepared', checkpoint: cp })).rejects.toMatchObject({ code: 'restore_failed' });
      }
      const duplicateId = '5'.repeat(32), duplicateDetails = { version: 1, checkpointId: duplicateId, checkpoint: cp };
      const duplicateSummary = renderCheckpointSummary(cp, duplicateId, checkpointIdentity(cp), 65536);
      appendClaim('DUPLICATE_A', duplicateDetails, true, 'COMPACT_ENTRY', duplicateSummary);
      // Both exact-ID claims are on the active parent chain. The second is a
      // child of the first; ABANDONED_CONFLICT remains a sibling outside it.
      appendClaim('DUPLICATE_B', duplicateDetails, true, 'DUPLICATE_A', duplicateSummary);
      await expect(recovery.reconcileCheckpoint({ checkpointId: duplicateId, phase: 'prepared', checkpoint: cp })).rejects.toMatchObject({ code: 'restore_failed' });
      expect(opens).toBe(opensBeforeFreshMatrix + freshReadbackScenarios.length * 2 + invalid.length + 1);
      expect(calls.filter(call => call === 'compact')).toHaveLength(1 + freshReadbackScenarios.length);
      expect(calls.filter(call => call === 'compacted')).toHaveLength(1 + freshReadbackScenarios.length);
      expect(calls.filter(call => call === 'restored')).toHaveLength(1);
      await session.prompt(w.event, 'SYNTHETIC'); expect(session.isFresh()).toBe(false);
      await session.dispose();
    } finally { spy.mockRestore(); await host.close(); await rm(root, { recursive: true, force: true }); }
  });
});
