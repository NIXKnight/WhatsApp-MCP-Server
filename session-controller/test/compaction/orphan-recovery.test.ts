import { mkdtemp, rm, utimes, writeFile } from 'node:fs/promises';
import { join } from 'node:path';
import { afterEach, describe, expect, it } from 'vitest';
import {
  canonicalChatId, eventId, fingerprintEvent,
  type CheckpointRecord, type CheckpointV1, type NotificationV2, type SessionBinding
} from '../../src/domain.js';
import { checkpointIdentity, renderCheckpointSummary } from '../../src/compaction/checkpoint.js';
import { provePreparedCompactionOrphan } from '../../src/compaction/orphan-recovery.js';
import { legacyCompactionRepairDigest, type CompactionRepairClaim } from '../../src/redis/repository.js';
import { BINDING_TYPE, COMPLETION_TYPE } from '../../src/pi/completion-index.js';
import { MUTATION_TYPE } from '../../src/pi/tool-journal-extension.js';

const chat = canonicalChatId('100@g.us');
const pausedEvent = eventId('a'.repeat(32));
const event: NotificationV2 = {
  schema_version: 2, event_type: 'whatsapp.message.received', event_id: pausedEvent,
  message_id: 'TEST_MESSAGE', event_timestamp: '2033-05-18T03:33:20Z', received_at: '2033-05-18T03:33:21Z',
  chat_sequence: 9, sender_jid: '500@s.whatsapp.net', is_from_me: false,
  chat_kind: 'group', chat_jid: chat, canonical_chat_jid: chat
};
const checkpoint: CheckpointV1 = {
  version: 1 as const, canonicalChatId: chat, lastCompletedSequence: 8,
  policyVersion: 'test-v1', templateVersion: 'test-v1', createdAt: '2033-05-18T03:33:24Z',
  goals: [], decisions: [], constraints: [], verifiedFacts: [], pendingWork: [],
  approvalBoundaries: [], nextSteps: [], confirmedToolOutcomes: []
};
const prepared: Extract<CheckpointRecord, { phase: 'prepared' }> = {
  checkpointId: 'b'.repeat(32), phase: 'prepared', checkpoint
};
const timestamp = '2033-05-18T03:33:23Z';
let roots: string[] = [];
function custom(id: string, parentId: string | null, customType: string, data: unknown) {
  return { type: 'custom', id, parentId, timestamp, customType, data };
}
async function fixture(extra: readonly Record<string, unknown>[] = [], bindingOverride?: Partial<SessionBinding>, entryTimestamp = timestamp) {
  const root = await mkdtemp('/tmp/whatsapp-orphan-proof-'); roots.push(root);
  const directory = join(root, 'owned');
  const { mkdir } = await import('node:fs/promises'); await mkdir(directory);
  const binding: SessionBinding = {
    canonicalChatId: chat, sessionId: 'TEST_SESSION', sessionFile: join(directory, 'session.jsonl'), ...bindingOverride
  };
  const rows: Record<string, unknown>[] = [
    { type: 'session', version: 3, id: 'TEST_SESSION', cwd: '/tmp/synthetic-project', timestamp: entryTimestamp },
    { ...custom('BINDING', null, BINDING_TYPE, binding), timestamp: entryTimestamp },
    ...extra
  ];
  await writeFile(binding.sessionFile, rows.map(row => JSON.stringify(row)).join('\n') + '\n', { mode: 0o600 });
  return { root, binding };
}
function compaction(id: string, parentId: string, checkpointId: string, value = checkpoint) {
  const summary = renderCheckpointSummary(value, checkpointId, checkpointIdentity(value), 65536);
  return {
    type: 'compaction', id, parentId, timestamp, summary, firstKeptEntryId: 'BINDING', tokensBefore: 50000,
    fromHook: true, details: { version: 1, checkpointId, checkpoint: value }
  };
}
afterEach(async () => { await Promise.all(roots.splice(0).map(root => rm(root, { recursive: true, force: true }))); });

describe('strict read-only prepared checkpoint orphan proof', () => {
  it('accepts the exact mapped post-compaction branch only when no current evidence exists', async () => {
    const historical = compaction('OLD_COMPACTION', 'BINDING', 'c'.repeat(32));
    const f = await fixture([historical]);
    await expect(provePreparedCompactionOrphan(f.root, f.binding, pausedEvent, prepared, 65536)).resolves.toMatchObject({
      device: expect.any(String), inode: expect.any(String), size: expect.any(String),
      mtimeNs: expect.any(String), ctimeNs: expect.any(String)
    });
  });

  it('binds recovery to the exact preparation-time generation and rejects any later generation', async () => {
    const f = await fixture();
    const observed = await provePreparedCompactionOrphan(f.root, f.binding, pausedEvent, prepared, 65536);
    const bound: Extract<CheckpointRecord, { phase: 'prepared' }> = { ...prepared, recovery: {
      version: 1, eventId: pausedEvent, eventFingerprint: fingerprintEvent(event), streamId: '1-0',
      session: f.binding, generation: observed
    } };
    await expect(provePreparedCompactionOrphan(f.root, f.binding, pausedEvent, bound, 65536)).resolves.toEqual(observed);
    const mismatched = { ...bound, recovery: { ...bound.recovery!, generation: {
      ...observed, size: String(BigInt(observed.size) + 1n)
    } } };
    await expect(provePreparedCompactionOrphan(f.root, f.binding, pausedEvent, mismatched, 65536)).rejects.toThrow('restore_failed');
    await writeFile(f.binding.sessionFile, JSON.stringify(custom('LATER_GENERATION', 'BINDING', 'synthetic.noncontroller', {})) + '\n', { flag: 'a' });
    await expect(provePreparedCompactionOrphan(f.root, f.binding, pausedEvent, bound, 65536)).rejects.toThrow('restore_failed');
  });

  it('changes the legacy manifest digest when only the stable file generation changes', async () => {
    const f = await fixture();
    const generation = await provePreparedCompactionOrphan(f.root, f.binding, pausedEvent, prepared, 65536);
    const fence = { owner: 'e'.repeat(32), epoch: '1', expiresAtMs: 1 };
    const claim: CompactionRepairClaim = { work: { streamId: '1-0', event, ownership: { app: fence, chat: fence } },
      binding: f.binding, checkpoint: prepared };
    const first = legacyCompactionRepairDigest(claim, generation);
    const second = legacyCompactionRepairDigest(claim, { ...generation, ctimeNs: String(BigInt(generation.ctimeNs) + 1n) });
    expect(first).not.toBe(second);
  });

  it('denies legacy proof unless both filesystem mtime and ctime strictly precede createdAt', async () => {
    const oldTimestamp = '1999-05-18T03:33:23Z';
    const oldPrepared = { ...prepared, checkpoint: { ...checkpoint, createdAt: '2000-05-18T03:33:24Z' } };
    const ctimeAfter = await fixture([], undefined, oldTimestamp);
    await expect(provePreparedCompactionOrphan(ctimeAfter.root, ctimeAfter.binding, pausedEvent, oldPrepared, 65536))
      .rejects.toThrow('restore_failed');
    const mtimeEqual = await fixture();
    await utimes(mtimeEqual.binding.sessionFile, new Date(checkpoint.createdAt), new Date(checkpoint.createdAt));
    await expect(provePreparedCompactionOrphan(mtimeEqual.root, mtimeEqual.binding, pausedEvent, prepared, 65536))
      .rejects.toThrow('restore_failed');
  });

  it('fails closed for a turn marker, mutation journal, or wrong exact session', async () => {
    const completion = custom('COMPLETION', 'BINDING', COMPLETION_TYPE, {
      version: 1, eventId: pausedEvent, eventFingerprint: fingerprintEvent(event), canonicalChatId: chat,
      chatSequence: 9, completion: 'turn', completedAt: timestamp, confirmedSends: []
    });
    const mutation = custom('MUTATION', 'BINDING', MUTATION_TYPE, {
      version: 1, eventFingerprint: fingerprintEvent(event),
      session: { canonicalChatId: chat, sessionId: 'TEST_SESSION', sessionFile: 'REPLACED' },
      state: {
        version: 1, attemptId: 'd'.repeat(32), eventId: pausedEvent, canonicalChatId: chat,
        toolCallId: 'TEST_CALL', tool: 'send_message', destination: chat, startedAt: timestamp,
        ownership: {
          app: { owner: 'e'.repeat(32), epoch: '1', expiresAtMs: 1 },
          chat: { owner: 'f'.repeat(32), epoch: '1', expiresAtMs: 1 }
        }, status: 'attempting'
      }
    });
    const turn = await fixture([completion]);
    await expect(provePreparedCompactionOrphan(turn.root, turn.binding, pausedEvent, prepared, 65536)).rejects.toThrow('restore_failed');
    const journal = await fixture([]);
    mutation.data = { ...(mutation.data as Record<string, unknown>), session: journal.binding };
    await writeFile(journal.binding.sessionFile, [
      { type: 'session', version: 3, id: journal.binding.sessionId, cwd: '/tmp/synthetic-project', timestamp },
      custom('BINDING', null, BINDING_TYPE, journal.binding), mutation
    ].map(row => JSON.stringify(row)).join('\n') + '\n');
    await expect(provePreparedCompactionOrphan(journal.root, journal.binding, pausedEvent, prepared, 65536)).rejects.toThrow('restore_failed');
    const wrong = await fixture([], { sessionId: 'WRONG_SESSION' });
    await expect(provePreparedCompactionOrphan(wrong.root, wrong.binding, pausedEvent, prepared, 65536)).rejects.toThrow('restore_failed');
  });

  it('rejects message, custom, and branch activity appended at or after preparation', async () => {
    const post = '2033-05-18T03:33:25Z';
    const entries = [
      { type: 'message', id: 'POST_MESSAGE', parentId: 'BINDING', timestamp: post,
        message: { role: 'user', content: 'transient synthetic content', timestamp: 1 } },
      custom('POST_CUSTOM', 'BINDING', 'synthetic.noncontroller', { value: 'transient' }),
      { type: 'branch_summary', id: 'POST_BRANCH', parentId: 'BINDING', timestamp: post,
        summary: 'transient summary', fromId: 'BINDING' }
    ];
    entries[1]!.timestamp = post;
    for (const entry of entries) {
      const f = await fixture([entry]);
      await expect(provePreparedCompactionOrphan(f.root, f.binding, pausedEvent, prepared, 65536))
        .rejects.toThrow('restore_failed');
    }
  });

  it('fails closed for matching, multiple, malformed, or mismatched current compaction entries', async () => {
    const changed = { ...checkpoint, goals: ['MISMATCH'] };
    const cases: readonly Record<string, unknown>[][] = [
      [compaction('CURRENT', 'BINDING', prepared.checkpointId)],
      [compaction('CURRENT1', 'BINDING', prepared.checkpointId), compaction('CURRENT2', 'CURRENT1', prepared.checkpointId)],
      [{ ...compaction('MALFORMED', 'BINDING', prepared.checkpointId), details: { version: 2, checkpointId: prepared.checkpointId } }],
      [{ ...compaction('MISSING_DETAILS', 'BINDING', 'c'.repeat(32)), timestamp: '2033-05-18T03:33:25Z', details: undefined }],
      [compaction('MISMATCHED', 'BINDING', prepared.checkpointId, changed)],
      [{ ...compaction('WRONG_ID', 'BINDING', 'c'.repeat(32)), timestamp: '2033-05-18T03:33:25Z' }]
    ];
    for (const entries of cases) {
      const f = await fixture(entries);
      await expect(provePreparedCompactionOrphan(f.root, f.binding, pausedEvent, prepared, 65536)).rejects.toThrow('restore_failed');
    }
  });
});
