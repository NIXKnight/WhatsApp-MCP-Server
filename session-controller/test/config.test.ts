import { describe, expect, it } from 'vitest';
import Value from 'typebox/value';
import { loadConfig, validateConfig } from '../src/config.js';
import { createDiagnostics } from '../src/diagnostics.js';
import { AcceptanceSchema, canonicalChatId, CheckpointV1Schema, CheckpointRecordSchema, CompletionMarkerSchema, CompletionProofSchema, ControllerError, eventId, MutationStateSchema, OwnershipSchema, WorkStateSchema } from '../src/domain.js';

const testKey = 'TEST_ONLY_NON_SENSITIVE_HMAC_FIXTURE';
function environment(): Record<string, string> {
  return {
    BRIDGE_NOTIFY_SECRET: testKey,
    BRIDGE_NOTIFY_GROUP_ALLOWLIST: '100@g.us,200@g.us,100@g.us',
    BRIDGE_NOTIFY_OPERATOR_JIDS: '300@lid,400@s.whatsapp.net,300@lid',
    SESSION_CONTROLLER_REDIS_URL: 'redis://127.0.0.1:6379/0',
    SESSION_CONTROLLER_SAFE_WORKING_WINDOW_TOKENS: '32000',
    SESSION_CONTROLLER_PI_CWD: '/tmp/synthetic-project',
    SESSION_CONTROLLER_PI_AGENT_DIR: '/tmp/synthetic-agent',
    SESSION_CONTROLLER_SESSION_DIR: '/tmp/synthetic-sessions',
    SESSION_CONTROLLER_SYSTEM_PROMPT_PATH: '/tmp/synthetic-policy.md',
    SESSION_CONTROLLER_PRE_COMPACTION_PATH: '/tmp/synthetic-pre.md',
    SESSION_CONTROLLER_COMPACTION_SUMMARY_PATH: '/tmp/synthetic-summary.md',
    SESSION_CONTROLLER_MCP_COMMAND: 'uv',
    SESSION_CONTROLLER_MCP_ARGS_JSON: '["run","whatsapp-mcp"]',
    SESSION_CONTROLLER_MCP_CWD: '/tmp/synthetic-mcp'
  };
}
function rejects(env: Record<string, string>): void {
  let code = '';
  try { loadConfig(env); } catch (e) { code = e instanceof ControllerError ? e.code : 'unexpected'; }
  expect(code).toBe('config_invalid');
}
describe('strict configuration', () => {
  it('deduplicates ordered aliases and requires an explicit safe cap', () => {
    const c = loadConfig(environment());
    expect(c.admission.groups).toEqual(['100@g.us', '200@g.us']);
    expect(c.admission.operatorJids).toEqual(['300@lid', '400@s.whatsapp.net']);
    expect(c.compaction.headroomPercent).toBe(15);
    expect(c.bridge).toEqual({ baseUrl: 'http://127.0.0.1:8080', pollMs: 1000, requestTimeoutMs: 2000 });
    const explicit = { ...environment(), SESSION_CONTROLLER_BRIDGE_BASE_URL: 'http://[::1]:18080/', SESSION_CONTROLLER_BRIDGE_POLL_MS: '250', SESSION_CONTROLLER_BRIDGE_REQUEST_TIMEOUT_MS: '750' };
    expect(loadConfig(explicit).bridge).toEqual({ baseUrl: 'http://[::1]:18080', pollMs: 250, requestTimeoutMs: 750 });
    const e = environment(); delete e['SESSION_CONTROLLER_SAFE_WORKING_WINDOW_TOKENS']; rejects(e);
  });
  it.each([
    ['BRIDGE_NOTIFY_SECRET', ''], ['BRIDGE_NOTIFY_SECRET', 'INSECURE_NO_AUTH'], ['BRIDGE_NOTIFY_SECRET', 'TEST_SHORT'],
    ['BRIDGE_NOTIFY_GROUP_ALLOWLIST', '*'], ['BRIDGE_NOTIFY_GROUP_ALLOWLIST', '100@g.us,'],
    ['BRIDGE_NOTIFY_GROUP_ALLOWLIST', '300@lid'], ['BRIDGE_NOTIFY_OPERATOR_JIDS', '300@bot'],
    ['SESSION_CONTROLLER_SAFE_WORKING_WINDOW_TOKENS', '32000x'], ['SESSION_CONTROLLER_SAFE_WORKING_WINDOW_TOKENS', '3.2e4'],
    ['SESSION_CONTROLLER_SAFE_WORKING_WINDOW_TOKENS', '9007199254740992'], ['SESSION_CONTROLLER_SAFE_WORKING_WINDOW_TOKENS', '1024'],
    ['SESSION_CONTROLLER_COMPACTION_HEADROOM_PERCENT', '0'], ['SESSION_CONTROLLER_COMPACTION_HEADROOM_PERCENT', '100'],
    ['SESSION_CONTROLLER_REDIS_URL', 'redis://public.example.test'], ['SESSION_CONTROLLER_REDIS_URL', 'redis://127.0.0.1/1'],
    ['SESSION_CONTROLLER_REDIS_URL', 'redis://test-user@test-host/0'], ['SESSION_CONTROLLER_REDIS_URL', 'redis://127.0.0.1?test=1'],
    ['SESSION_CONTROLLER_BRIDGE_BASE_URL', 'https://127.0.0.1:8080'], ['SESSION_CONTROLLER_BRIDGE_BASE_URL', 'http://localhost:8080'],
    ['SESSION_CONTROLLER_BRIDGE_BASE_URL', 'http://10.0.0.1:8080'], ['SESSION_CONTROLLER_BRIDGE_BASE_URL', 'http://127.1:8080'],
    ['SESSION_CONTROLLER_BRIDGE_BASE_URL', 'http://2130706433:8080'], ['SESSION_CONTROLLER_BRIDGE_BASE_URL', 'http://[::ffff:127.0.0.1]:8080'],
    ['SESSION_CONTROLLER_BRIDGE_BASE_URL', 'http://127.0.0.1:8080/api'], ['SESSION_CONTROLLER_BRIDGE_BASE_URL', 'http://test-user@127.0.0.1:8080'],
    ['SESSION_CONTROLLER_BRIDGE_BASE_URL', 'http://127.0.0.1:8080?test=1'], ['SESSION_CONTROLLER_BRIDGE_BASE_URL', 'http://127.0.0.1:8080\n'],
    ['SESSION_CONTROLLER_BRIDGE_POLL_MS', '99'], ['SESSION_CONTROLLER_BRIDGE_POLL_MS', '60001'],
    ['SESSION_CONTROLLER_BRIDGE_REQUEST_TIMEOUT_MS', '99'], ['SESSION_CONTROLLER_BRIDGE_REQUEST_TIMEOUT_MS', '30001'],
    ['SESSION_CONTROLLER_PI_CWD', 'relative'], ['SESSION_CONTROLLER_PI_CWD', '/tmp/test\u0000path'],
    ['SESSION_CONTROLLER_MCP_ARGS_JSON', '{"test":true}'], ['SESSION_CONTROLLER_MCP_ARGS_JSON', 'not-json'],
    ['SESSION_CONTROLLER_PI_PROVIDER', 'test-provider'], ['SESSION_CONTROLLER_RENEW_MS', '10000'],
    ['SESSION_CONTROLLER_WEBHOOK_PATH', '/wrong'], ['SESSION_CONTROLLER_PORT', '0']
  ])('rejects invalid %s without returning values', (key, value) => { rejects({ ...environment(), [key]: value }); });
  it('counts HMAC key bytes and allows the internal Compose Redis hostname', () => {
    const e = environment(); e['BRIDGE_NOTIFY_SECRET'] = 'é'.repeat(15) + 'x'; rejects(e);
    e['BRIDGE_NOTIFY_SECRET'] = 'é'.repeat(16); e['SESSION_CONTROLLER_REDIS_URL'] = 'redis://redis:6379';
    expect(loadConfig(e).redis.url.startsWith('redis://redis:')).toBe(true);
  });
  it('rejects unknown nested config keys and numeric coercion', () => {
    const c = loadConfig(environment());
    expect(() => validateConfig({ ...c, http: { ...c.http, extra: true } })).toThrow('config_invalid');
    expect(() => validateConfig({ ...c, http: { ...c.http, port: '8644' } })).toThrow('config_invalid');
  });
});
describe('domain contracts and redacted diagnostics', () => {
  it('requires structured outcomes, fences and discriminated work/mutation fields', () => {
    expect(Value.Check(AcceptanceSchema, { status: 'accepted', extra: true })).toBe(false);
    expect(Value.Check(OwnershipSchema, { app: { owner: 'a'.repeat(32), epoch: 1, expiresAtMs: 1 } })).toBe(false);
    expect(Value.Check(WorkStateSchema, { phase: 'leased' })).toBe(false);
    expect(Value.Check(MutationStateSchema, { status: 'confirmed' })).toBe(false);
    expect(Value.Check(CompletionMarkerSchema, { version: 1 })).toBe(false);
    expect(Value.Check(CheckpointV1Schema, { version: 2 })).toBe(false);
    expect(() => eventId('TEST_INVALID')).toThrow('payload_invalid');
  });
  it('validates destination-scoped completion evidence and checkpoint phase requirements', () => {
    const marker = { version: 1, eventId: 'a'.repeat(32), eventFingerprint: 'sha256:v1:' + 'a'.repeat(64), canonicalChatId: '100@g.us', chatSequence: 1, completion: 'turn', completedAt: '2033-05-18T03:33:20Z', confirmedSends: [{ destination: '200@g.us', messageId: 'TEST_SENT1' }] };
    expect(Value.Check(CompletionMarkerSchema, marker)).toBe(true);
    expect(Value.Check(CompletionMarkerSchema, { ...marker, confirmedSends: [{ messageId: 'TEST_SENT1' }] })).toBe(false);
    const legacy = Object.fromEntries(Object.entries(marker).filter(([key]) => key !== 'eventFingerprint'));
    const proof = { marker, entryId: 'TEST_COMPLETION1', session: { canonicalChatId: '100@g.us', sessionId: 'TEST_SESSION1', sessionFile: '/tmp/synthetic-sessions/session1.jsonl' } };
    expect(Value.Check(CompletionProofSchema, proof)).toBe(true);
    expect(Value.Check(CompletionMarkerSchema, legacy)).toBe(false);
    expect(Value.Check(CompletionProofSchema, { ...proof, marker: legacy })).toBe(false);
    for (const eventFingerprint of [undefined, null, '', 'a'.repeat(64), 'sha256:v1:' + 'A'.repeat(64), 'sha256:v1:' + 'g'.repeat(64), 'sha256:v1:' + 'a'.repeat(63), 'sha256:v1:' + 'a'.repeat(65), 'sha256:v1:' + 'a'.repeat(64) + '\n', 'sha256:v2:' + 'a'.repeat(64)]) {
      const invalid = { ...marker, eventFingerprint };
      expect(Value.Check(CompletionMarkerSchema, invalid)).toBe(false);
      expect(Value.Check(CompletionProofSchema, { ...proof, marker: invalid })).toBe(false);
    }
    expect(Value.Check(CompletionMarkerSchema, { ...marker, rawEvent: {} })).toBe(false);
    expect(Value.Check(CompletionMarkerSchema, { ...marker, content: 'TEST_ONLY_CONTENT' })).toBe(false);
    const checkpoint = { version: 1, canonicalChatId: '100@g.us', lastCompletedSequence: 0, policyVersion: '1', templateVersion: '1', createdAt: '2033-05-18T03:33:20Z', goals: [], decisions: [], constraints: [], verifiedFacts: [], pendingWork: [], approvalBoundaries: [], nextSteps: [], confirmedToolOutcomes: [] };
    expect(Value.Check(CheckpointV1Schema, checkpoint)).toBe(true);
    const record = { checkpointId: 'b'.repeat(32), checkpoint, phase: 'prepared' };
    expect(Value.Check(CheckpointRecordSchema, record)).toBe(true);
    expect(Value.Check(CheckpointRecordSchema, { ...record, phase: 'restored' })).toBe(false);
    expect(Value.Check(CheckpointRecordSchema, { ...record, phase: 'restored', compactionEntryId: 'TEST_ENTRY1' })).toBe(true);
    expect(Value.Check(CheckpointV1Schema, { ...checkpoint, extra: 'TEST_EXTRA' })).toBe(false);
  });
  it('emits only fixed class, opaque ID, hash, bounded count and timestamp', () => {
    const lines: string[] = [];
    const sink = createDiagnostics(line => lines.push(line), { now: () => 0 });
    const input = { class: 'intake_accepted' as const, eventId: eventId('a'.repeat(32)), chat: canonicalChatId('100@g.us'), count: Number.MAX_SAFE_INTEGER, httpStatus: 202, raw: 'TEST_RAW_NOT_FOR_DIAGNOSTICS' };
    sink.emit(input);
    const record = JSON.parse(lines[0] ?? '{}') as Record<string, unknown>;
    expect(Object.keys(record).sort()).toEqual(['chat_ref', 'class', 'count', 'event_id', 'http_status', 'timestamp']);
    expect(record['count']).toBe(1000000000);
    expect(String(record['chat_ref'])).toMatch(/^[0-9a-f]{64}$/);
    expect(lines.join('').includes(input.chat)).toBe(false);
    expect(lines.join('').includes(input.raw)).toBe(false);
    expect(() => createDiagnostics(() => { throw new Error('TEST_SINK_ERROR'); }).emit({ class: 'ready' })).not.toThrow();
  });
});
