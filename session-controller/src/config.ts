import { isAbsolute } from 'node:path';
import { Type, type Static } from 'typebox';
import { checked, ControllerError, GroupJidSchema, PrivateJidSchema, VersionSchema } from './domain.js';
const closed = { additionalProperties: false } as const;
const integer = (minimum: number, maximum: number) => Type.Integer({ minimum, maximum });
const path = Type.String({ minLength: 1, maxLength: 4096 });
export const ConfigSchema = Type.Object({
  http: Type.Object({ host: Type.Union([Type.Literal('127.0.0.1'), Type.Literal('::1'), Type.Literal('0.0.0.0')]), port: integer(1, 65535), webhookPath: Type.String({ pattern: '^/webhooks/[A-Za-z0-9_-]{1,64}$' }), maxBodyBytes: integer(1024, 1048576), requestTimeoutMs: integer(1000, 60000), maxInflight: integer(1, 1024) }, closed),
  auth: Type.Object({ secret: Type.String({ minLength: 1, maxLength: 4096 }), freshnessSeconds: integer(1, 3600) }, closed),
  admission: Type.Object({ groups: Type.Array(GroupJidSchema, { minItems: 1, uniqueItems: true }), operatorJids: Type.Array(PrivateJidSchema, { minItems: 1, uniqueItems: true }) }, closed),
  redis: Type.Object({ url: Type.String({ minLength: 1, maxLength: 4096 }), waitAofMs: integer(1, 30000), blockMs: integer(1, 5000) }, closed),
  bridge: Type.Object({ baseUrl: Type.String({ minLength: 1, maxLength: 512 }), pollMs: integer(100, 60000), requestTimeoutMs: integer(100, 30000) }, closed),
  scheduler: Type.Object({ concurrency: integer(1, 64), leaseMs: integer(3000, 300000), renewMs: integer(100, 100000), gapTimeoutMs: integer(1000, 3600000) }, closed),
  pi: Type.Object({ cwd: path, agentDir: path, sessionDir: path, systemPromptPath: path, preCompactionPath: path, compactionSummaryPath: path, policyVersion: VersionSchema, templateVersion: VersionSchema, provider: Type.Optional(Type.String({ minLength: 1, maxLength: 128 })), model: Type.Optional(Type.String({ minLength: 1, maxLength: 256 })) }, closed),
  mcp: Type.Object({ command: Type.String({ minLength: 1, maxLength: 4096 }), args: Type.Array(Type.String({ maxLength: 4096 }), { maxItems: 64 }), cwd: path }, closed),
  compaction: Type.Object({ safeWorkingWindowTokens: integer(1024, 10000000), defaultContextWindowTokens: integer(1024, 10000000), headroomPercent: integer(1, 99), responseReserveTokens: integer(1, 1000000), preparationReserveTokens: integer(1, 1000000), summaryReserveTokens: integer(1, 1000000), checkpointMaxBytes: integer(1024, 262144) }, closed)
}, closed);
export type ControllerConfig = Static<typeof ConfigSchema>;
export const RecoveryConfigSchema = Type.Object({
  admission: ConfigSchema.properties.admission,
  redis: ConfigSchema.properties.redis,
  scheduler: Type.Object({ leaseMs: integer(3000, 300000), renewMs: integer(100, 100000) }, closed),
  pi: Type.Object({ sessionDir: path }, closed),
  compaction: Type.Object({ checkpointMaxBytes: integer(1024, 262144) }, closed),
  manifestPath: Type.Optional(path)
}, closed);
export type RecoveryConfig = Static<typeof RecoveryConfigSchema>;
export function validateConfig(value: unknown): ControllerConfig {
  const c = checked(ConfigSchema, value, 'config_invalid');
  if (c.auth.secret.trim() === '' || c.auth.secret === 'INSECURE_NO_AUTH' || Buffer.byteLength(c.auth.secret, 'utf8') < 32) throw new ControllerError('config_invalid');
  let u: URL;
  try { u = new URL(c.redis.url); } catch { throw new ControllerError('config_invalid'); }
  if (u.protocol !== 'redis:' || !['127.0.0.1', '[::1]', 'localhost', 'redis'].includes(u.hostname) || u.username || u.password || u.search || u.hash || (u.pathname !== '' && u.pathname !== '/' && u.pathname !== '/0') || (u.port && !/^[1-9]\d*$/.test(u.port))) throw new ControllerError('config_invalid');
  const literalBridge = /^http:\/\/(?:127\.0\.0\.1|\[::1\])(?::[1-9]\d{0,4})?\/?$/.exec(c.bridge.baseUrl);
  if (!literalBridge || literalBridge[0] !== c.bridge.baseUrl) throw new ControllerError('config_invalid');
  let bridge: URL;
  try { bridge = new URL(c.bridge.baseUrl); } catch { throw new ControllerError('config_invalid'); }
  if (bridge.protocol !== 'http:' || !['127.0.0.1', '[::1]'].includes(bridge.hostname) || bridge.username || bridge.password || bridge.search || bridge.hash || bridge.pathname !== '/' || (bridge.port && !/^[1-9]\d*$/.test(bridge.port))) throw new ControllerError('config_invalid');
  if (c.scheduler.renewMs * 3 >= c.scheduler.leaseMs) throw new ControllerError('config_invalid');
  if ([c.pi.cwd, c.pi.agentDir, c.pi.sessionDir, c.pi.systemPromptPath, c.pi.preCompactionPath, c.pi.compactionSummaryPath, c.mcp.cwd].some(p => !isAbsolute(p) || /[\x00-\x1f]/.test(p))) throw new ControllerError('config_invalid');
  if ((c.pi.provider === undefined) !== (c.pi.model === undefined)) throw new ControllerError('config_invalid');
  const b = c.compaction;
  const window = Math.min(b.safeWorkingWindowTokens, b.defaultContextWindowTokens);
  if (b.responseReserveTokens + b.preparationReserveTokens + b.summaryReserveTokens >= window) throw new ControllerError('config_invalid');
  return { ...c, bridge: { ...c.bridge, baseUrl: bridge.origin } };
}
function envReaders(env: Readonly<Record<string, string | undefined>>) {
  const required = (key: string): string => { const v = env[key]; if (v === undefined || v === '') throw new ControllerError('config_invalid'); return v; };
  const number = (key: string, fallback?: number): number => {
    const v = env[key];
    if (v === undefined && fallback !== undefined) return fallback;
    if (v === undefined || !/^(0|[1-9]\d*)$/.test(v)) throw new ControllerError('config_invalid');
    const n = Number(v); if (!Number.isSafeInteger(n)) throw new ControllerError('config_invalid'); return n;
  };
  const list = (key: string): string[] => [...new Set(required(key).split(',').map(v => v.trim()))];
  return { required, number, list };
}
function validateRecoveryRedis(value: Static<typeof ConfigSchema>['redis']): void {
  let u: URL;
  try { u = new URL(value.url); } catch { throw new ControllerError('config_invalid'); }
  if (u.protocol !== 'redis:' || !['127.0.0.1', '[::1]', 'localhost', 'redis'].includes(u.hostname) ||
      u.username || u.password || u.search || u.hash ||
      (u.pathname !== '' && u.pathname !== '/' && u.pathname !== '/0') ||
      (u.port && !/^[1-9]\d*$/.test(u.port))) throw new ControllerError('config_invalid');
}
/** Recovery deliberately parses only Redis/admission/lease/session proof inputs.
 * Webhook secrets, provider credentials, prompts, MCP commands and bridge
 * settings are outside this process and cannot leak through recovery errors. */
export function loadRecoveryConfig(env: Readonly<Record<string, string | undefined>> = process.env): RecoveryConfig {
  const { required, number, list } = envReaders(env);
  const value = checked(RecoveryConfigSchema, {
    admission: { groups: list('BRIDGE_NOTIFY_GROUP_ALLOWLIST'), operatorJids: list('BRIDGE_NOTIFY_OPERATOR_JIDS') },
    redis: { url: required('SESSION_CONTROLLER_REDIS_URL'), waitAofMs: number('SESSION_CONTROLLER_WAIT_AOF_MS', 3000), blockMs: number('SESSION_CONTROLLER_BLOCK_MS', 1000) },
    scheduler: { leaseMs: number('SESSION_CONTROLLER_LEASE_MS', 30000), renewMs: number('SESSION_CONTROLLER_RENEW_MS', 5000) },
    pi: { sessionDir: required('SESSION_CONTROLLER_SESSION_DIR') },
    compaction: { checkpointMaxBytes: number('SESSION_CONTROLLER_CHECKPOINT_MAX_BYTES', 65536) },
    ...(env['SESSION_CONTROLLER_COMPACTION_RECOVERY_MANIFEST'] ? { manifestPath: env['SESSION_CONTROLLER_COMPACTION_RECOVERY_MANIFEST'] } : {})
  }, 'config_invalid');
  validateRecoveryRedis(value.redis);
  // Recovery drains app/chat renewal before terminal check/abort or release.
  // Reject leases that cannot cover the final serialized durability barriers
  // plus one renewal interval of scheduling headroom.
  if (value.scheduler.renewMs * 3 >= value.scheduler.leaseMs ||
      value.scheduler.renewMs + value.redis.waitAofMs * 4 >= value.scheduler.leaseMs || !isAbsolute(value.pi.sessionDir) ||
      /[\x00-\x1f]/.test(value.pi.sessionDir) ||
      (value.manifestPath !== undefined && (!isAbsolute(value.manifestPath) || /[\x00-\x1f]/.test(value.manifestPath)))) {
    throw new ControllerError('config_invalid');
  }
  return value;
}
export function loadConfig(env: Readonly<Record<string, string | undefined>> = process.env): ControllerConfig {
  const { required, number, list } = envReaders(env);
  let args: unknown;
  try { args = JSON.parse(required('SESSION_CONTROLLER_MCP_ARGS_JSON')); } catch { throw new ControllerError('config_invalid'); }
  return validateConfig({
    http: { host: env['SESSION_CONTROLLER_HOST'] ?? '127.0.0.1', port: number('SESSION_CONTROLLER_PORT', 8644), webhookPath: env['SESSION_CONTROLLER_WEBHOOK_PATH'] ?? '/webhooks/whatsapp', maxBodyBytes: number('SESSION_CONTROLLER_MAX_BODY_BYTES', 65536), requestTimeoutMs: number('SESSION_CONTROLLER_REQUEST_TIMEOUT_MS', 10000), maxInflight: number('SESSION_CONTROLLER_MAX_INFLIGHT', 128) },
    auth: { secret: required('BRIDGE_NOTIFY_SECRET'), freshnessSeconds: number('SESSION_CONTROLLER_FRESHNESS_SECONDS', 300) },
    admission: { groups: list('BRIDGE_NOTIFY_GROUP_ALLOWLIST'), operatorJids: list('BRIDGE_NOTIFY_OPERATOR_JIDS') },
    redis: { url: required('SESSION_CONTROLLER_REDIS_URL'), waitAofMs: number('SESSION_CONTROLLER_WAIT_AOF_MS', 3000), blockMs: number('SESSION_CONTROLLER_BLOCK_MS', 1000) },
    bridge: { baseUrl: env['SESSION_CONTROLLER_BRIDGE_BASE_URL'] ?? 'http://127.0.0.1:8080', pollMs: number('SESSION_CONTROLLER_BRIDGE_POLL_MS', 1000), requestTimeoutMs: number('SESSION_CONTROLLER_BRIDGE_REQUEST_TIMEOUT_MS', 2000) },
    scheduler: { concurrency: number('SESSION_CONTROLLER_CONCURRENCY', 4), leaseMs: number('SESSION_CONTROLLER_LEASE_MS', 30000), renewMs: number('SESSION_CONTROLLER_RENEW_MS', 5000), gapTimeoutMs: number('SESSION_CONTROLLER_GAP_TIMEOUT_MS', 30000) },
    pi: { cwd: required('SESSION_CONTROLLER_PI_CWD'), agentDir: required('SESSION_CONTROLLER_PI_AGENT_DIR'), sessionDir: required('SESSION_CONTROLLER_SESSION_DIR'), systemPromptPath: required('SESSION_CONTROLLER_SYSTEM_PROMPT_PATH'), preCompactionPath: required('SESSION_CONTROLLER_PRE_COMPACTION_PATH'), compactionSummaryPath: required('SESSION_CONTROLLER_COMPACTION_SUMMARY_PATH'), policyVersion: env['SESSION_CONTROLLER_POLICY_VERSION'] ?? '1', templateVersion: env['SESSION_CONTROLLER_TEMPLATE_VERSION'] ?? '1', ...(env['SESSION_CONTROLLER_PI_PROVIDER'] !== undefined ? { provider: env['SESSION_CONTROLLER_PI_PROVIDER'] } : {}), ...(env['SESSION_CONTROLLER_PI_MODEL'] !== undefined ? { model: env['SESSION_CONTROLLER_PI_MODEL'] } : {}) },
    mcp: { command: required('SESSION_CONTROLLER_MCP_COMMAND'), args, cwd: required('SESSION_CONTROLLER_MCP_CWD') },
    compaction: { safeWorkingWindowTokens: number('SESSION_CONTROLLER_SAFE_WORKING_WINDOW_TOKENS'), defaultContextWindowTokens: number('SESSION_CONTROLLER_DEFAULT_CONTEXT_WINDOW_TOKENS', 262144), headroomPercent: number('SESSION_CONTROLLER_COMPACTION_HEADROOM_PERCENT', 15), responseReserveTokens: number('SESSION_CONTROLLER_RESPONSE_RESERVE_TOKENS', 4096), preparationReserveTokens: number('SESSION_CONTROLLER_PREPARATION_RESERVE_TOKENS', 2048), summaryReserveTokens: number('SESSION_CONTROLLER_SUMMARY_RESERVE_TOKENS', 2048), checkpointMaxBytes: number('SESSION_CONTROLLER_CHECKPOINT_MAX_BYTES', 65536) }
  });
}
