import { ControllerError, type UsageSnapshot } from '../domain.js';
import type { ControllerConfig } from '../config.js';
export type UsagePolicyConfig = ControllerConfig['compaction'];
export interface IncomingTokenBudget { event: number; tools: number; prompt: number }
export type UsageDecision = { status: 'ok' | 'compact'; effectiveWindow: number; projectedTokens: number; remainingTokens: number; remainingPercent: number } | { status: 'unknown' };
export interface UsagePolicy { decide(usage: UsageSnapshot | undefined, incoming: IncomingTokenBudget): UsageDecision }
const integer = (value: number, minimum: number) => Number.isSafeInteger(value) && value >= minimum;
export function createUsagePolicy(config: UsagePolicyConfig): UsagePolicy {
  const values = [config.safeWorkingWindowTokens, config.defaultContextWindowTokens, config.headroomPercent,
    config.responseReserveTokens, config.preparationReserveTokens, config.summaryReserveTokens, config.checkpointMaxBytes];
  if (values.some(value => !integer(value, 1)) || config.safeWorkingWindowTokens < 1024 || config.safeWorkingWindowTokens > 10000000 ||
      config.defaultContextWindowTokens < 1024 || config.defaultContextWindowTokens > 10000000 || config.headroomPercent > 99 ||
      config.responseReserveTokens > 1000000 || config.preparationReserveTokens > 1000000 || config.summaryReserveTokens > 1000000 ||
      config.checkpointMaxBytes < 1024 || config.checkpointMaxBytes > 262144) throw new ControllerError('config_invalid');
  const reserve = config.responseReserveTokens + config.preparationReserveTokens + config.summaryReserveTokens;
  if (!Number.isSafeInteger(reserve) || reserve >= Math.min(config.safeWorkingWindowTokens, config.defaultContextWindowTokens)) throw new ControllerError('config_invalid');
  const fixed = Object.freeze({ ...config });
  return { decide(usage, incoming) {
    const dynamic = [incoming.event, incoming.tools, incoming.prompt];
    if (dynamic.some(value => !integer(value, 0))) throw new ControllerError('usage_unknown');
    if (!usage || usage.tokens === null || !integer(usage.tokens, 0) || !integer(usage.contextWindow, 1) ||
        usage.percent === null || !Number.isFinite(usage.percent) || usage.percent < 0 || usage.percent > 100) return { status: 'unknown' };
    const effectiveWindow = Math.min(usage.contextWindow, fixed.safeWorkingWindowTokens);
    const extra = dynamic.reduce((total, value) => total + value, reserve);
    if (!Number.isSafeInteger(extra) || !Number.isSafeInteger(usage.tokens + extra)) throw new ControllerError('usage_unknown');
    const projectedTokens = usage.tokens + extra;
    const remainingTokens = Math.max(0, effectiveWindow - projectedTokens);
    const remainingPercent = remainingTokens * 100 / effectiveWindow;
    return { status: remainingTokens * 100 <= effectiveWindow * fixed.headroomPercent ? 'compact' : 'ok', effectiveWindow, projectedTokens, remainingTokens, remainingPercent };
  } };
}
