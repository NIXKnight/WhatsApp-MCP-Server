import { describe, expect, it } from 'vitest';
import { createUsagePolicy } from '../../src/compaction/usage-policy.js';
import type { ControllerConfig } from '../../src/config.js';
import type { UsageSnapshot } from '../../src/domain.js';
const config = { safeWorkingWindowTokens: 10000, defaultContextWindowTokens: 20000, headroomPercent: 15,
  responseReserveTokens: 40, preparationReserveTokens: 50, summaryReserveTokens: 50, checkpointMaxBytes: 65536 } satisfies ControllerConfig['compaction'];
const incoming = { event: 10, tools: 20, prompt: 30 };
describe('safe usage policy', () => {
  const policy = createUsagePolicy(config);
  it.each([[16, 'ok'], [15, 'compact'], [14, 'compact']] as const)('uses inclusive %s percent boundary with all internal reserves', (remaining, status) => {
    const projected = 10000 * (100 - remaining) / 100;
    expect(policy.decide({ tokens: projected - 200, contextWindow: 20000, percent: 1 }, incoming)).toMatchObject({ status, projectedTokens: projected });
  });
  it('uses the smaller provider window and explicit cap and cannot bypass reserves', () => {
    expect(policy.decide({ tokens: 7000, contextWindow: 8000, percent: 87.5 }, { event: 0, tools: 0, prompt: 0 })).toMatchObject({ status: 'compact', effectiveWindow: 8000, projectedTokens: 7140 });
    expect(policy.decide({ tokens: 8310, contextWindow: 10000, percent: 83.1 }, incoming)).toMatchObject({ status: 'compact', projectedTokens: 8510 });
  });
  it.each([
    undefined,
    { tokens: null, contextWindow: 10000, percent: 0 },
    { tokens: 1, contextWindow: 10000, percent: null },
    { tokens: 1, contextWindow: 10000, percent: Number.NaN },
    { tokens: 1, contextWindow: 10000, percent: Number.POSITIVE_INFINITY },
    { tokens: 1, contextWindow: 10000, percent: -1 },
    { tokens: 1, contextWindow: 10000, percent: 101 },
    { tokens: 1, contextWindow: 0, percent: 1 },
    { tokens: 1, contextWindow: 1.5, percent: 1 },
    { tokens: Number.NaN, contextWindow: 10000, percent: 1 }
  ] as Array<UsageSnapshot | undefined>)('maps every absent/null/nonfinite/invalid snapshot to unknown', snapshot => {
    expect(policy.decide(snapshot, incoming)).toEqual({ status: 'unknown' });
  });
});
