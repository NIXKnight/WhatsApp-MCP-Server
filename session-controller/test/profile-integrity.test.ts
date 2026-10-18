import { createHash } from 'node:crypto';
import { readFile } from 'node:fs/promises';
import { join } from 'node:path';
import { describe, expect, it } from 'vitest';
import { CheckpointV1Schema } from '../src/domain.js';

const configDir = join(import.meta.dirname, '../config');
const profileName = 'cabal-system-profile.md';
const manifestName = 'cabal-system-profile.manifest.md';
const generatedArtifacts = [
  profileName,
  'system-prompt.md',
  'pre-compaction.md',
  'compaction-summary.md'
] as const;

async function bytes(name: string): Promise<Buffer> {
  return readFile(join(configDir, name));
}
function sha256(value: Buffer): string {
  return createHash('sha256').update(value).digest('hex');
}
function section(markdown: string, heading: string, nextHeading?: string): string {
  const start = markdown.indexOf(heading);
  const end = nextHeading === undefined ? markdown.length : markdown.indexOf(nextHeading, start + heading.length);
  expect(start, `${heading} exists`).toBeGreaterThanOrEqual(0);
  expect(end, `${nextHeading ?? 'end of document'} exists after ${heading}`).toBeGreaterThan(start);
  return markdown.slice(start + heading.length, end);
}
function strictInlineCode(value: string, pattern: RegExp, label: string): string {
  const match = value.match(pattern);
  expect(Boolean(match), label).toBe(true);
  return match?.[1] ?? '';
}

describe('reviewed CABAL profile integrity', () => {
  it('has a valid manifest identity and complete unique source coverage', async () => {
    const manifest = (await bytes(manifestName)).toString('utf8');
    const commit = strictInlineCode(manifest, /^- Source commit: `([^`]+)`\.$/m, 'source commit field');
    const sourceCount = strictInlineCode(manifest, /^- Source file count: `([^`]+)`\.$/m, 'source count field');
    const coverage = strictInlineCode(manifest, /^Coverage: `([^`]+)` source Markdown files represented\.$/m, 'coverage field');
    expect(/^[0-9a-f]{40}$/.test(commit), 'source commit is a lowercase full Git hash').toBe(true);
    expect(sourceCount).toBe('16');
    expect(coverage).toBe('16/16');

    const coverageTable = section(manifest, '## Source coverage', '## Sanitization and neutralization policy');
    const rowLines = coverageTable.split('\n').filter(line => /^\|\s*\d+\s*\|/.test(line));
    expect(rowLines).toHaveLength(16);
    const paths: string[] = [];
    for (const [index, line] of rowLines.entries()) {
      const cells = line.split('|').slice(1, -1).map(cell => cell.trim());
      expect(cells, `source row ${index + 1} has six columns`).toHaveLength(6);
      expect(cells[0], `source row ${index + 1} sequence`).toBe(String(index + 1));
      const pathMatch = cells[1]?.match(/^`([^`]+)`$/);
      const hashMatch = cells[2]?.match(/^`([0-9a-f]{64})`$/);
      expect(Boolean(pathMatch), `source row ${index + 1} relative path syntax`).toBe(true);
      expect(Boolean(hashMatch), `source row ${index + 1} SHA-256 syntax`).toBe(true);
      const relativePath = pathMatch?.[1] ?? '';
      expect(relativePath.startsWith('/') || relativePath.includes('\\') || relativePath.split('/').includes('..'), `source row ${index + 1} is normalized and relative`).toBe(false);
      paths.push(relativePath);
    }
    expect(new Set(paths).size, 'source rows are unique').toBe(paths.length);
  });

  it('matches every generated artifact byte count and SHA-256', async () => {
    const manifest = (await bytes(manifestName)).toString('utf8');
    const hashTable = section(manifest, '## Companion artifact hashes');
    const rows = hashTable.split('\n').filter(line => /^\|\s*`[^`]+`\s*\|/.test(line));
    expect(rows).toHaveLength(generatedArtifacts.length);
    const recorded = new Map<string, { size: number; hash: string }>();
    for (const line of rows) {
      const cells = line.split('|').slice(1, -1).map(cell => cell.trim());
      expect(cells, 'artifact row has three columns').toHaveLength(3);
      const name = cells[0]?.match(/^`([^`]+)`$/)?.[1] ?? '';
      const size = cells[1] ?? '';
      const hash = cells[2]?.match(/^`([0-9a-f]{64})`$/)?.[1] ?? '';
      expect(generatedArtifacts.includes(name as typeof generatedArtifacts[number]), `listed generated artifact ${name}`).toBe(true);
      expect(/^(0|[1-9]\d*)$/.test(size), `artifact ${name} byte syntax`).toBe(true);
      expect(hash).toHaveLength(64);
      expect(recorded.has(name), `artifact ${name} is listed once`).toBe(false);
      recorded.set(name, { size: Number(size), hash });
    }
    for (const name of generatedArtifacts) {
      const content = await bytes(name);
      expect(recorded.get(name), `${name} manifest integrity`).toEqual({ size: content.byteLength, hash: sha256(content) });
    }

    const profile = await bytes(profileName);
    const outputBytes = Number(strictInlineCode(manifest, /^- Output bytes: `([^`]+)`\.$/m, 'profile output byte field'));
    const outputHash = strictInlineCode(manifest, /^- Output SHA-256: `([^`]+)`\.$/m, 'profile output hash field');
    expect(outputBytes).toBe(profile.byteLength);
    expect(outputHash).toBe(sha256(profile));
  });

  it('is a bounded standalone Pi system profile with required behavioral concepts', async () => {
    const profile = (await bytes(profileName)).toString('utf8');
    const profileBytes = Buffer.byteLength(profile, 'utf8');
    expect(profileBytes).toBeGreaterThanOrEqual(8 * 1024);
    expect(profileBytes).toBeLessThan(65_536);
    const concepts: ReadonlyArray<readonly [string, RegExp]> = [
      ['precedence', /higher precedence[\s\S]*Runtime safety beats persona/i],
      ['engagement', /directly mentions[\s\S]*directly replies[\s\S]*substantive question/i],
      ['silence', /remain silent[\s\S]*Silence is preferable/i],
      ['brevity', /one or two sentences/i],
      ['uncertainty', /Honest uncertainty[\s\S]*what is unknown/i]
    ];
    for (const [label, pattern] of concepts) expect(pattern.test(profile), `${label} concept`).toBe(true);
    expect(profile.includes('<!-- EVENT TEMPLATE -->'), 'profile is not an event-template companion').toBe(false);
    expect(/\{\{\s*\w+\s*\}\}/.test(profile), 'profile has no runtime template slots').toBe(false);
  });

  it('contains no private identity, path, secret assignment, cron, or executable timing pattern', async () => {
    const profile = (await bytes(profileName)).toString('utf8');
    const forbidden: ReadonlyArray<readonly [string, RegExp]> = [
      ['JID', /\b\d+@(?:g\.us|s\.whatsapp\.net|lid|bot|hosted(?:\.lid)?)\b/i],
      ['international phone', /\+\d[\d ().-]{6,}\d/],
      ['formatted phone', /\b\d{3}[- .]\d{3}[- .]\d{4}\b/],
      ['private absolute path', /(?:^|[\s`"'(])(?:\/home\/[^/\s]+|\/Users\/[^/\s]+|\/root(?:\/|\b)|[A-Za-z]:\\Users\\)/m],
      ['secret assignment', /\b[A-Z][A-Z0-9_]*(?:SECRET|TOKEN|PASSWORD|PASSWD|API_KEY|PRIVATE_KEY)[A-Z0-9_]*\s*=\s*\S+/i],
      ['five-field cron expression', /^\s*(?:`{3}\w*\s*)?(?:\*|[0-9,*/-]+)\s+(?:\*|[0-9,*/-]+)\s+(?:\*|[0-9,*/-]+)\s+(?:\*|[0-9,*/-]+)\s+(?:\*|[0-9,*/-]+)(?:\s|$)/m],
      ['executable recurring timing', /\b(?:run|execute|poll|check|send|monitor|schedule|trigger)\b[^.\n]{0,80}\b(?:every|each)\s+\d+\s*(?:seconds?|minutes?|hours?|days?)\b/i]
    ];
    for (const [label, pattern] of forbidden) expect(pattern.test(profile), label).toBe(false);
  });

  it('keeps controller policy bounded, singular, and stronger than profile style', async () => {
    const policy = (await bytes('system-prompt.md')).toString('utf8');
    expect(Buffer.byteLength(policy, 'utf8')).toBeLessThan(65_536);
    expect(policy.split('<!-- EVENT TEMPLATE -->')).toHaveLength(2);
    expect(/companion\s+`?cabal-system-profile\.md`?/i.test(policy), 'no implicit companion discovery').toBe(false);
    for (const [label, pattern] of [
      ['base profile source', /base Pi system profile supplies/i],
      ['attribution default deny', /Attribution is absent by default/i],
      ['quote metadata deny', /Quote metadata is prohibited/i],
      ['unknown mutation deny', /unknown mutation, DENY/i],
      ['approval is not inferred', /Approval must be requested there, not inferred/i],
      ['ambiguous mutation no replay', /Uncertain effects must not be automatically repeated/i],
      ['runtime guard precedence', /hard runtime guards have higher precedence/i]
    ] as const) expect(pattern.test(policy), label).toBe(true);
  });

  it('requests exactly the CheckpointV1 fields and no unsupported checkpoint fields', async () => {
    const preparation = (await bytes('pre-compaction.md')).toString('utf8');
    const fieldLine = preparation.match(/^Populate only these CheckpointV1 fields: (.+)\.$/m)?.[1] ?? '';
    const requested = [...fieldLine.matchAll(/`([^`]+)`/g)].map(match => match[1]);
    expect(requested).toEqual(Object.keys(CheckpointV1Schema.properties));
    expect(new Set(requested).size).toBe(requested.length);
    const summary = (await bytes('compaction-summary.md')).toString('utf8');
    expect(/goals, decisions, constraints, verified facts, pending work, approval boundaries, executable next steps, and confirmed tool outcomes/i.test(summary), 'summary uses supported semantic sections').toBe(true);
  });
});
