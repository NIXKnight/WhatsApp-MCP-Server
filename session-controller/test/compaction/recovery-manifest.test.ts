import { chmod, mkdtemp, readFile, rm, stat, writeFile } from 'node:fs/promises';
import { join } from 'node:path';
import { afterEach, describe, expect, it } from 'vitest';
import {
  consumeRecoveryManifest, discardRecoveryManifest, publishRecoveryManifest,
  stageRecoveryManifest
} from '../../src/compaction/recovery-manifest.js';

const digest = 'sha256:v1:' + 'a'.repeat(64);
const writeRecoveryManifest = async (path: string, value: string) =>
  publishRecoveryManifest(await stageRecoveryManifest(path, value));
const roots: string[] = [];
afterEach(async () => { await Promise.all(roots.splice(0).map(root => rm(root, { recursive: true, force: true }))); });

describe('private one-shot compaction recovery manifest', () => {
  it('creates only a fixed mode-0600 digest envelope and consumes it without returning identifiers', async () => {
    const root = await mkdtemp('/tmp/controller-recovery-manifest-'); roots.push(root);
    const path = join(root, 'intent.json');
    await writeRecoveryManifest(path, digest);
    expect((await stat(path)).mode & 0o777).toBe(0o600);
    expect(JSON.parse(await readFile(path, 'utf8'))).toEqual({ version: 1, expectedStateDigest: digest });
    await expect(writeRecoveryManifest(path, digest)).rejects.toThrow('config_invalid');
    await expect(consumeRecoveryManifest(path)).resolves.toBe(digest);
    await expect(stat(path)).rejects.toMatchObject({ code: 'ENOENT' });
  });

  it('keeps a durable staged intent unauthorized until explicit publication and discards it safely', async () => {
    const root = await mkdtemp('/tmp/controller-recovery-manifest-'); roots.push(root);
    const path = join(root, 'intent.json');
    const discarded = await stageRecoveryManifest(path, digest);
    await expect(stat(discarded.stagedPath)).resolves.toMatchObject({ mode: expect.any(Number) });
    await expect(stat(path)).rejects.toMatchObject({ code: 'ENOENT' });
    await expect(consumeRecoveryManifest(path)).rejects.toThrow('config_invalid');
    await discardRecoveryManifest(discarded);
    await expect(stat(discarded.stagedPath)).rejects.toMatchObject({ code: 'ENOENT' });

    const published = await stageRecoveryManifest(path, digest);
    await publishRecoveryManifest(published);
    await expect(stat(published.stagedPath)).rejects.toMatchObject({ code: 'ENOENT' });
    await expect(consumeRecoveryManifest(path)).resolves.toBe(digest);
  });

  it('rejects permissive mode, malformed versions, and digest mismatch material without consuming it', async () => {
    const root = await mkdtemp('/tmp/controller-recovery-manifest-'); roots.push(root);
    for (const [name, value] of [
      ['mode.json', { version: 1, expectedStateDigest: digest }],
      ['version.json', { version: 2, expectedStateDigest: digest }],
      ['digest.json', { version: 1, expectedStateDigest: 'sha256:v1:' + '0'.repeat(63) }]
    ] as const) {
      const path = join(root, name);
      await writeFile(path, JSON.stringify(value), { mode: name === 'mode.json' ? 0o644 : 0o600 });
      if (name === 'mode.json') await chmod(path, 0o644);
      await expect(consumeRecoveryManifest(path)).rejects.toThrow('config_invalid');
      await expect(stat(path)).resolves.toBeDefined();
    }
  });
});
