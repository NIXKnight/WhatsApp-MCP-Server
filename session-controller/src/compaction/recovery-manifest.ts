import { randomBytes } from 'node:crypto';
import { constants, type BigIntStats } from 'node:fs';
import { link, lstat, open, unlink } from 'node:fs/promises';
import { dirname, isAbsolute } from 'node:path';
import { Type } from 'typebox';
import { checked, ControllerError } from '../domain.js';

const ManifestSchema = Type.Object({
  version: Type.Literal(1),
  expectedStateDigest: Type.String({ pattern: '^sha256:v1:[0-9a-f]{64}$', minLength: 74, maxLength: 74 })
}, { additionalProperties: false });
const fail = () => new ControllerError('config_invalid');

function assertPath(path: string): void {
  if (!isAbsolute(path) || /[\x00-\x1f]/u.test(path)) throw fail();
}
function assertPrivate(status: BigIntStats): void {
  const uid = typeof process.getuid === 'function' ? process.getuid() : undefined;
  if (!status.isFile() || status.isSymbolicLink() || status.nlink !== 1n ||
      (status.mode & 0o777n) !== 0o600n || (uid !== undefined && status.uid !== BigInt(uid)) ||
      status.size < 1n || status.size > 512n) throw fail();
}
function sameStageFile(stage: StagedRecoveryManifest, status: BigIntStats): boolean {
  return status.dev === stage.device && status.ino === stage.inode && status.size === stage.size &&
    status.mtimeNs === stage.mtimeNs && status.ctimeNs === stage.ctimeNs;
}
function samePublishedFile(stage: StagedRecoveryManifest, status: BigIntStats): boolean {
  return status.dev === stage.device && status.ino === stage.inode && status.size === stage.size &&
    status.mtimeNs === stage.mtimeNs;
}
async function absent(path: string): Promise<boolean> {
  try { await lstat(path); return false; }
  catch (error) { return (error as NodeJS.ErrnoException).code === 'ENOENT'; }
}

export interface StagedRecoveryManifest {
  readonly targetPath: string;
  readonly stagedPath: string;
  readonly device: bigint;
  readonly inode: bigint;
  readonly size: bigint;
  readonly mtimeNs: bigint;
  readonly ctimeNs: bigint;
}

/** Read and unlink one private operator-intent manifest without ever exposing its
 * value. The directory is fsynced so successful consumption is durable. */
export async function consumeRecoveryManifest(path: string): Promise<string> {
  assertPath(path);
  const directory = await open(dirname(path), constants.O_RDONLY | constants.O_DIRECTORY | constants.O_NOFOLLOW);
  let file: Awaited<ReturnType<typeof open>> | undefined;
  try {
    file = await open(path, constants.O_RDONLY | constants.O_NOFOLLOW);
    const before = await file.stat({ bigint: true }); assertPrivate(before);
    let parsed: unknown;
    try { parsed = JSON.parse(await file.readFile({ encoding: 'utf8' })); } catch { throw fail(); }
    const manifest = checked(ManifestSchema, parsed, 'config_invalid');
    const named = await lstat(path, { bigint: true }); assertPrivate(named);
    if (before.dev !== named.dev || before.ino !== named.ino || before.size !== named.size ||
        before.mtimeNs !== named.mtimeNs || before.ctimeNs !== named.ctimeNs) throw fail();
    await unlink(path);
    await directory.sync();
    return manifest.expectedStateDigest;
  } catch { throw fail(); }
  finally { try { await file?.close(); } finally { await directory.close(); } }
}

/** Write and fsync a private manifest under an unguessable sibling name. The
 * staged file is deliberately not at targetPath, so it cannot authorize
 * recovery before its repair fence has been durably released. */
export async function stageRecoveryManifest(path: string, digest: string): Promise<StagedRecoveryManifest> {
  assertPath(path);
  const manifest = checked(ManifestSchema, { version: 1, expectedStateDigest: digest }, 'config_invalid');
  const parent = dirname(path);
  const directory = await open(parent, constants.O_RDONLY | constants.O_DIRECTORY | constants.O_NOFOLLOW);
  let file: Awaited<ReturnType<typeof open>> | undefined, stagedPath: string | undefined;
  try {
    if (!await absent(path)) throw fail();
    for (let attempt = 0; attempt < 8 && !file; attempt++) {
      stagedPath = `${parent}/.compaction-recovery-${randomBytes(16).toString('hex')}.pending`;
      try {
        file = await open(stagedPath, constants.O_WRONLY | constants.O_CREAT | constants.O_EXCL | constants.O_NOFOLLOW, 0o600);
      } catch (error) {
        if ((error as NodeJS.ErrnoException).code !== 'EEXIST') throw error;
      }
    }
    if (!file || !stagedPath) throw fail();
    await file.writeFile(JSON.stringify(manifest) + '\n', 'utf8');
    await file.sync();
    const status = await file.stat({ bigint: true }); assertPrivate(status);
    await directory.sync();
    return {
      targetPath: path, stagedPath, device: status.dev, inode: status.ino, size: status.size,
      mtimeNs: status.mtimeNs, ctimeNs: status.ctimeNs
    };
  } catch {
    if (stagedPath) { try { await unlink(stagedPath); await directory.sync(); } catch {} }
    throw fail();
  } finally { try { await file?.close(); } finally { await directory.close(); } }
}

/** Publish an exact staged inode without replacing an existing manifest. A hard
 * link makes publication atomic; the pending name is removed before the parent
 * directory is fsynced. */
export async function publishRecoveryManifest(stage: StagedRecoveryManifest): Promise<void> {
  assertPath(stage.targetPath); assertPath(stage.stagedPath);
  const parent = dirname(stage.targetPath);
  if (dirname(stage.stagedPath) !== parent) throw fail();
  const directory = await open(parent, constants.O_RDONLY | constants.O_DIRECTORY | constants.O_NOFOLLOW);
  let file: Awaited<ReturnType<typeof open>> | undefined, linked = false;
  try {
    file = await open(stage.stagedPath, constants.O_RDONLY | constants.O_NOFOLLOW);
    const before = await file.stat({ bigint: true }); assertPrivate(before);
    const named = await lstat(stage.stagedPath, { bigint: true }); assertPrivate(named);
    if (!sameStageFile(stage, before) || !sameStageFile(stage, named)) throw fail();
    await link(stage.stagedPath, stage.targetPath); linked = true;
    const published = await lstat(stage.targetPath, { bigint: true });
    if (!samePublishedFile(stage, published)) throw fail();
    await unlink(stage.stagedPath);
    const final = await lstat(stage.targetPath, { bigint: true }); assertPrivate(final);
    if (!samePublishedFile(stage, final)) throw fail();
    await directory.sync();
  } catch {
    if (linked) {
      try {
        const published = await lstat(stage.targetPath, { bigint: true });
        if (samePublishedFile(stage, published)) await unlink(stage.targetPath);
      } catch {}
    }
    try { await unlink(stage.stagedPath); } catch {}
    try { await directory.sync(); } catch {}
    throw fail();
  } finally { try { await file?.close(); } finally { await directory.close(); } }
}

/** Remove only the exact unpublished staged inode. Failure can leave a private
 * pending file, but never an intent at the configured recovery path. */
export async function discardRecoveryManifest(stage: StagedRecoveryManifest): Promise<void> {
  assertPath(stage.targetPath); assertPath(stage.stagedPath);
  const parent = dirname(stage.targetPath);
  if (dirname(stage.stagedPath) !== parent) throw fail();
  const directory = await open(parent, constants.O_RDONLY | constants.O_DIRECTORY | constants.O_NOFOLLOW);
  try {
    let status: BigIntStats;
    try { status = await lstat(stage.stagedPath, { bigint: true }); }
    catch (error) {
      if ((error as NodeJS.ErrnoException).code === 'ENOENT') return;
      throw fail();
    }
    assertPrivate(status);
    if (!sameStageFile(stage, status)) throw fail();
    await unlink(stage.stagedPath);
    await directory.sync();
  } catch { throw fail(); }
  finally { await directory.close(); }
}
