import { randomBytes, timingSafeEqual } from 'node:crypto';
import { isDeepStrictEqual } from 'node:util';
import { realpathSync } from 'node:fs';
import { readFile } from 'node:fs/promises';
import { join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { loadConfig, loadRecoveryConfig, type ControllerConfig, type RecoveryConfig } from './config.js';
import {
  ControllerError,
  type CompletionIndexPort,
  type DiagnosticInput,
  type DiagnosticPort,
  type PiHostPort,
  type PiSessionPort,
  type WaitPort,
  type SessionFileGeneration
} from './domain.js';
import { createDiagnostics } from './diagnostics.js';
import { createWebhookServer } from './intake/server.js';
import { connectRedis, type RedisConnections } from './redis/client.js';
import { createRepository, legacyCompactionRepairDigest, readSessionMappings, type CompactionRepairClaim, type RedisRepository } from './redis/repository.js';
import { createCompletionIndex, type CompletionIndex } from './pi/completion-index.js';
import { loadPromptPolicy } from './pi/prompt-policy.js';
import { loadNativeSdk, type SdkSession } from './pi/mcp-loader.js';
import { createToolJournal, type JournalAttachment } from './pi/tool-journal-extension.js';
import type { CheckpointExtension } from './compaction/checkpoint-extension.js';
import { createCompactor, type Compactor } from './compaction/compactor.js';
import { provePreparedCompactionOrphan } from './compaction/orphan-recovery.js';
import {
  consumeRecoveryManifest, discardRecoveryManifest, publishRecoveryManifest, stageRecoveryManifest,
  type StagedRecoveryManifest
} from './compaction/recovery-manifest.js';
import { createSessionHost, type SessionHost } from './pi/session-host.js';
import {
  createCoordinator,
  timerWait,
  type Coordinator,
  type CoordinatorRepository
} from './scheduler/coordinator.js';
import { createJournalTurns } from './scheduler/chat-worker.js';

async function validateTemplate(path: string): Promise<void> {
  try {
    const value = await readFile(path, 'utf8');
    if (!value.trim() || Buffer.byteLength(value, 'utf8') > 65536) throw new Error();
  } catch { throw new ControllerError('config_invalid'); }
}

interface BridgeStatus { readonly state?: unknown; readonly is_connected?: unknown }
async function bridgeConnected(
  config: ReturnType<typeof loadConfig>['bridge'],
  signal: AbortSignal,
  request: typeof fetch
): Promise<boolean> {
  try {
    const response = await request(`${config.baseUrl}/api/status`, {
      method: 'GET', headers: { Accept: 'application/json' },
      signal: AbortSignal.any([signal, AbortSignal.timeout(config.requestTimeoutMs)])
    });
    if (!response.ok) {
      await response.body?.cancel();
      if (signal.aborted) throw new ControllerError('intake_closed');
      return false;
    }
    const status = await response.json() as BridgeStatus;
    return status !== null && typeof status === 'object' &&
      status.state === 'CONNECTED' && status.is_connected === true;
  } catch {
    if (signal.aborted) throw new ControllerError('intake_closed');
    return false;
  }
}

export async function waitForBridge(
  config: ReturnType<typeof loadConfig>['bridge'],
  signal: AbortSignal,
  request: typeof fetch = fetch,
  wait: WaitPort = timerWait
): Promise<void> {
  while (!signal.aborted) {
    if (await bridgeConnected(config, signal, request)) return;
    if (signal.aborted) break;
    try { await wait.wait(config.pollMs, signal); }
    catch {
      if (signal.aborted) throw new ControllerError('intake_closed');
      throw new ControllerError('internal_failed');
    }
  }
  throw new ControllerError('intake_closed');
}

export async function monitorBridge(
  config: ReturnType<typeof loadConfig>['bridge'],
  signal: AbortSignal,
  request: typeof fetch = fetch,
  wait: WaitPort = timerWait
): Promise<void> {
  while (!signal.aborted) {
    try { await wait.wait(config.pollMs, signal); }
    catch {
      if (signal.aborted) break;
      throw new ControllerError('internal_failed');
    }
    if (signal.aborted) break;
    // Operational probes are fail-closed: one timeout, transport failure,
    // non-OK response, malformed body, or non-exact status is terminal.
    if (!await bridgeConnected(config, signal, request)) throw new ControllerError('internal_failed');
  }
  throw new ControllerError('intake_closed');
}

interface DeferredCheckpoint extends Pick<CheckpointExtension, 'extension' | 'run'> {
  initialize(value: CheckpointExtension): void;
}
function createDeferredCheckpoint(): DeferredCheckpoint {
  let delegate: CheckpointExtension | undefined;
  return {
    initialize(value) {
      if (delegate) throw new ControllerError('session_failed');
      delegate = value;
    },
    extension(pi) {
      if (!delegate) throw new ControllerError('session_failed');
      return delegate.extension(pi);
    },
    run(...args) {
      if (!delegate) return Promise.reject(new ControllerError('session_failed'));
      return delegate.run(...args);
    }
  };
}

export interface LazyPiHostWiring {
  readonly journal: JournalAttachment;
  readonly extensions: NonNullable<Parameters<typeof loadNativeSdk>[3]>;
}
export interface PromptModelRuntimeOptions {
  readonly authPath: string;
  readonly modelsPath: string;
  readonly allowModelNetwork: false;
}
export type PromptModelRuntimeFactory =
  (options: PromptModelRuntimeOptions) => Promise<Parameters<typeof loadNativeSdk>[2]>;
/** Production prompt runtimes must complete the SDK's offline credential and
 * availability refresh. AgentSession.prompt consults that snapshot before its
 * request path, whereas prepareRequest resolves credentials directly. */
export async function createPromptModelRuntime(
  config: ControllerConfig,
  create: PromptModelRuntimeFactory = async options => {
    const { ModelRuntime } = await import('@earendil-works/pi-coding-agent');
    return ModelRuntime.create(options);
  }
): Promise<Parameters<typeof loadNativeSdk>[2]> {
  return create({
    authPath: join(config.pi.agentDir, 'auth.json'),
    modelsPath: join(config.pi.agentDir, 'models.json'),
    allowModelNetwork: false
  });
}
export function createLazyPiHost(options: {
  config: ReturnType<typeof loadConfig>;
  policy: Awaited<ReturnType<typeof loadPromptPolicy>>;
  repository: RedisRepository;
  index: CompletionIndex;
  journal: JournalAttachment;
  checkpoint: DeferredCheckpoint;
  compactor: Compactor;
}, initializeOverride?: (wiring: LazyPiHostWiring) => Promise<SessionHost>): PiHostPort & { attach(session: PiSessionPort, context: import('./scheduler/chat-worker.js').TurnContext): Promise<void> } {
  let closed = false;
  let initialized: SessionHost | undefined;
  let initialization: Promise<SessionHost> | undefined;
  const initialize = (): Promise<SessionHost> => {
    if (closed) return Promise.reject(new ControllerError('session_failed'));
    if (!initialization) {
      const extensions = [options.journal.extension, options.checkpoint.extension] as const;
      initialization = initializeOverride ? initializeOverride({ journal: options.journal, extensions }) : (async () => {
        const [{ createCheckpointExtension }, modelRuntime] = await Promise.all([
          import('./compaction/checkpoint-extension.js'),
          createPromptModelRuntime(options.config)
        ]);
        options.checkpoint.initialize(createCheckpointExtension(options.repository, {
          ...options.config.compaction, sessionDir: options.config.pi.sessionDir
        }));
        const sdk = await loadNativeSdk(options.config, options.policy, modelRuntime, extensions);
        const host = createSessionHost({
          cwd: options.config.pi.cwd,
          root: options.config.pi.sessionDir,
          sdk,
          index: options.index,
          // Identity is intentional: this is the exact attachment whose
          // extension was supplied above and whose confirmed view backs turns.
          journal: options.journal,
          checkpointMaxBytes: options.config.compaction.checkpointMaxBytes,
          current: async work => {
            if (!await options.repository.checkWork(work)) throw new ControllerError('lease_lost');
          },
          bind: (work, binding) => options.repository.bindSession(work, binding),
          compact: async (session, context, instructions, seed) => {
            await context.current();
            const state = await options.repository.readChat(context.work().event.canonical_chat_jid);
            if (!state.session) throw new ControllerError('compaction_failed');
            return options.checkpoint.run(state.session.sessionId, state.session, session, context, seed, instructions, '');
          }
        });
        initialized = host;
        return host;
      })();
    }
    return initialization;
  };
  return {
    findEvent: (chat, id) => options.index.findEvent(chat, id),
    classifyOwn: (chat, actual, id) => options.journal.classifyOwn(
      chat, actual, id, () => options.index.classifyOwn(chat, actual, id)),
    async open(work, binding) {
      const current = await initialize();
      if (closed) throw new ControllerError('session_failed');
      return binding === undefined ? current.open(work) : current.open(work, binding);
    },
    async attach(session, context) {
      const current = await initialize();
      if (closed) throw new ControllerError('session_failed');
      return current.attach(session, context);
    },
    async close() {
      closed = true;
      if (initialized) { await initialized.close(); return; }
      if (!initialization) return;
      let current: SessionHost;
      try { current = await initialization; }
      catch { return; } // Suppress only failed initialization: no host exists.
      // Deliberately outside the initialization catch: cleanup failure is real.
      await current.close();
    }
  };
}

interface AsyncClose { close(): Promise<void> }
interface LifecycleRedis extends AsyncClose { isReady(): boolean }
interface LifecycleCoordinator { start(): Promise<void>; failClosed(): void; stopScheduling(): void; drain(): Promise<void> }
interface LifecycleServer extends AsyncClose { stopIntake(): void }
export interface LifecycleResources {
  redis?: LifecycleRedis;
  repository?: AsyncClose;
  host?: AsyncClose;
  coordinator?: LifecycleCoordinator;
  server?: LifecycleServer;
}
export interface LifecycleContext {
  readonly signal: AbortSignal;
  readonly diagnostics: DiagnosticPort;
  readonly resources: LifecycleResources;
  isReady(): boolean;
  requestStop(code?: number): void;
  ensureRunning(): void;
}
export interface LifecycleComposition {
  prepare(context: LifecycleContext): Promise<void>;
  waitForBridge(context: LifecycleContext): Promise<void>;
  startCoordinator(context: LifecycleContext): Promise<void>;
  monitorBridge(context: LifecycleContext): Promise<void>;
}
export type LifecycleSignal = 'SIGINT' | 'SIGTERM';
export interface SignalPort {
  once(signal: LifecycleSignal, listener: () => void): void;
  off(signal: LifecycleSignal, listener: () => void): void;
}
export interface RunOptions {
  readonly writeDiagnostic?: (line: string) => void;
  readonly signals?: SignalPort;
}
const processSignals: SignalPort = {
  once: (signal, listener) => { process.once(signal, listener); },
  off: (signal, listener) => { process.off(signal, listener); }
};

export function createProductionComposition(): LifecycleComposition {
  let config: ControllerConfig | undefined;
  let coordinator: Coordinator | undefined;
  return {
    async prepare(context) {
      config = loadConfig();
      const policy = await loadPromptPolicy(config.pi.systemPromptPath, config.admission);
      await Promise.all([
        validateTemplate(config.pi.preCompactionPath),
        validateTemplate(config.pi.compactionSummaryPath)
      ]);
      context.ensureRunning();
      const redis = await connectRedis(config.redis);
      context.resources.redis = redis;
      context.ensureRunning();

      let index: CompletionIndex | undefined;
      const deferred: CompletionIndexPort = {
        findEvent(chat, id) {
          if (!index) throw new ControllerError('session_failed');
          return index.findEvent(chat, id);
        },
        classifyOwn(chat, actual, id) {
          if (!index) throw new ControllerError('session_failed');
          return index.classifyOwn(chat, actual, id);
        }
      };
      const repository = await createRepository(redis, config.admission, deferred);
      context.resources.repository = repository;
      context.ensureRunning();
      index = createCompletionIndex(
        config.pi.sessionDir,
        config.admission,
        { list: () => readSessionMappings({ mapping: redis.mapping }, config!.admission) },
        async (actual, messageId) => (await repository.ownMutation(actual, messageId)) === 'uncertain'
      );
      const journal = createToolJournal(repository);
      const checkpoint = createDeferredCheckpoint();
      const compactor = createCompactor({
        config,
        repository,
        incoming: (_session, turn) => ({
          event: Buffer.byteLength(JSON.stringify(turn.work().event), 'utf8'),
          tools: 0,
          prompt: Buffer.byteLength(policy.instructions(turn.work().event), 'utf8')
        })
      });
      const host = createLazyPiHost({ config, policy, repository, index, journal, checkpoint, compactor });
      context.resources.host = host;
      const turns = createJournalTurns(journal, compactor.prepare, compactor.afterPrompt);
      const appLeaseRepository: CoordinatorRepository = { ...repository };
      coordinator = createCoordinator(appLeaseRepository, {
        scheduler: config.scheduler,
        pollMs: config.redis.blockMs,
        ids: { next: () => randomBytes(16).toString('hex') },
        worker: {
          instructions: work => policy.instructions(work.event),
          host,
          turns,
          wait: timerWait,
          diagnostics: context.diagnostics
        }
      });
      context.resources.coordinator = coordinator;
      const server = createWebhookServer(config, repository, context, context.diagnostics);
      context.resources.server = server;
      await server.listen();
      context.ensureRunning();
    },
    async waitForBridge(context) {
      if (!config) throw new ControllerError('internal_failed');
      await waitForBridge(config.bridge, context.signal);
    },
    async startCoordinator(context) {
      if (!coordinator) throw new ControllerError('internal_failed');
      await coordinator.start(); // resolves only after the application lease exists
      context.ensureRunning();
    },
    async monitorBridge(context) {
      if (!config) throw new ControllerError('internal_failed');
      await monitorBridge(config.bridge, context.signal);
    }
  };
}

/** Typed lifecycle seam: tests inject only inert composition, signals and a line sink. */
export async function runLifecycle(
  composition: LifecycleComposition,
  options: RunOptions = {}
): Promise<number> {
  let operational = false;
  let shuttingDown = false;
  let stopRequested = false;
  let exitCode = 0;
  let terminate: (() => void) | undefined;
  const termination = new Promise<void>(resolve => { terminate = resolve; });
  const startup = new AbortController();
  const resources: LifecycleResources = {};
  let bridgeMonitor: Promise<void> | undefined;
  let schedulingStopped = false;
  let failClosedInvoked = false;
  let failClosedInProgress = false;
  let leaseLossReported = false;

  // This idempotent terminal gate deliberately exists before diagnostics.
  const requestStop = (code = 0): void => {
    operational = false; // readiness drops synchronously on terminal loss
    stopRequested = true;
    exitCode = Math.max(exitCode, code);
    startup.abort();
    terminate?.();
  };
  const failClosedOnce = (): void => {
    operational = false;
    if (failClosedInvoked) return;
    // Set both guards before calling the real coordinator: failClosed may
    // synchronously emit lease_lost back through the diagnostic adapter.
    failClosedInvoked = true;
    failClosedInProgress = true;
    try { resources.coordinator?.failClosed(); }
    catch { /* Terminal disposition remains authoritative. */ }
    finally { failClosedInProgress = false; }
    requestStop(1);
  };
  const diagnostics = createDiagnostics(options.writeDiagnostic ?? (line => process.stdout.write(line)));
  const lifecycleDiagnostics: DiagnosticPort = {
    emit(input: DiagnosticInput) {
      if (input.class === 'lease_lost') {
        // A synchronous report from failClosed is reentrant implementation
        // detail; the originating worker/bridge path reports after stop.
        if (leaseLossReported || failClosedInProgress) return;
        failClosedOnce();
        leaseLossReported = true;
        diagnostics.emit({ class: 'lease_lost' });
        return;
      }
      if (shuttingDown && (input.class === 'internal_failed' || input.class === 'session_failed' || input.class === 'store_unavailable')) exitCode = 1;
      diagnostics.emit(input);
    }
  };
  const ensureRunning = (): void => {
    if (stopRequested || shuttingDown) throw new ControllerError('intake_closed');
  };
  const stopScheduling = (): void => {
    if (schedulingStopped || !resources.coordinator) return;
    resources.coordinator.stopScheduling();
    schedulingStopped = true;
  };
  const context: LifecycleContext = {
    signal: startup.signal,
    diagnostics: lifecycleDiagnostics,
    resources,
    isReady: () => operational && resources.redis?.isReady() === true,
    requestStop,
    ensureRunning
  };
  const signals = options.signals ?? processSignals;
  const onSignal = (): void => { requestStop(0); };
  signals.once('SIGINT', onSignal);
  signals.once('SIGTERM', onSignal);

  try {
    await composition.prepare(context);
    ensureRunning();
    await composition.waitForBridge(context);
    ensureRunning();
    await composition.startCoordinator(context);
    ensureRunning();
    operational = true;
    lifecycleDiagnostics.emit({ class: 'ready' });
    bridgeMonitor = (async () => {
      try {
        await composition.monitorBridge(context);
        if (!stopRequested) throw new ControllerError('internal_failed');
      } catch (error: unknown) {
        const expectedStopAbort = stopRequested && error instanceof ControllerError && error.code === 'intake_closed';
        if (expectedStopAbort) return;
        // A failed operational probe and worker-originated lease loss share one
        // idempotent/reentrant terminal path. The explicit report is absorbed if
        // real failClosed synchronously emitted lease_lost first.
        failClosedOnce();
        lifecycleDiagnostics.emit({ class: 'lease_lost' });
      }
    })();
    await termination;
    await bridgeMonitor;
  } catch (error: unknown) {
    // Any already-requested stop owns the terminal diagnostic and exit code.
    // An ensureRunning() intake_closed after that stop is expected even when
    // lease loss or another terminal path has already raised the code to 1.
    const expectedStopAbort = stopRequested && error instanceof ControllerError && error.code === 'intake_closed';
    if (!expectedStopAbort) {
      lifecycleDiagnostics.emit({ class: error instanceof ControllerError ? error.code : 'internal_failed' });
      exitCode = 1;
    }
  } finally {
    shuttingDown = true;
    operational = false;
    startup.abort();
    signals.off('SIGINT', onSignal);
    signals.off('SIGTERM', onSignal);
    const cleanup = async (operation: (() => void | Promise<void>) | undefined, failure: DiagnosticInput['class']): Promise<void> => {
      if (!operation) return;
      try { await operation(); }
      catch { exitCode = 1; lifecycleDiagnostics.emit({ class: failure }); }
    };
    await cleanup(resources.server ? () => resources.server!.stopIntake() : undefined, 'internal_failed');
    await cleanup(resources.coordinator ? stopScheduling : undefined, 'internal_failed');
    // Never race cleanup: drain is intentionally unbounded and every later step
    // is independently attempted even when an earlier one rejects.
    await cleanup(resources.coordinator ? () => resources.coordinator!.drain() : undefined, 'internal_failed');
    await cleanup(resources.server ? () => resources.server!.close() : undefined, 'internal_failed');
    await cleanup(resources.host ? () => resources.host!.close() : undefined, 'session_failed');
    await cleanup(resources.repository ? () => resources.repository!.close() : undefined, 'store_unavailable');
    await cleanup(resources.redis ? () => resources.redis!.close() : undefined, 'store_unavailable');
    lifecycleDiagnostics.emit({ class: 'stopped' });
  }
  return exitCode;
}

export async function run(): Promise<number> {
  return runLifecycle(createProductionComposition());
}

export interface InventoryResourceRoots {
  readonly root: string;
  readonly cwd: string;
  readonly agentDir: string;
  readonly home: string;
  readonly sessionDir: string;
}
export interface InventoryPreflightDependencies {
  load(): ControllerConfig;
  loadPolicy(config: ControllerConfig): ReturnType<typeof loadPromptPolicy>;
  validate(path: string): Promise<void>;
  createTemporary(): Promise<InventoryResourceRoots>;
  removeTemporary(roots: InventoryResourceRoots): Promise<void>;
  enterEnvironment(roots: InventoryResourceRoots): () => void;
  createRuntime(config: ControllerConfig): Promise<Parameters<typeof loadNativeSdk>[2]>;
  loadSdk: typeof loadNativeSdk;
  status(value: 'pi_inventory_ready' | 'pi_inventory_failed'): void;
}

const productionInventoryPreflight: InventoryPreflightDependencies = {
  load: loadConfig,
  loadPolicy: config => loadPromptPolicy(config.pi.systemPromptPath, config.admission),
  validate: validateTemplate,
  async createTemporary() {
    const [{ mkdir, mkdtemp, rm }, { tmpdir }] = await Promise.all([
      import('node:fs/promises'), import('node:os')
    ]);
    const root = await mkdtemp(join(tmpdir(), 'whatsapp-session-controller-inventory-'));
    const roots: InventoryResourceRoots = {
      root,
      cwd: join(root, 'cwd'),
      agentDir: join(root, 'agent'),
      home: join(root, 'home'),
      sessionDir: join(root, 'sessions')
    };
    try {
      await Promise.all([
        mkdir(roots.cwd, { recursive: true, mode: 0o700 }),
        mkdir(roots.agentDir, { recursive: true, mode: 0o700 }),
        mkdir(join(roots.home, '.agents'), { recursive: true, mode: 0o700 }),
        mkdir(join(roots.root, 'packages'), { recursive: true, mode: 0o700 }),
        mkdir(roots.sessionDir, { recursive: true, mode: 0o700 })
      ]);
      return roots;
    } catch (error) {
      try { await rm(root, { recursive: true, force: true }); } catch {}
      throw error;
    }
  },
  async removeTemporary(roots) {
    const { rm } = await import('node:fs/promises');
    await rm(roots.root, { recursive: true, force: true });
  },
  enterEnvironment(roots) {
    const replacements = {
      HOME: roots.home,
      PI_CODING_AGENT_DIR: roots.agentDir,
      PI_PACKAGE_DIR: join(roots.root, 'packages')
    } as const;
    const previous = Object.keys(replacements).map(key => ({
      key: key as keyof typeof replacements,
      present: Object.prototype.hasOwnProperty.call(process.env, key),
      value: process.env[key]
    }));
    let restored = false;
    const restore = (): void => {
      if (restored) return;
      restored = true;
      for (const snapshot of previous) {
        if (snapshot.present) process.env[snapshot.key] = snapshot.value!;
        else delete process.env[snapshot.key];
      }
    };
    try {
      for (const [key, value] of Object.entries(replacements)) process.env[key] = value;
    } catch (error) {
      restore();
      throw error;
    }
    return restore;
  },
  async createRuntime(config) {
    const { ModelRuntime } = await import('@earendil-works/pi-coding-agent');
    return ModelRuntime.create({
      authPath: join(config.pi.agentDir, 'auth.json'),
      modelsPath: join(config.pi.agentDir, 'models.json'),
      allowModelNetwork: false,
      refreshOnCreate: false
    });
  },
  loadSdk: loadNativeSdk,
  status(value) {
    const line = `{"status":"${value}"}\n`;
    (value === 'pi_inventory_ready' ? process.stdout : process.stderr).write(line);
  }
};

let inventoryPreflightActive = false;
function silenceInventoryConsole(): () => void {
  const methods = ['debug', 'info', 'log', 'warn', 'error'] as const;
  const original = Object.fromEntries(methods.map(method => [method, console[method]])) as
    Record<(typeof methods)[number], typeof console.log>;
  for (const method of methods) console[method] = (() => {}) as typeof console.log;
  return () => { for (const method of methods) console[method] = original[method] as never; };
}
/** Explicit operator preflight only. It never prompts or constructs service lifecycle resources. */
export async function checkPiInventory(
  overrides: Partial<InventoryPreflightDependencies> = {}
): Promise<number> {
  const dependencies: InventoryPreflightDependencies = { ...productionInventoryPreflight, ...overrides };
  if (inventoryPreflightActive) {
    dependencies.status('pi_inventory_failed');
    return 1;
  }
  inventoryPreflightActive = true;
  // The adapter has fixed informational console logging. This CLI path is
  // process-exclusive and returns only its closed status contract.
  const restoreConsole = silenceInventoryConsole();
  let roots: InventoryResourceRoots | undefined;
  let restoreEnvironment: (() => void) | undefined;
  let session: SdkSession | undefined;
  let failed = false;
  try {
    try {
      const config = dependencies.load();
      const policy = await dependencies.loadPolicy(config);
      await Promise.all([
        dependencies.validate(config.pi.systemPromptPath),
        dependencies.validate(config.pi.preCompactionPath),
        dependencies.validate(config.pi.compactionSummaryPath)
      ]);
      roots = await dependencies.createTemporary();
      // These process-global overrides are safe only because exact CLI dispatch
      // selects this exclusive preflight before normal concurrent work exists.
      // Auth/model files and selected provider/model deliberately remain the
      // explicit validated operator paths supplied to runtime creation.
      restoreEnvironment = dependencies.enterEnvironment(roots);
      const runtime = await dependencies.createRuntime(config);
      const sdk = await dependencies.loadSdk(config, policy, runtime, [], {
        kind: 'inventory-preflight', root: roots.root,
        cwd: roots.cwd, agentDir: roots.agentDir
      });
      const manager = sdk.create(roots.cwd, roots.sessionDir);
      session = await sdk.connect(manager, async () => {});
      session.setAutoCompactionEnabled(false);
      await session.bindExtensions({});
      await sdk.ready(session, async () => {});
      // Deliberately no prompt: successful inventory is the terminal operation.
    } catch { failed = true; }
    finally {
      if (session) {
        try { await session.dispose(); } catch { failed = true; }
      }
      if (restoreEnvironment) {
        try { restoreEnvironment(); } catch { failed = true; }
      }
      if (roots) {
        try { await dependencies.removeTemporary(roots); } catch { failed = true; }
      }
    }
    const status = failed ? 'pi_inventory_failed' : 'pi_inventory_ready';
    dependencies.status(status);
    return failed ? 1 : 0;
  } finally {
    restoreConsole();
    inventoryPreflightActive = false;
  }
}

export interface CompactionRecoveryStatus {
  readonly status: 'compaction_recovery_complete' | 'compaction_recovery_failed';
  readonly scanned: number;
  readonly repaired: number;
  readonly skipped: number;
  readonly failed: number;
}
interface CompactionRecoveryRepository extends Pick<RedisRepository,
  'chats' | 'acquireApp' | 'renewApp' | 'releaseApp' | 'createCompactionRepairIntent' |
  'claimCompactionRepair' | 'releaseCompactionRepair' | 'abortCompactionRepair' |
  'renew' | 'checkWork' | 'close'> {}
export interface CompactionRecoveryDependencies {
  load(): RecoveryConfig;
  connect(config: RecoveryConfig['redis']): Promise<RedisConnections>;
  repository(connections: RedisConnections, config: RecoveryConfig): Promise<CompactionRecoveryRepository>;
  prove(config: RecoveryConfig, claim: CompactionRepairClaim): Promise<SessionFileGeneration>;
  consumeManifest(path: string): Promise<string>;
  id(): string;
  status(value: CompactionRecoveryStatus): void;
}
const productionCompactionRecovery: CompactionRecoveryDependencies = {
  load: loadRecoveryConfig,
  connect: connectRedis,
  repository: (connections, config) => createRepository(connections, config.admission, {
    findEvent: async () => { throw new ControllerError('session_failed'); },
    classifyOwn: async () => { throw new ControllerError('session_failed'); }
  }, undefined, { initialize: false }),
  prove: (config, claim) => provePreparedCompactionOrphan(
    config.pi.sessionDir, claim.binding, claim.work.event.event_id,
    claim.checkpoint, config.compaction.checkpointMaxBytes
  ),
  consumeManifest: consumeRecoveryManifest,
  id: () => randomBytes(16).toString('hex'),
  status(value) { process.stdout.write(`${JSON.stringify(value)}\n`); }
};
function equalPrivateDigest(left: string, right: string): boolean {
  const a = Buffer.from(left, 'utf8'), b = Buffer.from(right, 'utf8');
  return a.length === b.length && timingSafeEqual(a, b);
}
let compactionRecoveryActive = false;
/** Explicit one-time operator repair. It owns no listener, coordinator, bridge,
 * Pi runtime, model, MCP process, prompt, provider, or tool execution path. */
export async function recoverCompactionFailures(
  overrides: Partial<CompactionRecoveryDependencies> = {}
): Promise<number> {
  const dependencies = { ...productionCompactionRecovery, ...overrides };
  const counts = { scanned: 0, repaired: 0, skipped: 0, failed: 0 };
  if (compactionRecoveryActive) {
    dependencies.status({ status: 'compaction_recovery_failed', ...counts, failed: 1 });
    return 1;
  }
  compactionRecoveryActive = true;
  const restoreConsole = silenceInventoryConsole();
  let connections: RedisConnections | undefined;
  let repository: CompactionRecoveryRepository | undefined;
  let app: Awaited<ReturnType<CompactionRecoveryRepository['acquireApp']>>;
  try {
    try {
      const config = dependencies.load();
      connections = await dependencies.connect(config.redis);
      repository = await dependencies.repository(connections, config);
      app = await repository.acquireApp(dependencies.id(), config.scheduler.leaseMs);
      if (!app) throw new ControllerError('lease_lost');
      // A manifest is one-shot operator intent. Bound records do not need one.
      const legacyIntent = config.manifestPath === undefined
        ? undefined : await dependencies.consumeManifest(config.manifestPath);
      for (const chat of repository.chats) {
        counts.scanned++;
        const renewedApp = await repository.renewApp(app, config.scheduler.leaseMs);
        if (!renewedApp) throw new ControllerError('lease_lost');
        app = renewedApp;
        const candidate = await repository.claimCompactionRepair(
          chat, app, dependencies.id(), config.scheduler.leaseMs, legacyIntent
        );
        if (candidate.status === 'ineligible') { counts.skipped++; continue; }
        if (candidate.status !== 'claimed') { counts.failed++; continue; }

        let claim = candidate.claim;
        let repaired = false, failed = false, heartbeatFailed = false;
        const stop = new AbortController();
        const heartbeat = (async () => {
          while (!stop.signal.aborted) {
            await timerWait.wait(config.scheduler.renewMs, stop.signal);
            if (stop.signal.aborted) break;
            const nextApp = await repository!.renewApp(app!, config.scheduler.leaseMs);
            if (!nextApp) throw new ControllerError('lease_lost');
            app = nextApp;
            claim = { ...claim, work: { ...claim.work, ownership: { ...claim.work.ownership, app: nextApp } } };
            const ownership = await repository!.renew(claim.work, config.scheduler.leaseMs);
            if (!ownership) throw new ControllerError('lease_lost');
            claim = { ...claim, work: { ...claim.work, ownership } };
          }
        })().catch(() => { heartbeatFailed = true; });
        try {
          const first = await dependencies.prove(config, claim);
          if (heartbeatFailed) throw new ControllerError('lease_lost');
          const second = await dependencies.prove(config, claim);
          if (heartbeatFailed || !isDeepStrictEqual(first, second)) throw new ControllerError('restore_failed');
          if (!claim.checkpoint.recovery) {
            const expected = claim.legacyIntentDigest;
            if (!expected || !equalPrivateDigest(legacyCompactionRepairDigest(claim, second), expected)) {
              throw new ControllerError('restore_failed');
            }
          }
          // Drain both serialized renewal operations before either terminal
          // operation can be queued. The validated recovery lease headroom
          // covers check-work plus durable abort after this final renewal.
          stop.abort(); await heartbeat;
          if (heartbeatFailed) throw new ControllerError('lease_lost');
          if (!await repository.checkWork(claim.work) || !await repository.abortCompactionRepair(claim, second)) {
            throw new ControllerError('restore_failed');
          }
          repaired = true;
        } catch { failed = true; }
        finally {
          stop.abort();
          try { await heartbeat; } catch { failed = true; }
          if (!repaired) {
            try { if (!await repository.releaseCompactionRepair(claim)) failed = true; }
            catch { failed = true; }
          }
        }
        if (repaired && !failed) counts.repaired++; else counts.failed++;
      }
      if (!await repository.releaseApp(app)) counts.failed++;
      app = undefined;
    } catch { counts.failed++; }
    finally {
      if (app && repository) {
        try { if (!await repository.releaseApp(app)) counts.failed++; } catch { counts.failed++; }
      }
      if (repository) {
        try { await repository.close(); } catch { counts.failed++; }
      } else if (connections) {
        try { await connections.close(); } catch { counts.failed++; }
      }
    }
    const failed = counts.failed > 0;
    dependencies.status({ status: failed ? 'compaction_recovery_failed' : 'compaction_recovery_complete', ...counts });
    return failed ? 1 : 0;
  } finally {
    restoreConsole();
    compactionRecoveryActive = false;
  }
}

export interface CompactionRecoveryIntentStatus {
  readonly status: 'compaction_recovery_intent_created' | 'compaction_recovery_intent_failed';
  readonly scanned: number;
  readonly eligible: number;
  readonly failed: number;
}
interface CompactionRecoveryManifestDependencies extends CompactionRecoveryDependencies {
  stageManifest(path: string, digest: string): Promise<StagedRecoveryManifest>;
  publishManifest(stage: StagedRecoveryManifest): Promise<void>;
  discardManifest(stage: StagedRecoveryManifest): Promise<void>;
}
/** Separate explicit operator step for legacy records. It privately snapshots
 * one exact Redis tuple, fsyncs an unpublished digest manifest, releases the
 * repair fence, then atomically publishes that one-shot intent. */
export async function createCompactionRecoveryManifest(
  overrides: Partial<CompactionRecoveryManifestDependencies> = {}
): Promise<number> {
  const dependencies = {
    ...productionCompactionRecovery,
    stageManifest: stageRecoveryManifest,
    publishManifest: publishRecoveryManifest,
    discardManifest: discardRecoveryManifest,
    ...overrides
  };
  const counts = { scanned: 0, eligible: 0, failed: 0 };
  let connections: RedisConnections | undefined, repository: CompactionRecoveryRepository | undefined;
  let app: Awaited<ReturnType<CompactionRecoveryRepository['acquireApp']>>;
  const restoreConsole = silenceInventoryConsole();
  try {
    try {
      const config = dependencies.load();
      if (!config.manifestPath) throw new ControllerError('config_invalid');
      connections = await dependencies.connect(config.redis);
      repository = await dependencies.repository(connections, config);
      app = await repository.acquireApp(dependencies.id(), config.scheduler.leaseMs);
      if (!app) throw new ControllerError('lease_lost');
      let digest: string | undefined, selected: CompactionRepairClaim | undefined;
      let selectedReleased = false, releaseAttempted = false, heartbeatFailed = false;
      let staged: StagedRecoveryManifest | undefined, published = false;
      const stop = new AbortController();
      let heartbeat: Promise<void> | undefined;
      try {
        for (const chat of repository.chats) {
          counts.scanned++;
          const renewed = await repository.renewApp(app, config.scheduler.leaseMs);
          if (!renewed) throw new ControllerError('lease_lost');
          app = renewed;
          const candidate = await repository.createCompactionRepairIntent(
            chat, app, dependencies.id(), config.scheduler.leaseMs
          );
          if (candidate.status === 'ineligible') continue;
          if (candidate.status !== 'claimed') throw new ControllerError('restore_failed');
          counts.eligible++;
          if (selected) {
            if (!await repository.releaseCompactionRepair(candidate.claim)) throw new ControllerError('lease_lost');
            continue;
          }
          selected = candidate.claim;
          heartbeat = (async () => {
            while (!stop.signal.aborted) {
              await timerWait.wait(config.scheduler.renewMs, stop.signal);
              if (stop.signal.aborted) break;
              const nextApp = await repository!.renewApp(app!, config.scheduler.leaseMs);
              if (!nextApp) throw new ControllerError('lease_lost');
              app = nextApp;
              selected = { ...selected!, work: { ...selected!.work, ownership: {
                ...selected!.work.ownership, app: nextApp
              } } };
              const ownership = await repository!.renew(selected.work, config.scheduler.leaseMs);
              if (!ownership) throw new ControllerError('lease_lost');
              selected = { ...selected, work: { ...selected.work, ownership } };
            }
          })().catch(() => { heartbeatFailed = true; });
          const first = await dependencies.prove(config, selected);
          const second = await dependencies.prove(config, selected);
          if (heartbeatFailed || !isDeepStrictEqual(first, second)) throw new ControllerError('restore_failed');
          digest = legacyCompactionRepairDigest(selected, second);
        }
        if (counts.eligible !== 1 || !digest || !selected || heartbeatFailed) throw new ControllerError('restore_failed');
        // The staged inode is durable but cannot be consumed at the configured
        // path. Keep renewal active for file I/O, then drain it before release.
        staged = await dependencies.stageManifest(config.manifestPath, digest);
        stop.abort(); if (heartbeat) await heartbeat;
        if (heartbeatFailed) throw new ControllerError('lease_lost');
        releaseAttempted = true;
        if (!await repository.releaseCompactionRepair(selected)) throw new ControllerError('lease_lost');
        selectedReleased = true;
        await dependencies.publishManifest(staged);
        published = true;
      } finally {
        // No selected-fence terminal operation may race a queued app/chat
        // renewal. A failed terminal release is never retried as success and
        // its unpublished file can never authorize later recovery.
        stop.abort(); if (heartbeat) await heartbeat;
        if (staged && !published) {
          try { await dependencies.discardManifest(staged); } catch {}
        }
        if (selected && !selectedReleased && !releaseAttempted) {
          try { selectedReleased = await repository.releaseCompactionRepair(selected); } catch {}
        }
      }
      if (heartbeatFailed || !selectedReleased || !published) throw new ControllerError('lease_lost');
      if (!await repository.releaseApp(app)) counts.failed++;
      app = undefined;
    } catch { counts.failed++; }
    finally {
      if (app && repository) { try { if (!await repository.releaseApp(app)) counts.failed++; } catch { counts.failed++; } }
      if (repository) { try { await repository.close(); } catch { counts.failed++; } }
      else if (connections) { try { await connections.close(); } catch { counts.failed++; } }
    }
    const failed = counts.failed > 0;
    process.stdout.write(`${JSON.stringify({ status: failed ? 'compaction_recovery_intent_failed' : 'compaction_recovery_intent_created', ...counts } satisfies CompactionRecoveryIntentStatus)}\n`);
    return failed ? 1 : 0;
  } finally { restoreConsole(); }
}

export interface CliDependencies {
  lifecycle(): Promise<number>;
  inventory(): Promise<number>;
  recovery(): Promise<number>;
  recoveryIntent(): Promise<number>;
  invalid(): void;
}
const productionCli: CliDependencies = {
  lifecycle: run,
  inventory: checkPiInventory,
  recovery: recoverCompactionFailures,
  recoveryIntent: createCompactionRecoveryManifest,
  invalid: () => { process.stderr.write('{"status":"command_invalid"}\n'); }
};
export async function runCli(
  args: readonly string[] = process.argv.slice(2),
  dependencies: CliDependencies = productionCli
): Promise<number> {
  if (args.length === 0) return dependencies.lifecycle();
  if (args.length === 1 && args[0] === '--check-pi-inventory') return dependencies.inventory();
  if (args.length === 1 && args[0] === '--recover-compaction-failures') return dependencies.recovery();
  if (args.length === 1 && args[0] === '--create-compaction-recovery-manifest') return dependencies.recoveryIntent();
  dependencies.invalid();
  return 2;
}

function isDirectExecution(): boolean {
  const entry = process.argv[1];
  if (!entry) return false;
  try {
    return realpathSync(fileURLToPath(import.meta.url)) === realpathSync(entry);
  } catch {
    return false;
  }
}

if (isDirectExecution()) process.exitCode = await runCli();
