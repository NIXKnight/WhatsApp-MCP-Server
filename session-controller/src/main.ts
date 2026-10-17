import { randomBytes } from 'node:crypto';
import { realpathSync } from 'node:fs';
import { readFile } from 'node:fs/promises';
import { join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { loadConfig, type ControllerConfig } from './config.js';
import {
  ControllerError,
  type CompletionIndexPort,
  type DiagnosticInput,
  type DiagnosticPort,
  type PiHostPort,
  type PiSessionPort,
  type WaitPort
} from './domain.js';
import { createDiagnostics } from './diagnostics.js';
import { createWebhookServer } from './intake/server.js';
import { connectRedis } from './redis/client.js';
import { createRepository, readSessionMappings, type RedisRepository } from './redis/repository.js';
import { createCompletionIndex, type CompletionIndex } from './pi/completion-index.js';
import { loadPromptPolicy } from './pi/prompt-policy.js';
import { loadNativeSdk, type SdkSession } from './pi/mcp-loader.js';
import { createToolJournal, type JournalAttachment } from './pi/tool-journal-extension.js';
import type { CheckpointExtension } from './compaction/checkpoint-extension.js';
import { createCompactor, type Compactor } from './compaction/compactor.js';
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
        const [{ ModelRuntime }, { createCheckpointExtension }] = await Promise.all([
          import('@earendil-works/pi-coding-agent'),
          import('./compaction/checkpoint-extension.js')
        ]);
        options.checkpoint.initialize(createCheckpointExtension(options.repository, options.config.compaction));
        const modelRuntime = await ModelRuntime.create({
          authPath: join(options.config.pi.agentDir, 'auth.json'),
          modelsPath: join(options.config.pi.agentDir, 'models.json'),
          allowModelNetwork: false,
          refreshOnCreate: false
        });
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
            return options.checkpoint.run(state.session.sessionId, session, context, seed, instructions, '');
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

export interface CliDependencies {
  lifecycle(): Promise<number>;
  inventory(): Promise<number>;
  invalid(): void;
}
const productionCli: CliDependencies = {
  lifecycle: run,
  inventory: checkPiInventory,
  invalid: () => { process.stderr.write('{"status":"command_invalid"}\n'); }
};
export async function runCli(
  args: readonly string[] = process.argv.slice(2),
  dependencies: CliDependencies = productionCli
): Promise<number> {
  if (args.length === 0) return dependencies.lifecycle();
  if (args.length === 1 && args[0] === '--check-pi-inventory') return dependencies.inventory();
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
