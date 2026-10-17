import { execFile } from 'node:child_process';
import { mkdir, mkdtemp, readFile, rm, stat, symlink, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { dirname, isAbsolute, join, resolve, sep } from 'node:path';
import { fileURLToPath, pathToFileURL } from 'node:url';
import { promisify } from 'node:util';
import { describe, expect, it, vi } from 'vitest';
import { ControllerError, type PiSessionPort } from '../src/domain.js';
import { connectRedis } from '../src/redis/client.js';
import { readSessionMappings } from '../src/redis/repository.js';
import { createCoordinator, type CoordinatorRepository } from '../src/scheduler/coordinator.js';
import { createJournalTurns } from '../src/scheduler/chat-worker.js';
import {
  ADAPTER_WHATSAPP_SERVER, MCP_ADAPTER_EXTENSION, MCP_ADAPTER_SOURCE_PATH,
  MCP_STATUS_OBSERVER_SOURCE_PATH, createControllerMcpConfig,
  isConfiguredMcpAdapterExtension, isConfiguredMcpAdapterPackage, loadNativeSdk,
  REQUIRED_WHATSAPP_TOOLS, WHATSAPP_SERVER, withoutConfiguredMcpAdapter,
  withoutConfiguredMcpAdapterExtensions
} from '../src/pi/mcp-loader.js';
import type { SessionHost } from '../src/pi/session-host.js';
import type {
  LifecycleComposition,
  LifecycleContext,
  LifecycleSignal,
  SignalPort
} from '../src/main.js';

const inventoryFsProbe = { active: false, paths: [] as unknown[] };
const inventoryEnvironmentKeys = ['HOME', 'PI_CODING_AGENT_DIR', 'PI_PACKAGE_DIR'] as const;
type InventoryEnvironmentKey = (typeof inventoryEnvironmentKeys)[number];
interface EnvironmentSnapshot {
  readonly key: InventoryEnvironmentKey;
  readonly present: boolean;
  readonly value: string | undefined;
}
function snapshotInventoryEnvironment(): readonly EnvironmentSnapshot[] {
  return inventoryEnvironmentKeys.map(key => ({
    key,
    present: Object.prototype.hasOwnProperty.call(process.env, key),
    value: process.env[key]
  }));
}
function restoreInventoryEnvironment(snapshot: readonly EnvironmentSnapshot[]): void {
  for (const entry of snapshot) {
    if (entry.present) process.env[entry.key] = entry.value!;
    else delete process.env[entry.key];
  }
}
function expectInventoryEnvironment(snapshot: readonly EnvironmentSnapshot[], label?: string): void {
  for (const entry of snapshot) {
    expect(Object.prototype.hasOwnProperty.call(process.env, entry.key), `${label ?? ''}:${entry.key}:present`)
      .toBe(entry.present);
    if (entry.present) expect(process.env[entry.key], `${label ?? ''}:${entry.key}:value`).toBe(entry.value);
  }
}

// Vitest does not fake node:timers/promises. Route the production delay through
// the fakeable global timer while preserving the abort-aware timer contract.
vi.mock('node:timers/promises', () => ({
  setTimeout: <T = void>(ms: number, value?: T, options?: { readonly signal?: AbortSignal }): Promise<T> => new Promise<T>((resolve, reject) => {
    const signal = options?.signal;
    if (signal?.aborted) { reject(signal.reason); return; }
    const abort = (): void => { clearTimeout(timer); reject(signal?.reason); };
    const timer = setTimeout(() => { signal?.removeEventListener('abort', abort); resolve(value as T); }, ms);
    signal?.addEventListener('abort', abort, { once: true });
  })
}));

class FakeSignals implements SignalPort {
  private readonly listeners = new Map<LifecycleSignal, Set<() => void>>();
  private readonly onceWrappers = new Map<LifecycleSignal, Map<() => void, () => void>>();
  once(signal: LifecycleSignal, listener: () => void): void {
    const wrapped = (): void => { this.off(signal, listener); listener(); };
    const current = this.listeners.get(signal) ?? new Set<() => void>();
    const wrappers = this.onceWrappers.get(signal) ?? new Map<() => void, () => void>();
    current.add(wrapped); wrappers.set(listener, wrapped);
    this.listeners.set(signal, current); this.onceWrappers.set(signal, wrappers);
  }
  off(signal: LifecycleSignal, listener: () => void): void {
    const wrappers = this.onceWrappers.get(signal);
    const wrapped = wrappers?.get(listener) ?? listener;
    this.listeners.get(signal)?.delete(wrapped);
    wrappers?.delete(listener);
  }
  emit(signal: LifecycleSignal): void {
    for (const listener of [...(this.listeners.get(signal) ?? [])]) listener();
  }
  count(): number { return [...this.listeners.values()].reduce((sum, value) => sum + value.size, 0); }
}

function deferred(): { promise: Promise<void>; release(): void } {
  let release: (() => void) | undefined;
  const promise = new Promise<void>(resolve => { release = resolve; });
  return { promise, release: () => { release?.(); } };
}

function installResources(context: LifecycleContext, effects: string[], failures: ReadonlySet<string> = new Set()): void {
  const operation = (name: string) => async (): Promise<void> => {
    effects.push(name); if (failures.has(name)) throw new Error(`SENSITIVE_${name}`);
  };
  context.resources.redis = { isReady: () => true, close: operation('redis.close') };
  context.resources.repository = { close: operation('repository.close') };
  context.resources.host = { close: operation('host.close') };
  context.resources.coordinator = {
    start: async () => {},
    failClosed: () => { effects.push('coordinator.failClosed'); context.diagnostics.emit({ class: 'lease_lost' }); },
    stopScheduling: () => { effects.push('coordinator.stop'); if (failures.has('coordinator.stop')) throw new Error('SENSITIVE_STOP'); },
    drain: operation('coordinator.drain')
  };
  context.resources.server = {
    stopIntake: () => { effects.push('server.stop'); if (failures.has('server.stop')) throw new Error('SENSITIVE_INTAKE'); },
    close: operation('server.close')
  };
}

function composition(overrides: Partial<LifecycleComposition> = {}): LifecycleComposition {
  return {
    prepare: overrides.prepare ?? (async context => { installResources(context, []); }),
    waitForBridge: overrides.waitForBridge ?? (async () => {}),
    startCoordinator: overrides.startCoordinator ?? (async context => { queueMicrotask(() => context.requestStop(0)); }),
    monitorBridge: overrides.monitorBridge ?? (async context => {
      if (!context.signal.aborted) await new Promise<void>(resolve => context.signal.addEventListener('abort', () => resolve(), { once: true }));
      throw new ControllerError('intake_closed');
    })
  };
}

async function mainModule(): Promise<typeof import('../src/main.js')> {
  return import('../src/main.js');
}
const execFileAsync = promisify(execFile);

describe('main lifecycle composition', () => {
  it('qualifies only exact adapter package and explicit extension provenance without importing it', async () => {
    const root = await mkdtemp(join(tmpdir(), 'controller-adapter-qualification-'));
    const agentDir = join(root, 'agent');
    const projectBase = join(root, 'workspace', '.pi');
    const adapter = join(root, 'packages', 'adapter');
    const lookalike = join(root, 'packages', 'lookalike');
    const unrelated = join(root, 'packages', 'unrelated');
    const malformed = join(root, 'packages', 'malformed');
    try {
      await Promise.all([agentDir, projectBase, adapter, lookalike, unrelated, malformed]
        .map(path => mkdir(path, { recursive: true })));
      await writeFile(join(adapter, 'package.json'), JSON.stringify({ name: 'pi-mcp-adapter' }));
      await writeFile(join(adapter, 'index.ts'), 'throw new Error("QUALIFICATION_MUST_NOT_IMPORT");\n');
      await writeFile(join(lookalike, 'package.json'), JSON.stringify({ name: 'pi-mcp-adapter-extra' }));
      await writeFile(join(lookalike, 'index.ts'), 'export default () => {};\n');
      await writeFile(join(unrelated, 'package.json'), JSON.stringify({ name: 'other-plugin' }));
      await writeFile(join(unrelated, 'index.ts'), 'export default () => {};\n');
      await writeFile(join(malformed, 'package.json'), '{not-json');
      await writeFile(join(malformed, 'index.ts'), 'export default () => {};\n');
      await symlink(adapter, join(agentDir, 'adapter-dir-link'));
      await symlink(join(adapter, 'index.ts'), join(projectBase, 'adapter-file-link.ts'));

      const globalEntries = [
        '../packages/adapter/index.ts', 'adapter-dir-link', '../packages/lookalike/index.ts',
        '../packages/malformed/index.ts', './missing.ts', 'builtin:mcp',
        '+../packages/adapter/index.ts', '../packages/*/index.ts',
        'git:https://example.test/pi-mcp-adapter', 'https://example.test/adapter.ts'
      ];
      const projectEntries = [
        '../../packages/adapter', 'adapter-file-link.ts', '../../packages/unrelated/index.ts'
      ];
      expect(globalEntries.map(value => isConfiguredMcpAdapterExtension(value, agentDir))).toEqual([
        true, true, false, false, false, false, false, false, false, false
      ]);
      expect(projectEntries.map(value => isConfiguredMcpAdapterExtension(value, projectBase))).toEqual([
        true, true, false
      ]);
      expect(withoutConfiguredMcpAdapterExtensions(globalEntries, agentDir)).toEqual(globalEntries.slice(2));
      expect(withoutConfiguredMcpAdapterExtensions(projectEntries, projectBase)).toEqual([
        '../../packages/unrelated/index.ts'
      ]);
      expect(globalEntries[0]).toBe('../packages/adapter/index.ts');
    } finally {
      await rm(root, { recursive: true, force: true });
    }
  });

  it('retries startup status/fetch failures, disposes non-OK bodies, fails retry-timer errors, and strictly monitors bridge state', async () => {
    const { monitorBridge, runLifecycle, waitForBridge } = await mainModule();
    const config = { baseUrl: 'http://127.0.0.1:8080', pollMs: 17, requestTimeoutMs: 100 };
    let outstandingBodies = 0;
    const canceledBodies: string[] = [];
    const nonOK = (id: string): Response => {
      outstandingBodies++;
      return new Response(new ReadableStream<Uint8Array>({
        async cancel() { canceledBodies.push(id); outstandingBodies--; }
      }), { status: 503 });
    };
    const replies: Array<() => Response | Promise<Response>> = [
      () => new Response(JSON.stringify({ state: 'CONNECTED', is_connected: false }), { status: 200 }),
      () => new Response(JSON.stringify({ state: 'CONNECTING', is_connected: true }), { status: 200 }),
      () => new Response(JSON.stringify({ state: 'connected', is_connected: true }), { status: 200 }),
      () => new Response('{', { status: 200 }),
      () => nonOK('first'),
      () => { expect({ canceledBodies, outstandingBodies }).toEqual({ canceledBodies: ['first'], outstandingBodies: 0 }); return nonOK('second'); },
      async () => { expect({ canceledBodies, outstandingBodies }).toEqual({ canceledBodies: ['first', 'second'], outstandingBodies: 0 }); throw new Error('SENSITIVE_FETCH_EXCEPTION'); },
      () => new Response(JSON.stringify({ state: 'CONNECTED', is_connected: true }), { status: 200 })
    ];
    const waits: number[] = [];
    const request: typeof fetch = async () => {
      const reply = replies.shift();
      if (!reply) throw new Error('TEST_REPLY_MISSING');
      return reply();
    };
    await waitForBridge(config, new AbortController().signal, request, {
      wait: async ms => { waits.push(ms); }
    });
    expect(waits).toEqual([17, 17, 17, 17, 17, 17, 17]);
    expect(replies).toHaveLength(0);
    expect({ canceledBodies, outstandingBodies }).toEqual({ canceledBodies: ['first', 'second'], outstandingBodies: 0 });

    const disposalAbort = new AbortController();
    const cancelEntered = deferred();
    const cancelRelease = deferred();
    let disposalFetches = 0;
    let postDisposalWaits = 0;
    const disposing = waitForBridge(config, disposalAbort.signal, async () => {
      disposalFetches++;
      return new Response(new ReadableStream<Uint8Array>({
        async cancel() { cancelEntered.release(); await cancelRelease.promise; }
      }), { status: 503 });
    }, { wait: async () => { postDisposalWaits++; } });
    await cancelEntered.promise;
    disposalAbort.abort();
    cancelRelease.release();
    await expect(disposing).rejects.toMatchObject({ code: 'intake_closed' });
    expect({ disposalFetches, postDisposalWaits }).toEqual({ disposalFetches: 1, postDisposalWaits: 0 });

    for (const response of [
      new Response(JSON.stringify({ state: 'CONNECTING', is_connected: false }), { status: 200 }),
      new Response('{', { status: 200 }),
      new Response('{}', { status: 503 })
    ]) {
      let calls = 0;
      await expect(monitorBridge(
        config,
        new AbortController().signal,
        async () => { calls++; return calls === 1 ? new Response(JSON.stringify({ state: 'CONNECTED', is_connected: true })) : response; },
        { wait: async () => {} }
      )).rejects.toMatchObject({ code: 'internal_failed' });
      expect(calls).toBe(2);
    }

    const effects: string[] = [];
    const lines: string[] = [];
    const signals = new FakeSignals();
    let fetches = 0;
    let timerWaits = 0;
    const code = await runLifecycle(composition({
      prepare: async context => { installResources(context, effects); },
      waitForBridge: async context => waitForBridge(
        config,
        context.signal,
        async () => { fetches++; return new Response('{}', { status: 503 }); },
        { wait: async () => { timerWaits++; throw new Error('SENSITIVE_TIMER_FAILURE'); } }
      ),
      startCoordinator: async () => { throw new Error('START_MUST_NOT_RUN'); }
    }), { signals, writeDiagnostic: line => { lines.push(line); } });
    expect(code).toBe(1);
    expect(signals.count()).toBe(0);
    expect({ fetches, timerWaits }).toEqual({ fetches: 1, timerWaits: 1 });
    expect(effects).toEqual([
      'server.stop', 'coordinator.stop', 'coordinator.drain', 'server.close',
      'host.close', 'repository.close', 'redis.close'
    ]);
    expect(lines.map(line => (JSON.parse(line) as { class: string }).class)).toEqual(['internal_failed', 'stopped']);
    expect(lines.join('')).not.toContain('SENSITIVE_TIMER_FAILURE');
  });

  it('maps abort during an in-flight bridge fetch or retry wait to intake_closed', async () => {
    const { waitForBridge } = await mainModule();
    const config = { baseUrl: 'http://127.0.0.1:8080', pollMs: 17, requestTimeoutMs: 30000 };
    const fetchAbort = new AbortController();
    const enteredFetch = deferred();
    const request: typeof fetch = async (_input, init) => new Promise<Response>((_resolve, reject) => {
      enteredFetch.release();
      init?.signal?.addEventListener('abort', () => reject(new Error('SENSITIVE_FETCH_EXCEPTION')), { once: true });
    });
    const fetching = waitForBridge(config, fetchAbort.signal, request);
    await enteredFetch.promise; fetchAbort.abort();
    await expect(fetching).rejects.toMatchObject({ code: 'intake_closed' });

    const waitAbort = new AbortController();
    const enteredWait = deferred();
    const waiting = waitForBridge(config, waitAbort.signal, async () => new Response('{}'), {
      wait: async (_ms, signal) => new Promise<void>((_resolve, reject) => {
        enteredWait.release();
        signal.addEventListener('abort', () => reject(new Error('SENSITIVE_WAIT_EXCEPTION')), { once: true });
      })
    });
    await enteredWait.promise; waitAbort.abort();
    await expect(waiting).rejects.toMatchObject({ code: 'intake_closed' });
  });

  it('keeps readiness false until strict bridge completion and application-lease acquisition', async () => {
    const { runLifecycle } = await mainModule();
    const signals = new FakeSignals();
    const states: boolean[] = [];
    let current: LifecycleContext | undefined;
    const code = await runLifecycle(composition({
      prepare: async context => { current = context; installResources(context, []); states.push(context.isReady()); },
      waitForBridge: async context => { states.push(context.isReady()); },
      startCoordinator: async context => { states.push(context.isReady()); }
    }), {
      signals,
      writeDiagnostic: line => {
        if ((JSON.parse(line) as { class: string }).class === 'ready') {
          states.push(current?.isReady() ?? false); queueMicrotask(() => signals.emit('SIGTERM'));
        }
      }
    });
    expect(code).toBe(0);
    expect(states).toEqual([false, false, false, true]);
    expect(signals.count()).toBe(0);
  });

  it('lazily opens one fully inventoried host with one journal and preserves in-flight close outcomes', async () => {
    const { checkPiInventory, createLazyPiHost, runCli, runLifecycle } = await mainModule();
    const effects: string[] = [];
    const session = { binding: { canonicalChatId: '100@g.us', sessionId: 'S', sessionFile: '/synthetic/S.jsonl' }, isFresh: () => true } as PiSessionPort;
    const journalExtension = (() => {}) as never;
    const checkpointExtension = (() => {}) as never;
    const confirmed = vi.fn(async () => []);
    const sharedJournal = {
      extension: journalExtension,
      confirmed,
      classifyOwn: async () => 'manual' as const
    };
    const checkpoint = { extension: checkpointExtension };
    expect(REQUIRED_WHATSAPP_TOOLS).toHaveLength(21);
    expect(new Set(REQUIRED_WHATSAPP_TOOLS).size).toBe(REQUIRED_WHATSAPP_TOOLS.length);
    const expectedInventory = REQUIRED_WHATSAPP_TOOLS.map(
      name => `mcp__${WHATSAPP_SERVER}__${name}`
    );
    expect(new Set(expectedInventory).size).toBe(expectedInventory.length);
    expect(MCP_ADAPTER_SOURCE_PATH).toBe(`<inline:${MCP_ADAPTER_EXTENSION}>`);
    expect([
      'npm:pi-mcp-adapter', 'npm:pi-mcp-adapter@5.3.1',
      '  npm:pi-mcp-adapter@5.3.1\n',
      { source: 'npm:pi-mcp-adapter', extensions: ['index.ts'] },
      { source: '\t npm:pi-mcp-adapter \r\n', extensions: ['index.ts'] }
    ].every(value => isConfiguredMcpAdapterPackage(value))).toBe(true);
    expect([
      './pi-mcp-adapter', 'npm:pi-mcp-adapter-extra',
      'npm:alias@npm:pi-mcp-adapter', 'git:https://example.test/pi-mcp-adapter'
    ].some(value => isConfiguredMcpAdapterPackage(value))).toBe(false);
    const configuredPackages = [
      'npm:pi-subagents', 'npm:pi-mcp-adapter',
      { source: 'npm:hindsight', extensions: ['extension.ts'] },
      { source: '  npm:pi-mcp-adapter@5.3.1\n', extensions: ['index.ts'] },
      './operator-plugin'
    ];
    expect(withoutConfiguredMcpAdapter(configuredPackages)).toEqual([
      'npm:pi-subagents',
      { source: 'npm:hindsight', extensions: ['extension.ts'] },
      './operator-plugin'
    ]);
    const validTools = expectedInventory.map(name => ({
      name, description: '', parameters: { type: 'object' }, exposure: 'direct' as const,
      sourceInfo: { path: MCP_ADAPTER_SOURCE_PATH, source: 'inline', scope: 'temporary', origin: 'top-level' }
    }));
    const loaderConfig = {
      bridge: { baseUrl: 'http://127.0.0.1:8080' },
      pi: { cwd: '/synthetic/workspace' },
      mcp: { command: '/synthetic/mcp', args: [], cwd: '/synthetic' }
    } as never;
    const originalOutputGuard = process.env['MCP_OUTPUT_GUARD'];
    const originalDirectTools = process.env['MCP_DIRECT_TOOLS'];
    const importSdk = vi.fn(async () => ({} as never));
    const importAdapter = vi.fn(async () => ({} as never));
    try {
      process.env['MCP_OUTPUT_GUARD'] = '0';
      await expect(loadNativeSdk(loaderConfig, {} as never, {} as never, [], { kind: 'full' }, importSdk, importAdapter))
        .rejects.toMatchObject({ code: 'config_invalid' });
      expect(importSdk).not.toHaveBeenCalled();
      expect(importAdapter).not.toHaveBeenCalled();
      delete process.env['MCP_OUTPUT_GUARD'];
      process.env['MCP_DIRECT_TOOLS'] = '__none__';
      await expect(loadNativeSdk(loaderConfig, {} as never, {} as never, [], { kind: 'full' }, importSdk, importAdapter))
        .rejects.toMatchObject({ code: 'config_invalid' });
      expect(importSdk).not.toHaveBeenCalled();
      expect(importAdapter).not.toHaveBeenCalled();
    } finally {
      if (originalOutputGuard === undefined) delete process.env['MCP_OUTPUT_GUARD'];
      else process.env['MCP_OUTPUT_GUARD'] = originalOutputGuard;
      if (originalDirectTools === undefined) delete process.env['MCP_DIRECT_TOOLS'];
      else process.env['MCP_DIRECT_TOOLS'] = originalDirectTools;
    }

    type InventoryTool = {
      name: string;
      description: string;
      parameters: { type: string };
      exposure: 'direct' | 'codemode' | 'hidden';
      sourceInfo: { path: string; source: string; scope: string; origin: string };
    };
    interface InventoryState {
      tools: InventoryTool[];
      activeNames: string[];
      callableNames: string[];
      reads: { tools: number; active: number; callable: number };
    }
    interface SyntheticExtensionApi {
      readonly events: {
        on(name: string, listener: (value: unknown) => void): void;
        emit(name: string, value: unknown): void;
      };
      on(name: string, listener: (...args: readonly unknown[]) => unknown): void;
    }
    type SyntheticExtensionFactory = (api: SyntheticExtensionApi) => void | Promise<void>;
    interface SyntheticLoaderOptions {
      readonly extensionFactories?: readonly { readonly factory: SyntheticExtensionFactory }[];
    }
    class SyntheticResourceLoader {
      readonly factories: readonly SyntheticExtensionFactory[];
      constructor(options: SyntheticLoaderOptions) {
        this.factories = options.extensionFactories?.map(extension => extension.factory) ?? [];
      }
      async reload(): Promise<void> {}
      getExtensions() { return { extensions: [], errors: [] }; }
    }
    const createdStates: InventoryState[] = [];
    const publishedStatuses: unknown[] = [];
    const syntheticSdk = {
      SessionManager: {
        create: () => ({}),
        open: () => ({})
      },
      SettingsManager: {
        create: () => ({
          getGlobalSettings: () => ({}),
          getProjectSettings: () => ({}),
          isProjectTrusted: () => true
        }),
        fromStorage: (storage: {
          withLock(scope: 'global' | 'project', fn: (current?: string) => string | undefined): void;
        }, options: { projectTrusted?: boolean }) => {
          const read = (scope: 'global' | 'project'): Record<string, unknown> => {
            let value: string | undefined;
            storage.withLock(scope, current => { value = current; return undefined; });
            return value ? JSON.parse(value) as Record<string, unknown> : {};
          };
          return {
            getGlobalSettings: () => read('global'),
            getProjectSettings: () => options.projectTrusted === false ? {} : read('project'),
            isProjectTrusted: () => options.projectTrusted !== false,
            async reload() {}
          };
        }
      },
      DefaultResourceLoader: SyntheticResourceLoader,
      createCodemodeExtension: () => (() => {}),
      createToolSearchExtension: () => (() => {}),
      async createAgentSession(options: { resourceLoader: SyntheticResourceLoader }) {
        const listeners = new Map<string, Set<(value: unknown) => void>>();
        const api: SyntheticExtensionApi = {
          events: {
            on(name, listener) {
              const registered = listeners.get(name) ?? new Set<(value: unknown) => void>();
              registered.add(listener);
              listeners.set(name, registered);
            },
            emit(name, value) {
              for (const listener of listeners.get(name) ?? []) listener(value);
            }
          },
          on: () => {}
        };
        for (const factory of options.resourceLoader.factories) await factory(api);
        const state: InventoryState = {
          tools: validTools.map(tool => ({ ...tool, parameters: { ...tool.parameters }, sourceInfo: { ...tool.sourceInfo } })),
          activeNames: [...expectedInventory],
          callableNames: [...expectedInventory],
          reads: { tools: 0, active: 0, callable: 0 }
        };
        createdStates.push(state);
        return { session: {
          isIdle: true,
          extensionRunner: { async emit() {} },
          async prompt() {},
          subscribe: () => () => {},
          async waitForIdle() {},
          getContextUsage: () => undefined,
          async compact() { throw new Error('SYNTHETIC_UNUSED'); },
          async bindExtensions() {},
          setAutoCompactionEnabled() {},
          getAllTools() { state.reads.tools++; return state.tools; },
          getActiveToolNames() { state.reads.active++; return state.activeNames; },
          getCallableToolNames() { state.reads.callable++; return state.callableNames; },
          getToolDefinition: () => undefined,
          async dispose() {}
        } };
      }
    };
    const native = await loadNativeSdk(
      loaderConfig, { system: '' } as never, {} as never, [], { kind: 'full' },
      async () => syntheticSdk as never,
      async () => ({
        createMcpAdapter: () => ((pi: SyntheticExtensionApi) => {
          const snapshot = {
            version: 1,
            servers: [{
              name: ADAPTER_WHATSAPP_SERVER, status: 'connected', listenState: 'active',
              toolCount: 21, directToolCount: 21, disabled: false, catalogStale: false
            }]
          };
          publishedStatuses.push(snapshot);
          pi.events.emit('pi-mcp-adapter/status/v1', snapshot);
        }) as never
      })
    );
    const connectValidInventory = async (): Promise<{ session: Awaited<ReturnType<typeof native.connect>>; state: InventoryState }> => {
      const stateCount = createdStates.length;
      const statusCount = publishedStatuses.length;
      const session = await native.connect(native.create('/synthetic/workspace', '/synthetic/sessions'), async () => {});
      expect(createdStates, 'connected-state').toHaveLength(stateCount + 1);
      expect(publishedStatuses, 'connected-status').toHaveLength(statusCount + 1);
      expect(publishedStatuses.at(-1)).toEqual({
        version: 1,
        servers: [{
          name: ADAPTER_WHATSAPP_SERVER, status: 'connected', listenState: 'active',
          toolCount: 21, directToolCount: 21, disabled: false, catalogStale: false
        }]
      });
      return { session, state: createdStates.at(-1)! };
    };
    const genericTool = (name: string, exposure: InventoryTool['exposure']): InventoryTool => ({
      ...validTools[0]!, name, exposure, sourceInfo: { ...validTools[0]!.sourceInfo }
    });
    const invalidInventories: readonly {
      readonly kind: string;
      readonly mutate: (state: InventoryState) => void;
    }[] = [
      { kind: 'missing', mutate: state => { state.tools = state.tools.slice(1); } },
      { kind: 'duplicate', mutate: state => { state.tools.push({ ...state.tools[0]!, sourceInfo: { ...state.tools[0]!.sourceInfo } }); } },
      {
        kind: 'extra',
        mutate: state => {
          const name = `mcp__${WHATSAPP_SERVER}__synthetic_extra`;
          state.tools.push(genericTool(name, 'direct'));
          state.activeNames.push(name);
          state.callableNames.push(name);
        }
      },
      {
        kind: 'wrong-path',
        mutate: state => {
          state.tools[0] = { ...state.tools[0]!, sourceInfo: { ...state.tools[0]!.sourceInfo, path: '<inline:shadow>' } };
        }
      },
      {
        kind: 'wrong-source',
        mutate: state => {
          state.tools[0] = { ...state.tools[0]!, sourceInfo: { ...state.tools[0]!.sourceInfo, source: 'extension' } };
        }
      },
      {
        kind: 'hidden-required',
        mutate: state => { state.tools[0] = { ...state.tools[0]!, exposure: 'hidden' }; }
      },
      { kind: 'inactive-required', mutate: state => { state.activeNames = state.activeNames.slice(1); } },
      {
        kind: 'active-generic',
        mutate: state => {
          state.tools.push(genericTool('mcp', 'direct'));
          state.activeNames.push('mcp');
          state.callableNames.push('mcp');
        }
      },
      {
        kind: 'callable-script',
        mutate: state => {
          state.tools.push(genericTool('mcpScript', 'codemode'));
          state.callableNames.push('mcpScript');
        }
      },
      {
        kind: 'active-namespace-proxy',
        mutate: state => {
          const name = `mcp__${ADAPTER_WHATSAPP_SERVER}`;
          state.tools.push(genericTool(name, 'direct'));
          state.activeNames.push(name);
          state.callableNames.push(name);
        }
      }
    ];

    const validInventory = await connectValidInventory();
    const validCurrent = vi.fn(async () => {});
    await expect(native.ready(validInventory.session, validCurrent)).resolves.toBeUndefined();
    expect(validCurrent).toHaveBeenCalledTimes(1);
    expect(validInventory.state.reads).toEqual({ tools: 1, active: 1, callable: 1 });
    await validInventory.session.dispose();

    vi.useFakeTimers();
    try {
      for (const { kind, mutate } of invalidInventories) {
        const { session: readySession, state } = await connectValidInventory();
        const baselineCurrent = vi.fn(async () => {});
        await expect(native.ready(readySession, baselineCurrent), `${kind}:connected-baseline`).resolves.toBeUndefined();
        expect(baselineCurrent, `${kind}:connected-baseline`).toHaveBeenCalledTimes(1);
        expect(state.reads, `${kind}:connected-baseline`).toEqual({ tools: 1, active: 1, callable: 1 });
        mutate(state);
        state.reads = { tools: 0, active: 0, callable: 0 };
        const current = vi.fn(async () => {});
        const readiness = native.ready(readySession, current);
        const rejected = expect(readiness, kind).rejects.toMatchObject({ code: 'session_failed' });
        await Promise.resolve(); // schedule the first production delay
        for (let delay = 0; delay < 100; delay++) {
          expect(vi.getTimerCount(), `${kind}:delay:${delay}`).toBe(1);
          await vi.advanceTimersByTimeAsync(100);
        }
        await rejected;
        expect(current, kind).toHaveBeenCalledTimes(100);
        expect(state.reads, kind).toEqual({ tools: 100, active: 100, callable: 100 });
        expect(vi.getTimerCount(), kind).toBe(0);
        await readySession.dispose();
      }
    } finally { vi.useRealTimers(); }

    // Exercise the installed public loader, not a synthetic substitute. Hostile
    // configured roots and the real preflight HOME contain every resource kind;
    // only the owned temporary roots may be probed by inventory discovery.
    const hostileRoot = await mkdtemp(join(tmpdir(), 'phase9-hostile-resources-'));
    const ownedRoot = await mkdtemp(join(tmpdir(), 'phase9-owned-resources-'));
    const hostileCwd = join(hostileRoot, 'configured-cwd');
    const hostileAgentDir = join(hostileRoot, 'configured-agent');
    const hostileHome = join(hostileRoot, 'configured-home');
    const hostilePackageDir = join(hostileRoot, 'configured-packages');
    const ownedRoots = {
      root: ownedRoot,
      cwd: join(ownedRoot, 'cwd'),
      agentDir: join(ownedRoot, 'agent'),
      home: join(ownedRoot, 'home'),
      sessionDir: join(ownedRoot, 'sessions')
    };
    for (const directory of [
      ownedRoots.cwd, ownedRoots.agentDir, join(ownedRoots.home, '.agents'),
      join(ownedRoots.root, 'packages'), ownedRoots.sessionDir
    ]) await mkdir(directory, { recursive: true, mode: 0o700 });
    const hostileFiles = [
      join(hostileCwd, '.pi', 'extensions', 'hostile.ts'),
      join(hostileCwd, '.pi', 'skills', 'hostile', 'SKILL.md'),
      join(hostileCwd, '.pi', 'prompts', 'hostile.md'),
      join(hostileCwd, '.pi', 'themes', 'hostile.json'),
      join(hostileCwd, '.agents', 'skills', 'hostile', 'SKILL.md'),
      join(hostileCwd, 'AGENTS.md'),
      join(hostileCwd, '.pi', 'SYSTEM.md'),
      join(hostileCwd, '.pi', 'APPEND_SYSTEM.md'),
      join(hostileCwd, '.pi', 'mcp.json'),
      join(hostileCwd, '.pi', 'settings.json'),
      join(hostileAgentDir, 'extensions', 'hostile.ts'),
      join(hostileAgentDir, 'skills', 'hostile', 'SKILL.md'),
      join(hostileAgentDir, 'prompts', 'hostile.md'),
      join(hostileAgentDir, 'themes', 'hostile.json'),
      join(hostileAgentDir, 'AGENTS.md'),
      join(hostileAgentDir, 'SYSTEM.md'),
      join(hostileAgentDir, 'APPEND_SYSTEM.md'),
      join(hostileAgentDir, 'mcp.json'),
      join(hostileAgentDir, 'settings.json'),
      join(hostileHome, '.agents', 'skills', 'hostile', 'SKILL.md'),
      join(hostilePackageDir, 'hostile-package', 'package.json')
    ];
    for (const path of hostileFiles) {
      await mkdir(dirname(path), { recursive: true, mode: 0o700 });
      await writeFile(path, 'HOSTILE_RESOURCE_MUST_NOT_BE_READ\n', { mode: 0o600 });
    }
    const preflightConfig = {
      bridge: { baseUrl: 'http://127.0.0.1:8080' },
      pi: {
        systemPromptPath: join(hostileRoot, 'operator-system.md'),
        preCompactionPath: join(hostileRoot, 'operator-pre.md'),
        compactionSummaryPath: join(hostileRoot, 'operator-summary.md'),
        agentDir: hostileAgentDir,
        cwd: hostileCwd,
        sessionDir: join(hostileRoot, 'persistent-must-not-be-used'),
        provider: 'operator-provider', model: 'operator-model'
      },
      mcp: { command: '/synthetic/owned-mcp', args: ['--owned'], cwd: '/synthetic/mcp-cwd' }
    } as never;
    const fs = (await import('node:fs')).default;
    const { syncBuiltinESMExports } = await import('node:module');
    const mutableFs = fs as unknown as {
      existsSync: typeof fs.existsSync; lstatSync: typeof fs.lstatSync;
      readFileSync: typeof fs.readFileSync; readdirSync: typeof fs.readdirSync;
      realpathSync: typeof fs.realpathSync; statSync: typeof fs.statSync;
    };
    const originalFs = {
      existsSync: fs.existsSync, lstatSync: fs.lstatSync, readFileSync: fs.readFileSync,
      readdirSync: fs.readdirSync, realpathSync: fs.realpathSync, statSync: fs.statSync
    };
    const recordFsPath = (value: unknown): void => {
      if (inventoryFsProbe.active) inventoryFsProbe.paths.push(value);
    };
    mutableFs.existsSync = ((path: Parameters<typeof fs.existsSync>[0]) => {
      recordFsPath(path); return originalFs.existsSync(path);
    }) as typeof fs.existsSync;
    mutableFs.lstatSync = ((path: Parameters<typeof fs.lstatSync>[0], ...rest: unknown[]) => {
      recordFsPath(path); return Reflect.apply(originalFs.lstatSync, fs, [path, ...rest]);
    }) as typeof fs.lstatSync;
    mutableFs.readFileSync = ((path: Parameters<typeof fs.readFileSync>[0], ...rest: unknown[]) => {
      recordFsPath(path); return Reflect.apply(originalFs.readFileSync, fs, [path, ...rest]);
    }) as typeof fs.readFileSync;
    mutableFs.readdirSync = ((path: Parameters<typeof fs.readdirSync>[0], ...rest: unknown[]) => {
      recordFsPath(path); return Reflect.apply(originalFs.readdirSync, fs, [path, ...rest]);
    }) as typeof fs.readdirSync;
    mutableFs.realpathSync = ((path: Parameters<typeof fs.realpathSync>[0], ...rest: unknown[]) => {
      recordFsPath(path); return Reflect.apply(originalFs.realpathSync, fs, [path, ...rest]);
    }) as typeof fs.realpathSync;
    mutableFs.statSync = ((path: Parameters<typeof fs.statSync>[0], ...rest: unknown[]) => {
      recordFsPath(path); return Reflect.apply(originalFs.statSync, fs, [path, ...rest]);
    }) as typeof fs.statSync;
    syncBuiltinESMExports();
    const installedPi = await import('@earendil-works/pi-coding-agent');
    const selectedModel = { id: 'operator-model' };
    const getSelectedModel = vi.fn((provider: string, model: string) => {
      expect({ provider, model }).toEqual({ provider: 'operator-provider', model: 'operator-model' });
      return selectedModel;
    });
    const suppliedRuntime = { getModel: getSelectedModel } as never;
    const installedEffects: string[] = [];
    const nativeDispose = vi.fn(async () => { installedEffects.push('dispose'); });
    let nativeSession: Record<string, unknown> | undefined;
    const inventoryPi = {
      ...installedPi,
      SessionManager: {
        create(cwd: string, directory: string) {
          installedEffects.push(`manager:${cwd}:${directory}`);
          return {} as never;
        },
        open() { throw new Error('INVENTORY_OPEN_MUST_NOT_RUN'); }
      },
      createMcpExtension() { throw new Error('BUILTIN_MCP_MUST_NOT_LOAD'); },
      async createAgentSession(options: Parameters<typeof installedPi.createAgentSession>[0]) {
        expect(options?.cwd).toBe(ownedRoots.cwd);
        expect(options?.agentDir).toBe(ownedRoots.agentDir);
        expect(options?.modelRuntime).toBe(suppliedRuntime);
        expect(options?.model).toBe(selectedModel);
        expect(options?.settingsManager?.isProjectTrusted()).toBe(false);
        expect(options?.noTools).toBe('builtin');
        const loader = options?.resourceLoader;
        expect(loader).toBeDefined();
        expect(loader!.getExtensions().extensions.map(extension => extension.path)).toEqual([
          MCP_STATUS_OBSERVER_SOURCE_PATH, MCP_ADAPTER_SOURCE_PATH
        ]);
        expect(loader!.getExtensions().errors).toEqual([]);
        expect(loader!.getSkills()).toEqual({ skills: [], diagnostics: [] });
        expect(loader!.getPrompts()).toEqual({ prompts: [], diagnostics: [] });
        expect(loader!.getThemes()).toEqual({ themes: [], diagnostics: [] });
        expect(loader!.getAgentsFiles()).toEqual({ agentsFiles: [] });
        expect(loader!.getSystemPrompt()).toBeUndefined();
        expect(loader!.getAppendSystemPrompt()).toEqual([]);
        expect(loader!.getExtensions().runtime.mcpServers.list()).toEqual([]);
        nativeSession = {
          isIdle: true,
          getAllTools: () => validTools,
          getActiveToolNames: () => expectedInventory,
          getCallableToolNames: () => expectedInventory,
          getToolDefinition: () => undefined,
          extensionRunner: { async emit(event: { type: string; reason: string }) {
            expect(event).toEqual({ type: 'session_shutdown', reason: 'quit' });
            installedEffects.push('shutdown');
          } },
          setAutoCompactionEnabled(enabled: boolean) { installedEffects.push(`auto:${String(enabled)}`); },
          async bindExtensions() { installedEffects.push('bind'); },
          async prompt() {},
          subscribe() { return () => {}; },
          async waitForIdle() {},
          getContextUsage: () => undefined,
          async compact() { throw new Error('SYNTHETIC_UNUSED'); },
          dispose: nativeDispose
        };
        return { session: nativeSession as never };
      }
    } as unknown as typeof installedPi;
    const installedStatuses: string[] = [];
    const initialEnvironment = snapshotInventoryEnvironment();
    process.env.HOME = hostileHome;
    process.env.PI_CODING_AGENT_DIR = hostileAgentDir;
    process.env.PI_PACKAGE_DIR = hostilePackageDir;
    const hostileEnvironment = snapshotInventoryEnvironment();
    const originalConsoleLog = console.log;
    const stdout = vi.spyOn(process.stdout, 'write');
    const stderr = vi.spyOn(process.stderr, 'write');
    inventoryFsProbe.paths.splice(0);
    inventoryFsProbe.active = true;
    try {
      const installedCode = await checkPiInventory({
        load: () => preflightConfig,
        loadPolicy: async () => ({} as never),
        validate: async () => {},
        createRuntime: async config => {
          expect(config.pi.agentDir).toBe(hostileAgentDir);
          expect(process.env.HOME).toBe(ownedRoots.home);
          expect(process.env.PI_CODING_AGENT_DIR).toBe(ownedRoots.agentDir);
          expect(process.env.PI_PACKAGE_DIR).toBe(join(ownedRoots.root, 'packages'));
          return suppliedRuntime;
        },
        createTemporary: async () => ownedRoots,
        removeTemporary: async roots => { expect(roots).toBe(ownedRoots); },
        loadSdk: (async (
          config: Parameters<typeof loadNativeSdk>[0],
          policy: Parameters<typeof loadNativeSdk>[1],
          runtime: Parameters<typeof loadNativeSdk>[2],
          extra: Parameters<typeof loadNativeSdk>[3],
          mode: Parameters<typeof loadNativeSdk>[4]
        ) => loadNativeSdk(config, policy, runtime, extra, mode, async () => inventoryPi, async () => ({
          createMcpAdapter(options) {
            console.log('SENSITIVE_ADAPTER_STARTUP_MUST_NOT_BE_FORWARDED');
            const adapterConfig = options?.config;
            expect(adapterConfig).toEqual(createControllerMcpConfig(preflightConfig));
            expect(adapterConfig).toBeDefined();
            expect(adapterConfig!.mcpServers[ADAPTER_WHATSAPP_SERVER]).toMatchObject({
              env: {
                MCP_TRANSPORT: 'stdio', BRIDGE_URL: 'http://127.0.0.1:8080',
                WORKSPACE_DIR: hostileCwd, LOG_LEVEL: 'INFO'
              },
              literalEnv: true,
              inheritEnv: false
            });
            return ((pi: { events: { emit(channel: string, value: unknown): void } }) => {
              installedEffects.push('owned-mcp-factory');
              pi.events.emit('pi-mcp-adapter/status/v1', {
                version: 1,
                servers: [{
                  name: ADAPTER_WHATSAPP_SERVER, status: 'connected', listenState: 'active',
                  toolCount: 21, directToolCount: 21, disabled: false
                }],
                totalTools: 21, totalResources: 0, connectedCount: 1, disabledCount: 0
              });
            }) as never;
          }
        }))) as never,
        status: value => { installedStatuses.push(value); }
      });
      expect(installedCode).toBe(0);
      expect(installedStatuses).toEqual(['pi_inventory_ready']);
      expect(console.log).toBe(originalConsoleLog);
      expect(stdout.mock.calls.flat().join('')).not.toContain('SENSITIVE_ADAPTER_STARTUP_MUST_NOT_BE_FORWARDED');
      expectInventoryEnvironment(hostileEnvironment, 'inventory-success');
      expect(getSelectedModel).toHaveBeenCalledTimes(1);
      expect(installedEffects).toEqual([
        `manager:${ownedRoots.cwd}:${ownedRoots.sessionDir}`, 'owned-mcp-factory',
        'auto:false', 'bind', 'shutdown', 'dispose'
      ]);
      expect(nativeSession?.['dispose']).toBe(nativeDispose);
      expect(nativeDispose).toHaveBeenCalledTimes(1);
      const probedPaths = inventoryFsProbe.paths.flatMap(value => {
        if (typeof value === 'string' && isAbsolute(value)) return [resolve(value)];
        if (Buffer.isBuffer(value)) return [resolve(value.toString())];
        if (value instanceof URL && value.protocol === 'file:') return [resolve(fileURLToPath(value))];
        return [];
      });
      expect(probedPaths.length).toBeGreaterThan(0);
      expect(probedPaths.every(path => path === ownedRoot || path.startsWith(`${ownedRoot}${sep}`))).toBe(true);
      expect(probedPaths.some(path => [hostileCwd, hostileAgentDir, hostileHome, hostilePackageDir]
        .some(hostile => path === hostile || path.startsWith(`${hostile}${sep}`)))).toBe(false);
      inventoryFsProbe.active = false;

      const failingShutdown = vi.fn(async () => { throw new Error('SYNTHETIC_SHUTDOWN_FAILURE'); });
      const failingNativeDispose = vi.fn(async () => {});
      const failingPi = {
        ...inventoryPi,
        async createAgentSession() {
          return { session: {
            isIdle: true,
            getAllTools: () => validTools,
            getActiveToolNames: () => expectedInventory,
            getCallableToolNames: () => expectedInventory,
            getToolDefinition: () => undefined,
            extensionRunner: { emit: failingShutdown },
            setAutoCompactionEnabled() {}, bindExtensions: async () => {},
            prompt: async () => {}, subscribe: () => () => {}, waitForIdle: async () => {},
            getContextUsage: () => undefined,
            compact: async () => { throw new Error('SYNTHETIC_UNUSED'); },
            dispose: failingNativeDispose
          } as never };
        }
      } as never;
      const failingSdk = await loadNativeSdk(
        preflightConfig, {} as never, suppliedRuntime, [],
        { kind: 'inventory-preflight', root: ownedRoot, cwd: ownedRoots.cwd, agentDir: ownedRoots.agentDir },
        async () => failingPi,
        async () => ({ createMcpAdapter: () => (() => {}) }) as never
      );
      const failingSession = await failingSdk.connect(
        failingSdk.create(ownedRoots.cwd, ownedRoots.sessionDir), async () => {}
      );
      const firstDisposal = failingSession.dispose();
      const secondDisposal = failingSession.dispose();
      expect(secondDisposal).toBe(firstDisposal);
      await expect(firstDisposal).rejects.toThrow('SYNTHETIC_SHUTDOWN_FAILURE');
      await expect(secondDisposal).rejects.toThrow('SYNTHETIC_SHUTDOWN_FAILURE');
      expect(failingShutdown).toHaveBeenCalledTimes(1);
      expect(failingNativeDispose).toHaveBeenCalledTimes(1);
      expect(stderr).not.toHaveBeenCalled();
    } finally {
      inventoryFsProbe.active = false;
      Object.assign(mutableFs, originalFs);
      syncBuiltinESMExports();
      stdout.mockRestore();
      stderr.mockRestore();
      restoreInventoryEnvironment(initialEnvironment);
      await rm(hostileRoot, { recursive: true, force: true });
      await rm(ownedRoot, { recursive: true, force: true });
    }

    const preflightEffects: string[] = [];
    const preflightStatuses: string[] = [];
    let preflightRoots: {
      root: string; cwd: string; agentDir: string; home: string; sessionDir: string;
    } | undefined;
    const environmentBeforeSuccess = snapshotInventoryEnvironment();
    const prohibited = {
      lifecycle: vi.fn(async () => 77),
      prompt: vi.fn(async () => {}),
      redis: vi.fn(), bridgeEvent: vi.fn(), mapping: vi.fn(), scheduler: vi.fn(), webhook: vi.fn()
    };
    const preflightSession = {
      prompt: prohibited.prompt,
      setAutoCompactionEnabled(enabled: boolean) { preflightEffects.push(`auto:${String(enabled)}`); },
      async bindExtensions() { preflightEffects.push('bind'); },
      async dispose() { preflightEffects.push('dispose'); }
    } as never;
    const preflightSdk = {
      create(cwd: string, root: string) {
        preflightEffects.push(`create:${cwd}:${root}`);
        return { cwd, root };
      },
      async connect() { preflightEffects.push('connect'); return preflightSession; },
      async ready() { preflightEffects.push('ready'); }
    } as never;
    const preflightCode = await checkPiInventory({
      load() { preflightEffects.push('load'); return preflightConfig; },
      async loadPolicy() { preflightEffects.push('policy'); return {} as never; },
      async validate(path) { preflightEffects.push(`validate:${path}`); },
      async createRuntime(config) {
        preflightEffects.push('runtime');
        expect(config.pi.agentDir).toBe(hostileAgentDir);
        expect(process.env.PI_PACKAGE_DIR).toBeDefined();
        await expect(stat(process.env.PI_PACKAGE_DIR!)).resolves.toMatchObject({ mode: expect.any(Number) });
        return {} as never;
      },
      loadSdk: (async (
        _config: Parameters<typeof loadNativeSdk>[0],
        _policy: Parameters<typeof loadNativeSdk>[1],
        _runtime: Parameters<typeof loadNativeSdk>[2],
        extra: Parameters<typeof loadNativeSdk>[3],
        mode: Parameters<typeof loadNativeSdk>[4]
      ) => {
        preflightEffects.push('sdk');
        expect(extra).toEqual([]);
        if (!mode || mode.kind !== 'inventory-preflight') throw new Error('SYNTHETIC_WRONG_MODE');
        preflightRoots = {
          root: mode.root,
          cwd: mode.cwd,
          agentDir: mode.agentDir,
          home: join(mode.root, 'home'),
          sessionDir: join(mode.root, 'sessions')
        };
        expect(process.env.HOME).toBe(preflightRoots.home);
        expect(process.env.PI_CODING_AGENT_DIR).toBe(preflightRoots.agentDir);
        expect(process.env.PI_PACKAGE_DIR).toBe(join(preflightRoots.root, 'packages'));
        for (const directory of [
          preflightRoots.root, preflightRoots.cwd, preflightRoots.agentDir, preflightRoots.home,
          join(preflightRoots.home, '.agents'), process.env.PI_PACKAGE_DIR!, preflightRoots.sessionDir
        ]) {
          expect((await stat(directory)).mode & 0o777, directory).toBe(0o700);
        }
        return preflightSdk;
      }) as never,
      status(value) { preflightEffects.push(`status:${value}`); preflightStatuses.push(value); }
    });
    expect(preflightCode).toBe(0);
    expect(preflightRoots).toBeDefined();
    await expect(stat(preflightRoots!.root)).rejects.toMatchObject({ code: 'ENOENT' });
    expectInventoryEnvironment(environmentBeforeSuccess, 'production-temporary-success');
    expect(preflightStatuses).toEqual(['pi_inventory_ready']);
    expect(preflightEffects).toEqual([
      'load', 'policy',
      `validate:${join(hostileRoot, 'operator-system.md')}`,
      `validate:${join(hostileRoot, 'operator-pre.md')}`,
      `validate:${join(hostileRoot, 'operator-summary.md')}`,
      'runtime', 'sdk',
      `create:${preflightRoots!.cwd}:${preflightRoots!.sessionDir}`,
      'connect', 'auto:false', 'bind', 'ready', 'dispose', 'status:pi_inventory_ready'
    ]);
    expect(Object.values(prohibited).every(spy => spy.mock.calls.length === 0)).toBe(true);

    const environmentBeforeTemporaryFailure = snapshotInventoryEnvironment();
    let failedTemporaryRoot: string | undefined;
    const failureStatuses: string[] = [];
    const forbiddenFailureLoad = vi.fn();
    const temporaryFailureCode = await checkPiInventory({
      load: () => preflightConfig,
      loadPolicy: async () => ({} as never),
      validate: async () => {},
      createRuntime: async config => {
        expect(config.pi.agentDir).toBe(hostileAgentDir);
        expect(process.env.PI_PACKAGE_DIR).toBeDefined();
        failedTemporaryRoot = dirname(process.env.PI_PACKAGE_DIR!);
        throw new Error('SENSITIVE_RUNTIME_FAILURE');
      },
      loadSdk: forbiddenFailureLoad as never,
      status: value => { failureStatuses.push(value); }
    });
    expect(temporaryFailureCode).toBe(1);
    expect(failedTemporaryRoot).toBeDefined();
    await expect(stat(failedTemporaryRoot!)).rejects.toMatchObject({ code: 'ENOENT' });
    expectInventoryEnvironment(environmentBeforeTemporaryFailure, 'production-temporary-failure');
    expect(failureStatuses).toEqual(['pi_inventory_failed']);
    expect(forbiddenFailureLoad).not.toHaveBeenCalled();

    const dispatchEffects: string[] = [];
    const cli = {
      lifecycle: async () => { dispatchEffects.push('lifecycle'); return 7; },
      inventory: async () => { dispatchEffects.push('inventory'); return 0; },
      invalid: () => { dispatchEffects.push('{"status":"command_invalid"}'); }
    };
    expect(await runCli([], cli)).toBe(7);
    expect(dispatchEffects.splice(0)).toEqual(['lifecycle']);
    expect(await runCli(['--check-pi-inventory'], cli)).toBe(0);
    expect(dispatchEffects.splice(0)).toEqual(['inventory']);
    for (const args of [['--unknown'], ['--check-pi-inventory', 'extra'], ['', '--check-pi-inventory']]) {
      expect(await runCli(args, cli)).toBe(2);
      expect(dispatchEffects.splice(0)).toEqual(['{"status":"command_invalid"}']);
    }

    for (const failure of ['connect', 'ready', 'dispose', 'remove', 'ready+dispose+remove'] as const) {
      const rowEffects: string[] = [];
      const rowStatuses: string[] = [];
      const fails = (stage: string): boolean => failure.split('+').includes(stage);
      const rowSession = {
        setAutoCompactionEnabled() { rowEffects.push('auto'); },
        async bindExtensions() { rowEffects.push('bind'); },
        async dispose() {
          rowEffects.push('dispose');
          if (fails('dispose')) throw new Error('SENSITIVE_DISPOSE_FAILURE');
        }
      } as never;
      const rowSdk = {
        create() { rowEffects.push('create'); return {}; },
        async connect() {
          rowEffects.push('connect');
          if (fails('connect')) throw new Error('SENSITIVE_CONNECT_FAILURE');
          return rowSession;
        },
        async ready() {
          rowEffects.push('ready');
          if (fails('ready')) throw new Error('SENSITIVE_READY_FAILURE');
        }
      } as never;
      const environmentBeforeRow = snapshotInventoryEnvironment();
      const expectedEnvironmentPresent = failure !== 'connect';
      for (const key of inventoryEnvironmentKeys) {
        if (expectedEnvironmentPresent) process.env[key] = `/synthetic/hostile-environment/${failure}/${key}`;
        else delete process.env[key];
      }
      const expectedEnvironmentAfterRow = snapshotInventoryEnvironment();
      const code = await checkPiInventory({
        load: () => preflightConfig,
        loadPolicy: async () => ({} as never),
        validate: async () => {},
        createRuntime: async () => {
          expect(process.env.HOME, failure).toBe('/synthetic/owned-temp/home');
          expect(process.env.PI_CODING_AGENT_DIR, failure).toBe('/synthetic/owned-temp/agent');
          expect(process.env.PI_PACKAGE_DIR, failure).toBe('/synthetic/owned-temp/packages');
          return {} as never;
        },
        createTemporary: async () => {
          rowEffects.push('temporary');
          return {
            root: '/synthetic/owned-temp', cwd: '/synthetic/owned-temp/cwd',
            agentDir: '/synthetic/owned-temp/agent', home: '/synthetic/owned-temp/home',
            sessionDir: '/synthetic/owned-temp/sessions'
          };
        },
        removeTemporary: async () => {
          rowEffects.push('remove');
          if (fails('remove')) throw new Error('SENSITIVE_REMOVE_FAILURE');
        },
        loadSdk: (async () => rowSdk) as never,
        status(value) { rowEffects.push(`status:${value}`); rowStatuses.push(value); }
      });
      expect(code, failure).toBe(1);
      expectInventoryEnvironment(expectedEnvironmentAfterRow, `inventory-failure:${failure}`);
      restoreInventoryEnvironment(environmentBeforeRow);
      expect(rowStatuses, failure).toEqual(['pi_inventory_failed']);
      expect(rowEffects.at(-1), failure).toBe('status:pi_inventory_failed');
      expect(rowEffects, failure).toContain('remove');
      if (failure !== 'connect') expect(rowEffects, failure).toContain('dispose');
      if (failure === 'ready+dispose+remove') {
        expect(rowEffects.indexOf('dispose')).toBeLessThan(rowEffects.indexOf('remove'));
      }
      expect(JSON.stringify(rowStatuses), failure).not.toContain('SENSITIVE_');
    }

    type RedisFailure = 'writer-connect' | 'writer-baseline' | 'reader-connect' |
      'mapping-connect' | 'mapping-baseline' | 'cleanup-chaos';
    for (const failure of [
      'writer-connect', 'writer-baseline', 'reader-connect',
      'mapping-connect', 'mapping-baseline', 'cleanup-chaos'
    ] as const satisfies readonly RedisFailure[]) {
      const rowEffects: string[] = [];
      const names = ['writer', 'reader', 'mapping'] as const;
      const clients = names.map(name => ({
        on: vi.fn(),
        async connect() {
          rowEffects.push(`connect:${name}`);
          if (failure === `${name}-connect`) throw new Error(`SENSITIVE_${name}_CONNECT`);
        },
        async sendCommand(args: readonly string[]): Promise<unknown> {
          rowEffects.push(`command:${name}:${args.join(':')}`);
          const baselineFailure = failure === `${name}-baseline` || failure === 'cleanup-chaos' && name === 'writer';
          if (baselineFailure && args[0] === 'INFO' && args[1] === 'server') {
            throw new Error(`SENSITIVE_${name}_BASELINE`);
          }
          if (args[0] === 'INFO' && args[1] === 'server') return 'redis_version:7.2.6\r\n';
          if (args[0] === 'INFO' && args[1] === 'persistence') {
            return 'aof_enabled:1\r\naof_last_write_status:ok\r\n';
          }
          if (args[0] === 'CONFIG' && args[1] === 'GET') {
            const key = args[2];
            if (key === 'appendonly') return [key, 'yes'];
            if (key === 'appendfsync') return [key, 'always'];
            if (key === 'maxmemory-policy') return [key, 'noeviction'];
          }
          throw new Error('SENSITIVE_UNEXPECTED_COMMAND');
        },
        get isOpen() {
          rowEffects.push(`isOpen:${name}`);
          if (failure === 'cleanup-chaos' && name === 'mapping') throw new Error('SENSITIVE_IS_OPEN');
          return true;
        },
        get isReady() { return true; },
        destroy() {
          rowEffects.push(`destroy:${name}`);
          if (failure === 'cleanup-chaos' && name === 'reader') throw new Error('SENSITIVE_DESTROY');
        },
        async close() {}
      }));
      let created = 0;
      const factory = vi.fn(() => clients[created++]!);
      let rejected: unknown;
      try {
        await connectRedis({ url: 'redis://synthetic.invalid:6379/0', waitAofMs: 1 } as never, factory as never);
      } catch (error: unknown) { rejected = error; }
      expect(rejected, failure).toMatchObject({ code: 'store_unavailable' });
      expect(String(rejected), failure).not.toContain('SENSITIVE_');
      expect(factory, failure).toHaveBeenCalledTimes(3);
      expect(rowEffects.filter(value => value.startsWith('isOpen:')), failure).toEqual([
        'isOpen:mapping', 'isOpen:reader', 'isOpen:writer'
      ]);
      if (failure === 'cleanup-chaos') {
        expect(rowEffects.filter(value => value.startsWith('destroy:')), failure).toEqual([
          'destroy:reader', 'destroy:writer'
        ]);
      } else {
        expect(rowEffects.filter(value => value.startsWith('destroy:')), failure).toEqual([
          'destroy:mapping', 'destroy:reader', 'destroy:writer'
        ]);
      }
    }

    let inventoryReady = false;
    const concrete: SessionHost = {
      findEvent: async () => undefined,
      classifyOwn: async () => 'manual',
      open: async () => { expect(inventoryReady).toBe(true); effects.push('host.open'); return session; },
      attach: async () => {},
      close: async () => { effects.push('concrete.close'); }
    };
    const lazyOptions = (): Parameters<typeof createLazyPiHost>[0] => ({
      config: {} as never, policy: {} as never, repository: {} as never,
      index: {} as never, journal: sharedJournal as never,
      checkpoint: checkpoint as never, compactor: {} as never
    });
    let initialized = 0;
    const lazy = createLazyPiHost(lazyOptions(), async wiring => {
      initialized++;
      expect(wiring.journal).toBe(sharedJournal);
      expect(wiring.extensions).toHaveLength(2);
      expect(wiring.extensions[0]).toBe(journalExtension);
      expect(wiring.extensions[1]).toBe(checkpointExtension);
      expect(expectedInventory).toHaveLength(21);
      expect(inventoryReady).toBe(false);
      expect(validTools.map(tool => tool.name)).toEqual(expectedInventory);
      inventoryReady = true;
      effects.push('pi+checkpoint+mcp:inventory-ready');
      return concrete;
    });
    const turnContext = {} as never;
    await createJournalTurns(sharedJournal as never, async () => {}).confirmed(turnContext);
    expect(confirmed).toHaveBeenCalledTimes(1);
    expect(confirmed).toHaveBeenCalledWith(turnContext);

    const signals = new FakeSignals();
    const code = await runLifecycle(composition({
      prepare: async context => { installResources(context, effects); context.resources.host = lazy; },
      waitForBridge: async () => { expect(initialized).toBe(0); effects.push('bridge.ready'); },
      startCoordinator: async context => {
        expect(initialized).toBe(0);
        await lazy.open({} as never); await lazy.open({} as never);
        queueMicrotask(() => context.requestStop(0));
      }
    }), { signals, writeDiagnostic: () => {} });
    expect(code).toBe(0);
    expect(signals.count()).toBe(0);
    expect(initialized).toBe(1);
    expect(effects.slice(0, 4)).toEqual([
      'bridge.ready', 'pi+checkpoint+mcp:inventory-ready', 'host.open', 'host.open'
    ]);

    const rejectionGate = deferred();
    const rejecting = createLazyPiHost(lazyOptions(), async () => {
      await rejectionGate.promise;
      throw new Error('SENSITIVE_INITIALIZATION_REJECTION');
    });
    const rejectedOpen = rejecting.open({} as never);
    const rejectedClose = rejecting.close();
    rejectionGate.release();
    await expect(rejectedOpen).rejects.toThrow('SENSITIVE_INITIALIZATION_REJECTION');
    await expect(rejectedClose).resolves.toBeUndefined();

    const resolutionGate = deferred();
    const closeFailure = new Error('SENSITIVE_RESOLVED_HOST_CLOSE_FAILURE');
    const closeFailureHost: SessionHost = {
      ...concrete,
      close: async () => { throw closeFailure; }
    };
    const resolving = createLazyPiHost(lazyOptions(), async () => {
      await resolutionGate.promise;
      return closeFailureHost;
    });
    const closedOpen = resolving.open({} as never);
    const resolvedClose = resolving.close();
    resolutionGate.release();
    await expect(closedOpen).rejects.toMatchObject({ code: 'session_failed' });
    await expect(resolvedClose).rejects.toBe(closeFailure);

    const lines: string[] = [];
    const cleanupSignals = new FakeSignals();
    const lifecycleCode = await runLifecycle(composition({
      prepare: async context => { installResources(context, []); context.resources.host = resolving; },
      startCoordinator: async context => { queueMicrotask(() => context.requestStop(0)); }
    }), { signals: cleanupSignals, writeDiagnostic: line => { lines.push(line); } });
    expect(lifecycleCode).toBe(1);
    expect(cleanupSignals.count()).toBe(0);
    expect(lines.map(line => (JSON.parse(line) as { class: string }).class)).toContain('session_failed');
  });

  it('reads mappings only through the independent mapping command port', async () => {
    const commands: readonly string[][] = [];
    const mutable = commands as string[][];
    const rows = await readSessionMappings({
      mapping: { sendCommand: async args => { mutable.push([...args]); return null; } }
    }, { groups: ['100@g.us'], operatorJids: ['300@lid'] });
    expect(rows).toEqual([{ chat: '100@g.us' }, { chat: '300@lid' }]);
    expect(commands).toHaveLength(2);
    expect(commands.every(command => command[0] === 'HGET')).toBe(true);
  });

  it('routes real worker, loop, heartbeat, and bridge fail-closed loss through lifecycle without another prompt', async () => {
    const { runLifecycle } = await mainModule();
    for (const source of ['worker', 'loop', 'heartbeat'] as const) {
      let current: LifecycleContext | undefined;
      let claimed = false;
      let prompts = 0;
      let leaseDiagnostics = 0;
      let failClosedCalls = 0;
      let startCoordinatorReturned = false;
      const enteredWorkerRenew = deferred();
      const releaseWorkerRenew = deferred();
      const failureArmed = deferred();
      const triggerFailure = deferred();
      const readiness: boolean[] = [];
      const lines: string[] = [];
      const signals = new FakeSignals();
      const app = { owner: 'a'.repeat(32), epoch: '1', expiresAtMs: 2000000000000 };
      const code = await runLifecycle(composition({
        prepare: async context => {
          current = context;
          installResources(context, []);
          const repository = {
            chats: ['100@g.us', '200@g.us'],
            acquireApp: async () => app,
            renewApp: async () => {
              if (source === 'heartbeat') {
                await enteredWorkerRenew.promise;
                failureArmed.release();
                await triggerFailure.promise;
                throw new Error('SENSITIVE_HEARTBEAT_FAILURE');
              }
              return app;
            },
            releaseApp: async () => true,
            recoverPending: async (chat: string) => {
              if (source === 'loop' && chat === '200@g.us') {
                failureArmed.release();
                await triggerFailure.promise;
                throw new Error('SENSITIVE_LOOP_FAILURE');
              }
              return { status: 'ok' as const, next: '0-0', claimed: 0, deleted: 0 };
            },
            claimNext: async (chat: string) => {
              if (chat !== '100@g.us' || claimed) return { status: 'empty' as const };
              claimed = true;
              return { status: 'claimed' as const, work: {} as never };
            },
            renew: async () => {
              enteredWorkerRenew.release();
              if (source === 'worker') {
                failureArmed.release();
                await triggerFailure.promise;
                return undefined;
              }
              await releaseWorkerRenew.promise;
              return {} as never;
            },
            checkWork: async () => { throw new Error('APPCURRENT_FALSE_MUST_SHORT_CIRCUIT'); },
            readChat: async () => { throw new Error('PRE_PROMPT_READ_MUST_NOT_RUN'); },
            bindSession: async () => { throw new Error('PRE_PROMPT_BIND_MUST_NOT_RUN'); },
            turnState: async () => { throw new Error('PRE_PROMPT_TURN_MUST_NOT_RUN'); },
            beginTurn: async () => { throw new Error('PRE_PROMPT_BEGIN_MUST_NOT_RUN'); },
            pause: async () => { throw new Error('PRE_PROMPT_PAUSE_MUST_NOT_RUN'); },
            complete: async () => { throw new Error('PRE_PROMPT_COMPLETE_MUST_NOT_RUN'); }
          } as unknown as CoordinatorRepository;
          const coordinator = createCoordinator(repository, {
            scheduler: { concurrency: 2, leaseMs: 120000, renewMs: 100, gapTimeoutMs: 1000 },
            pollMs: 1,
            ids: { next: (() => { let id = 0; return () => (++id).toString(16).padStart(32, '0'); })() },
            worker: {
              instructions: () => 'SYNTHETIC',
              host: {
                findEvent: async () => undefined,
                classifyOwn: async () => 'manual',
                open: async () => { prompts++; throw new Error('PROMPT_PATH_MUST_NOT_OPEN'); },
                close: async () => {}
              },
              turns: {
                attach: async () => {}, prepare: async () => {}, run: async () => {}, confirmed: async () => []
              },
              wait: {
                async wait(ms, signal) {
                  if (signal.aborted) return;
                  if (source === 'heartbeat' && ms === 100) return;
                  await new Promise<void>(resolve => {
                    const immediate = setImmediate(resolve);
                    signal.addEventListener('abort', () => { clearImmediate(immediate); resolve(); }, { once: true });
                  });
                }
              },
              diagnostics: {
                emit(input) {
                  if (input.class === 'lease_lost') leaseDiagnostics++;
                  context.diagnostics.emit(input);
                }
              }
            }
          });
          context.resources.coordinator = {
            start: () => coordinator.start(),
            failClosed: () => { failClosedCalls++; coordinator.failClosed(); },
            stopScheduling: () => coordinator.stopScheduling(),
            drain: () => coordinator.drain()
          };
        },
        startCoordinator: async context => {
          await context.resources.coordinator!.start();
          await enteredWorkerRenew.promise;
          await failureArmed.promise;
          startCoordinatorReturned = true;
        }
      }), {
        signals,
        writeDiagnostic: line => {
          lines.push(line);
          const event = JSON.parse(line) as { class: string };
          if (event.class === 'ready') {
            expect(startCoordinatorReturned).toBe(true);
            readiness.push(current?.isReady() ?? false);
            triggerFailure.release();
          }
          if (event.class === 'lease_lost') {
            readiness.push(current?.isReady() ?? true);
            releaseWorkerRenew.release();
          }
        }
      });
      expect(code).toBe(1);
      expect(signals.count()).toBe(0);
      expect(claimed).toBe(true);
      expect(readiness).toEqual([true, false]);
      expect(failClosedCalls).toBe(1);
      expect(leaseDiagnostics).toBe(2); // first observer plus failClosed's guarded reentrant report
      expect(prompts).toBe(0);
      expect(lines.map(line => (JSON.parse(line) as { class: string }).class)).toEqual(['ready', 'lease_lost', 'stopped']);
    }

    let current: LifecycleContext | undefined;
    let claimed = false;
    let prompts = 0;
    let leaseDiagnostics = 0;
    let failClosedCalls = 0;
    const enteredRenew = deferred();
    const releaseRenew = deferred();
    const monitorFailure = deferred();
    const readiness: boolean[] = [];
    const lines: string[] = [];
    const signals = new FakeSignals();
    const code = await runLifecycle(composition({
      prepare: async context => {
        current = context;
        installResources(context, []);
        const repository = {
          chats: ['100@g.us'],
          acquireApp: async () => ({ owner: 'a'.repeat(32), epoch: '1', expiresAtMs: 2000000000000 }),
          renewApp: async () => ({ owner: 'a'.repeat(32), epoch: '1', expiresAtMs: 2000000000000 }),
          releaseApp: async () => true,
          recoverPending: async () => ({ status: 'ok' as const, next: '0-0', claimed: 0, deleted: 0 }),
          claimNext: async () => {
            if (claimed) return { status: 'empty' as const };
            claimed = true;
            return { status: 'claimed' as const, work: {} as never };
          },
          renew: async () => { enteredRenew.release(); await releaseRenew.promise; return {} as never; },
          checkWork: async () => { throw new Error('APPCURRENT_FALSE_MUST_SHORT_CIRCUIT'); },
          readChat: async () => { throw new Error('PRE_PROMPT_READ_MUST_NOT_RUN'); },
          bindSession: async () => { throw new Error('PRE_PROMPT_BIND_MUST_NOT_RUN'); },
          turnState: async () => { throw new Error('PRE_PROMPT_TURN_MUST_NOT_RUN'); },
          beginTurn: async () => { throw new Error('PRE_PROMPT_BEGIN_MUST_NOT_RUN'); },
          pause: async () => { throw new Error('PRE_PROMPT_PAUSE_MUST_NOT_RUN'); },
          complete: async () => { throw new Error('PRE_PROMPT_COMPLETE_MUST_NOT_RUN'); }
        } as unknown as CoordinatorRepository;
        const coordinator = createCoordinator(repository, {
          scheduler: { concurrency: 1, leaseMs: 120000, renewMs: 10000, gapTimeoutMs: 1000 },
          pollMs: 1,
          ids: { next: (() => { let id = 0; return () => (++id).toString(16).padStart(32, '0'); })() },
          worker: {
            instructions: () => 'SYNTHETIC',
            host: {
              findEvent: async () => undefined,
              classifyOwn: async () => 'manual',
              open: async () => { prompts++; throw new Error('PROMPT_PATH_MUST_NOT_OPEN'); },
              close: async () => {}
            },
            turns: {
              attach: async () => {}, prepare: async () => {}, run: async () => {}, confirmed: async () => []
            },
            wait: {
              async wait(ms, signal) {
                if (signal.aborted) return;
                await new Promise<void>(resolve => {
                  const immediate = ms === 1 ? setImmediate(resolve) : undefined;
                  signal.addEventListener('abort', () => {
                    if (immediate) clearImmediate(immediate);
                    resolve();
                  }, { once: true });
                });
              }
            },
            diagnostics: {
              emit(input) {
                if (input.class === 'lease_lost') leaseDiagnostics++;
                context.diagnostics.emit(input);
              }
            }
          }
        });
        context.resources.coordinator = {
          start: () => coordinator.start(),
          failClosed: () => { failClosedCalls++; coordinator.failClosed(); },
          stopScheduling: () => coordinator.stopScheduling(),
          drain: () => coordinator.drain()
        };
      },
      startCoordinator: async context => {
        await context.resources.coordinator!.start();
        await enteredRenew.promise; // claimed and fenced, but not yet prompted
      },
      monitorBridge: async () => { await monitorFailure.promise; throw new ControllerError('internal_failed'); }
    }), {
      signals,
      writeDiagnostic: line => {
        lines.push(line);
        const event = JSON.parse(line) as { class: string };
        if (event.class === 'ready') {
          readiness.push(current?.isReady() ?? false);
          queueMicrotask(() => monitorFailure.release());
        }
        if (event.class === 'lease_lost') {
          readiness.push(current?.isReady() ?? true);
          releaseRenew.release();
        }
      }
    });
    expect(code).toBe(1);
    expect(signals.count()).toBe(0);
    expect(claimed).toBe(true);
    expect(readiness).toEqual([true, false]);
    expect(failClosedCalls).toBe(1);
    expect(leaseDiagnostics).toBe(2); // failClosed plus the active worker's appCurrent fence
    expect(prompts).toBe(0);
    expect(lines.map(line => (JSON.parse(line) as { class: string }).class)).toEqual(['ready', 'lease_lost', 'stopped']);
  });

  it('handles SIGINT and SIGTERM with exact controller-first indefinite drain order', async () => {
    const { runLifecycle } = await mainModule();
    for (const signal of ['SIGINT', 'SIGTERM'] as const) {
      const signals = new FakeSignals();
      const effects: string[] = [];
      const gate = deferred();
      const entered = deferred();
      const running = runLifecycle(composition({
        prepare: async context => {
          installResources(context, effects);
          context.resources.coordinator = {
            start: async () => {},
            failClosed: () => { throw new Error('SIGNAL_MUST_NOT_FAIL_CLOSED'); },
            stopScheduling: () => { effects.push('coordinator.stop'); },
            drain: async () => { effects.push('coordinator.drain:start'); entered.release(); await gate.promise; effects.push('coordinator.drain:end'); }
          };
        },
        startCoordinator: async () => { queueMicrotask(() => signals.emit(signal)); }
      }), { signals, writeDiagnostic: () => {} });
      await entered.promise;
      expect(effects).toEqual(['server.stop', 'coordinator.stop', 'coordinator.drain:start']);
      gate.release(); expect(await running).toBe(0);
      expect(effects).toEqual([
        'server.stop', 'coordinator.stop', 'coordinator.drain:start', 'coordinator.drain:end',
        'server.close', 'host.close', 'repository.close', 'redis.close'
      ]);
      expect(signals.count()).toBe(0);
    }
  });

  it('cleans every acquired partial-startup boundary exactly once', async () => {
    const { runLifecycle } = await mainModule();
    for (const boundary of [1, 2, 3, 4, 5]) {
      const effects: string[] = [];
      const signals = new FakeSignals();
      const code = await runLifecycle(composition({
        prepare: async context => {
          const allEffects: string[] = [];
          installResources(context, allEffects);
          if (boundary < 5) delete context.resources.server;
          if (boundary < 4) delete context.resources.coordinator;
          if (boundary < 3) delete context.resources.host;
          if (boundary < 2) delete context.resources.repository;
          const wrap = (name: string, close: () => Promise<void>): (() => Promise<void>) => async () => { effects.push(name); await close(); };
          if (context.resources.redis) context.resources.redis.close = wrap('redis', context.resources.redis.close);
          if (context.resources.repository) context.resources.repository.close = wrap('repository', context.resources.repository.close);
          if (context.resources.host) context.resources.host.close = wrap('host', context.resources.host.close);
          if (context.resources.coordinator) context.resources.coordinator.drain = wrap('coordinator', context.resources.coordinator.drain);
          if (context.resources.server) context.resources.server.close = wrap('server', context.resources.server.close);
          throw new ControllerError('config_invalid');
        }
      }), { signals, writeDiagnostic: () => {} });
      expect(code).toBe(1);
      expect(signals.count()).toBe(0);
      expect(new Set(effects).size).toBe(effects.length);
      expect(effects).toHaveLength(boundary);
    }
  });

  it('attempts all cleanup after repository and Redis close rejection and computes a nonzero exit only after the last attempt', async () => {
    const { runLifecycle } = await mainModule();
    const effects: string[] = [];
    const failures = new Set(['coordinator.drain', 'server.close', 'host.close', 'repository.close', 'redis.close']);
    const lines: string[] = [];
    const signals = new FakeSignals();
    const code = await runLifecycle(composition({
      prepare: async context => { installResources(context, effects, failures); },
      startCoordinator: async context => { queueMicrotask(() => context.requestStop(0)); }
    }), { signals, writeDiagnostic: line => { lines.push(line); } });
    expect(code).toBe(1);
    expect(signals.count()).toBe(0);
    expect(effects).toEqual([
      'server.stop', 'coordinator.stop', 'coordinator.drain', 'server.close',
      'host.close', 'repository.close', 'redis.close'
    ]);
    const classes = lines.map(line => (JSON.parse(line) as { class: string }).class);
    expect(classes.filter(value => value === 'store_unavailable')).toHaveLength(2);
    expect(classes.at(-1)).toBe('stopped');
  });

  it('redacts sensitive markers, paths, URLs, payloads, credentials, and exception text from diagnostics', async () => {
    const { runLifecycle } = await mainModule();
    const lines: string[] = [];
    const signals = new FakeSignals();
    const sensitive = [
      'SECRET_MARKER', '/home/operator/private/session.jsonl', 'redis://user:pass@127.0.0.1:6379/0',
      '{"message":"payload"}', 'Bearer TEST_CREDENTIAL', 'EXCEPTION_TEXT'
    ];
    const code = await runLifecycle(composition({
      prepare: async context => {
        installResources(context, []);
        throw new Error(sensitive.join(' :: '));
      }
    }), { signals, writeDiagnostic: line => { lines.push(line); } });
    expect(code).toBe(1);
    expect(signals.count()).toBe(0);
    expect(lines.map(line => (JSON.parse(line) as { class: string }).class)).toEqual(['internal_failed', 'stopped']);
    for (const marker of sensitive) expect(lines.join('')).not.toContain(marker);
  });

  async function verifyNodeRuntimePolicy(): Promise<void> {
    const repositoryRoot = resolve(dirname(fileURLToPath(import.meta.url)), '../..');
    const packageJson = JSON.parse(await readFile(join(repositoryRoot, 'session-controller/package.json'), 'utf8')) as {
      engines: { node: string }; volta: { node: string };
    };
    const packageLock = JSON.parse(await readFile(join(repositoryRoot, 'session-controller/package-lock.json'), 'utf8')) as {
      packages: { '': { engines: { node: string } } };
    };
    expect(packageJson.engines.node).toBe('22.19.0 || 24.12.0');
    expect(packageJson.volta.node).toBe('24.12.0');
    expect(packageLock.packages[''].engines.node).toBe('22.19.0 || 24.12.0');

    const installer = await readFile(join(repositoryRoot, 'scripts/install-session-controller.sh'), 'utf8');
    const setup = await readFile(join(repositoryRoot, 'scripts/setup-systemd.sh'), 'utf8');
    for (const source of [installer, setup]) {
      expect(source).toContain('"v22.19.0" || "${node_version}" == "v24.12.0"');
    }
    expect(installer).toContain('NPM_CLI="$(resolve_executable npm)"');
    expect(installer.match(/"\$\{NODE_BIN\}" "\$\{NPM_CLI\}"/g)).toHaveLength(4);

    const root = await mkdtemp(join(tmpdir(), 'phase10-systemd-node-'));
    const bin = join(root, 'bin');
    const home = join(root, 'home');
    const runtime = join(root, 'runtime');
    await Promise.all([mkdir(bin), mkdir(home), mkdir(runtime)]);
    const executable = async (name: string, body: string): Promise<string> => {
      const path = join(bin, name);
      await writeFile(path, `#!/usr/bin/env bash\n${body}\n`, { mode: 0o700 });
      return path;
    };
    const node = await executable('node', "printf '%s\\n' 'v24.12.0'");
    await executable('redis-server', "printf '%s\\n' 'Redis server v=7.2.6'");
    for (const name of ['redis-cli', 'rsync', 'systemctl', 'systemd-analyze']) {
      await executable(name, 'exit 0');
    }
    try {
      await execFileAsync('/usr/bin/bash', [join(repositoryRoot, 'scripts/setup-systemd.sh'), 'session-controller'], {
        cwd: repositoryRoot,
        env: { HOME: home, XDG_RUNTIME_DIR: runtime, PATH: `${bin}:/usr/bin:/bin` },
        timeout: 10000
      });
      const unit = await readFile(join(home, '.config/systemd/user/whatsapp-session-controller.service'), 'utf8');
      expect(unit).toContain(`ExecStart=${node} %h/.local/share/whatsapp-session-controller/current/dist/src/main.js`);
    } finally {
      await rm(root, { recursive: true, force: true });
    }
  }

  it('imports main in an isolated child without config, Redis, listener, Pi, auth, or MCP effects', async () => {
    await verifyNodeRuntimePolicy();
    const root = await mkdtemp(join(tmpdir(), 'phase9-main-import-'));
    const probe = join(root, 'import.test.mjs');
    const mainUrl = new URL('../src/main.ts', import.meta.url).href;
    const vitestCli = fileURLToPath(new URL('../node_modules/vitest/vitest.mjs', import.meta.url));
    const probeSource = String.raw`
import { expect, test, vi } from 'vitest';
import childProcess from 'node:child_process';
import net from 'node:net';
const { readFile } = vi.hoisted(() => ({
  readFile: vi.fn(async () => { throw new Error('FILESYSTEM_READ_GUARD'); })
}));
vi.mock('node:fs/promises', async importOriginal => {
  const actual = await importOriginal();
  return { ...actual, readFile };
});
test('isolated import', async () => {
  const effects = [];
  const originalEnv = process.env;
  const originalOn = process.on;
  const originalOnce = process.once;
  const beforeSignals = [process.listenerCount('SIGINT'), process.listenerCount('SIGTERM')];
  const stdout = vi.spyOn(process.stdout, 'write').mockImplementation(() => true);
  const stderr = vi.spyOn(process.stderr, 'write').mockImplementation(() => true);
  const fetchSpy = vi.spyOn(globalThis, 'fetch').mockImplementation(async () => { effects.push('fetch'); throw new Error('fetch'); });
  const onSpy = vi.spyOn(process, 'on').mockImplementation(function (event, listener) {
    if (event === 'SIGINT' || event === 'SIGTERM') effects.push('listener:' + String(event));
    return originalOn.call(this, event, listener);
  });
  const onceSpy = vi.spyOn(process, 'once').mockImplementation(function (event, listener) {
    if (event === 'SIGINT' || event === 'SIGTERM') effects.push('listener:' + String(event));
    return originalOnce.call(this, event, listener);
  });
  const netConnect = vi.spyOn(net, 'connect').mockImplementation(() => { effects.push('redis/network'); throw new Error('network'); });
  const netCreate = vi.spyOn(net, 'createConnection').mockImplementation(() => { effects.push('redis/network'); throw new Error('network'); });
  const spawn = vi.spyOn(childProcess, 'spawn').mockImplementation(() => { effects.push('pi/mcp-child'); throw new Error('spawn'); });
  process.env = new Proxy({ ...originalEnv }, {
    get(target, key, receiver) {
      if (typeof key === 'string' && /^(SESSION_CONTROLLER_|BRIDGE_NOTIFY_|BRIDGE_URL$)/.test(key)) effects.push('config:' + key);
      return Reflect.get(target, key, receiver);
    }
  });
  let loaded;
  try {
    loaded = await import(${JSON.stringify(mainUrl)});
    if (stdout.mock.calls.length > 0 || stderr.mock.calls.length > 0) effects.push('output');
    if (beforeSignals[0] !== process.listenerCount('SIGINT') || beforeSignals[1] !== process.listenerCount('SIGTERM')) effects.push('listener-count');
  } finally {
    process.env = originalEnv;
    stdout.mockRestore(); stderr.mockRestore(); fetchSpy.mockRestore();
    onSpy.mockRestore(); onceSpy.mockRestore();
    netConnect.mockRestore(); netCreate.mockRestore(); spawn.mockRestore();
  }
  expect(typeof loaded.runLifecycle).toBe('function');
  expect(readFile).not.toHaveBeenCalled();
  expect(effects).toEqual([]);
});
`;
    const childEnv: NodeJS.ProcessEnv = { NODE_ENV: 'test', NO_COLOR: '1' };
    if (process.env['PATH']) childEnv['PATH'] = process.env['PATH'];
    try {
      await writeFile(probe, probeSource, { encoding: 'utf8', mode: 0o600 });
      await execFileAsync(process.execPath, [vitestCli, 'run', '--root', root, '--reporter=dot'], {
        cwd: root, env: childEnv, timeout: 30000, maxBuffer: 1024 * 1024
      });

      const releaseA = join(root, 'release-a');
      const releaseB = join(root, 'release-b');
      const current = join(root, 'current');
      const output = join(root, 'direct-output');
      const importProbe = join(root, 'import-probe.mjs');
      await mkdir(releaseA); await mkdir(releaseB);
      await writeFile(join(releaseA, 'version.mjs'), "export const version = 'A';\n", 'utf8');
      await writeFile(join(releaseB, 'version.mjs'), "export const version = 'B';\n", 'utf8');
      const releaseMain = String.raw`
import { realpathSync } from 'node:fs';
import { appendFile, symlink, unlink } from 'node:fs/promises';
import { fileURLToPath } from 'node:url';
function isDirectExecution() {
  const entry = process.argv[1];
  if (!entry) return false;
  try { return realpathSync(fileURLToPath(import.meta.url)) === realpathSync(entry); }
  catch { return false; }
}
if (isDirectExecution()) {
  await appendFile(process.env.PHASE9_OUTPUT, 'entered:A\n');
  await unlink(process.env.PHASE9_CURRENT);
  await symlink('release-b', process.env.PHASE9_CURRENT, 'dir');
  const { version } = await import('./version.mjs');
  await appendFile(process.env.PHASE9_OUTPUT, 'dynamic:' + version + '\n');
}
`;
      await writeFile(join(releaseA, 'main.mjs'), releaseMain, 'utf8');
      await symlink('release-a', current, 'dir');
      const currentMain = join(current, 'main.mjs');
      await writeFile(importProbe, `await import(${JSON.stringify(pathToFileURL(currentMain).href)});\n`, 'utf8');
      const probeEnv: NodeJS.ProcessEnv = {
        ...childEnv, PHASE9_CURRENT: current, PHASE9_OUTPUT: output
      };
      await execFileAsync(process.execPath, [importProbe], { cwd: root, env: probeEnv });
      await expect(readFile(output, 'utf8')).rejects.toMatchObject({ code: 'ENOENT' });
      await execFileAsync(process.execPath, [currentMain], { cwd: root, env: probeEnv });
      expect(await readFile(output, 'utf8')).toBe('entered:A\ndynamic:A\n');
      expect(await readFile(join(current, 'version.mjs'), 'utf8')).toContain("'B'");
    } finally { await rm(root, { recursive: true, force: true }); }
  }, 35000);
});
