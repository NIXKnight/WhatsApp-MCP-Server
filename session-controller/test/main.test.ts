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
import { loadNativeSdk, REQUIRED_WHATSAPP_TOOLS, WHATSAPP_SERVER } from '../src/pi/mcp-loader.js';
import type { SessionHost } from '../src/pi/session-host.js';
import type {
  LifecycleComposition,
  LifecycleContext,
  LifecycleSignal,
  SignalPort
} from '../src/main.js';

const inventoryFsProbe = { active: false, paths: [] as unknown[] };

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
    const session = { binding: { canonicalChatId: '100@g.us', sessionId: 'S', sessionFile: '/synthetic/S.jsonl' } } as PiSessionPort;
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
    const namespace = `mcp__${WHATSAPP_SERVER}`;
    const validTools = expectedInventory.map(name => ({
      name, description: '', parameters: { type: 'object' }, exposure: 'direct',
      namespace: { name: namespace },
      sourceInfo: { path: '<inline:mcp>', source: 'inline', scope: 'temporary', origin: 'top-level' }
    }));
    const native = await loadNativeSdk({} as never, {} as never, {} as never);
    const readySession = (tools: readonly unknown[], getAllTools: () => readonly unknown[] = () => tools) =>
      ({ getAllTools }) as never;
    const invalidInventories = [
      { kind: 'missing', tools: validTools.slice(1) },
      { kind: 'duplicate', tools: [...validTools, { ...validTools[0]! }] },
      {
        kind: 'extra',
        tools: [...validTools, {
          ...validTools[0]!,
          name: `mcp__${WHATSAPP_SERVER}__synthetic_extra`
        }]
      },
      { kind: 'wrong-namespace', tools: validTools.map((tool, index) => index === 0 ? { ...tool, namespace: { name: 'mcp__shadow' } } : tool) },
      { kind: 'wrong-path', tools: validTools.map((tool, index) => index === 0 ? { ...tool, sourceInfo: { ...tool.sourceInfo, path: '<inline:shadow>' } } : tool) },
      { kind: 'wrong-source', tools: validTools.map((tool, index) => index === 0 ? { ...tool, sourceInfo: { ...tool.sourceInfo, source: 'extension' } } : tool) }
    ] as const;
    vi.useFakeTimers();
    try {
      for (const { kind, tools } of invalidInventories) {
        const getAllTools = vi.fn(() => tools);
        const current = vi.fn(async () => {});
        const readiness = native.ready(readySession(tools, getAllTools), current);
        const rejected = expect(readiness, kind).rejects.toMatchObject({ code: 'session_failed' });
        await Promise.resolve(); // schedule the first production delay
        for (let delay = 0; delay < 100; delay++) {
          expect(vi.getTimerCount(), `${kind}:delay:${delay}`).toBe(1);
          await vi.advanceTimersByTimeAsync(100);
        }
        await rejected;
        expect(current, kind).toHaveBeenCalledTimes(100);
        expect(getAllTools, kind).toHaveBeenCalledTimes(100);
        expect(vi.getTimerCount(), kind).toBe(0);
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
    const ownedRoots = {
      root: ownedRoot,
      cwd: join(ownedRoot, 'cwd'),
      agentDir: join(ownedRoot, 'agent'),
      home: join(ownedRoot, 'home'),
      sessionDir: join(ownedRoot, 'sessions')
    };
    for (const directory of [
      ownedRoots.cwd, ownedRoots.agentDir, join(ownedRoots.home, '.agents'), ownedRoots.sessionDir
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
      join(hostileHome, '.agents', 'skills', 'hostile', 'SKILL.md')
    ];
    for (const path of hostileFiles) {
      await mkdir(dirname(path), { recursive: true, mode: 0o700 });
      await writeFile(path, 'HOSTILE_RESOURCE_MUST_NOT_BE_READ\n', { mode: 0o600 });
    }
    const preflightConfig = {
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
    const inventoryPi = {
      ...installedPi,
      SessionManager: {
        create(cwd: string, directory: string) {
          installedEffects.push(`manager:${cwd}:${directory}`);
          return {} as never;
        },
        open() { throw new Error('INVENTORY_OPEN_MUST_NOT_RUN'); }
      },
      createMcpExtension(options?: {
        loadConfig?: () => { servers: unknown[]; errors: string[]; autoEnableCodemode?: boolean };
        logPath?: string;
      }) {
        expect(options?.logPath).toBe(join(ownedRoot, 'mcp.log'));
        expect(options?.loadConfig?.()).toEqual({ servers: [], errors: [], autoEnableCodemode: false });
        return (() => { installedEffects.push('owned-mcp-factory'); }) as never;
      },
      async createAgentSession(options: Parameters<typeof installedPi.createAgentSession>[0]) {
        expect(options?.cwd).toBe(ownedRoots.cwd);
        expect(options?.agentDir).toBe(ownedRoots.agentDir);
        expect(options?.modelRuntime).toBe(suppliedRuntime);
        expect(options?.model).toBe(selectedModel);
        expect(options?.settingsManager?.isProjectTrusted()).toBe(false);
        expect(options?.noTools).toBe('builtin');
        const loader = options?.resourceLoader;
        expect(loader).toBeDefined();
        expect(loader!.getExtensions().extensions.map(extension => extension.path).sort()).toEqual([
          '<inline:mcp>', `<inline:${WHATSAPP_SERVER}>`
        ].sort());
        expect(loader!.getExtensions().errors).toEqual([]);
        expect(loader!.getSkills()).toEqual({ skills: [], diagnostics: [] });
        expect(loader!.getPrompts()).toEqual({ prompts: [], diagnostics: [] });
        expect(loader!.getThemes()).toEqual({ themes: [], diagnostics: [] });
        expect(loader!.getAgentsFiles()).toEqual({ agentsFiles: [] });
        expect(loader!.getSystemPrompt()).toBeUndefined();
        expect(loader!.getAppendSystemPrompt()).toEqual([]);
        expect(loader!.getExtensions().runtime.mcpServers.list()).toEqual([{
          name: WHATSAPP_SERVER,
          config: {
            type: 'stdio', command: '/synthetic/owned-mcp', args: ['--owned'],
            cwd: '/synthetic/mcp-cwd', exposure: 'direct'
          },
          extensionPath: `<inline:${WHATSAPP_SERVER}>`
        }]);
        return { session: {
          getAllTools: () => validTools,
          setAutoCompactionEnabled(enabled: boolean) { installedEffects.push(`auto:${String(enabled)}`); },
          async bindExtensions() { installedEffects.push('bind'); },
          async dispose() { installedEffects.push('dispose'); }
        } as never };
      }
    } as never;
    const installedStatuses: string[] = [];
    const hadInitialHome = Object.prototype.hasOwnProperty.call(process.env, 'HOME');
    const initialHome = process.env.HOME;
    process.env.HOME = hostileHome;
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
        ) => loadNativeSdk(config, policy, runtime, extra, mode, async () => inventoryPi)) as never,
        status: value => { installedStatuses.push(value); }
      });
      expect(installedCode).toBe(0);
      expect(installedStatuses).toEqual(['pi_inventory_ready']);
      expect(process.env.HOME).toBe(hostileHome);
      expect(getSelectedModel).toHaveBeenCalledTimes(1);
      expect(installedEffects).toEqual([
        `manager:${ownedRoots.cwd}:${ownedRoots.sessionDir}`, 'owned-mcp-factory',
        'auto:false', 'bind', 'dispose'
      ]);
      expect(stderr).not.toHaveBeenCalled();
      const probedPaths = inventoryFsProbe.paths.flatMap(value => {
        if (typeof value === 'string' && isAbsolute(value)) return [resolve(value)];
        if (Buffer.isBuffer(value)) return [resolve(value.toString())];
        if (value instanceof URL && value.protocol === 'file:') return [resolve(fileURLToPath(value))];
        return [];
      });
      expect(probedPaths.length).toBeGreaterThan(0);
      expect(probedPaths.every(path => path === ownedRoot || path.startsWith(`${ownedRoot}${sep}`))).toBe(true);
      expect(probedPaths.some(path => [hostileCwd, hostileAgentDir, hostileHome]
        .some(hostile => path === hostile || path.startsWith(`${hostile}${sep}`)))).toBe(false);
    } finally {
      inventoryFsProbe.active = false;
      Object.assign(mutableFs, originalFs);
      syncBuiltinESMExports();
      stderr.mockRestore();
      if (hadInitialHome) process.env.HOME = initialHome!;
      else delete process.env.HOME;
      await rm(hostileRoot, { recursive: true, force: true });
      await rm(ownedRoot, { recursive: true, force: true });
    }

    const preflightEffects: string[] = [];
    const preflightStatuses: string[] = [];
    let preflightRoots: {
      root: string; cwd: string; agentDir: string; home: string; sessionDir: string;
    } | undefined;
    const homeBeforeSuccess = process.env.HOME;
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
      async createRuntime() { preflightEffects.push('runtime'); return {} as never; },
      async createTemporary() {
        preflightEffects.push('temporary');
        const root = await mkdtemp(join(tmpdir(), 'phase9-inventory-'));
        preflightRoots = {
          root, cwd: join(root, 'cwd'), agentDir: join(root, 'agent'),
          home: join(root, 'home'), sessionDir: join(root, 'sessions')
        };
        await Promise.all([
          mkdir(preflightRoots.cwd, { recursive: true }),
          mkdir(preflightRoots.agentDir, { recursive: true }),
          mkdir(join(preflightRoots.home, '.agents'), { recursive: true }),
          mkdir(preflightRoots.sessionDir, { recursive: true })
        ]);
        return preflightRoots;
      },
      async removeTemporary(roots) {
        preflightEffects.push('remove');
        await rm(roots.root, { recursive: true, force: true });
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
        expect(mode).toEqual({
          kind: 'inventory-preflight', root: preflightRoots?.root,
          cwd: preflightRoots?.cwd, agentDir: preflightRoots?.agentDir
        });
        return preflightSdk;
      }) as never,
      status(value) { preflightEffects.push(`status:${value}`); preflightStatuses.push(value); }
    });
    expect(preflightCode).toBe(0);
    expect(preflightRoots).toBeDefined();
    await expect(stat(preflightRoots!.root)).rejects.toMatchObject({ code: 'ENOENT' });
    expect(process.env.HOME).toBe(homeBeforeSuccess);
    expect(preflightStatuses).toEqual(['pi_inventory_ready']);
    expect(preflightEffects).toEqual([
      'load', 'policy',
      `validate:${join(hostileRoot, 'operator-system.md')}`,
      `validate:${join(hostileRoot, 'operator-pre.md')}`,
      `validate:${join(hostileRoot, 'operator-summary.md')}`,
      'runtime', 'temporary', 'sdk',
      `create:${preflightRoots!.cwd}:${preflightRoots!.sessionDir}`,
      'connect', 'auto:false', 'bind', 'ready', 'dispose', 'remove', 'status:pi_inventory_ready'
    ]);
    expect(Object.values(prohibited).every(spy => spy.mock.calls.length === 0)).toBe(true);

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
      const hadRowHome = Object.prototype.hasOwnProperty.call(process.env, 'HOME');
      const previousRowHome = process.env.HOME;
      const expectedHomePresent = failure !== 'connect';
      if (expectedHomePresent) process.env.HOME = `/synthetic/hostile-home/${failure}`;
      else delete process.env.HOME;
      const code = await checkPiInventory({
        load: () => preflightConfig,
        loadPolicy: async () => ({} as never),
        validate: async () => {},
        createRuntime: async () => ({} as never),
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
      expect(Object.prototype.hasOwnProperty.call(process.env, 'HOME'), failure).toBe(expectedHomePresent);
      if (expectedHomePresent) expect(process.env.HOME, failure).toBe(`/synthetic/hostile-home/${failure}`);
      if (hadRowHome) process.env.HOME = previousRowHome!;
      else delete process.env.HOME;
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
    const currentGate = vi.fn(async () => {});
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
      await native.ready(readySession(validTools), currentGate);
      expect(currentGate).toHaveBeenCalledTimes(1);
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

  it('imports main in an isolated child without config, Redis, listener, Pi, auth, or MCP effects', async () => {
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
