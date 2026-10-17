import type {
  AgentSession, ExtensionFactory, LoadExtensionsResult, ModelRuntime,
  ResourceLoader, SessionManager, SettingsManager
} from '@earendil-works/pi-coding-agent';
import type { McpAdapterOptions, ServerEntry } from 'pi-mcp-adapter/types';
import { setTimeout as delay } from 'node:timers/promises';
import { createJiti } from 'jiti';
import { ControllerError } from '../domain.js';
import type { ControllerConfig } from '../config.js';
import type { PromptPolicy } from './prompt-policy.js';
export interface SdkManager {
  getSessionId(): string; getSessionFile(): string | undefined;
  getHeader(): unknown; getEntries(): readonly unknown[];
  appendCustomEntry(type: string, data: unknown): string;
}
type SdkSessionDelegate = Pick<AgentSession,
  'prompt' | 'subscribe' | 'waitForIdle' | 'isIdle' | 'getContextUsage' | 'compact' |
  'bindExtensions' | 'setAutoCompactionEnabled' | 'getAllTools' | 'getActiveToolNames' |
  'getCallableToolNames' | 'getToolDefinition' | 'extensionRunner'>;
export interface SdkSession extends SdkSessionDelegate {
  dispose(): void | Promise<void>;
}
export interface SdkPort {
  create(cwd: string, directory: string): SdkManager;
  open(file: string, directory: string): SdkManager;
  connect(manager: SdkManager, current: () => Promise<void>): Promise<SdkSession>;
  ready(session: SdkSession, current: () => Promise<void>): Promise<void>;
}
export type NativeSdkLoadMode =
  | { readonly kind: 'full' }
  | {
      readonly kind: 'inventory-preflight';
      readonly root: string;
      readonly cwd: string;
      readonly agentDir: string;
    };
type PiSdkModule = typeof import('@earendil-works/pi-coding-agent');
export type PiSdkImporter = () => Promise<PiSdkModule>;
type AdapterMcpConfig = NonNullable<McpAdapterOptions['config']>;
type ControllerMcpServer = ServerEntry & {
  command: string; args: string[]; cwd: string; env: Record<string, string>;
  literalEnv: true; inheritEnv: false; lifecycle: 'keep-alive'; directTools: true;
  toolPrefix: 'mcp'; approveTools: false; exposeResources: false; openUi: false;
};
export interface ControllerMcpConfig extends AdapterMcpConfig {
  mcpServers: Record<string, ControllerMcpServer>;
  settings: NonNullable<AdapterMcpConfig['settings']> & {
    toolPrefix: 'mcp'; scriptMode: false; namespaceProxyTools: false;
    disableProxyTool: true; directTools: false; hostConfigDiscovery: 'off';
    autoAuth: false; sampling: false; elicitation: false;
    notifyOnStartupConnect: false; mcpFooterStatus: 'off';
    freezeDirectTools: true; strictDirectToolArguments: true;
    directToolResultDetails: 'bounded'; outputGuard: true;
    exposeResources: false; approveTools: false; allowInstall: false;
  };
}
interface McpAdapterModule {
  createMcpAdapter(options?: McpAdapterOptions): ExtensionFactory;
}
export type McpAdapterImporter = () => Promise<McpAdapterModule>;
export const WHATSAPP_SERVER = 'whatsapp_session_controller';
/** Adapter transport key. Its trailing underscore is intentional: the adapter's
 * `mcp` formatter inserts one separator and therefore preserves the established
 * `mcp__whatsapp_session_controller__<tool>` public names. */
export const ADAPTER_WHATSAPP_SERVER = `${WHATSAPP_SERVER}_`;
// Reuse Pi's replaceable built-in slot so the controller has exactly one MCP
// implementation while the factory itself comes from pi-mcp-adapter.
export const MCP_ADAPTER_EXTENSION = 'mcp';
export const MCP_ADAPTER_SOURCE_PATH = `<inline:${MCP_ADAPTER_EXTENSION}>`;
export const MCP_STATUS_OBSERVER_EXTENSION = 'controller-mcp-status-observer';
export const MCP_STATUS_OBSERVER_SOURCE_PATH = `<inline:${MCP_STATUS_OBSERVER_EXTENSION}>`;
const MCP_STATUS_EVENT = 'pi-mcp-adapter/status/v1';
const MCP_STATUS_SNAPSHOT_VERSION = 1;
export const REQUIRED_WHATSAPP_TOOLS = [
  'get_message', 'send_message', 'check_new_messages', 'get_messages',
  'get_unread_chats', 'get_unread_messages', 'get_group_context', 'send_reaction',
  'edit_message', 'revoke_message', 'check_triggers', 'list_groups', 'get_group',
  'send_group_message', 'send_auto_message', 'send_media', 'download_media',
  'analyze_media', 'list_contacts', 'get_contact', 'semantic_search'
] as const;

export const whatsappToolName = (tool: string): string => `mcp__${WHATSAPP_SERVER}__${tool}`;
const isAdapterSource = (tool: ReturnType<SdkSession['getAllTools']>[number]): boolean =>
  tool.sourceInfo?.path === MCP_ADAPTER_SOURCE_PATH && tool.sourceInfo?.source === 'inline';

/** Complete controller-owned adapter snapshot. It cannot discover, merge, or
 * mutate project/global adapter configuration. */
export function createControllerMcpConfig(config: ControllerConfig): ControllerMcpConfig {
  return {
    mcpServers: {
      [ADAPTER_WHATSAPP_SERVER]: {
        command: config.mcp.command,
        args: [...config.mcp.args],
        cwd: config.mcp.cwd,
        env: {
          MCP_TRANSPORT: 'stdio',
          BRIDGE_URL: config.bridge.baseUrl,
          WORKSPACE_DIR: config.pi.cwd,
          LOG_LEVEL: 'INFO'
        },
        literalEnv: true,
        inheritEnv: false,
        lifecycle: 'keep-alive',
        directTools: true,
        toolPrefix: 'mcp',
        approveTools: false,
        exposeResources: false,
        openUi: false
      }
    },
    settings: {
      toolPrefix: 'mcp',
      scriptMode: false,
      namespaceProxyTools: false,
      disableProxyTool: true,
      directTools: false,
      hostConfigDiscovery: 'off',
      autoAuth: false,
      sampling: false,
      elicitation: false,
      notifyOnStartupConnect: false,
      mcpFooterStatus: 'off',
      freezeDirectTools: true,
      strictDirectToolArguments: true,
      directToolResultDetails: 'bounded',
      outputGuard: true,
      exposeResources: false,
      approveTools: false,
      allowInstall: false
    }
  };
}

function packageSource(value: ReturnType<SettingsManager['getPackages']>[number]): string {
  return (typeof value === 'string' ? value : value.source).trim();
}
/** Match only the npm declaration whose package identity is pi-mcp-adapter.
 * Local paths, git URLs, aliases, and similarly named packages are untouched. */
export function isConfiguredMcpAdapterPackage(value: ReturnType<SettingsManager['getPackages']>[number]): boolean {
  const source = packageSource(value);
  return /^npm:pi-mcp-adapter(?:@[^/]+)?$/.test(source);
}
export function withoutConfiguredMcpAdapter(
  packages: ReturnType<SettingsManager['getPackages']>
): ReturnType<SettingsManager['getPackages']> {
  return packages.filter(value => !isConfiguredMcpAdapterPackage(value));
}

const ADAPTER_RUNTIME_STATUSES = [
  'connected', 'cached', 'failed', 'needs-auth', 'not-connected', 'blocked', 'disabled'
] as const;
const ADAPTER_LISTEN_STATES = [
  'active', 'dropped', 're-establishing', 'legacy', 'not-listening', 'disconnected'
] as const;
type AdapterRuntimeStatus = (typeof ADAPTER_RUNTIME_STATUSES)[number];
type AdapterListenState = (typeof ADAPTER_LISTEN_STATES)[number];
interface AdapterServerStatus {
  readonly name: string;
  readonly status: AdapterRuntimeStatus;
  readonly listenState: AdapterListenState;
  readonly toolCount: number;
  readonly directToolCount: number;
  readonly disabled: boolean;
  readonly catalogStale?: boolean;
}
interface AdapterStatusSnapshot {
  readonly version: 1;
  readonly servers: readonly AdapterServerStatus[];
}
interface AdapterStatusTracker { latest?: AdapterStatusSnapshot }

function isRecord(value: unknown): value is Record<string, unknown> {
  return value !== null && typeof value === 'object' && !Array.isArray(value);
}
function isAdapterRuntimeStatus(value: unknown): value is AdapterRuntimeStatus {
  return typeof value === 'string' && (ADAPTER_RUNTIME_STATUSES as readonly string[]).includes(value);
}
function isAdapterListenState(value: unknown): value is AdapterListenState {
  return typeof value === 'string' && (ADAPTER_LISTEN_STATES as readonly string[]).includes(value);
}
function parseAdapterStatusSnapshot(value: unknown): AdapterStatusSnapshot | undefined {
  if (!isRecord(value) || value['version'] !== MCP_STATUS_SNAPSHOT_VERSION || !Array.isArray(value['servers'])) return undefined;
  const servers: AdapterServerStatus[] = [];
  for (const candidate of value['servers']) {
    if (!isRecord(candidate) || typeof candidate['name'] !== 'string' ||
        !isAdapterRuntimeStatus(candidate['status']) || !isAdapterListenState(candidate['listenState']) ||
        !Number.isSafeInteger(candidate['toolCount']) || (candidate['toolCount'] as number) < 0 ||
        !Number.isSafeInteger(candidate['directToolCount']) || (candidate['directToolCount'] as number) < 0 ||
        typeof candidate['disabled'] !== 'boolean' ||
        candidate['catalogStale'] !== undefined && typeof candidate['catalogStale'] !== 'boolean') return undefined;
    servers.push({
      name: candidate['name'], status: candidate['status'], listenState: candidate['listenState'],
      toolCount: candidate['toolCount'] as number, directToolCount: candidate['directToolCount'] as number,
      disabled: candidate['disabled'],
      ...(candidate['catalogStale'] !== undefined ? { catalogStale: candidate['catalogStale'] as boolean } : {})
    });
  }
  return { version: MCP_STATUS_SNAPSHOT_VERSION, servers };
}

/** Installed before the adapter for each SDK connection so even a load-time
 * keep-alive publication is captured. It observes only the documented public
 * event channel and deliberately registers no tools or commands. */
function statusObserverExtensionFactory(tracker: AdapterStatusTracker): ExtensionFactory {
  return pi => {
    pi.events.on(MCP_STATUS_EVENT, value => {
      const snapshot = parseAdapterStatusSnapshot(value);
      if (snapshot) tracker.latest = snapshot;
    });
  };
}

// Pi 1.1.0 has no public InMemoryResourceLoader. This deliberately implements
// only its public ResourceLoader contract around an already-loaded owned set.
class OwnedInventoryResourceLoader implements ResourceLoader {
  constructor(private readonly result: LoadExtensionsResult) {}
  getExtensions(): LoadExtensionsResult { return this.result; }
  getSkills() { return { skills: [], diagnostics: [] }; }
  getPrompts() { return { prompts: [], diagnostics: [] }; }
  getThemes() { return { themes: [], diagnostics: [] }; }
  getAgentsFiles() { return { agentsFiles: [] }; }
  getSystemPrompt(): string | undefined { return undefined; }
  getSystemPromptSource(): { path: string } | undefined { return undefined; }
  getAppendSystemPrompt(): string[] { return []; }
  getAppendSystemPromptSources(): Array<{ path: string }> { return []; }
  extendResources(paths: Parameters<ResourceLoader['extendResources']>[0]): void {
    if ((paths.skillPaths?.length ?? 0) || (paths.promptPaths?.length ?? 0) ||
      (paths.themePaths?.length ?? 0)) throw new ControllerError('session_failed');
  }
  async reload(): Promise<void> {}
}

async function createOwnedInventoryLoader(
  sdk: PiSdkModule,
  settingsManager: SettingsManager,
  observer: ExtensionFactory,
  adapter: ExtensionFactory,
  roots: Extract<NativeSdkLoadMode, { kind: 'inventory-preflight' }>
): Promise<ResourceLoader> {
  const materializer = new sdk.DefaultResourceLoader({
    cwd: roots.cwd,
    agentDir: roots.agentDir,
    settingsManager,
    noExtensions: true,
    noSkills: true,
    noPromptTemplates: true,
    noThemes: true,
    noContextFiles: true,
    systemPrompt: '',
    appendSystemPrompt: [],
    extensionFactories: [
      { name: MCP_STATUS_OBSERVER_EXTENSION, factory: observer },
      { name: MCP_ADAPTER_EXTENSION, factory: adapter }
    ]
  });
  await materializer.reload();
  const result = materializer.getExtensions();
  const paths = result.extensions.map(extension => extension.path);
  if (result.errors.length !== 0 || paths.length !== 2 ||
      paths[0] !== MCP_STATUS_OBSERVER_SOURCE_PATH || paths[1] !== MCP_ADAPTER_SOURCE_PATH) {
    throw new ControllerError('session_failed');
  }
  return new OwnedInventoryResourceLoader(result);
}

/** Called only by later executable composition. Importing this file does not
 * load Pi, discover configuration, access credentials or start an MCP child.
 * Runtime/model storage is explicitly supplied by the composition. */
export async function loadNativeSdk(
  config: ControllerConfig,
  policy: PromptPolicy,
  modelRuntime: ModelRuntime,
  extra: readonly ExtensionFactory[] = [],
  mode: NativeSdkLoadMode = { kind: 'full' },
  importSdk: PiSdkImporter = () => import('@earendil-works/pi-coding-agent'),
  importAdapter: McpAdapterImporter = async () => {
    // The package intentionally exports TypeScript source from node_modules.
    // Use the same production loader family as Pi rather than relying on Node's
    // built-in type stripping, which refuses node_modules source entries.
    const jiti = createJiti(import.meta.url, { moduleCache: true });
    return await jiti.import('pi-mcp-adapter') as McpAdapterModule;
  }
): Promise<SdkPort> {
  const outputGuardOverride = process.env['MCP_OUTPUT_GUARD']?.trim().toLowerCase();
  if (outputGuardOverride && ['0', 'false', 'no', 'off'].includes(outputGuardOverride)) {
    throw new ControllerError('config_invalid');
  }
  // The adapter otherwise lets this ambient selector replace programmatic
  // direct-tool and namespace/proxy policy, including through "__none__".
  if (Object.prototype.hasOwnProperty.call(process.env, 'MCP_DIRECT_TOOLS')) {
    throw new ControllerError('config_invalid');
  }
  const [sdk, adapterModule] = await Promise.all([importSdk(), importAdapter()]);
  const adapter = adapterModule.createMcpAdapter({ config: createControllerMcpConfig(config) });
  const statusBySession = new WeakMap<SdkSession, AdapterStatusTracker>();
  return {
    create: (cwd, dir) => sdk.SessionManager.create(cwd, dir),
    open: (file, dir) => sdk.SessionManager.open(file, dir),
    async ready(session, current) {
      const prefix = `mcp__${WHATSAPP_SERVER}__`;
      const required = new Set(REQUIRED_WHATSAPP_TOOLS.map(whatsappToolName));
      const forbiddenAdapterSurfaces = new Set(['mcp', 'mcpScript', `mcp__${ADAPTER_WHATSAPP_SERVER}`]);
      for (let attempt = 0; attempt < 100; attempt++) {
        await current();
        const tools = session.getAllTools();
        const activeNames = session.getActiveToolNames();
        const active = new Set(activeNames);
        const callable = new Set(session.getCallableToolNames());
        const qualified = tools.filter(tool => tool.name.startsWith(prefix));
        const names = qualified.map(tool => tool.name);
        const activeQualified = activeNames.filter(name => name.startsWith(prefix));
        const exactSet = qualified.length === required.size &&
          new Set(names).size === required.size && names.every(name => required.has(name));
        const exactActiveSet = activeQualified.length === required.size &&
          new Set(activeQualified).size === required.size && activeQualified.every(name => required.has(name));
        const exactDirectExposure = qualified.every(tool => tool.exposure === 'direct');
        const exactProvenance = qualified.every(isAdapterSource);
        const noWrongShadow = tools.every(tool => !required.has(tool.name) || isAdapterSource(tool));
        const noForbiddenCallableSurface = [...forbiddenAdapterSurfaces].every(name =>
          !active.has(name) && !callable.has(name));
        const status = statusBySession.get(session)?.latest;
        const matchingStatuses = status?.servers.filter(server => server.name === ADAPTER_WHATSAPP_SERVER) ?? [];
        const serverStatus = matchingStatuses[0];
        const liveStatus = matchingStatuses.length === 1 && serverStatus !== undefined &&
          serverStatus.status === 'connected' && serverStatus.listenState !== 'disconnected' &&
          serverStatus.toolCount === required.size && serverStatus.directToolCount === required.size &&
          serverStatus.catalogStale !== true && serverStatus.disabled === false;
        if (liveStatus && exactSet && exactActiveSet && exactDirectExposure && exactProvenance &&
          noWrongShadow && noForbiddenCallableSurface) return;
        await delay(100);
      }
      // No partial, hidden/inactive, duplicate, extra, shadowed, or generic
      // adapter surface can reach a prompt or nested tool call.
      throw new ControllerError('session_failed');
    },
    async connect(manager, current) {
      await current();
      const statusTracker: AdapterStatusTracker = {};
      const statusObserver = statusObserverExtensionFactory(statusTracker);
      let settingsManager: SettingsManager;
      let loader: ResourceLoader;
      if (mode.kind === 'full') {
        settingsManager = sdk.SettingsManager.create(config.pi.cwd, config.pi.agentDir);
        // Session-only override: preserve every configured package except the
        // exact adapter npm declaration, then inject one controller-owned copy.
        const packages = withoutConfiguredMcpAdapter(settingsManager.getPackages());
        settingsManager.applyOverrides({ compaction: { enabled: false }, packages });
        const leaseFence: ExtensionFactory = pi => {
          // Fencing only: no tool-name, destination, origin or argument policy gate.
          pi.on('tool_call', async () => {
            try { await current(); } catch { return { block: true, terminate: true, reason: 'lease_lost' }; }
            return undefined;
          });
          // Includes automatic extension follow-ups/retries. The SDK catches hook
          // exceptions; explicitly abort the operation rather than relying on throw.
          pi.on('before_provider_request', async (_event, ctx) => {
            try { await current(); } catch { ctx.abort(); }
          });
        };
        loader = new sdk.DefaultResourceLoader({ cwd: config.pi.cwd, agentDir: config.pi.agentDir, settingsManager,
          appendSystemPromptOverride: base => [...base, policy.system],
          extensionFactories: [
            { name: 'codemode', factory: sdk.createCodemodeExtension(), replaceable: true },
            { name: 'tool_search', factory: sdk.createToolSearchExtension(), replaceable: true },
            { name: MCP_STATUS_OBSERVER_EXTENSION, factory: statusObserver },
            { name: MCP_ADAPTER_EXTENSION, factory: adapter },
            ...extra,
            { name: 'controller-lease-fence', factory: leaseFence }
          ] });
      } else {
        if (extra.length !== 0) throw new ControllerError('session_failed');
        settingsManager = sdk.SettingsManager.inMemory(
          { compaction: { enabled: false }, packages: [] },
          { projectTrusted: false }
        );
        loader = await createOwnedInventoryLoader(sdk, settingsManager, statusObserver, adapter, mode);
      }
      const model = config.pi.provider && config.pi.model ? modelRuntime.getModel(config.pi.provider, config.pi.model) : undefined;
      if (config.pi.provider && !model) throw new ControllerError('config_invalid');
      await loader.reload();
      const resourceCwd = mode.kind === 'inventory-preflight' ? mode.cwd : config.pi.cwd;
      const resourceAgentDir = mode.kind === 'inventory-preflight' ? mode.agentDir : config.pi.agentDir;
      const result = await sdk.createAgentSession({ cwd: resourceCwd, agentDir: resourceAgentDir,
        modelRuntime, ...(model ? { model } : {}), settingsManager, resourceLoader: loader,
        ...(mode.kind === 'inventory-preflight' ? { noTools: 'builtin' as const } : {}),
        sessionManager: manager as SessionManager });
      // AgentSession.dispose() invalidates extensions but does not emit their
      // asynchronous shutdown event. A controller-owned facade preserves the
      // native object and closes the adapter's keep-alive child exactly once.
      const session = result.session;
      const nativeDispose = session.dispose.bind(session);
      let disposing: Promise<void> | undefined;
      const owned: SdkSession = {
        get isIdle() { return session.isIdle; },
        get extensionRunner() { return session.extensionRunner; },
        prompt: session.prompt.bind(session),
        subscribe: session.subscribe.bind(session),
        waitForIdle: session.waitForIdle.bind(session),
        getContextUsage: session.getContextUsage.bind(session),
        compact: session.compact.bind(session),
        bindExtensions: session.bindExtensions.bind(session),
        setAutoCompactionEnabled: session.setAutoCompactionEnabled.bind(session),
        getAllTools: session.getAllTools.bind(session),
        getActiveToolNames: session.getActiveToolNames.bind(session),
        getCallableToolNames: session.getCallableToolNames.bind(session),
        getToolDefinition: session.getToolDefinition.bind(session),
        dispose() {
          disposing ??= (async () => {
            try { await session.extensionRunner.emit({ type: 'session_shutdown', reason: 'quit' }); }
            finally { await nativeDispose(); }
          })();
          return disposing;
        }
      };
      statusBySession.set(owned, statusTracker);
      try {
        // Materialize a disposable owner before rejecting extension diagnostics
        // or a fence lost while the keep-alive adapter was connecting.
        if (loader.getExtensions().errors.length) throw new ControllerError('session_failed');
        await current();
        return owned;
      } catch (error) {
        try { await owned.dispose(); } catch { /* Preserve the primary fixed failure. */ }
        throw error;
      }
    }
  };
}
