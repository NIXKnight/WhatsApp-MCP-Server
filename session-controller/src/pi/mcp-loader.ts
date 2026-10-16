import type {
  AgentSession, ExtensionFactory, LoadExtensionsResult, ModelRuntime,
  ResourceLoader, SessionManager, SettingsManager
} from '@earendil-works/pi-coding-agent';
import { readFile } from 'node:fs/promises';
import { setTimeout as delay } from 'node:timers/promises';
import { join } from 'node:path';
import { ControllerError } from '../domain.js';
import type { ControllerConfig } from '../config.js';
import type { PromptPolicy } from './prompt-policy.js';
export interface SdkManager {
  getSessionId(): string; getSessionFile(): string | undefined;
  getHeader(): unknown; getEntries(): readonly unknown[];
  appendCustomEntry(type: string, data: unknown): string;
}
export type SdkSession = Pick<AgentSession, 'prompt' | 'subscribe' | 'waitForIdle' | 'isIdle' | 'dispose' | 'getContextUsage' | 'compact' | 'bindExtensions' | 'setAutoCompactionEnabled' | 'getAllTools'>;
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
export const WHATSAPP_SERVER = 'whatsapp_session_controller';
export const REQUIRED_WHATSAPP_TOOLS = [
  'get_message', 'send_message', 'check_new_messages', 'get_messages',
  'get_unread_chats', 'get_unread_messages', 'get_group_context', 'send_reaction',
  'edit_message', 'revoke_message', 'check_triggers', 'list_groups', 'get_group',
  'send_group_message', 'send_auto_message', 'send_media', 'download_media',
  'analyze_media', 'list_contacts', 'get_contact', 'semantic_search'
] as const;

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
  registration: ExtensionFactory,
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
      {
        name: 'mcp',
        factory: sdk.createMcpExtension({
          loadConfig: () => ({ servers: [], errors: [], autoEnableCodemode: false }),
          logPath: join(roots.root, 'mcp.log')
        }),
        replaceable: true
      },
      { name: WHATSAPP_SERVER, factory: registration }
    ]
  });
  await materializer.reload();
  const result = materializer.getExtensions();
  const paths = result.extensions.map(extension => extension.path);
  const owned = new Set([`<inline:mcp>`, `<inline:${WHATSAPP_SERVER}>`]);
  if (result.errors.length !== 0 || paths.length !== owned.size ||
    new Set(paths).size !== owned.size || paths.some(path => !owned.has(path))) {
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
  importSdk: PiSdkImporter = () => import('@earendil-works/pi-coding-agent')
): Promise<SdkPort> {
  const sdk = await importSdk();
  return {
    create: (cwd, dir) => sdk.SessionManager.create(cwd, dir),
    open: (file, dir) => sdk.SessionManager.open(file, dir),
    async ready(session, current) {
      const namespace = `mcp__${WHATSAPP_SERVER}`;
      const required = new Set(REQUIRED_WHATSAPP_TOOLS.map(name => `${namespace}__${name}`));
      for (let attempt = 0; attempt < 100; attempt++) {
        await current();
        const tools = session.getAllTools();
        const namespaced = tools.filter(tool => tool.namespace?.name === namespace);
        const names = namespaced.map(tool => tool.name);
        const exactSet = namespaced.length === required.size &&
          new Set(names).size === required.size && names.every(name => required.has(name));
        const exactProvenance = namespaced.every(tool =>
          tool.sourceInfo?.path === '<inline:mcp>' && tool.sourceInfo?.source === 'inline');
        const noWrongNamespaceShadow = tools.every(tool =>
          !required.has(tool.name) || tool.namespace?.name === namespace);
        if (exactSet && exactProvenance && noWrongNamespaceShadow) return;
        await delay(100);
      }
      // No partial, duplicate, extra, missing, or shadowed transport can prompt.
      throw new ControllerError('session_failed');
    },
    async connect(manager, current) {
      await current();
      let settingsManager: SettingsManager;
      let loader: ResourceLoader;
      if (mode.kind === 'full') {
        // Preserve normal shadow rejection and discovery without rewriting unrelated servers.
        for (const path of [join(config.pi.agentDir, 'mcp.json'), join(config.pi.cwd, '.pi', 'mcp.json')]) {
          try {
            const raw: unknown = JSON.parse(await readFile(path, 'utf8'));
            const servers = (raw as { mcpServers?: Record<string, unknown> }).mcpServers;
            if (servers && Object.keys(servers).some(name => name.replaceAll('-', '_') === WHATSAPP_SERVER)) throw new ControllerError('config_invalid');
          } catch (error) { if ((error as NodeJS.ErrnoException).code !== 'ENOENT') throw new ControllerError('config_invalid'); }
        }
        settingsManager = sdk.SettingsManager.create(config.pi.cwd, config.pi.agentDir);
        settingsManager.applyOverrides({ compaction: { enabled: false } });
        const registration: ExtensionFactory = pi => {
          pi.registerMcpServer(WHATSAPP_SERVER, { type: 'stdio', command: config.mcp.command,
            args: [...config.mcp.args], cwd: config.mcp.cwd, exposure: 'direct' });
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
            { name: 'mcp', factory: sdk.createMcpExtension(), replaceable: true },
            ...extra, registration
          ] });
      } else {
        if (extra.length !== 0) throw new ControllerError('session_failed');
        settingsManager = sdk.SettingsManager.inMemory(
          { compaction: { enabled: false } },
          { projectTrusted: false }
        );
        const registration: ExtensionFactory = pi => {
          pi.registerMcpServer(WHATSAPP_SERVER, { type: 'stdio', command: config.mcp.command,
            args: [...config.mcp.args], cwd: config.mcp.cwd, exposure: 'direct' });
        };
        loader = await createOwnedInventoryLoader(sdk, settingsManager, registration, mode);
      }
      await current(); await loader.reload();
      if (loader.getExtensions().errors.length) throw new ControllerError('session_failed');
      const model = config.pi.provider && config.pi.model ? modelRuntime.getModel(config.pi.provider, config.pi.model) : undefined;
      if (config.pi.provider && !model) throw new ControllerError('config_invalid');
      await current();
      const resourceCwd = mode.kind === 'inventory-preflight' ? mode.cwd : config.pi.cwd;
      const resourceAgentDir = mode.kind === 'inventory-preflight' ? mode.agentDir : config.pi.agentDir;
      const result = await sdk.createAgentSession({ cwd: resourceCwd, agentDir: resourceAgentDir,
        modelRuntime, ...(model ? { model } : {}), settingsManager, resourceLoader: loader,
        ...(mode.kind === 'inventory-preflight' ? { noTools: 'builtin' as const } : {}),
        sessionManager: manager as SessionManager });
      return result.session;
    }
  };
}
