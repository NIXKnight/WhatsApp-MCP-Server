import type { AgentSession, SessionManager, ExtensionFactory, ModelRuntime } from '@earendil-works/pi-coding-agent';
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
export const WHATSAPP_SERVER = 'whatsapp_session_controller';
/** Called only by later executable composition. Importing this file does not
 * load Pi, discover configuration, access credentials or start an MCP child.
 * Runtime/model storage is explicitly supplied by the composition. */
export async function loadNativeSdk(config: ControllerConfig, policy: PromptPolicy, modelRuntime: ModelRuntime,
  extra: readonly ExtensionFactory[] = []): Promise<SdkPort> {
  const sdk = await import('@earendil-works/pi-coding-agent');
  return {
    create: (cwd, dir) => sdk.SessionManager.create(cwd, dir),
    open: (file, dir) => sdk.SessionManager.open(file, dir),
    async ready(session, current) {
      const required = ['get_message', 'send_message', 'check_new_messages', 'get_messages', 'get_unread_chats', 'get_unread_messages', 'get_group_context', 'send_reaction', 'edit_message', 'revoke_message', 'check_triggers', 'list_groups', 'get_group', 'send_group_message', 'send_auto_message', 'send_media', 'download_media', 'analyze_media', 'list_contacts', 'get_contact', 'semantic_search'];
      for (let attempt = 0; attempt < 100; attempt++) {
        await current();
        const names = new Set(session.getAllTools().map(tool => tool.name));
        if (required.every(name => names.has(`mcp__${WHATSAPP_SERVER}__${name}`))) return;
        await delay(100);
      }
      // No partial/missing/shadowed replacement transport can silently prompt.
      throw new ControllerError('session_failed');
    },
    async connect(manager, current) {
      await current();
      // Refuse namespace shadowing, do not remove/rewrite unrelated servers.
      for (const path of [join(config.pi.agentDir, 'mcp.json'), join(config.pi.cwd, '.pi', 'mcp.json')]) {
        try {
          const raw: unknown = JSON.parse(await readFile(path, 'utf8'));
          const servers = (raw as { mcpServers?: Record<string, unknown> }).mcpServers;
          if (servers && Object.keys(servers).some(name => name.replaceAll('-', '_') === WHATSAPP_SERVER)) throw new ControllerError('config_invalid');
        } catch (error) { if ((error as NodeJS.ErrnoException).code !== 'ENOENT') throw new ControllerError('config_invalid'); }
      }
      const settingsManager = sdk.SettingsManager.create(config.pi.cwd, config.pi.agentDir);
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
      const loader = new sdk.DefaultResourceLoader({ cwd: config.pi.cwd, agentDir: config.pi.agentDir, settingsManager,
        appendSystemPromptOverride: base => [...base, policy.system],
        extensionFactories: [
          { name: 'codemode', factory: sdk.createCodemodeExtension(), replaceable: true },
          { name: 'tool_search', factory: sdk.createToolSearchExtension(), replaceable: true },
          { name: 'mcp', factory: sdk.createMcpExtension(), replaceable: true },
          ...extra, registration
        ] });
      await current(); await loader.reload();
      if (loader.getExtensions().errors.length) throw new ControllerError('session_failed');
      const model = config.pi.provider && config.pi.model ? modelRuntime.getModel(config.pi.provider, config.pi.model) : undefined;
      if (config.pi.provider && !model) throw new ControllerError('config_invalid');
      await current();
      const result = await sdk.createAgentSession({ cwd: config.pi.cwd, agentDir: config.pi.agentDir,
        modelRuntime, ...(model ? { model } : {}), settingsManager, resourceLoader: loader,
        sessionManager: manager as SessionManager });
      return result.session;
    }
  };
}
