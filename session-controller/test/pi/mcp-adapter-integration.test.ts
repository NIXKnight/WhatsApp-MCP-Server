import { mkdir, mkdtemp, readFile, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { describe, expect, it } from 'vitest';
import { ModelRuntime, type ToolResultEvent } from '@earendil-works/pi-coding-agent';
import { computeServerHash, saveMetadataCache } from 'pi-mcp-adapter/metadata-cache';
import { confirmedMessage } from '../../src/pi/tool-journal-extension.js';
import {
  ADAPTER_WHATSAPP_SERVER, MCP_ADAPTER_SOURCE_PATH, REQUIRED_WHATSAPP_TOOLS,
  createControllerMcpConfig, loadNativeSdk, whatsappToolName, type SdkSession
} from '../../src/pi/mcp-loader.js';

const fakeServer = String.raw`import { appendFileSync, readFileSync } from 'node:fs';
import readline from 'node:readline';
const [lifecycle, mode = 'normal'] = process.argv.slice(2);
appendFileSync(lifecycle, 'start:' + process.pid + '\nmode:' + mode + '\n');
const startCount = [...readFileSync(lifecycle, 'utf8').matchAll(/^start:/gm)].length;
const disconnectAfterCatalog = mode === 'first-live-rest-unreachable' && startCount > 1;
process.on('SIGTERM', () => {
  if (mode === 'stubborn') { appendFileSync(lifecycle, 'term\n'); return; }
  appendFileSync(lifecycle, 'stop\n'); process.exit(0);
});
if (mode === 'stubborn') setInterval(() => {}, 1000);
const names = ${JSON.stringify([
  'get_message', 'send_message', 'check_new_messages', 'get_messages',
  'get_unread_chats', 'get_unread_messages', 'get_group_context', 'send_reaction',
  'edit_message', 'revoke_message', 'check_triggers', 'list_groups', 'get_group',
  'send_group_message', 'send_auto_message', 'send_media', 'download_media',
  'analyze_media', 'list_contacts', 'get_contact', 'semantic_search'
])};
const send = value => process.stdout.write(JSON.stringify(value) + '\n');
readline.createInterface({ input: process.stdin }).on('line', line => {
  let message; try { message = JSON.parse(line); } catch { return; }
  if (message.id === undefined) return;
  let result;
  if (message.method === 'initialize') result = {
    protocolVersion: '2025-06-18', capabilities: { tools: {} },
    serverInfo: { name: 'owned-controller-test', version: '1' }
  };
  else if (message.method === 'tools/list') result = { tools: names.map((name, index) => ({
    name,
    description: index === 0 ? JSON.stringify({
      inheritedSecret: process.env.SYNTHETIC_PRIVATE_VALUE ?? 'absent',
      transport: process.env.MCP_TRANSPORT, bridge: process.env.BRIDGE_URL,
      workspace: process.env.WORKSPACE_DIR, level: process.env.LOG_LEVEL
    }) : name,
    inputSchema: { type: 'object', properties: {}, additionalProperties: false }
  })) };
  else if (message.method === 'ping') result = {};
  else if (message.method === 'tools/call') {
    const raw = JSON.stringify({ success: true, message_id: 'OWNED_ADAPTER_RESULT' });
    result = { content: [{ type: 'text', text: raw }], structuredContent: { result: raw } };
  }
  else { send({ jsonrpc: '2.0', id: message.id, error: { code: -32601, message: 'not found' } }); return; }
  send({ jsonrpc: '2.0', id: message.id, result });
  if (disconnectAfterCatalog && message.method === 'tools/list') setImmediate(() => process.exit(70));
});
`;

async function eventuallyRead(path: string, expected: string, attempts = 100): Promise<string> {
  for (let attempt = 0; attempt < attempts; attempt++) {
    try { const value = await readFile(path, 'utf8'); if (value.includes(expected)) return value; } catch { /* not written yet */ }
    await new Promise(resolve => setTimeout(resolve, 10));
  }
  throw new Error(`owned_fake_missing_${expected}`);
}
async function eventuallyStarted(path: string, count: number): Promise<number[]> {
  for (let attempt = 0; attempt < 200; attempt++) {
    try {
      const value = await readFile(path, 'utf8');
      const pids = [...value.matchAll(/^start:(\d+)$/gm)].map(match => Number(match[1]));
      if (pids.length === count) return pids;
    } catch { /* not written yet */ }
    await new Promise(resolve => setTimeout(resolve, 10));
  }
  throw new Error('owned_fake_start_count');
}
async function eventuallyStopped(pid: number): Promise<void> {
  for (let attempt = 0; attempt < 100; attempt++) {
    try { process.kill(pid, 0); } catch (error) {
      if ((error as NodeJS.ErrnoException).code === 'ESRCH') return;
      throw error;
    }
    await new Promise(resolve => setTimeout(resolve, 10));
  }
  throw new Error('owned_fake_still_running');
}

describe('controller-owned pi-mcp-adapter', () => {
  it('loads 21 direct tools with adapter provenance, isolated env, and teardown on disposal or fenced connect', async () => {
    const root = await mkdtemp(join(tmpdir(), 'whatsapp-adapter-owned-'));
    const roots = {
      cwd: join(root, '$SYNTHETIC_PRIVATE_VALUE-workspace'), agentDir: join(root, 'agent'), home: join(root, 'home'),
      sessions: join(root, 'sessions'), server: join(root, 'fake-mcp.mjs'), lifecycle: join(root, 'lifecycle.log')
    };
    await Promise.all([roots.cwd, roots.agentDir, roots.home, roots.sessions].map(path => mkdir(path, { recursive: true })));
    await writeFile(roots.server, fakeServer, { mode: 0o700 });
    const priorAgentDir = process.env['PI_CODING_AGENT_DIR'];
    const priorSecret = process.env['SYNTHETIC_PRIVATE_VALUE'];
    process.env['PI_CODING_AGENT_DIR'] = roots.agentDir;
    process.env['SYNTHETIC_PRIVATE_VALUE'] = 'MUST_NOT_REACH_CHILD';
    let session: SdkSession | undefined;
    let peerSession: SdkSession | undefined;
    let stubbornSession: SdkSession | undefined;
    try {
      const runtime = await ModelRuntime.create({
        authPath: join(roots.agentDir, 'auth.json'), modelsPath: join(roots.agentDir, 'models.json'),
        allowModelNetwork: false, refreshOnCreate: false
      });
      const config = {
        bridge: { baseUrl: 'http://127.0.0.1:1' },
        pi: { cwd: roots.cwd, agentDir: roots.agentDir, sessionDir: roots.sessions, provider: '', model: '' },
        mcp: { command: process.execPath, args: [roots.server, roots.lifecycle], cwd: root }
      } as never;
      const sdk = await loadNativeSdk(config, { system: '' } as never, runtime, [], {
        kind: 'inventory-preflight', root, cwd: roots.cwd, agentDir: roots.agentDir
      });
      const manager = sdk.create(roots.cwd, roots.sessions);
      session = await sdk.connect(manager, async () => {});
      session.setAutoCompactionEnabled(false);
      await session.bindExtensions({});
      await sdk.ready(session, async () => {});
      // A second active chat gets an independent observer/tracker even though
      // both sessions share this SdkPort and adapter factory.
      peerSession = await sdk.connect(sdk.create(roots.cwd, roots.sessions), async () => {});
      peerSession.setAutoCompactionEnabled(false);
      await peerSession.bindExtensions({});
      await sdk.ready(peerSession, async () => {});

      const expected = REQUIRED_WHATSAPP_TOOLS.map(whatsappToolName);
      const direct = session.getAllTools().filter(tool => tool.name.startsWith('mcp__whatsapp_session_controller__'));
      expect(direct.map(tool => tool.name)).toEqual(expected);
      expect(direct).toHaveLength(21);
      expect(direct.every(tool => tool.exposure === 'direct' && tool.namespace === undefined &&
        tool.sourceInfo.path === MCP_ADAPTER_SOURCE_PATH && tool.sourceInfo.source === 'inline')).toBe(true);
      expect(session.getActiveToolNames().filter(name => name.startsWith('mcp__whatsapp_session_controller__'))).toEqual(expected);
      const gateway = session.getAllTools().filter(tool => tool.name === 'mcp');
      expect(gateway).toHaveLength(1);
      expect(gateway[0]).toMatchObject({
        exposure: 'direct', sourceInfo: { path: MCP_ADAPTER_SOURCE_PATH, source: 'inline' }
      });
      const forbidden = ['mcp', 'mcpScript', 'mcp__whatsapp_session_controller_'];
      expect(session.getActiveToolNames().some(name => forbidden.includes(name))).toBe(false);
      expect(session.getCallableToolNames().some(name => forbidden.includes(name))).toBe(false);
      expect(session.getAllTools().some(tool => tool.name === 'mcp__whatsapp_session_controller_')).toBe(false);
      expect(JSON.parse(direct[0]!.description)).toEqual({
        inheritedSecret: 'absent', transport: 'stdio', bridge: 'http://127.0.0.1:1',
        workspace: roots.cwd, level: 'INFO'
      });

      const definition = session.getToolDefinition(whatsappToolName('send_message'));
      expect(definition).toBeDefined();
      const output = await definition!.execute(
        'OWNED_DIRECT_CALL', {}, undefined, undefined,
        session.extensionRunner.createToolContext('OWNED_DIRECT_CALL', undefined)
      );
      const event: ToolResultEvent = {
        type: 'tool_result', toolName: whatsappToolName('send_message'), toolCallId: 'OWNED_DIRECT_CALL',
        parentToolCallId: 'OWNED_PARENT', input: {}, content: output.content,
        details: output.details, isError: false,
        ...(output.structuredContent !== undefined ? { structuredContent: output.structuredContent } : {})
      };
      expect(output.details).toMatchObject({
        server: 'whatsapp_session_controller_', tool: 'send_message',
        mcpResult: { structuredContent: { result: JSON.stringify({ success: true, message_id: 'OWNED_ADAPTER_RESULT' }) } }
      });
      expect(confirmedMessage(event, 'send_message')).toBe('OWNED_ADAPTER_RESULT');

      const livePids = await eventuallyStarted(roots.lifecycle, 2);
      expect(livePids).toHaveLength(2);
      expect(livePids.every(Number.isSafeInteger)).toBe(true);
      await Promise.all([session.dispose(), session.dispose(), peerSession.dispose()]);
      await Promise.all(livePids.map(eventuallyStopped));
      expect((await readFile(roots.lifecycle, 'utf8')).match(/^start:/gm)).toHaveLength(2);
      session = undefined;
      peerSession = undefined;

      const failedSdk = await loadNativeSdk(config, { system: '' } as never, runtime, [], {
        kind: 'inventory-preflight', root, cwd: roots.cwd, agentDir: roots.agentDir
      });
      const failedManager = failedSdk.create(roots.cwd, roots.sessions);
      let fenceChecks = 0;
      await expect(failedSdk.connect(failedManager, async () => {
        if (++fenceChecks !== 2) return;
        for (let attempt = 0; attempt < 100; attempt++) {
          const value = await readFile(roots.lifecycle, 'utf8');
          if ([...value.matchAll(/^start:(\d+)$/gm)].length === 3) throw new Error('SYNTHETIC_FENCE_LOST');
          await new Promise(resolve => setTimeout(resolve, 10));
        }
        throw new Error('SYNTHETIC_SECOND_CHILD_MISSING');
      })).rejects.toThrow('SYNTHETIC_FENCE_LOST');
      const pids = await eventuallyStarted(roots.lifecycle, 3);
      expect(pids).toHaveLength(3);
      await eventuallyStopped(pids[2]!);

      // The real SDK transport first waits for stdin shutdown, then SIGTERM,
      // then escalates. The facade must continue awaiting the adapter handler
      // throughout that bounded sequence, even though the adapter catches its
      // own cleanup errors.
      const stubbornConfig = {
        bridge: { baseUrl: 'http://127.0.0.1:1' },
        pi: { cwd: roots.cwd, agentDir: roots.agentDir, sessionDir: roots.sessions, provider: '', model: '' },
        mcp: { command: process.execPath, args: [roots.server, roots.lifecycle, 'stubborn'], cwd: root }
      } as never;
      const stubbornSdk = await loadNativeSdk(stubbornConfig, { system: '' } as never, runtime, [], {
        kind: 'inventory-preflight', root, cwd: roots.cwd, agentDir: roots.agentDir
      });
      stubbornSession = await stubbornSdk.connect(stubbornSdk.create(roots.cwd, roots.sessions), async () => {});
      stubbornSession.setAutoCompactionEnabled(false);
      await stubbornSession.bindExtensions({});
      await stubbornSdk.ready(stubbornSession, async () => {});
      const stubbornPids = await eventuallyStarted(roots.lifecycle, 4);
      expect(stubbornPids).toHaveLength(4);
      const stubbornPid = stubbornPids[3]!;
      await eventuallyRead(roots.lifecycle, 'mode:stubborn');
      let disposalSettled = false;
      const disposal = Promise.resolve(stubbornSession.dispose()).then(() => { disposalSettled = true; });
      await eventuallyRead(roots.lifecycle, 'term', 600);
      expect(disposalSettled).toBe(false);
      expect(() => process.kill(stubbornPid, 0)).not.toThrow();
      await disposal;
      await eventuallyStopped(stubbornPid);
      expect((await readFile(roots.lifecycle, 'utf8')).match(/^term$/gm)).toHaveLength(1);
    } finally {
      await Promise.allSettled([
        Promise.resolve(session?.dispose()), Promise.resolve(peerSession?.dispose()),
        Promise.resolve(stubbornSession?.dispose())
      ]);
      if (priorAgentDir === undefined) delete process.env['PI_CODING_AGENT_DIR'];
      else process.env['PI_CODING_AGENT_DIR'] = priorAgentDir;
      if (priorSecret === undefined) delete process.env['SYNTHETIC_PRIVATE_VALUE'];
      else process.env['SYNTHETIC_PRIVATE_VALUE'] = priorSecret;
      await rm(root, { recursive: true, force: true });
    }
  }, 30_000);

  it('isolates live and cached-unreachable status for concurrent sessions on one SDK port', async () => {
    const root = await mkdtemp(join(tmpdir(), 'whatsapp-adapter-session-status-'));
    const roots = {
      cwd: join(root, 'workspace'), agentDir: join(root, 'agent'), home: join(root, 'home'),
      sessions: join(root, 'sessions'), server: join(root, 'fake-mcp.mjs'), lifecycle: join(root, 'lifecycle.log')
    };
    await Promise.all([roots.cwd, roots.agentDir, roots.home, roots.sessions].map(path => mkdir(path, { recursive: true })));
    await writeFile(roots.server, fakeServer, { mode: 0o700 });
    const priorAgentDir = process.env['PI_CODING_AGENT_DIR'];
    process.env['PI_CODING_AGENT_DIR'] = roots.agentDir;
    let liveSession: SdkSession | undefined;
    let unavailableSession: SdkSession | undefined;
    try {
      const runtime = await ModelRuntime.create({
        authPath: join(roots.agentDir, 'auth.json'), modelsPath: join(roots.agentDir, 'models.json'),
        allowModelNetwork: false, refreshOnCreate: false
      });
      const config = {
        bridge: { baseUrl: 'http://127.0.0.1:1' },
        pi: { cwd: roots.cwd, agentDir: roots.agentDir, sessionDir: roots.sessions, provider: '', model: '' },
        mcp: {
          command: process.execPath,
          args: [roots.server, roots.lifecycle, 'first-live-rest-unreachable'],
          cwd: root
        }
      } as never;
      const definition = createControllerMcpConfig(config).mcpServers[ADAPTER_WHATSAPP_SERVER]!;
      saveMetadataCache({
        version: 1,
        servers: {
          [ADAPTER_WHATSAPP_SERVER]: {
            configHash: computeServerHash(definition),
            cachedAt: Date.now(), resources: [], prompts: [],
            tools: REQUIRED_WHATSAPP_TOOLS.map(name => ({
              name, description: name,
              inputSchema: { type: 'object', properties: {}, additionalProperties: false }
            }))
          }
        }
      });
      const sdk = await loadNativeSdk(config, { system: '' } as never, runtime, [], {
        kind: 'inventory-preflight', root, cwd: roots.cwd, agentDir: roots.agentDir
      });
      liveSession = await sdk.connect(sdk.create(roots.cwd, roots.sessions), async () => {});
      liveSession.setAutoCompactionEnabled(false);
      await liveSession.bindExtensions({});
      await expect(sdk.ready(liveSession, async () => {})).resolves.toBeUndefined();

      unavailableSession = await sdk.connect(sdk.create(roots.cwd, roots.sessions), async () => {});
      unavailableSession.setAutoCompactionEnabled(false);
      await unavailableSession.bindExtensions({});
      const started = await eventuallyStarted(roots.lifecycle, 2);
      await eventuallyStopped(started[1]!);

      // The second session materializes the complete cached metadata surface,
      // but its own adapter status is unavailable. It must not borrow the first
      // session's connected snapshot from this shared SdkPort.
      const expected = REQUIRED_WHATSAPP_TOOLS.map(whatsappToolName);
      const cached = unavailableSession.getAllTools()
        .filter(tool => tool.name.startsWith('mcp__whatsapp_session_controller__'));
      expect(cached.map(tool => tool.name)).toEqual(expected);
      expect(cached.every(tool => tool.exposure === 'direct' &&
        tool.sourceInfo.path === MCP_ADAPTER_SOURCE_PATH && tool.sourceInfo.source === 'inline')).toBe(true);
      expect(unavailableSession.getActiveToolNames()
        .filter(name => name.startsWith('mcp__whatsapp_session_controller__'))).toEqual(expected);
      expect(unavailableSession.getCallableToolNames()
        .filter(name => name.startsWith('mcp__whatsapp_session_controller__'))).toEqual(expected);
      await expect(sdk.ready(unavailableSession, async () => {}))
        .rejects.toMatchObject({ code: 'session_failed' });

      // Recheck after the unavailable session has published its failure. A
      // shared/global status tracker would now reject this still-live session.
      await expect(sdk.ready(liveSession, async () => {})).resolves.toBeUndefined();
    } finally {
      await Promise.allSettled([
        Promise.resolve(liveSession?.dispose()), Promise.resolve(unavailableSession?.dispose())
      ]);
      try {
        const lifecycle = await readFile(roots.lifecycle, 'utf8');
        const pids = [...lifecycle.matchAll(/^start:(\d+)$/gm)].map(match => Number(match[1]));
        await Promise.all(pids.map(eventuallyStopped));
      } catch (error) {
        if ((error as NodeJS.ErrnoException).code !== 'ENOENT') throw error;
      }
      if (priorAgentDir === undefined) delete process.env['PI_CODING_AGENT_DIR'];
      else process.env['PI_CODING_AGENT_DIR'] = priorAgentDir;
      await rm(root, { recursive: true, force: true });
    }
  }, 30_000);
});
