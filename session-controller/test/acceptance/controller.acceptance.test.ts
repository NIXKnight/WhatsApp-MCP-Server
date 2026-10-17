import { spawn, type ChildProcess } from 'node:child_process';
import { createHmac } from 'node:crypto';
import { once } from 'node:events';
import { mkdtemp, open, readFile, rm, writeFile } from 'node:fs/promises';
import { appendFileSync, existsSync, readFileSync } from 'node:fs';
import { createServer as createHttpServer, type Server as HttpServer } from 'node:http';
import { createConnection, createServer, type Server as NetServer, type Socket } from 'node:net';
import { dirname, isAbsolute, join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import { Duplex } from 'node:stream';
import { afterAll, beforeAll, describe, expect, it, vi } from 'vitest';
import { createClient } from 'redis';
import { validateConfig, type ControllerConfig } from '../../src/config.js';
import { ControllerError, canonicalChatId, eventId, fingerprintEvent, type CanonicalChatId, type CheckpointV1, type CompletionIndexPort, type CompletionProof, type ClaimedWork, type DiagnosticPort, type MutationAttempt, type NotificationV2, type SessionBinding, type UsageSnapshot, type WaitPort } from '../../src/domain.js';
import { waitForBridge } from '../../src/main.js';
import { createDiagnostics } from '../../src/diagnostics.js';
import { createWebhookServer, type WebhookServer } from '../../src/intake/server.js';
import { connectRedis, requireBaseline, type RedisConnections, type CommandPort } from '../../src/redis/client.js';
import { APP, keys } from '../../src/redis/keys.js';
import { createRepository, type RedisRepository } from '../../src/redis/repository.js';
import { runChatWorker, type TurnContext, type TurnPorts } from '../../src/scheduler/chat-worker.js';
import { createCoordinator, type Coordinator } from '../../src/scheduler/coordinator.js';
import { createRecovery } from '../../src/scheduler/recovery.js';
import { createUsagePolicy } from '../../src/compaction/usage-policy.js';
import { checkpointIdentity, containsSensitive, renderCheckpointSummary, validateCheckpoint } from '../../src/compaction/checkpoint.js';
import { createCompactor } from '../../src/compaction/compactor.js';
import { createCompletionIndex, type CompletionIndex, type MappingPort } from '../../src/pi/completion-index.js';
import { createSessionHost, type SessionHost } from '../../src/pi/session-host.js';
import { createToolJournal, MUTATION_TYPE, type JournalAttachment } from '../../src/pi/tool-journal-extension.js';
import { ADAPTER_WHATSAPP_SERVER, MCP_ADAPTER_SOURCE_PATH, WHATSAPP_SERVER, type SdkManager, type SdkPort } from '../../src/pi/mcp-loader.js';
import type { AgentSessionEvent, ExtensionAPI, ExtensionContext, CustomToolCallEvent, ToolResultEvent, ToolInfo } from '@earendil-works/pi-coding-agent';

const binary = process.env['PHASE10_OWNED_REDIS_BINARY'];
if (!binary || !isAbsolute(binary)) throw new Error('PHASE10_OWNED_REDIS_BINARY must be an explicit absolute path');
let suiteRedisProbe: ChildProcess | undefined;
const here = dirname(fileURLToPath(import.meta.url));
const group = canonicalChatId('100@g.us'), other = canonicalChatId('200@g.us'), operator = canonicalChatId('300@lid');
const secret = 'TEST_ONLY_NON_SENSITIVE_HMAC_KEY_32_BYTES', fixedNow = Date.parse('2033-05-18T03:33:25Z');
const policy = { groups: [group, other], operatorJids: [operator, '400@s.whatsapp.net'] };
const waitForAbort = { wait: async (_ms: number, signal: AbortSignal) => new Promise<void>(r => signal.addEventListener('abort', () => r(), { once: true })) };
const pause = (ms: number) => new Promise<void>(r => setTimeout(r, ms));
function deferred() { let release!: () => void; const promise = new Promise<void>(r => { release = r; }); return { promise, release }; }
/** Bound only arrival at a test-owned gate, never production graceful drain. */
async function enterGate(gate:{promise:Promise<void>},tasks:readonly Promise<unknown>[]=[]):Promise<void>{
  let timer:ReturnType<typeof setTimeout>|undefined;
  const failed=()=>{throw new Error('synthetic_gate_not_entered');};
  try{await Promise.race([gate.promise,...tasks.map(task=>task.then(failed,failed)),new Promise<never>((_r,j)=>{timer=setTimeout(()=>j(new Error('synthetic_gate_not_entered')),1000);})]);}
  finally{if(timer)clearTimeout(timer);}
}
/** A test-owned deadline cancels no work: the caller must still settle its child in finally. */
async function ownedProbeDeadline<T>(task:Promise<T>):Promise<T>{
  let timer:ReturnType<typeof setTimeout>|undefined;
  try{return await Promise.race([task,new Promise<never>((_resolve,reject)=>{timer=setTimeout(()=>reject(new Error('owned_probe_timeout')),2000);})]);}
  finally{if(timer)clearTimeout(timer);}
}
interface CleanupAttempt { readonly failure:string; readonly run:()=>void|Promise<void> }
async function cleanupAll(attempts:readonly CleanupAttempt[]):Promise<void>{
  let first:string|undefined;
  for(const attempt of attempts){
    const [result]=await Promise.allSettled([Promise.resolve().then(attempt.run)]);
    if(result?.status==='rejected')first??=attempt.failure;
  }
  if(first)throw new Error(first);
}
interface OwnedClose { code:number|null; signal:NodeJS.Signals|null }
interface OwnedTermination extends OwnedClose { escalated:boolean }
const ownedCloses=new WeakMap<ChildProcess,Promise<OwnedClose>>();
function trackOwned(child:ChildProcess):ChildProcess{
  const closed=new Promise<OwnedClose>(resolveClose=>child.once('close',(code,signal)=>resolveClose({code:typeof code==='number'?code:null,signal:(signal??null) as NodeJS.Signals|null})));
  ownedCloses.set(child,closed);return child;
}
function ownedClose(child:ChildProcess):Promise<OwnedClose>{const closed=ownedCloses.get(child);if(!closed)throw new Error('owned_child_close_untracked');return closed;}
async function terminateOwned(child:ChildProcess|undefined,signal:NodeJS.Signals='SIGTERM'):Promise<OwnedTermination>{
  if(!child)return{code:null,signal:null,escalated:false};
  const closed=ownedClose(child);
  if(child.exitCode===null&&child.signalCode===null)child.kill(signal);
  const natural=await Promise.race([closed,pause(2000).then(()=>undefined)]);
  if(natural)return{...natural,escalated:false};
  let escalated=false;
  if(child.exitCode===null&&child.signalCode===null){child.kill('SIGKILL');escalated=true;}
  const forced=await Promise.race([closed,pause(2000).then(()=>undefined)]);
  if(!forced)throw new Error('owned_child_exit_timeout');
  return{...forced,escalated};
}
interface BoundedChildOutput{stdout():string;stderr():string;guard<T>(promise:Promise<T>):Promise<T>}
function collectOwnedOutput(child:ChildProcess,limit=65536):BoundedChildOutput{
  if(!child.stdout||!child.stderr)throw new Error('owned_child_capture_unavailable');
  let stdout='',stderr='',bytes=0,overflow=false,rejectOverflow!:(error:Error)=>void;
  const overflowed=new Promise<never>((_resolve,reject)=>{rejectOverflow=reject;});
  const append=(target:'stdout'|'stderr',chunk:string)=>{
    if(overflow)return;
    const added=Buffer.byteLength(chunk,'utf8');
    if(bytes+added>limit){overflow=true;void terminateOwned(child).then(
      ()=>rejectOverflow(new Error('owned_child_output_overflow')),
      terminationError=>rejectOverflow(terminationError instanceof Error&&terminationError.message==='owned_child_exit_timeout'?terminationError:new Error('owned_child_exit_timeout'))
    );return;}
    bytes+=added;if(target==='stdout')stdout+=chunk;else stderr+=chunk;
  };
  child.stdout.setEncoding('utf8');child.stderr.setEncoding('utf8');
  child.stdout.on('data',chunk=>append('stdout',chunk));child.stderr.on('data',chunk=>append('stderr',chunk));
  return{stdout:()=>stdout,stderr:()=>stderr,async guard<T>(promise:Promise<T>){const value=await Promise.race([promise,overflowed]);if(overflow)return overflowed;return value;}};
}
async function requirePhase10Redis726():Promise<void>{
  let probe:ChildProcess|undefined;
  try{
    probe=trackOwned(spawn(binary!,['--version'],{stdio:['ignore','pipe','pipe'],env:{PATH:'/usr/bin:/bin'}}));
    suiteRedisProbe=probe;probe.on('error',()=>{});
    const output=collectOwnedOutput(probe,4096),closed=ownedClose(probe);
    const result=await output.guard(Promise.race([closed,pause(2000).then(()=>undefined)]));
    if(!result){await terminateOwned(probe);if(suiteRedisProbe===probe)suiteRedisProbe=undefined;throw new Error('phase10_redis_version_probe_failed');}
    if(suiteRedisProbe===probe)suiteRedisProbe=undefined;
    const version=/^Redis server v=([0-9]+\.[0-9]+\.[0-9]+)(?:\s|$)/.exec(output.stdout().trim())?.[1];
    if(result.code!==0||result.signal!==null||output.stderr()!==''||version!=='7.2.6')throw new Error('phase10_redis_version_probe_failed');
  }catch(error){
    if(probe&&suiteRedisProbe===probe){try{await terminateOwned(probe);suiteRedisProbe=undefined;}catch{throw new Error('owned_child_exit_timeout');}}
    if(error instanceof Error&&['phase10_redis_version_probe_failed','owned_child_output_overflow','owned_child_exit_timeout'].includes(error.message))throw error;
    throw new Error('phase10_redis_version_probe_failed');
  }
}
function event(sequence: number, chat: CanonicalChatId = group, id = sequence.toString(16).padStart(32, '0'), own = false): NotificationV2 {
  const direct = !chat.endsWith('@g.us'); return { schema_version: 2, event_type: 'whatsapp.message.received', event_id: eventId(id), message_id: `SYNTHETIC_MESSAGE_${sequence}`, event_timestamp: '2033-05-18T03:33:20.123456789Z', received_at: '2033-05-18T03:33:21Z', chat_sequence: sequence, sender_jid: direct ? '300@lid' : '500@s.whatsapp.net', is_from_me: own, chat_kind: direct ? 'operator_private' : 'group', chat_jid: chat, canonical_chat_jid: chat } as NotificationV2;
}
async function ephemeralPort() { const s = createServer(); s.listen(0, '127.0.0.1'); await once(s, 'listening'); const a = s.address(); if (!a || typeof a === 'string') throw new Error('synthetic_port_failed'); await new Promise<void>((r, j) => s.close(e => e ? j(e) : r())); return a.port; }
interface RedisIdentity { readonly pid:number; readonly runId:string }
function parseRedisIdentity(reply:unknown):RedisIdentity{
  if(typeof reply!=='string'||Buffer.byteLength(reply,'utf8')>16384)throw new Error('owned_redis_identity_invalid');
  const field=(name:string)=>{const rows=reply.split('\n').map(row=>row.replace(/\r$/,'')).filter(row=>row.startsWith(`${name}:`));if(rows.length!==1)throw new Error('owned_redis_identity_invalid');return rows[0]!.slice(name.length+1);};
  const pidText=field('process_id'),pid=Number(pidText),runId=field('run_id');
  if(field('redis_version')!=='7.2.6'||! /^[1-9]\d*$/.test(pidText)||!Number.isSafeInteger(pid)||! /^[0-9a-f]{40}$/.test(runId))throw new Error('owned_redis_identity_invalid');
  return{pid,runId};
}
async function verifyRedisIdentity(port:CommandPort,expected:RedisIdentity):Promise<void>{
  const actual=parseRedisIdentity(await Promise.race([port.sendCommand(['INFO','server']),pause(1000).then(()=>{throw new Error('owned_redis_identity_timeout');})]));
  if(actual.pid!==expected.pid||actual.runId!==expected.runId)throw new Error('owned_redis_identity_mismatch');
}
async function verifyConnections(redis:OwnedRedis,connections:RedisConnections,expected=redis.identity):Promise<void>{
  redis.assertLive();for(const port of[connections.writer,connections.reader,connections.mapping]){await verifyRedisIdentity(port,expected);redis.assertLive();}
}
async function redisExists(redis:OwnedRedis,key:string){const client=redis.client();try{await client.connect();redis.assertLive();await verifyRedisIdentity(client,redis.identity);redis.assertLive();return await client.exists(key);}finally{if(client.isOpen)client.destroy();}}
class OwnedRedis {
  directory='';port=0;child:ChildProcess|undefined;private pinned:RedisIdentity|undefined;private socketPath:string|undefined;
  get identity():RedisIdentity{if(!this.pinned)throw new Error('owned_redis_identity_missing');return this.pinned;}
  assertLive():void{const child=this.child;if(!child||!Number.isSafeInteger(child.pid)||!child.pid||child.exitCode!==null||child.signalCode!==null)throw new Error('owned_redis_child_not_live');}
  client(){const socket=this.socketPath?{path:this.socketPath,tls:false as const,reconnectStrategy:false as const,connectTimeout:100}:{host:'127.0.0.1',port:this.port,reconnectStrategy:false as const,connectTimeout:100};const client=createClient({disableOfflineQueue:true,socket});client.on('error',()=>{});return client;}
  get privatePath():string{if(!this.socketPath)throw new Error('owned_redis_private_socket_missing');return this.socketPath;}
  async start(options:{port?:number;privateSocket?:boolean}={}) {
    try {
      this.directory=await mkdtemp('/tmp/whatsapp-slice10-redis-');this.port=options.privateSocket?0:options.port??await ephemeralPort();this.socketPath=options.privateSocket?join(this.directory,'redis.sock'):undefined;
      const child=trackOwned(spawn(binary!,['--bind','127.0.0.1','--port',String(this.port),...(this.socketPath?['--unixsocket',this.socketPath,'--unixsocketperm','700']:[]),'--dir',this.directory,'--appendonly','yes','--appendfsync','always','--maxmemory-policy','noeviction','--save','','--daemonize','no'],{stdio:'ignore',env:{PATH:'/usr/bin:/bin'}}));this.child=child;child.on('error',()=>{});
      const spawned=await Promise.race([once(child,'spawn').then(()=>true),pause(2000).then(()=>false)]);if(!spawned)throw new Error('synthetic_redis_start_failed');this.assertLive();
      for(let i=0;i<300;i++){
        this.assertLive();const probe=this.client();let connected=false;
        try{await Promise.race([probe.connect(),ownedClose(child).then(()=>{throw new Error('owned_redis_child_not_live');})]);connected=true;this.assertLive();const identity=parseRedisIdentity(await Promise.race([probe.sendCommand(['INFO','server']),ownedClose(child).then(()=>{throw new Error('owned_redis_child_not_live');}),pause(1000).then(()=>{throw new Error('owned_redis_identity_timeout');})]));if(identity.pid!==child.pid)throw new Error('owned_redis_identity_mismatch');await Promise.race([requireBaseline(probe),ownedClose(child).then(()=>{throw new Error('owned_redis_child_not_live');}),pause(1000).then(()=>{throw new Error('owned_redis_identity_timeout');})]);this.assertLive();await verifyRedisIdentity(probe,identity);this.assertLive();this.pinned=identity;return;}
        catch{this.assertLive();if(connected)throw new Error('owned_redis_identity_mismatch');await pause(10);}
        finally{if(probe.isOpen)probe.destroy();}
      }
      throw new Error('synthetic_redis_start_failed');
    } catch {
      let cleanupFailure:Error|undefined;
      const child=this.child;let settled=!child;
      try{await terminateOwned(child);settled=true;}catch{cleanupFailure??=new Error('owned_child_exit_timeout');}
      if(settled&&this.child===child)this.child=undefined;
      const directory=this.directory;
      if(settled&&directory){try{await rm(directory,{recursive:true,force:true});if(this.directory===directory)this.directory='';}catch{cleanupFailure??=new Error('temporary_directory_cleanup_failed');}}
      if(settled&&this.directory==='')this.port=0;
      if(cleanupFailure)throw cleanupFailure;
      throw new Error('synthetic_redis_start_failed');
    }
  }
  get url(){return `redis://127.0.0.1:${this.port}/0`;}
  async stop(signal:NodeJS.Signals='SIGTERM'){const child=this.child;if(!child)return;await terminateOwned(child,signal);if(this.child===child)this.child=undefined;}
  async cleanup(){await this.stop();const directory=this.directory;if(directory){await rm(directory,{recursive:true,force:true});if(this.directory===directory)this.directory='';}if(this.directory==='')this.port=0;}
}
/** Test-only held TCP listener. Main cannot write to an unverified upstream.
 * The private Unix socket is reachable only beneath this owned Redis tree. */
class OwnedForwarder {
  private listener:NetServer|undefined;private sockets=new Map<Socket,Promise<void>>();private tasks=new Set<Promise<void>>();private stopping=false;
  port=0;verified=0;rejected=0;
  constructor(private readonly redis:OwnedRedis,private readonly expected=redis.identity){}
  private retain(socket:Socket):Socket{const closed=new Promise<void>(r=>socket.once('close',()=>{this.sockets.delete(socket);r();}));this.sockets.set(socket,closed);socket.on('error',()=>{});return socket;}
  async start():Promise<void>{
    this.listener=createServer(incoming=>{
      this.retain(incoming);incoming.pause();if(this.stopping){incoming.destroy();return;}
      const upstream=this.retain(createConnection({path:this.redis.privatePath}));
      const task=(async()=>{try{
        const reply=await new Promise<string>((ok,fail)=>{
          let bytes=Buffer.alloc(0),done=false;const timer=setTimeout(()=>finish(new Error('owned_forwarder_timeout')),1000);
          const finish=(error?:Error,value?:string)=>{if(done)return;done=true;clearTimeout(timer);upstream.off('data',data);upstream.off('error',failed);upstream.off('close',ended);error?fail(error):ok(value!);};
          const failed=()=>finish(new Error('owned_forwarder_failed')),ended=()=>finish(new Error('owned_forwarder_closed'));
          const data=(chunk:Buffer)=>{if(bytes.length+chunk.length>16448){finish(new Error('owned_forwarder_overflow'));return;}bytes=Buffer.concat([bytes,chunk]);const end=bytes.indexOf('\r\n');if(end<0)return;const header=bytes.subarray(0,end).toString('ascii');if(!/^\$[1-9]\d*$/.test(header)){finish(new Error('owned_forwarder_reply_invalid'));return;}const size=Number(header.slice(1));if(size>16384){finish(new Error('owned_forwarder_overflow'));return;}if(bytes.length<end+2+size+2)return;if(bytes.length!==end+2+size+2||bytes.subarray(-2).toString()!=='\r\n'){finish(new Error('owned_forwarder_reply_invalid'));return;}finish(undefined,bytes.subarray(end+2,-2).toString('utf8'));};
          upstream.on('data',data);upstream.once('error',failed);upstream.once('close',ended);upstream.once('connect',()=>{upstream.write('*2\r\n$4\r\nINFO\r\n$6\r\nserver\r\n');});
        });
        const actual=parseRedisIdentity(reply);this.redis.assertLive();if(actual.pid!==this.expected.pid||actual.runId!==this.expected.runId)throw new Error('owned_redis_identity_mismatch');
        if(this.stopping||incoming.destroyed)throw new Error('owned_forwarder_stopped');this.verified++;upstream.on('error',()=>incoming.destroy());incoming.on('error',()=>upstream.destroy());upstream.on('close',()=>incoming.destroy());incoming.on('close',()=>upstream.destroy());incoming.pipe(upstream);upstream.pipe(incoming);incoming.resume();
      }catch{this.rejected++;incoming.destroy();upstream.destroy();}})();this.tasks.add(task);void task.finally(()=>this.tasks.delete(task));
    });this.listener.listen(0,'127.0.0.1');await once(this.listener,'listening');const address=this.listener.address();if(!address||typeof address==='string')throw new Error('owned_forwarder_listen_failed');this.port=address.port;
  }
  get url(){return `redis://127.0.0.1:${this.port}/0`;}
  async stop():Promise<void>{this.stopping=true;const listener=this.listener;const stopped=listener?.listening?new Promise<void>((r,j)=>listener.close(e=>e?j(e):r())):Promise.resolve();const closed=[...this.sockets.values()];for(const socket of this.sockets.keys())socket.destroy();await Promise.allSettled([...this.tasks]);await Promise.all(closed);await stopped;this.listener=undefined;}
  get settled(){return !this.listener&&this.sockets.size===0&&this.tasks.size===0;}
}
class OwnedBridgeStatus {
  connected=false; requests=0; port=0; server:HttpServer|undefined;
  async start(){this.server=createHttpServer((request,response)=>{this.requests++;if(request.method!=='GET'||request.url!=='/api/status'){response.writeHead(404);response.end();return;}response.writeHead(200,{'Content-Type':'application/json'});response.end(JSON.stringify({state:this.connected?'CONNECTED':'CONNECTING',is_connected:this.connected,uptime:'0s',message_count:0,chat_count:0,started_at:'2033-05-18T03:33:20Z'}));});this.server.listen(0,'127.0.0.1');await once(this.server,'listening');const address=this.server.address();if(!address||typeof address==='string')throw new Error('synthetic_bridge_listen_failed');this.port=address.port;}
  get baseUrl(){return `http://127.0.0.1:${this.port}`;}
  async stop(){const current=this.server;this.server=undefined;if(current?.listening)await new Promise<void>((resolve,reject)=>current.close(error=>error?reject(error):resolve()));}
}
async function verifyBridgeWaitContracts(){
  let server:HttpServer|undefined;
  try {
    const replies=[{state:'CONNECTED',is_connected:false},{state:'CONNECTING',is_connected:true},{state:'CONNECTED',is_connected:true}];
    server=createHttpServer((request,response)=>{expect(request.url).toBe('/api/status');response.writeHead(200,{'Content-Type':'application/json'});response.end(JSON.stringify(replies.shift()??{}));});
    server.listen(0,'127.0.0.1');await once(server,'listening');const address=server.address();if(!address||typeof address==='string')throw new Error('synthetic_bridge_listen_failed');
    const waits:number[]=[];const wait:WaitPort={async wait(ms,signal){expect(signal.aborted).toBe(false);waits.push(ms);}};
    await waitForBridge({baseUrl:`http://127.0.0.1:${address.port}`,pollMs:100,requestTimeoutMs:100},new AbortController().signal,fetch,wait);
    expect(waits).toEqual([100,100]);expect(replies).toHaveLength(0);
  } finally {if(server?.listening)await new Promise<void>((resolve,reject)=>server!.close(error=>error?reject(error):resolve()));}
  const stop=new AbortController();const request:typeof fetch=async(_input,init)=>new Promise<Response>((_resolve,reject)=>{const signal=init?.signal;if(!signal)throw new Error('test_signal_missing');signal.addEventListener('abort',()=>reject(new Error('TEST_RESPONSE_MUST_NOT_ESCAPE')),{once:true});});
  const waiting=waitForBridge({baseUrl:'http://127.0.0.1:8080',pollMs:100,requestTimeoutMs:30000},stop.signal,request);stop.abort();
  await expect(waiting).rejects.toEqual(expect.objectContaining<Partial<ControllerError>>({code:'intake_closed'}));
}
function config(url:string,sessionDir:string):ControllerConfig{return validateConfig({http:{host:'127.0.0.1',port:8644,webhookPath:'/webhooks/whatsapp',maxBodyBytes:65536,requestTimeoutMs:5000,maxInflight:32},auth:{secret,freshnessSeconds:300},admission:policy,redis:{url,waitAofMs:2000,blockMs:10},bridge:{baseUrl:'http://127.0.0.1:8080',pollMs:100,requestTimeoutMs:100},scheduler:{concurrency:2,leaseMs:120000,renewMs:10000,gapTimeoutMs:1000},pi:{cwd:'/tmp',agentDir:'/tmp/synthetic-agent',sessionDir,systemPromptPath:'/tmp/system.md',preCompactionPath:'/tmp/pre.md',compactionSummaryPath:'/tmp/summary.md',policyVersion:'p1',templateVersion:'t1'},mcp:{command:'/tmp/synthetic-mcp',args:[],cwd:'/tmp'},compaction:{safeWorkingWindowTokens:10000,defaultContextWindowTokens:10000,headroomPercent:15,responseReserveTokens:100,preparationReserveTokens:100,summaryReserveTokens:100,checkpointMaxBytes:65536}});}
type FixtureResource='redis'|'directory'|'connections'|'repository'|'admin'|'index'|'journal'|'host';
interface FixtureSetupHooks {after?(resource:FixtureResource,value:unknown):void|Promise<void>;cleaned?(resource:FixtureResource):void}
interface BaseFixture {redis:OwnedRedis;root:string;cfg:ControllerConfig;connections:RedisConnections;repository:RedisRepository;admin:ReturnType<typeof createClient>;cleanup():Promise<void>}
async function cleanupBaseFixture(redis:OwnedRedis,root:string|undefined,connections:RedisConnections|undefined,repository:RedisRepository|undefined,admin:ReturnType<typeof createClient>|undefined,setup?:FixtureSetupHooks):Promise<void>{await cleanupAll([
  {failure:'synthetic_admin_cleanup_failed',run:async()=>{if(!admin)return;if(admin.isOpen)admin.destroy();setup?.cleaned?.('admin');}},
  {failure:'synthetic_repository_cleanup_failed',run:async()=>{if(!repository)return;setup?.cleaned?.('repository');}},
  {failure:'synthetic_connections_cleanup_failed',run:async()=>{if(!connections)return;await connections.close();setup?.cleaned?.('connections');}},
  {failure:'synthetic_redis_cleanup_failed',run:async()=>{await redis.cleanup();setup?.cleaned?.('redis');}},
  {failure:'temporary_directory_cleanup_failed',run:async()=>{if(!root)return;await rm(root,{recursive:true,force:true});setup?.cleaned?.('directory');}}
]);}
async function base(indexFactory:(repo:()=>RedisRepository)=>CompletionIndexPort,setup?:FixtureSetupHooks):Promise<BaseFixture>{const redis=new OwnedRedis();let root:string|undefined,connections:RedisConnections|undefined,repository:RedisRepository|undefined,admin:ReturnType<typeof createClient>|undefined;try{await redis.start();await setup?.after?.('redis',redis);root=await mkdtemp('/tmp/whatsapp-slice10-');await setup?.after?.('directory',root);const cfg=config(redis.url,root);connections=await connectRedis(cfg.redis);await setup?.after?.('connections',connections);await verifyConnections(redis,connections);repository=await createRepository(connections,cfg.admission,indexFactory(()=>repository!));await setup?.after?.('repository',repository);admin=redis.client();await admin.connect();await verifyRedisIdentity(admin,redis.identity);redis.assertLive();await setup?.after?.('admin',admin);return{redis,root,cfg,connections,repository,admin,async cleanup(){await cleanupBaseFixture(redis,root,connections,repository,admin,setup);}};}catch{await cleanupBaseFixture(redis,root,connections,repository,admin,setup);throw new Error('setup_failed');}}
async function repositoryFixture(setup?:FixtureSetupHooks){return base(()=>({findEvent:async()=>undefined,classifyOwn:async()=> 'manual' as const}),setup);}

type Handler=(event:unknown,context:ExtensionContext)=>Promise<unknown>;
interface Controls { effects:string[]; handlers:Record<string,Handler>; contexts:Map<string,ExtensionContext>; managers:Map<string,SdkManager & {getBranch():readonly unknown[]}>; prompt:(sessionId:string)=>Promise<void>; usage:(sessionId:string)=>UsageSnapshot|undefined; compact?:(manager:SdkManager & {getBranch():readonly unknown[]},context:TurnContext,checkpoint:any)=>Promise<any>; }
const toolInfo:ToolInfo={name:`mcp__${WHATSAPP_SERVER}__send_message`,description:'',parameters:{type:'object'},exposure:'direct',sourceInfo:{path:MCP_ADAPTER_SOURCE_PATH,source:'inline',scope:'temporary',origin:'top-level'}};
function sdk(controls:Controls,journal:JournalAttachment):SdkPort{let serial=0;const manager=(file:string,header:Record<string,unknown>,entries:Record<string,unknown>[])=>{const activeBranch=()=>{const byId=new Map(entries.map(entry=>[entry['id'],entry])),branch:Record<string,unknown>[]=[];let current=entries.at(-1);const seen=new Set<unknown>();while(current&&!seen.has(current['id'])){seen.add(current['id']);branch.push(current);const parent=current['parentId'];current=typeof parent==='string'?byId.get(parent):undefined;}return branch.reverse();};const m:SdkManager&{getBranch():readonly unknown[]}={getSessionId:()=>header['id'] as string,getSessionFile:()=>file,getHeader:()=>header,getEntries:()=>entries,getBranch:activeBranch,appendCustomEntry(customType,data){const id=`SYNTHETIC_ENTRY_${++serial}`;const row={type:'custom',id,parentId:(entries.at(-1)?.['id'] as string|undefined)??null,timestamp:'2033-05-18T03:33:22Z',customType,data};entries.push(row);if(existsSync(file))appendFileSync(file,JSON.stringify(row)+'\n');return id;}};controls.managers.set(m.getSessionId(),m);return m;};
  const pi={on(type:string,handler:Handler){controls.handlers[type]=handler;return()=>{};},getAllTools:()=>[toolInfo]} as unknown as ExtensionAPI; void journal.extension(pi);
  return{create(cwd,directory){controls.effects.push('create');const id=`SYNTHETIC_SESSION_${++serial}`;return manager(join(directory,`${id}.jsonl`),{type:'session',version:3,id,cwd,timestamp:'2033-05-18T03:33:20Z'},[]);},open(file){controls.effects.push(`open:${file}`);const rows=readFileSync(file,'utf8').trimEnd().split('\n').map(x=>JSON.parse(x) as Record<string,unknown>);return manager(file,rows.shift()!,rows);},async connect(m,current){await current();let idle=true;const listeners=new Set<(e:AgentSessionEvent)=>void>();const context={sessionManager:{getSessionId:()=>m.getSessionId()},abort:()=>{}} as unknown as ExtensionContext;controls.contexts.set(m.getSessionId(),context);return{get isIdle(){return idle;},setAutoCompactionEnabled(){},async bindExtensions(){await controls.handlers['session_start']?.({},context);},getAllTools:()=>[toolInfo],getActiveToolNames:()=>[toolInfo.name],getCallableToolNames:()=>[toolInfo.name],getToolDefinition:()=>undefined,extensionRunner:{} as never,subscribe(fn){listeners.add(fn);return()=>listeners.delete(fn);},async prompt(){controls.effects.push(`prompt:${m.getSessionId()}`);idle=false;await controls.prompt(m.getSessionId());idle=true;for(const fn of listeners)fn({type:'agent_settled',aborted:false} as AgentSessionEvent);},async waitForIdle(){},dispose(){controls.effects.push(`dispose:${m.getSessionId()}`);},getContextUsage:()=>controls.usage(m.getSessionId()),async compact(){throw new Error('external_model_only');}};},async ready(_s,current){await current();controls.effects.push('ready');}};
}
interface ProductionFixture extends BaseFixture {host:SessionHost;index:CompletionIndex;journal:JournalAttachment;controls:Controls;rows:{chat:CanonicalChatId;binding?:SessionBinding}[];mapped(chat?:CanonicalChatId):Promise<SessionBinding|undefined>;reconstruct():Promise<void>}
async function productionFixture(setup?:FixtureSetupHooks):Promise<ProductionFixture>{let index:CompletionIndex|undefined,host:SessionHost|undefined,journal:JournalAttachment|undefined;const f=await base(()=>({findEvent:(c,i)=>index!.findEvent(c,i),classifyOwn:(c,a,i)=>index!.classifyOwn(c,a,i)}),setup);try{const rows=f.repository.chats.map(chat=>({chat} as {chat:CanonicalChatId;binding?:SessionBinding}));const controls:Controls={effects:[],handlers:{},contexts:new Map(),managers:new Map(),prompt:async()=>{},usage:()=>({tokens:1000,contextWindow:10000,percent:10})};
  const build=async(track:boolean)=>{const mappings:MappingPort={list:async()=>rows.map(r=>({...r}))};index=createCompletionIndex(f!.root,f!.cfg.admission,mappings,async(a,i)=>(await f!.repository.ownMutation(a,i))==='uncertain');if(track)await setup?.after?.('index',index);journal=createToolJournal(f!.repository,{clock:{now:()=>fixedNow},id:(()=>{let n=0;return()=>String(++n).padStart(32,'0');})()});if(track)await setup?.after?.('journal',journal);host=createSessionHost({cwd:'/tmp',root:f!.root,sdk:sdk(controls,journal),index,journal,checkpointMaxBytes:f!.cfg.compaction.checkpointMaxBytes,current:async w=>{if(!await f!.repository.checkWork(w))throw new ControllerError('lease_lost');},bind:async(w,b)=>{const ok=await f!.repository.bindSession(w,b);if(ok)rows.find(r=>r.chat===w.event.canonical_chat_jid)!.binding=b;return ok;},compact:async(_raw,context,_instructions,checkpoint)=>{const m=controls.managers.get(context.work().event.canonical_chat_jid)??controls.managers.get(rows.find(r=>r.chat===context.work().event.canonical_chat_jid)?.binding?.sessionId??'');if(!m||!controls.compact)throw new ControllerError('compaction_failed');return controls.compact(m,context,checkpoint);}});if(track)await setup?.after?.('host',host);};await build(true);
  const baseCleanup=f.cleanup.bind(f);
  const result=Object.assign(f,{controls,rows,mapped:async(chat=group)=>(await f!.repository.readChat(chat)).session,async reconstruct(){await host!.close();await build(false);},async cleanup(){await cleanupAll([
    {failure:'synthetic_host_cleanup_failed',run:async()=>{if(!host)return;await host.close();setup?.cleaned?.('host');}},
    {failure:'synthetic_journal_cleanup_failed',run:async()=>{if(!journal)return;setup?.cleaned?.('journal');}},
    {failure:'synthetic_index_cleanup_failed',run:async()=>{if(!index)return;setup?.cleaned?.('index');}},
    {failure:'synthetic_fixture_cleanup_failed',run:baseCleanup}
  ]);}}) as ProductionFixture;
  Object.defineProperties(result,{host:{get:()=>host!},index:{get:()=>index!},journal:{get:()=>journal!}});return result;
 }catch{await cleanupAll([
   {failure:'synthetic_host_cleanup_failed',run:async()=>{if(!host)return;await host.close();setup?.cleaned?.('host');}},
   {failure:'synthetic_journal_cleanup_failed',run:async()=>{if(!journal)return;setup?.cleaned?.('journal');}},
   {failure:'synthetic_index_cleanup_failed',run:async()=>{if(!index)return;setup?.cleaned?.('index');}},
   {failure:'synthetic_fixture_cleanup_failed',run:()=>f.cleanup()}
  ]);throw new Error('setup_failed');}
}
async function claim(f:BaseFixture,value:NotificationV2,owner='a'.repeat(32)){expect((await f.repository.accept(value)).status).toBe('accepted');const app=await f.repository.acquireApp(owner,120000);if(!app)throw new Error('app_failed');const r=await f.repository.claimNext(value.canonical_chat_jid,app,'b'.repeat(32),120000,1000);if(r.status!=='claimed')throw new Error('claim_failed');return r.work;}
function turns(journal?:JournalAttachment):TurnPorts{return{attach:async()=>{},prepare:async()=>({usageRequiredAfterPrompt:false}),run:async(_s,_c,p)=>p(),confirmed:journal?c=>journal.confirmed(c):async()=>[]};}
async function run(f:ProductionFixture,w:ClaimedWork,custom:Partial<TurnPorts>={},diagnostics:DiagnosticPort={emit:()=>{}}){await runChatWorker(f.repository,w,{leaseMs:120000,renewMs:10000,appCurrent:()=>true,instructions:()=> 'SYNTHETIC',host:f.host,turns:{...turns(f.journal),...custom},wait:waitForAbort,diagnostics,clock:{now:()=>fixedNow}});}
function signed(body:string,t=Math.floor(fixedNow/1000)){return{'content-type':'application/json','x-request-id':(JSON.parse(body) as NotificationV2).event_id,'x-webhook-timestamp':String(t),'x-webhook-signature-v2':createHmac('sha256',secret).update(`${t}.`).update(body).digest('hex')};}
function nativeCall(id='SYNTHETIC_CALL',text='SYNTHETIC_ARGUMENT'):CustomToolCallEvent{return{type:'tool_call',toolName:toolInfo.name,toolCallId:id,input:{to:other,text},parentToolCallId:'SYNTHETIC_PARENT'};}
function nativeResult(call=nativeCall(),messageId='SYNTHETIC_SENT'):ToolResultEvent{const raw=JSON.stringify({success:true,message_id:messageId}),envelope={content:[{type:'text' as const,text:raw}],structuredContent:{result:raw}};return{type:'tool_result',toolName:call.toolName,toolCallId:call.toolCallId,parentToolCallId:call.parentToolCallId!,input:call.input,content:envelope.content,details:{server:ADAPTER_WHATSAPP_SERVER,tool:'send_message',mcpResult:envelope},isError:false,structuredContent:envelope};}
async function verifyCoordinatorDrain(diagnostics:DiagnosticPort):Promise<void>{
 for(const injected of[false,true]){
  const f=await productionFixture(),entered=deferred(),release=deferred(),trace:string[]=[];
  let coordinator:Coordinator|undefined,startTask:Promise<void>|undefined,draining:Promise<void>|undefined,primary='',done=false;
  try{
   expect((await f.repository.accept(event(1))).status).toBe('accepted');f.controls.prompt=async()=>{entered.release();await release.promise;};let ids=0;
   coordinator=createCoordinator(f.repository,{scheduler:f.cfg.scheduler,pollMs:1,ids:{next:()=>String(++ids).padStart(32,'0')},worker:{instructions:()=> 'SYNTHETIC',host:f.host,turns:turns(f.journal),wait:waitForAbort,diagnostics}});
   startTask=coordinator.start();await startTask;await enterGate(entered);if(injected)throw new Error('synthetic_held_gate_failure');
   coordinator.stopScheduling();draining=coordinator.drain().then(()=>{done=true;});await Promise.resolve();expect(done).toBe(false);release.release();await draining;
  }catch(error){if(!injected)throw error;primary=error instanceof Error?error.message:'non_error';}
  finally{
   coordinator?.stopScheduling();release.release();trace.push('released');
   await cleanupAll([
    {failure:'synthetic_start_cleanup_failed',run:async()=>{await startTask;}},
    {failure:'synthetic_drain_cleanup_failed',run:async()=>{if(coordinator&&!draining)draining=coordinator.drain().then(()=>{done=true;});await draining;trace.push('drained');coordinator=undefined;}},
    {failure:'synthetic_fixture_cleanup_failed',run:async()=>{if(coordinator)return;await f.cleanup();trace.push('fixture');}}
   ]);
  }
  expect(primary).toBe(injected?'synthetic_held_gate_failure':'');expect(done).toBe(true);expect(trace).toEqual(['released','drained','fixture']);expect(f.controls.effects.some(x=>x.startsWith('dispose:'))).toBe(true);expect(f.redis.child).toBeUndefined();expect(existsSync(f.root)).toBe(false);
 }
}
async function cleanupIdentityOwners(connections:Pick<RedisConnections,'close'>|undefined,collision:Pick<OwnedRedis,'cleanup'>,redis:Pick<OwnedRedis,'stop'|'cleanup'>,workers:Set<ChildProcess>):Promise<void>{
 let connectionsSettled=!connections;
 await cleanupAll([
  {failure:'owned_child_exit_timeout',run:()=>cleanupAll([...workers].map(child=>({failure:'owned_child_exit_timeout',run:async()=>{await terminateOwned(child);workers.delete(child);}})))},
  {failure:'synthetic_connections_cleanup_failed',run:async()=>{await connections?.close();connectionsSettled=true;}},
  {failure:'synthetic_collision_cleanup_failed',run:()=>collision.cleanup()},
  {failure:'synthetic_redis_cleanup_failed',run:async()=>{await redis.stop();if(connectionsSettled&&workers.size===0)await redis.cleanup();}}
 ]);
}
async function verifyOwnedIdentityFailures():Promise<void>{
 const redis=new OwnedRedis(),collision=new OwnedRedis(),workers=new Set<ChildProcess>();let connections:RedisConnections|undefined,repositoryCalls=0;
 try{
  await redis.start();const client=redis.client();try{await client.connect();await verifyRedisIdentity(client,redis.identity);await client.set('SYNTHETIC_SENTINEL','owned');}finally{if(client.isOpen)client.destroy();}
  const pinned={...redis.identity};await expect(collision.start({port:redis.port}).then(()=>{repositoryCalls++;})).rejects.toThrow('synthetic_redis_start_failed');
  expect(repositoryCalls).toBe(0);expect(collision.child).toBeUndefined();expect(collision.directory).toBe('');redis.assertLive();
  connections=await connectRedis(config(redis.url,'/tmp/synthetic-unused').redis);
  for(const port of[connections.writer,connections.reader,connections.mapping]){
   await verifyRedisIdentity(port,pinned);await expect(verifyRedisIdentity(port,{...pinned,pid:pinned.pid+1})).rejects.toThrow('owned_redis_identity_mismatch');await expect(verifyRedisIdentity(port,{...pinned,runId:'0'.repeat(40)})).rejects.toThrow('owned_redis_identity_mismatch');
  }
  await expect(verifyConnections(redis,connections,{...pinned,runId:'0'.repeat(40)})).rejects.toThrow('owned_redis_identity_mismatch');
  const script=join(here,'..','crash','run-crash-matrix.mjs'),root=resolve(here,'..','..');
  for(const mode of['accept','expire'])for(const wrong of[{pid:pinned.pid+1,runId:pinned.runId},{pid:pinned.pid,runId:'0'.repeat(40)}]){
   let child:ChildProcess|undefined;
   try{child=trackOwned(spawn(process.execPath,[script,'--controller-root',root,'--redis-binary',binary!],{stdio:['ignore','pipe','pipe'],env:{PATH:process.env['PATH']??'/usr/bin:/bin',SYNTHETIC_CRASH_WORKER:mode,SYNTHETIC_REDIS_PORT:String(redis.port),SYNTHETIC_REDIS_PID:String(wrong.pid),SYNTHETIC_REDIS_RUN_ID:wrong.runId,SYNTHETIC_SESSION_ROOT:redis.directory,SYNTHETIC_TURN_LOG:join(redis.directory,'unused.log'),SYNTHETIC_SEQUENCE:'1'}}));workers.add(child);const output=collectOwnedOutput(child),result=await output.guard(ownedProbeDeadline(ownedClose(child)));expect(result.code).toBe(1);expect(output.stdout()).toBe('');expect(output.stderr()).toBe('<REDACTED>\n');}finally{await terminateOwned(child);if(child)workers.delete(child);}
  }
  const admin=redis.client();try{await admin.connect();await verifyRedisIdentity(admin,pinned);expect(await admin.get('SYNTHETIC_SENTINEL')).toBe('owned');expect(await admin.keys('sc:*')).toEqual([]);}finally{if(admin.isOpen)admin.destroy();}
  // Inject before connection settlement, then reject a later owner too. The
  // first fixed failure wins; Redis is stopped but storage must remain owned.
  const directory=redis.directory,cleanupCalls:string[]=[];
  await expect(cleanupIdentityOwners(
   {close:async()=>{cleanupCalls.push('connections');throw new Error('synthetic_connection_cleanup_injection');}},
   {cleanup:async()=>{cleanupCalls.push('collision');await collision.cleanup();throw new Error('synthetic_collision_cleanup_injection');}},
   {stop:async()=>{cleanupCalls.push('redis-stop');await redis.stop();},cleanup:async()=>{cleanupCalls.push('redis-remove');await redis.cleanup();}},workers
  )).rejects.toThrow('synthetic_connections_cleanup_failed');
  expect(cleanupCalls).toEqual(['connections','collision','redis-stop']);expect(redis.child).toBeUndefined();expect(redis.directory).toBe(directory);expect(existsSync(directory)).toBe(true);
 }finally{await cleanupIdentityOwners(connections,collision,redis,workers);}
 expect(workers.size).toBe(0);expect(collision.child).toBeUndefined();expect(collision.directory).toBe('');expect(redis.child).toBeUndefined();expect(redis.directory).toBe('');
 const privateRedis=new OwnedRedis();let forwarder:OwnedForwarder|undefined;
 try{
  await privateRedis.start({privateSocket:true});forwarder=new OwnedForwarder(privateRedis,{...privateRedis.identity,runId:'0'.repeat(40)});await forwarder.start();
  const socket=createConnection({host:'127.0.0.1',port:forwarder.port});socket.on('error',()=>{});const closed=new Promise<void>(r=>socket.once('close',()=>r()));let received=0;socket.on('data',(chunk:Buffer)=>{received+=chunk.length;});
  try{await once(socket,'connect');socket.write('*3\r\n$3\r\nSET\r\n$24\r\nSYNTHETIC_MUST_NOT_WRITE\r\n$7\r\nblocked\r\n');await Promise.race([closed,pause(2000).then(()=>{throw new Error('owned_forwarder_rejection_timeout');})]);expect(received).toBe(0);}finally{socket.destroy();await closed;}
  expect(forwarder.verified).toBe(0);expect(forwarder.rejected).toBe(1);
  const admin=privateRedis.client();try{await admin.connect();await verifyRedisIdentity(admin,privateRedis.identity);expect(await admin.dbSize()).toBe(0);}finally{if(admin.isOpen)admin.destroy();}
 }finally{await forwarder?.stop();if(!forwarder||forwarder.settled)await privateRedis.cleanup();}
}
function checkpoint(chat:CanonicalChatId):CheckpointV1{return{version:1,canonicalChatId:chat,lastCompletedSequence:0,policyVersion:'p1',templateVersion:'t1',createdAt:'2033-05-18T03:33:22Z',goals:[],decisions:[],constraints:[],verifiedFacts:[],pendingWork:[],approvalBoundaries:[],nextSteps:[],confirmedToolOutcomes:[]};}
async function verifyFenceEffects():Promise<void>{
 for(const effect of['renew','begin','confirm','uncertain','pause','checkpoint-prepare','checkpoint-transition','complete-delete'] as const)for(const owner of['current','stale-app','stale-chat'] as const){
  const f=await productionFixture();let session:Awaited<ReturnType<SessionHost['open']>>|undefined;
  try{
   const w=await claim(f,event(1));session=await f.host.open(w);const context:TurnContext={work:()=>w,current:async()=>{if(!await f.repository.checkWork(w))throw new ControllerError('lease_lost');}};
   await f.host.attach(session,context);expect(await f.repository.beginTurn(w)).toBe(true);
   const attempt:MutationAttempt={version:1,attemptId:'c'.repeat(32),eventId:w.event.event_id,canonicalChatId:group,toolCallId:'FENCE_CALL',tool:'send_message',destination:other,startedAt:'2033-05-18T03:33:22Z',ownership:w.ownership,status:'attempting'};
   if(effect==='confirm'||effect==='uncertain')expect(await f.repository.nativeJournal.begin(w,attempt)).toBe(true);
   const prepared={checkpointId:'d'.repeat(32),phase:'prepared' as const,checkpoint:checkpoint(group)};
   if(effect==='checkpoint-transition')expect(await f.repository.checkpoints.prepare(w,prepared)).toBe(true);
   let proof:CompletionProof|undefined;
   if(effect==='complete-delete'){await session.prompt(w.event,'SYNTHETIC');proof=await session.appendCompletion({version:1,eventId:w.event.event_id,eventFingerprint:fingerprintEvent(w.event),canonicalChatId:group,chatSequence:1,completion:'turn',completedAt:'2033-05-18T03:33:22Z',confirmedSends:[]});expect(await f.index.findEvent(group,w.event.event_id)).toEqual(proof);}
   const target:ClaimedWork=owner==='current'?w:{...w,ownership:{...w.ownership,[owner==='stale-app'?'app':'chat']:{...w.ownership[owner==='stale-app'?'app':'chat'],epoch:'999'}}};
   const snapshot=async()=>{const k=keys(group);return{state:await f.admin.hGetAll(k.state),lease:await f.admin.hGetAll(k.lease),app:await f.admin.hGetAll(APP),events:await f.admin.hGetAll(k.events),sequences:await f.admin.hGetAll(k.sequences),tools:await f.admin.hGetAll(k.tools),checkpoint:await f.admin.get(k.checkpoint),stream:await f.admin.xRange(k.stream,'-','+'),pel:await f.admin.sendCommand(['XPENDING',k.stream,'controller-v1']),file:readFileSync(session!.binding.sessionFile,'utf8')};};
   const before=await snapshot();let result:boolean;
   switch(effect){
    case'renew':result=(await f.repository.renew(target,120000))!==undefined;break;
    case'begin':result=await f.repository.nativeJournal.begin(target,{...attempt,ownership:target.ownership});break;
    case'confirm':result=await f.repository.nativeJournal.confirm(target,attempt.attemptId,'SYNTHETIC_CONFIRMED');break;
    case'uncertain':result=await f.repository.nativeJournal.uncertain(target,attempt.attemptId);break;
    case'pause':result=await f.repository.pause(target,'mutation_uncertain');break;
    case'checkpoint-prepare':result=await f.repository.checkpoints.prepare(target,prepared);break;
    case'checkpoint-transition':result=await f.repository.checkpoints.transition(target,prepared.checkpointId,'compacted','SYNTHETIC_COMPACTION');break;
    case'complete-delete':result=await f.repository.complete(target,proof!);break;
   }
   expect(result,`${effect}:${owner}`).toBe(owner==='current');
   if(owner!=='current')expect(await snapshot(),`${effect}:${owner}:unchanged`).toEqual(before);
   else{
    if(effect==='begin')expect((await f.repository.nativeJournal.list(group))[0]?.status).toBe('attempting');
    if(effect==='confirm')expect((await f.repository.nativeJournal.list(group))[0]?.status).toBe('confirmed');
    if(effect==='uncertain'||effect==='pause')expect((await f.repository.readChat(group)).pause).toBe('mutation_uncertain');
    if(effect==='checkpoint-prepare')expect((await f.repository.checkpoints.latest(group))?.phase).toBe('prepared');
    if(effect==='checkpoint-transition')expect((await f.repository.checkpoints.latest(group))?.phase).toBe('compacted');
    if(effect==='complete-delete'){expect((await f.repository.readChat(group)).lastCompletedSequence).toBe(1);expect(await f.admin.xLen(keys(group).stream)).toBe(0);expect(await f.admin.hLen(keys(group).events)).toBe(0);expect(await f.admin.hLen(keys(group).sequences)).toBe(0);expect((await f.admin.sendCommand(['XPENDING',keys(group).stream,'controller-v1']) as unknown[])[0]).toBe(0);}
   }
  }finally{await cleanupAll([{failure:'synthetic_session_cleanup_failed',run:async()=>{await session?.dispose();}},{failure:'synthetic_fixture_cleanup_failed',run:()=>f.cleanup()}]);}
 }
}
async function verifyUnresolvedRecovery():Promise<void>{
 const f=await productionFixture();const entered=deferred(),release=deferred();let running:Promise<void>|undefined,calls=0;
 try{
  const w=await claim(f,event(1));let mapped:SessionBinding|undefined;
  f.controls.prompt=async id=>{
   expect(await f.controls.handlers['tool_call']!(nativeCall('RECOVERY_CALL'),f.controls.contexts.get(id)!)).toBeUndefined();mapped=await f.mapped();
   const rows=readFileSync(mapped!.sessionFile,'utf8').trim().split('\n').map(row=>JSON.parse(row) as {customType?:string;data?:{state?:{status?:string}}});
   expect(rows.some(row=>row.customType===MUTATION_TYPE&&row.data?.state?.status==='attempting')).toBe(true);expect((await f.repository.nativeJournal.list(group))[0]?.status).toBe('attempting');
   calls++;entered.release();await release.promise;
  };
  running=run(f,w);await enterGate(entered,[running]);expect(calls).toBe(1);
  // Only our pinned fixture admin may expire leases, after callback entry and
  // before prompt settles. Worker/journal fencing leaves attempting unpaused.
  await verifyRedisIdentity(f.admin,f.redis.identity);await f.admin.hSet(APP,'until','1');await f.admin.hSet(keys(group).lease,'until','1');release.release();await running;
  const before=await f.repository.readChat(group);expect(before.pause).toBeUndefined();expect(before.lastCompletedSequence).toBe(0);expect(await f.index.findEvent(group,w.event.event_id)).toBeUndefined();expect(await f.admin.xLen(keys(group).stream)).toBe(1);expect(await f.admin.hExists(keys(group).events,w.event.event_id)).toBe(1);expect((await f.repository.nativeJournal.list(group))[0]?.status).toBe('attempting');expect((await f.admin.sendCommand(['XPENDING',keys(group).stream,'controller-v1']) as unknown[])[0]).toBe(1);
  await f.reconstruct();const newApp=await f.repository.acquireApp('e'.repeat(32),120000);expect(newApp).toBeDefined();expect(newApp!.epoch).not.toBe(w.ownership.app.epoch);expect(await f.repository.checkWork(w)).toBe(false);
  const recovered:Awaited<ReturnType<RedisRepository['recoverPending']>>[]=[];
  const recovery=createRecovery({recoverPending:async(...args)=>{const result=await f.repository.recoverPending(...args);recovered.push(result);return result;}});
  expect(await recovery(group,newApp!,'f'.repeat(32))).toBe(true);expect(recovered).toEqual([{status:'ok',next:'0-0',claimed:1,deleted:0}]);
  const prompts=f.controls.effects.filter(x=>x.startsWith('prompt:')).length,opens=f.controls.effects.filter(x=>x.startsWith('open:')).length;
  expect(await f.repository.claimNext(group,newApp!,'f'.repeat(32),120000,1000)).toEqual({status:'paused'});
  expect((await f.repository.nativeJournal.list(group))[0]?.status).toBe('uncertain');expect((await f.repository.readChat(group)).pause).toBe('mutation_uncertain');expect((await f.repository.readChat(group)).lastCompletedSequence).toBe(0);expect(await f.mapped()).toEqual(mapped);expect(await f.admin.xLen(keys(group).stream)).toBe(1);expect(await f.admin.hExists(keys(group).events,w.event.event_id)).toBe(1);expect(await f.index.findEvent(group,w.event.event_id)).toBeUndefined();expect(calls).toBe(1);expect(f.controls.effects.filter(x=>x.startsWith('prompt:'))).toHaveLength(prompts);expect(f.controls.effects.filter(x=>x.startsWith('open:'))).toHaveLength(opens);
 }finally{release.release();await cleanupAll([{failure:'synthetic_worker_cleanup_failed',run:async()=>{await running;}},{failure:'synthetic_fixture_cleanup_failed',run:()=>f.cleanup()}]);}
}
let fixtureBody='';beforeAll(async()=>{await requirePhase10Redis726();fixtureBody=await readFile(join(here,'..','fixtures','v2-event.json'),'utf8');},12000);afterAll(async()=>{const probe=suiteRedisProbe;if(probe){await terminateOwned(probe);if(suiteRedisProbe===probe)suiteRedisProbe=undefined;}expect(fixtureBody).not.toContain('signature');});

describe('assembled synthetic controller acceptance',()=>{
it('C1 authenticates exact bytes and replies only after real Lua plus WAITAOF; canonical aliases share one stream',async()=>{
 for(const injected of[false,true]){
  const f=await repositoryFixture(),entered=deferred(),release=deferred(),trace:string[]=[],tasks:Promise<unknown>[]=[];
  let server:WebhookServer|undefined,primary='',cleanupError='',settled=false,requestsFulfilled=false;
  try{
   const real=f.connections.writer.sendCommand.bind(f.connections.writer);f.connections.writer.sendCommand=async args=>{if(args[0]==='WAITAOF'){entered.release();await release.promise;}return real(args);};
   server=createWebhookServer(f.cfg,f.repository,f.connections,{emit:()=>{}},{now:()=>fixedNow});const a=await server.listen(0,'127.0.0.1');
   const response=fetch(`http://127.0.0.1:${a.port}/webhooks/whatsapp`,{method:'POST',headers:signed(fixtureBody),body:fixtureBody}).then(x=>{settled=true;return x;});tasks.push(response);
   await enterGate(entered,tasks);if(injected)throw new Error('synthetic_held_gate_failure');
   expect(settled).toBe(false);expect(await f.admin.xLen(keys(group).stream)).toBe(1);release.release();expect((await response).status).toBe(202);
   expect((await fetch(`http://127.0.0.1:${a.port}/webhooks/whatsapp`,{method:'POST',headers:{...signed(fixtureBody),'x-webhook-signature-v2':'0'.repeat(64)},body:fixtureBody})).status).toBe(401);
   const alias={...event(1,operator,'2'.repeat(32)),chat_jid:'400@s.whatsapp.net'} as NotificationV2;expect((await f.repository.accept(alias)).status).toBe('accepted');expect(await f.admin.xLen(keys(operator).stream)).toBe(1);
  }catch(error){if(!injected)throw error;primary=error instanceof Error?error.message:'non_error';}
  finally{
   server?.stopIntake();release.release();trace.push('released');
   const results=await Promise.allSettled(tasks);trace.push('requests-settled');requestsFulfilled=results.every(r=>r.status==='fulfilled');
   try{await cleanupAll([
    {failure:'synthetic_server_cleanup_failed',run:async()=>{trace.push('server');await server?.close();if(injected)throw new Error('synthetic_cleanup_injection');}},
    {failure:'synthetic_fixture_cleanup_failed',run:async()=>{trace.push('fixture');await f.cleanup();}}
   ]);}catch(error){cleanupError=error instanceof Error?error.message:'non_error';}
  }
  expect(requestsFulfilled).toBe(true);expect(settled).toBe(true);expect(primary).toBe(injected?'synthetic_held_gate_failure':'');expect(cleanupError).toBe(injected?'synthetic_server_cleanup_failed':'');expect(trace).toEqual(['released','requests-settled','server','fixture']);expect(f.redis.child).toBeUndefined();expect(existsSync(f.root)).toBe(false);
 }
});

it('C2 enforces 42-after-41 ordering, durable local gaps, serialization and cross-chat concurrency through production host',async()=>{const f=await productionFixture();try{await f.admin.hSet(keys(group).state,'cursor','40');expect((await f.repository.accept({...event(42),event_id:eventId('4'.repeat(32))})).status).toBe('accepted');const app=await f.repository.acquireApp('a'.repeat(32),120000);if(!app)throw Error();expect((await f.repository.claimNext(group,app,'b'.repeat(32),120000,1000)).status).toBe('waiting');expect((await f.repository.accept({...event(41),event_id:eventId('3'.repeat(32))})).status).toBe('accepted');for(const n of [41,42]){const c=await f.repository.claimNext(group,app,'b'.repeat(32),120000,1000);if(c.status!=='claimed')throw Error(String(n));await run(f,c.work);}expect((await f.repository.accept(event(2,other,'7'.repeat(32)))).status).toBe('accepted');expect((await f.repository.claimNext(other,app,'c'.repeat(32),120000,1000)).status).toBe('waiting');await f.admin.hSet(keys(other).state,'gap','1');expect((await f.repository.claimNext(other,app,'c'.repeat(32),120000,1)).status).toBe('paused');expect((await f.repository.readChat(other)).pause).toBe('sequence_gap');}finally{await f.cleanup();}
 for(const injected of[false,true]){
  const c=await productionFixture(),gate=deferred(),both=deferred(),tasks:Promise<void>[]=[],trace:string[]=[];let primary='',active=0,max=0,workersFulfilled=false;
  try{
   const first=await claim(c,event(1)),app=first.ownership.app;expect((await c.repository.accept(event(2))).status).toBe('accepted');expect((await c.repository.accept(event(1,other,'6'.repeat(32)))).status).toBe('accepted');const oc=await c.repository.claimNext(other,app,'d'.repeat(32),120000,1000);if(oc.status!=='claimed')throw new Error('claim_failed');
   c.controls.prompt=async()=>{active++;max=Math.max(max,active);if(active===2)both.release();await gate.promise;active--;};tasks.push(run(c,first),run(c,oc.work));
   await enterGate(both,tasks);if(injected)throw new Error('synthetic_held_gate_failure');
   expect(max).toBe(2);expect((await c.repository.claimNext(group,app,'e'.repeat(32),120000,1000)).status).not.toBe('claimed');gate.release();await Promise.all(tasks);
   const next=await c.repository.claimNext(group,app,'f'.repeat(32),120000,1000);expect(next.status).toBe('claimed');if(next.status==='claimed'){const running=run(c,next.work);tasks.push(running);await running;}expect(c.controls.effects.filter(x=>x.startsWith('prompt:'))).toHaveLength(3);
  }catch(error){if(!injected)throw error;primary=error instanceof Error?error.message:'non_error';}
  finally{gate.release();trace.push('released');const results=await Promise.allSettled(tasks);trace.push('workers-settled');workersFulfilled=results.every(r=>r.status==='fulfilled');await c.cleanup();trace.push('fixture');}
  expect(primary).toBe(injected?'synthetic_held_gate_failure':'');expect(workersFulfilled).toBe(true);expect(active).toBe(0);expect(trace).toEqual(['released','workers-settled','fixture']);expect(c.redis.child).toBeUndefined();expect(existsSync(c.root)).toBe(false);
 }
});

it('C3 observes production file/parent fsync and readback before real cleanup, then reconstructs and exactly reopens',async()=>{const f=await productionFixture();let spy:ReturnType<typeof vi.spyOn>|undefined;try{const w=await claim(f,event(1));const h=await open(f.root,'r'),proto=Object.getPrototypeOf(h) as {sync():Promise<void>},original=proto.sync;await h.close();const order:string[]=[];spy=vi.spyOn(proto,'sync').mockImplementation(async function(this:any){order.push((await this.stat()).isFile()?'file':'parent');await original.call(this);});const real=f.repository.complete.bind(f.repository);let cleanupAt:string[]=[];f.repository.complete=async(...args)=>{cleanupAt=[...order];return real(...args);};await run(f,w);const mapped=await f.mapped();expect(mapped).toBeDefined();expect(order).toContain('file');expect(order).toContain('parent');expect(cleanupAt.at(-2)).toBe('file');expect(cleanupAt.at(-1)).toBe('parent');expect(readFileSync(mapped!.sessionFile,'utf8')).toContain(w.event.event_id);spy.mockRestore();spy=undefined;await f.reconstruct();expect((await f.index.findEvent(group,w.event.event_id))?.session).toEqual(mapped);expect((await f.repository.accept(event(1))).status).toBe('duplicate');expect((await f.repository.accept({...event(1),sender_alt_jid:'500@lid'})).status).toBe('conflict');expect(await f.repository.releaseApp(w.ownership.app)).toBe(true);const two=await claim(f,event(2),'c'.repeat(32));await run(f,two);expect(f.controls.effects.filter(x=>x===`open:${mapped!.sessionFile}`).length).toBeGreaterThanOrEqual(2);expect((await f.mapped())?.sessionFile).toBe(mapped!.sessionFile);}finally{spy?.mockRestore();await f.cleanup();}});

it('C4 verifies exact Redis 7.2.6 then restarts production crash paths',async()=>{const script=join(here,'..','crash','run-crash-matrix.mjs'),root=resolve(here,'..','..');let child:ChildProcess|undefined;try{child=trackOwned(spawn(process.execPath,[script,'--controller-root',root,'--redis-binary',binary!],{stdio:['ignore','pipe','pipe'],env:{PATH:process.env['PATH']??'/usr/bin:/bin'}}));const output=collectOwnedOutput(child),{code}=await output.guard(ownedClose(child));expect({code,stderr:output.stderr()}).toEqual({code:0,stderr:''});expect(JSON.parse(output.stdout().trim())).toEqual({status:'ok',redisVersion:'7.2.6',queuedRestart:true,pelRestart:true,redisAofRestart:true,logicalTurns:3});child=undefined;}finally{await terminateOwned(child);}},30000);

it('C5 uses host-bound journal fsync, fences stale effects, and restart recovery never resends uncertain work',async()=>{const f=await productionFixture();try{const w=await claim(f,event(1));let calls=0;f.controls.prompt=async id=>{const call=nativeCall();const context=f.controls.contexts.get(id)!;expect(await f.controls.handlers['tool_call']!(call,context)).toBeUndefined();const binding=await f.mapped();const rows=readFileSync(binding!.sessionFile,'utf8').trim().split('\n').map(row=>JSON.parse(row) as any);expect(rows.some(r=>r.customType===MUTATION_TYPE&&r.data.state.status==='attempting')).toBe(true);expect((await f.repository.nativeJournal.list(group))[0]?.status).toBe('attempting');calls++;await f.controls.handlers['tool_result']!(nativeResult(call),context);};await run(f,w);expect(calls).toBe(1);expect(await f.host.classifyOwn(other,other,'SYNTHETIC_SENT')).toBe('confirmed');calls=0;const binding=await f.mapped();expect(readFileSync(binding!.sessionFile,'utf8')).toContain('"status":"confirmed"');
 }finally{await f.cleanup();}await verifyUnresolvedRecovery();await verifyFenceEffects();},30000);

it('C6 covers usage/checkpoint matrix and malformed external compaction pauses only its production worker chat',async()=>{
 const f=await productionFixture();try{
  const usage=createUsagePolicy(f.cfg.compaction);expect([8100,8200,8300].map(tokens=>usage.decide({tokens,contextWindow:10000,percent:tokens/100},{event:0,tools:0,prompt:0}).status)).toEqual(['ok','compact','compact']);
  const cp={version:1 as const,canonicalChatId:group,lastCompletedSequence:0,policyVersion:'p1',templateVersion:'t1',createdAt:'2033-05-18T03:33:22Z',goals:['continue'],decisions:[],constraints:[],verifiedFacts:[],pendingWork:[],approvalBoundaries:[],nextSteps:[],confirmedToolOutcomes:[]},identity=checkpointIdentity(cp);expect(validateCheckpoint(cp,identity,65536)).toEqual(cp);
  for(const bad of[{...cp,extra:true},{...cp,version:2},{...cp,canonicalChatId:other},{...cp,lastCompletedSequence:9},{...cp,policyVersion:'stale'},{...cp,templateVersion:'stale'},{...cp,createdAt:'2033-05-18T03:33:23Z'},{...cp,goals:[1]},{...cp,goals:['x'.repeat(70000)]}])expect(()=>validateCheckpoint(bad,identity,65536)).toThrow('checkpoint_invalid');
  expect(containsSensitive({value:'authorization: Bearer SYNTHETIC_NOT_A_CREDENTIAL_1234'})).toBe(true);expect(()=>validateCheckpoint({...cp,goals:['authorization: Bearer SYNTHETIC_NOT_A_CREDENTIAL_1234']},identity,65536)).toThrow('checkpoint_invalid');
  let usageValue:UsageSnapshot|undefined={tokens:8200,contextWindow:10000,percent:82},exactCheckpointId='',originalCreatedAt='',exactSummary='';
  f.controls.usage=()=>usageValue;f.controls.prompt=async()=>{usageValue={tokens:1000,contextWindow:10000,percent:10};};
  f.controls.compact=async(m,context,checkpoint)=>{
   const id='d'.repeat(32),entry='COMPACT_ENTRY',entries=m.getEntries() as unknown as Record<string,unknown>[],parentId=entries.at(-1)?.['id'] as string|undefined;
   exactCheckpointId=id;originalCreatedAt=checkpoint.createdAt;exactSummary=renderCheckpointSummary(checkpoint,id,checkpointIdentity(checkpoint),f.cfg.compaction.checkpointMaxBytes);
   expect(await f.repository.checkpoints.prepare(context.work(),{checkpointId:id,phase:'prepared',checkpoint})).toBe(true);
   const row={type:'compaction',id:entry,parentId:parentId??null,timestamp:'2033-05-18T03:33:23Z',summary:exactSummary,firstKeptEntryId:parentId??entry,tokensBefore:8200,fromHook:true,details:{version:1,checkpointId:id,checkpoint}};entries.push(row);appendFileSync(m.getSessionFile()!,JSON.stringify(row)+'\n');usageValue={tokens:null,contextWindow:10000,percent:null};
   return{entryId:entry,checkpointId:id,checkpoint,compacted:()=>f.repository.checkpoints.transition(context.work(),id,'compacted',entry).then(ok=>{if(!ok)throw Error();}),restored:()=>f.repository.checkpoints.transition(context.work(),id,'restored',entry).then(ok=>{if(!ok)throw Error();})};
  };
  const comp=createCompactor({config:f.cfg,repository:f.repository,incoming:()=>({event:0,tools:0,prompt:0}),readTemplate:async()=> 'SYNTHETIC'});const badEvent=event(1,other,'9'.repeat(32));expect((await f.repository.accept(badEvent)).status).toBe('accepted');
  const good=await claim(f,event(1));await run(f,good,{prepare:comp.prepare,afterPrompt:comp.afterPrompt});expect((await f.repository.checkpoints.latest(group))?.phase).toBe('restored');
  const mapped=await f.mapped();expect(mapped).toBeDefined();await f.reconstruct();usageValue={tokens:1000,contextWindow:10000,percent:10};const mappedOpen=`open:${mapped!.sessionFile}`,mappedOpensBeforeResume=f.controls.effects.filter(effect=>effect===mappedOpen).length;f.controls.prompt=async()=>{expect(f.controls.effects.filter(effect=>effect===mappedOpen)).toHaveLength(mappedOpensBeforeResume+2);usageValue={tokens:1000,contextWindow:10000,percent:10};};
  expect((await f.repository.accept(event(2))).status).toBe('accepted');const resumed=await f.repository.claimNext(group,good.ownership.app,'8'.repeat(32),120000,1000);if(resumed.status!=='claimed')throw Error();await run(f,resumed.work,{prepare:comp.prepare,afterPrompt:comp.afterPrompt});expect(f.controls.effects.filter(effect=>effect===mappedOpen)).toHaveLength(mappedOpensBeforeResume+2);
  const manager=f.controls.managers.get(mapped!.sessionId);expect(manager).toBeDefined();const claims=(manager!.getBranch() as readonly Record<string,unknown>[]).filter(entry=>{const details=entry['details'];return entry['type']==='compaction'&&details!==null&&typeof details==='object'&&!Array.isArray(details)&&(details as Record<string,unknown>)['checkpointId']===exactCheckpointId;});expect(claims).toHaveLength(1);
  const persisted=claims[0]!,details=persisted['details'];expect(persisted['fromHook']).toBe(true);expect(details!==null&&typeof details==='object'&&!Array.isArray(details)).toBe(true);const envelope=details as Record<string,unknown>;expect(envelope['version']).toBe(1);expect(envelope['checkpointId']).toBe(exactCheckpointId);expect((envelope['checkpoint'] as Record<string,unknown>)['createdAt']).toBe(originalCreatedAt);expect(persisted['summary']).toBe(exactSummary);
  const bc=await f.repository.claimNext(other,good.ownership.app,'9'.repeat(32),120000,1000);if(bc.status!=='claimed')throw Error();usageValue={tokens:8200,contextWindow:10000,percent:82};let malformedPrepared=false;
  f.controls.compact=async(m,context,checkpoint)=>{const bad={...checkpoint,canonicalChatId:group},id='e'.repeat(32),entry='BAD',entries=m.getEntries() as unknown as Record<string,unknown>[],parentId=entries.at(-1)?.['id'] as string|undefined;malformedPrepared=await f.repository.checkpoints.prepare(context.work(),{checkpointId:id,phase:'prepared',checkpoint});if(!malformedPrepared)throw new Error('valid_prepare_failed');const row={type:'compaction',id:entry,parentId:parentId??null,timestamp:'2033-05-18T03:33:23Z',summary:renderCheckpointSummary(bad,id,checkpointIdentity(bad),f.cfg.compaction.checkpointMaxBytes),firstKeptEntryId:parentId??entry,tokensBefore:8200,fromHook:true,details:{version:1,checkpointId:id,checkpoint:bad}};entries.push(row);appendFileSync(m.getSessionFile()!,JSON.stringify(row)+'\n');return{entryId:entry,checkpointId:id,checkpoint:bad,compacted:()=>f.repository.checkpoints.transition(context.work(),id,'compacted',entry).then(()=>{}),restored:()=>f.repository.checkpoints.transition(context.work(),id,'restored',entry).then(()=>{})};};
  await run(f,bc.work,{prepare:comp.prepare,afterPrompt:comp.afterPrompt});expect(malformedPrepared).toBe(true);expect((await f.repository.checkpoints.latest(other))?.checkpoint.canonicalChatId).toBe(other);expect((await f.repository.readChat(other)).pause).toBe('compaction_failed');expect((await f.repository.readChat(other)).lastCompletedSequence).toBe(0);expect(await f.admin.xLen(keys(other).stream)).toBe(1);expect((await f.repository.readChat(group)).pause).toBeUndefined();expect((await f.repository.readChat(group)).lastCompletedSequence).toBe(2);
 }finally{await f.cleanup();}
});

it('C7 reaches production intake/store/tool failures, redacts, actively drains production host, and signal-stops bridge-gated main',async()=>{await verifyBridgeWaitContracts();const cleanupCalls:string[]=[];await expect(cleanupAll([
 {failure:'synthetic_first_cleanup_failed',run:async()=>{cleanupCalls.push('coordinator');throw new Error('SENSITIVE_COORDINATOR_CLEANUP');}},
 {failure:'synthetic_server_cleanup_failed',run:()=>{cleanupCalls.push('server');}},
 {failure:'synthetic_bridge_cleanup_failed',run:async()=>{cleanupCalls.push('bridge');throw new Error('SENSITIVE_BRIDGE_CLEANUP');}},
 {failure:'synthetic_redis_cleanup_failed',run:()=>{cleanupCalls.push('redis');}},
 {failure:'synthetic_host_cleanup_failed',run:async()=>{cleanupCalls.push('host');throw new Error('SENSITIVE_HOST_CLEANUP');}},
 {failure:'synthetic_connections_cleanup_failed',run:()=>{cleanupCalls.push('connections');}}
])).rejects.toThrow('synthetic_first_cleanup_failed');expect(cleanupCalls).toEqual(['coordinator','server','bridge','redis','host','connections']);const setupAcquired:FixtureResource[]=[],setupCleaned:FixtureResource[]=[];let setupRedis:OwnedRedis|undefined,setupRoot:string|undefined;const setupFailure=await productionFixture({after(resource,value){setupAcquired.push(resource);if(resource==='redis')setupRedis=value as OwnedRedis;if(resource==='directory')setupRoot=value as string;if(resource==='host')throw new Error('SENSITIVE_SETUP_REJECTION');},cleaned:resource=>setupCleaned.push(resource)}).then(()=> 'unexpected_success',(error:unknown)=>error instanceof Error?error.message:'non_error');expect(setupFailure).toBe('setup_failed');expect(setupAcquired).toEqual(['redis','directory','connections','repository','admin','index','journal','host']);expect(setupCleaned).toEqual(['host','journal','index','admin','repository','connections','redis','directory']);expect(setupRedis?.child).toBeUndefined();expect(setupRedis?.directory).toBe('');expect(setupRedis?.port).toBe(0);expect(setupRoot).toBeDefined();expect(existsSync(setupRoot!)).toBe(false);const failedCleanupCalls:FixtureResource[]=[];let failedCleanupRedis:OwnedRedis|undefined,failedCleanupRoot:string|undefined;const fixedCleanupFailure=await productionFixture({after(resource,value){if(resource==='redis')failedCleanupRedis=value as OwnedRedis;if(resource==='directory')failedCleanupRoot=value as string;if(resource==='host')throw new Error('SENSITIVE_SETUP_REJECTION');},cleaned(resource){failedCleanupCalls.push(resource);if(resource==='journal')throw new Error('SENSITIVE_SETUP_CLEANUP_REJECTION');}}).then(()=> 'unexpected_success',(error:unknown)=>error instanceof Error?error.message:'non_error');expect(fixedCleanupFailure).toBe('synthetic_journal_cleanup_failed');expect(failedCleanupCalls).toEqual(['host','journal','index','admin','repository','connections','redis','directory']);expect(failedCleanupRedis?.child).toBeUndefined();expect(failedCleanupRedis?.directory).toBe('');expect(failedCleanupRoot).toBeDefined();expect(existsSync(failedCleanupRoot!)).toBe(false);const f=await productionFixture();let server:WebhookServer|undefined,main:ChildProcess|undefined,mainForwarder:OwnedForwarder|undefined,mainRedis:OwnedRedis|undefined,mainBridge:OwnedBridgeStatus|undefined,mainAdmin:ReturnType<typeof createClient>|undefined;try{const lines:string[]=[];const diagnostics=createDiagnostics(v=>lines.push(v),{now:()=>fixedNow});const proxy=new Proxy(f.repository,{get(target,key,receiver){if(key==='accept')return async()=>{throw new Error('MALICIOUS_EXCEPTION_MARKER');};return Reflect.get(target,key,receiver);}});server=createWebhookServer(f.cfg,proxy,f.connections,diagnostics,{now:()=>fixedNow});let a=await server.listen(0,'127.0.0.1');const raw='{"MALICIOUS_RAW_MARKER":';expect((await fetch(`http://127.0.0.1:${a.port}/webhooks/whatsapp`,{method:'POST',headers:{'content-type':'application/json','x-request-id':'f'.repeat(32),'x-webhook-timestamp':String(Math.floor(fixedNow/1000)),'x-webhook-signature-v2':'0'.repeat(64)},body:raw})).status).toBe(401);const body=JSON.stringify(event(1));expect((await fetch(`http://127.0.0.1:${a.port}/webhooks/whatsapp`,{method:'POST',headers:signed(body),body})).status).toBe(503);await server.close();server=undefined;
 const malformed=await claim(f,event(1));f.controls.prompt=async id=>{const call=nativeCall('MALFORMED','MALICIOUS_TOOL_ARGUMENT');const ctx=f.controls.contexts.get(id)!;await f.controls.handlers['tool_call']!(call,ctx);const invalid=nativeResult(call);invalid.details={...(invalid.details as Record<string,unknown>),mcpResult:{content:invalid.content,structuredContent:{result:'MALICIOUS_RESULT_MARKER'}}};await f.controls.handlers['tool_result']!(invalid,ctx);};await run(f,malformed,{},diagnostics);expect((await f.repository.readChat(group)).pause).toBe('mutation_uncertain');expect(await f.index.findEvent(group,malformed.event.event_id)).toBeUndefined();expect(await f.repository.releaseApp(malformed.ownership.app)).toBe(true);const diagnosticClasses=lines.map(line=>(JSON.parse(line) as {class:string}).class);expect(diagnosticClasses).toEqual(expect.arrayContaining(['signature_invalid','store_unavailable','mutation_uncertain']));for(const marker of['MALICIOUS_RAW_MARKER','MALICIOUS_EXCEPTION_MARKER','MALICIOUS_TOOL_ARGUMENT','MALICIOUS_RESULT_MARKER',secret,f.root,f.redis.url,String(group)])expect(lines.join('')).not.toContain(marker);
 await verifyCoordinatorDrain(diagnostics);
 mainRedis=new OwnedRedis();await mainRedis.start({privateSocket:true});mainForwarder=new OwnedForwarder(mainRedis);await mainForwarder.start();mainAdmin=mainRedis.client();await mainAdmin.connect();await verifyRedisIdentity(mainAdmin,mainRedis.identity);mainRedis.assertLive();expect(await mainAdmin.xLen(keys(group).stream)).toBe(0);expect(await mainAdmin.xLen(keys(other).stream)).toBe(0);expect(await mainAdmin.exists(keys(group).state)).toBe(0);expect(await mainAdmin.exists(keys(other).state)).toBe(0);mainBridge=new OwnedBridgeStatus();await mainBridge.start();const mainPort=await ephemeralPort();main=trackOwned(spawn(process.execPath,[resolve(here,'..','..','dist','src','main.js')],{stdio:['ignore','pipe','pipe'],env:{PATH:process.env['PATH']??'/usr/bin:/bin',BRIDGE_NOTIFY_SECRET:secret,BRIDGE_NOTIFY_GROUP_ALLOWLIST:`${group},${other}`,BRIDGE_NOTIFY_OPERATOR_JIDS:`${operator},400@s.whatsapp.net`,SESSION_CONTROLLER_REDIS_URL:mainForwarder.url,SESSION_CONTROLLER_BRIDGE_BASE_URL:mainBridge.baseUrl,SESSION_CONTROLLER_BRIDGE_POLL_MS:'100',SESSION_CONTROLLER_BRIDGE_REQUEST_TIMEOUT_MS:'100',SESSION_CONTROLLER_PORT:String(mainPort),SESSION_CONTROLLER_PI_CWD:f.root,SESSION_CONTROLLER_PI_AGENT_DIR:join(f.root,'agent'),SESSION_CONTROLLER_SESSION_DIR:join(f.root,'main-sessions'),SESSION_CONTROLLER_SYSTEM_PROMPT_PATH:resolve(here,'..','..','config','system-prompt.md'),SESSION_CONTROLLER_PRE_COMPACTION_PATH:resolve(here,'..','..','config','pre-compaction.md'),SESSION_CONTROLLER_COMPACTION_SUMMARY_PATH:resolve(here,'..','..','config','compaction-summary.md'),SESSION_CONTROLLER_MCP_COMMAND:'/bin/false',SESSION_CONTROLLER_MCP_ARGS_JSON:'[]',SESSION_CONTROLLER_MCP_CWD:f.root,SESSION_CONTROLLER_SAFE_WORKING_WINDOW_TOKENS:'10000'}}));const mainOutput=collectOwnedOutput(main);for(let n=0;n<500&&mainBridge.requests===0;n++)await mainOutput.guard(pause(10));expect(mainBridge.requests).toBeGreaterThan(0);expect(mainOutput.stdout()).not.toContain('"class":"ready"');expect(await redisExists(mainRedis,APP)).toBe(0);mainBridge.connected=true;for(let n=0;n<500&&!mainOutput.stdout().includes('"class":"ready"');n++)await mainOutput.guard(pause(10));expect(mainOutput.stdout()).toContain('"class":"ready"');expect(await redisExists(mainRedis,APP)).toBe(1);const stopped=await mainOutput.guard(terminateOwned(main,'SIGTERM'));expect(stopped).toEqual({code:0,signal:null,escalated:false});main=undefined;expect(mainOutput.stderr()).toBe('');expect(mainOutput.stdout()).toContain('"class":"stopped"');expect(mainOutput.stdout()).not.toContain(secret);expect(mainForwarder.verified).toBe(3);expect(mainForwarder.rejected).toBe(0);}finally{await cleanupAll([
 {failure:'synthetic_server_cleanup_failed',run:async()=>{const current=server;if(!current)return;await current.close();if(server===current)server=undefined;}},
 {failure:'synthetic_main_cleanup_failed',run:async()=>{const current=main;if(!current)return;await terminateOwned(current);if(main===current)main=undefined;}},
 {failure:'synthetic_forwarder_cleanup_failed',run:async()=>{await mainForwarder?.stop();}},
 {failure:'synthetic_bridge_cleanup_failed',run:async()=>{const current=mainBridge;if(!current)return;await current.stop();if(mainBridge===current)mainBridge=undefined;}},
 {failure:'synthetic_admin_cleanup_failed',run:async()=>{const current=mainAdmin;if(!current)return;if(current.isOpen)current.destroy();if(mainAdmin===current)mainAdmin=undefined;}},
 {failure:'synthetic_redis_cleanup_failed',run:async()=>{const current=mainRedis;if(!current||main||mainForwarder&&!mainForwarder.settled)return;await current.cleanup();if(mainRedis===current)mainRedis=undefined;}},
 {failure:'synthetic_fixture_cleanup_failed',run:async()=>{if(main||mainForwarder&&!mainForwarder.settled)return;await f.cleanup();}}
]);}},30000);

it('C8 keeps fixtures unsigned, Compose isolated, binary validation redacted, orchestration owned, and child waits bounded',async()=>{
 const contracts:readonly{file:'acceptance'|'crash';needle:string;replacement:string}[]=[
  {file:'acceptance',needle:"process.env['PHASE10_OWNED_REDIS_BINARY']",replacement:'undefined'},
  {file:'acceptance',needle:"spawn(binary!,['--version']",replacement:"spawn(binary!,['--invalid']"},
  {file:'acceptance',needle:'collectOwnedOutput(probe,4096)',replacement:'collectOwnedOutput(probe,4)'},
  {file:'acceptance',needle:"version!=='7.2.6'",replacement:"version!=='7.2.7'"},
  {file:'acceptance',needle:'await requirePhase10Redis726();fixtureBody=await readFile',replacement:'fixtureBody=await readFile'},
  {file:'acceptance',needle:'},12000);afterAll',replacement:'},1);afterAll'},
  {file:'acceptance',needle:'if(probe){await terminateOwned(probe);if(suiteRedisProbe===probe)suiteRedisProbe=undefined;}',replacement:'if(probe){suiteRedisProbe=undefined;}'},
  {file:'acceptance',needle:"if(probe&&suiteRedisProbe===probe){try{await terminateOwned(probe);suiteRedisProbe=undefined;}catch{throw new Error('owned_child_exit_timeout');}}",replacement:'suiteRedisProbe=undefined;'},
  {file:'acceptance',needle:'const closed=ownedClose(child);',replacement:'return{code:null,signal:null,escalated:false};'},
  {file:'acceptance',needle:'bytes+added>limit',replacement:'bytes+added>999999'},
  {file:'acceptance',needle:"const added=Buffer.byteLength(chunk,'utf8');",replacement:'const added=chunk.length;'},
  {file:'acceptance',needle:'first??=attempt.failure',replacement:'first=attempt.failure'},
  {file:'acceptance',needle:'await verifyConnections(redis,connections);repository=await createRepository',replacement:'repository=await createRepository'},
  {file:'acceptance',needle:'await terminateOwned(child);settled=true;',replacement:'settled=true;'},
  {file:'crash',needle:'if (!rawBinary || !isAbsolute(rawBinary))',replacement:'if (!rawBinary)'},
  {file:'crash',needle:"else { const requireAtRoot = createRequire(join(root, 'package.json')); ({ createClient } = requireAtRoot('redis'));",replacement:'else {'},
  {file:'crash',needle:'const version = await redisVersion(versionProbe);',replacement:'const version = "7.2.6";'},
  {file:'crash',needle:"version !== '7.2.6'",replacement:"version !== '7.2.7'"},
  {file:'crash',needle:'const captureLimit = 4096;',replacement:'const captureLimit = 999999;'},
  {file:'crash',needle:'capturedBytes + added > captureLimit',replacement:'added > captureLimit'},
  {file:'crash',needle:'const barrierCaptureLimit = 4096;',replacement:'const barrierCaptureLimit = 999999;'},
  {file:'crash',needle:"Buffer.byteLength(buffered, 'utf8') + Buffer.byteLength(chunk, 'utf8')",replacement:'buffered.length + chunk.length'},
  {file:'crash',needle:'ownership.child = child; ownership.closed = closed; ownership.settled = false;',replacement:'ownership.settled = true;'},
  {file:'crash',needle:'if (controllerSettled && redisSettled)',replacement:'if (true)'},
  {file:'crash',needle:'const closed = childClose(child).then(() => true, () => false);',replacement:'return;'},
  {file:'crash',needle:'await verifyConnections(connections, expected);',replacement:'void expected;'},
  {file:'crash',needle:'await verifyRedisIdentity(admin, expected);',replacement:'void expected;'},
  {file:'acceptance',needle:'function collectOwnedOutput(child:ChildProcess,limit=65536)',replacement:'function collectOwnedOutput(child:ChildProcess,limit=999999)'},
  {file:'acceptance',needle:"terminationError instanceof Error&&terminationError.message==='owned_child_exit_timeout'?terminationError:new Error('owned_child_exit_timeout')",replacement:"new Error('owned_child_output_overflow')"},
  {file:'acceptance',needle:"const result=await output.guard(Promise.race([closed,pause(2000).then(()=>undefined)]));",replacement:'const result=await closed;'},
  {file:'crash',needle:"const added = Buffer.byteLength(value, 'utf8');",replacement:'const added = value.length;'},
  {file:'crash',needle:"terminationError instanceof Error && terminationError.message === 'owned_child_exit_timeout' ? terminationError : new Error('owned_child_exit_timeout')",replacement:"new Error('barrier_output_overflow')"},
  {file:'crash',needle:"    const ownedDirectory = directory;\n    if (controllerSettled && redisSettled) {\n      await attempt(async () => { if (!ownedDirectory) return; await rm(ownedDirectory, { recursive: true, force: true });",replacement:"    const ownedDirectory = directory;\n    await rm(ownedDirectory, { recursive: true, force: true });\n    if (controllerSettled && redisSettled) {\n      await attempt(async () => { if (!ownedDirectory) return;"},
  {file:'crash',needle:"await closed;\n    if (ownership.child === child) ownership.child = undefined;\n    throw new Error('owned_child_spawn_failed');",replacement:"ownership.child = undefined; throw new Error('owned_child_spawn_failed');"},
  {file:'crash',needle:'const versionProbe = { child: undefined };',replacement:'const versionProbe = {};'},
  {file:'crash',needle:'const closed = childClose(child).then(result => { ownership.settled = true; return result; }); ownership.closed = closed;',replacement:'ownership.settled = true;'},
  {file:'crash',needle:'cleanupFailure ??= new Error(fixed)',replacement:'cleanupFailure = new Error(fixed)'},
  {file:'crash',needle:'const previousIdentity = identity;',replacement:'const previousIdentity = { runId: "" };'}
 ];
 function validateContracts(implementation:string,runner:string):void{for(const c of contracts){const source=c.file==='acceptance'?implementation:runner;if(source.split(c.needle).length!==2)throw new Error('source_contract_invalid');}}
const compose=await readFile(join(here,'..','..','docker-compose.test.yml'),'utf8'),crash=await readFile(join(here,'..','crash','run-crash-matrix.mjs'),'utf8'),acceptance=await readFile(fileURLToPath(import.meta.url),'utf8'),value=JSON.parse(fixtureBody) as Record<string,unknown>;
 const implementation=acceptance.slice(0,acceptance.indexOf("describe('assembled synthetic controller acceptance'"));
 validateContracts(implementation,crash);
 for(const contract of contracts){const original=contract.file==='acceptance'?implementation:crash;expect(original.split(contract.needle)).toHaveLength(2);const mutated=original.replace(contract.needle,contract.replacement);expect(()=>validateContracts(contract.file==='acceptance'?mutated:implementation,contract.file==='crash'?mutated:crash)).toThrow('source_contract_invalid');}
 expect(Object.keys(value)).not.toContain('signature');expect(fixtureBody).not.toContain(secret);expect(compose).toContain('redis:7.2.6-bookworm');expect(compose).toContain('appendfsync');expect(compose).toContain('noeviction');expect(compose).not.toMatch(/\bports:/);expect(compose).not.toMatch(/^volumes:/m);expect(compose).not.toMatch(/^networks:/m);
 await verifyOwnedIdentityFailures();
 const script=join(here,'..','crash','run-crash-matrix.mjs'),root=resolve(here,'..','..');
 const failed=async(redisBinary:string,extra:NodeJS.ProcessEnv={},controllerRoot=root)=>{let child:ChildProcess|undefined;try{child=trackOwned(spawn(process.execPath,[script,'--controller-root',controllerRoot,'--redis-binary',redisBinary],{stdio:['ignore','pipe','pipe'],env:{PATH:process.env['PATH']??'/usr/bin:/bin',...extra}}));const output=collectOwnedOutput(child),closed=ownedClose(child);const result=await output.guard(Promise.race([closed.then(({code})=>({code,timeout:false})),pause(5000).then(()=>({code:null,timeout:true}))]));expect({...result,stdout:output.stdout(),stderr:output.stderr()}).toEqual({code:1,timeout:false,stdout:'',stderr:'<REDACTED>\n'});child=undefined;}finally{await terminateOwned(child);}};
 await failed('relative-redis');await failed('/tmp/synthetic-missing-redis-binary');
 const fakeRoot=await mkdtemp('/tmp/whatsapp-slice10-failures-');
 const fakeExecutable=async(name:string,source:string)=>{const file=join(fakeRoot,`${name}.mjs`);await writeFile(file,`#!${process.execPath}\n${source}\n`,{mode:0o700});return file;};
 try{
  await failed(binary!,{},join(fakeRoot,'missing-controller-root'));
  const versionTimeout=await fakeExecutable('version-timeout',"if(process.argv[2]==='--version')setInterval(()=>{},1000);");
  const versionSplitOverflow=await fakeExecutable('version-split-overflow',"if(process.argv[2]==='--version'){process.stdout.write('X'.repeat(2500));process.stderr.write('Y'.repeat(2500));}");
  const postVersionSpawnFailure=await fakeExecutable('post-version-spawn-failure',["import { unlinkSync } from 'node:fs';","if(process.argv[2]==='--version'){process.stdout.write('Redis server v=7.2.6\\n');unlinkSync(process.argv[1]);}"].join('\n'));
  await failed(versionTimeout);await failed(versionSplitOverflow);await failed(postVersionSpawnFailure);
 }finally{await rm(fakeRoot,{recursive:true,force:true});}
 let overflowChild:ChildProcess|undefined;try{overflowChild=trackOwned(spawn(process.execPath,['-e',"process.stdout.write('é'.repeat(20000));process.stderr.write('é'.repeat(20000));setInterval(()=>{},1000);"],{stdio:['ignore','pipe','pipe']}));const output=collectOwnedOutput(overflowChild);await expect(output.guard(ownedClose(overflowChild))).rejects.toThrow('owned_child_output_overflow');expect(overflowChild.exitCode!==null||overflowChild.signalCode!==null).toBe(true);await ownedClose(overflowChild);overflowChild=undefined;}finally{await terminateOwned(overflowChild);}
 let delayedRunner:ChildProcess|undefined;try{delayedRunner=trackOwned(spawn(process.execPath,[script,'--controller-root',root,'--redis-binary',binary!],{stdio:['ignore','pipe','pipe'],env:{PATH:process.env['PATH']??'/usr/bin:/bin',SYNTHETIC_CRASH_TEST_DELAYED_CLOSE:'1'}}));const output=collectOwnedOutput(delayedRunner),result=await output.guard(ownedClose(delayedRunner));expect(result.code).toBe(0);expect(output.stdout()).toBe('{"status":"close-settled"}\n');expect(output.stderr()).toBe('');delayedRunner=undefined;}finally{await terminateOwned(delayedRunner);}
 const holder="process.stdin.resume();process.stdin.once('end',()=>process.exit(0));process.send('ready');process.disconnect();";
 const leader=`const {spawn}=require('node:child_process');const held=spawn(process.execPath,['-e',${JSON.stringify(holder)}],{stdio:[3,1,2,'ipc']});held.once('error',()=>process.exit(1));held.once('message',message=>{if(message!=='ready')process.exit(1);held.unref();process.exit(0);});`;
 let delayedCloseChild:ChildProcess|undefined,releasePipe:Duplex|undefined,closing:Promise<OwnedTermination>|undefined;
 try{
  delayedCloseChild=trackOwned(spawn(process.execPath,['-e',leader],{stdio:['ignore','pipe','pipe','pipe']}));
  const pipe=delayedCloseChild.stdio[3];if(!(pipe instanceof Duplex))throw new Error('delayed_close_fixture_invalid');releasePipe=pipe;pipe.on('error',()=>{});
  const closePromise=ownedClose(delayedCloseChild);let closeSettled=false,terminationSettled=false;void closePromise.then(()=>{closeSettled=true;});
  await ownedProbeDeadline(once(delayedCloseChild,'exit'));await new Promise<void>(resolveTurn=>setImmediate(resolveTurn));
  expect(delayedCloseChild.exitCode).toBe(0);expect(closeSettled).toBe(false);
  closing=terminateOwned(delayedCloseChild).then(result=>{terminationSettled=true;return result;});void closing.catch(()=>{});
  await new Promise<void>(resolveTurn=>setImmediate(resolveTurn));expect(closeSettled).toBe(false);expect(terminationSettled).toBe(false);
  pipe.end();expect(await closing).toEqual({code:0,signal:null,escalated:false});expect(await closePromise).toEqual({code:0,signal:null});expect(closeSettled).toBe(true);
 }finally{releasePipe?.end();await cleanupAll([
  {failure:'owned_child_exit_timeout',run:async()=>{await closing;}},
  {failure:'owned_child_exit_timeout',run:async()=>{await terminateOwned(delayedCloseChild);}}
 ]);}
 // A live, quiet probe must hit the very same deadline and still reach finally.
 let deadlineChild:ChildProcess|undefined;try{deadlineChild=trackOwned(spawn(process.execPath,['-e','setInterval(()=>{},1000)'],{stdio:['ignore','pipe','pipe']}));const output=collectOwnedOutput(deadlineChild);await expect(output.guard(ownedProbeDeadline(ownedClose(deadlineChild)))).rejects.toThrow('owned_probe_timeout');expect(deadlineChild.exitCode).toBeNull();expect(deadlineChild.signalCode).toBeNull();}finally{await terminateOwned(deadlineChild);}expect(await ownedClose(deadlineChild!)).toEqual({code:null,signal:'SIGTERM'});
 let timeoutChild:ChildProcess|undefined;try{timeoutChild=trackOwned(spawn(process.execPath,[script,'--controller-root',root,'--redis-binary',binary!],{stdio:['ignore','pipe','pipe'],env:{PATH:process.env['PATH']??'/usr/bin:/bin',SYNTHETIC_CRASH_TEST_BARRIER_TIMEOUT:'1'}}));const output=collectOwnedOutput(timeoutChild),closed=ownedClose(timeoutChild);const result=await output.guard(Promise.race([closed.then(({code})=>({code,timeout:false})),pause(5000).then(()=>({code:null,timeout:true}))]));expect(result).toEqual({code:1,timeout:false});expect(output.stdout()).toBe('');expect(output.stderr()).toBe('<REDACTED>\n');timeoutChild=undefined;}finally{await terminateOwned(timeoutChild);}},30000);
});
