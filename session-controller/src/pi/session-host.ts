import { createHash } from 'node:crypto';
import { constants } from 'node:fs';
import { open } from 'node:fs/promises';
import { dirname, join } from 'node:path';
import { isDeepStrictEqual } from 'node:util';
import {
  checked, ControllerError, SessionBindingSchema, CompletionMarkerSchema, fingerprintEvent,
  type ClaimedWork, type PiHostPort, type PiSessionPort, type CompletionProof, type CheckpointV1
} from '../domain.js';
import type { TurnContext } from '../scheduler/chat-worker.js';
import { BINDING_TYPE, COMPLETION_TYPE, assertSessionPath, durableDirectory, syncSession, syncDirectory, type CompletionIndex } from './completion-index.js';
import { WHATSAPP_SERVER, type SdkPort, type SdkSession } from './mcp-loader.js';
import { MUTATION_TYPE, type JournalAttachment } from './tool-journal-extension.js';
export interface SessionHost extends PiHostPort { attach(session: PiSessionPort, context: TurnContext): Promise<void> }
export interface SessionHostOptions {
  cwd: string; root: string; sdk: SdkPort; index: CompletionIndex;
  /** Joint unexpired app/chat gate; never calls Pi/index or logs raw errors. */
  current(work: ClaimedWork): Promise<void>;
  /** Existing repository.bindSession; durable fenced mapping before startup/prompt. */
  bind(work: ClaimedWork, binding: PiSessionPort['binding']): Promise<boolean>;
  /** Slice 8 performs checkpoint prepare/compact/restore through this injected
   * port, with context.current before each effect; no future module import. */
  /** Native composition MUST pass this and loadNativeSdk(...,[journal.extension]).
   * Optional only for non-executing fake SDK proofs of Slice 6. */
  journal?: JournalAttachment;
  compact?: (raw: SdkSession, context: TurnContext, instructions: string, checkpoint: CheckpointV1) => Promise<{ entryId: string; checkpoint: CheckpointV1 }>;
}
export function createSessionHost(options: SessionHostOptions): SessionHost {
  let closed = false, closingHost = false, closeTask: Promise<void> | undefined;
  const active = new Map<string, PiSessionPort>();
  const attaching = new WeakMap<PiSessionPort, (context: TurnContext) => Promise<void>>();
  const opening = new Set<string>();
  const host: SessionHost = {
    findEvent: (chat, id) => options.index.findEvent(chat, id),
    classifyOwn: (chat, actual, id) => options.journal
      ? options.journal.classifyOwn(chat, actual, id, () => options.index.classifyOwn(chat, actual, id))
      : options.index.classifyOwn(chat, actual, id),
    async attach(session, context) {
      const attach = attaching.get(session); if (!attach) throw new ControllerError('session_failed');
      await attach(context);
    },
    async open(work, existing) {
      const chat = work.event.canonical_chat_jid;
      if (closed || closingHost || active.has(chat) || opening.has(chat)) throw new ControllerError('session_failed');
      opening.add(chat);
      let raw: SdkSession | undefined; let journalBinding: PiSessionPort['binding'] | undefined;
      try {
        const initialCurrent = () => options.current(work);
        await initialCurrent();
        let binding = existing;
        const dir = existing ? dirname(existing.sessionFile) : join(options.root, createHash('sha256').update(chat).digest('hex'));
        if (existing) {
          if (existing.canonicalChatId !== chat) throw new ControllerError('session_failed');
          // Strict reader before SDK.open: permissive SDK migration/repair must
          // never silently erase evidence or create a missing mapped file.
          await options.index.exclusive(() => options.index.evidence(existing));
        } else {
          await durableDirectory(dir, initialCurrent);
          await initialCurrent();
          const created = options.sdk.create(options.cwd, dir);
          const file = created.getSessionFile(); if (!file) throw new ControllerError('session_failed');
          binding = checked(SessionBindingSchema, { canonicalChatId: chat, sessionId: created.getSessionId(), sessionFile: file }, 'session_failed');
          assertSessionPath(options.root, binding);
          created.appendCustomEntry(BINDING_TYPE, binding);
          // SDK 1.1.0 defers new-file persistence until a conversation message.
          // Seed its PUBLIC header/entries, no invented context message/private
          // flush call. Exact open of this durable file sets normal append mode.
          const seed = [created.getHeader(), ...created.getEntries()].map(entry => JSON.stringify(entry)).join('\n') + '\n';
          await initialCurrent();
          const fd = await open(file, constants.O_WRONLY | constants.O_CREAT | constants.O_EXCL | constants.O_NOFOLLOW, 0o600);
          try { await fd.writeFile(seed); await initialCurrent(); await fd.sync(); } finally { await fd.close(); }
          await initialCurrent(); await syncDirectory(dir);
          await options.index.exclusive(() => options.index.evidence(binding!));
        }
        if (!binding) throw new ControllerError('session_failed');
        await initialCurrent();
        if (!await options.bind(work, binding)) throw new ControllerError('lease_lost');
        await initialCurrent();
        const manager = options.sdk.open(binding.sessionFile, dir);
        if (manager.getSessionId() !== binding.sessionId || manager.getSessionFile() !== binding.sessionFile) throw new ControllerError('session_failed');
        let context: TurnContext | undefined, disposed = false, disposing = false, busy = false, turnSettled = false;
        let operation: Promise<void> = Promise.resolve(), finished: (() => void) | undefined, disposeTask: Promise<void> | undefined;
        const begin = () => { busy = true; operation = new Promise<void>(resolve => { finished = resolve; }); };
        const end = () => { busy = false; finished?.(); finished = undefined; };
        const current = async () => {
          if (disposed || closed) throw new ControllerError('session_failed');
          if (context) await context.current(); else await initialCurrent();
        };
        if (options.journal) {
          options.journal.bind({ session: binding, async record(value) {
            try {
              await current(); manager.appendCustomEntry(MUTATION_TYPE, value);
              await current(); await syncSession(binding!);
              await current(); await syncDirectory(dirname(binding!.sessionFile));
              await current();
            } catch { options.index.invalidate(binding!); throw new ControllerError('mutation_uncertain'); }
          } });
          journalBinding = binding;
        }
        raw = await options.sdk.connect(manager, current);
        const runtime = raw;
        await current(); runtime.setAutoCompactionEnabled(false);
        await current(); await runtime.bindExtensions({}); // starts stdio MCP, untouched stdout framing
        await current(); await options.sdk.ready(runtime, current);
        // Native MCP inventory may never run without an installed journal.
        // Fake Slice 6 sessions have no tools and remain valid proof ports.
        if (!options.journal && runtime.getAllTools().some(t => t.name.startsWith(`mcp__${WHATSAPP_SERVER}__`))) throw new ControllerError('session_failed');
        const requireAttached = async () => { if (!context || disposing) throw new ControllerError('session_failed'); await current(); if (disposing) throw new ControllerError('session_failed'); };
        const port: PiSessionPort = {
          binding,
          async prompt(event, instructions) {
            await requireAttached();
            if (busy || !runtime.isIdle || fingerprintEvent(event) !== fingerprintEvent(work.event)) throw new ControllerError('session_failed');
            begin(); let settled = false, aborted = false;
            const unsubscribe = runtime.subscribe(e => { if (e.type === 'agent_settled') { settled = true; aborted = e.aborted; } });
            try {
              await current();
              const invoke = async () => {
                await runtime.prompt(instructions, { expandPromptTemplates: false });
                await runtime.waitForIdle();
              };
              if (options.journal) await options.journal.run(port.binding, invoke); else await invoke();
              if (!settled || aborted || !runtime.isIdle) throw new ControllerError('session_failed');
              await current(); turnSettled = true;
            } catch (error) {
              if (error instanceof ControllerError) throw error;
              throw new ControllerError('session_failed');
            } finally { unsubscribe(); end(); }
          },
          usage() { if (disposed || busy || !runtime.isIdle) return undefined; return runtime.getContextUsage(); },
          async compact(instructions, checkpoint) {
            await requireAttached();
            if (busy || !runtime.isIdle || !options.compact || !context) throw new ControllerError('compaction_failed');
            begin();
            try {
              await current(); const result = await options.compact(runtime, context, instructions, checkpoint);
              await runtime.waitForIdle(); await current();
              // Never hold evidence lock during SDK compaction/hooks: intake's
              // writer→index ordering would otherwise deadlock journal gates.
              await current(); await syncSession(port.binding);
              await options.index.exclusive(() => options.index.evidence(port.binding));
              return result;
            } catch { options.index.invalidate(port.binding); throw new ControllerError('compaction_failed'); }
            finally { end(); }
          },
          async appendCompletion(input): Promise<CompletionProof> {
            await requireAttached();
            if (busy || !runtime.isIdle) throw new ControllerError('session_failed');
            const marker = checked(CompletionMarkerSchema, input, 'session_failed');
            const event = work.event;
            if (marker.eventId !== event.event_id || marker.canonicalChatId !== chat || marker.chatSequence !== event.chat_sequence || marker.eventFingerprint !== fingerprintEvent(event)) throw new ControllerError('session_failed');
            if (marker.completion === 'turn' && !turnSettled) throw new ControllerError('session_failed');
            if (marker.completion === 'context_only' && (!event.is_from_me || await host.classifyOwn(chat, event.chat_jid, event.message_id) !== 'confirmed')) throw new ControllerError('session_failed');
            begin(); let reserved = false;
            try {
              // Never await Redis while holding the file-only interval. Reserve
              // unpublished evidence; intake gets unavailable until publish,
              // rather than deadlocking or seeing an un-fsynced marker.
              await current();
              const existing = await options.index.exclusive(async () => {
                const before = await options.index.evidence(port.binding);
                const found = before.find(p => p.marker.eventId === marker.eventId);
                if (found) { if (!isDeepStrictEqual(found.marker, marker)) throw new ControllerError('session_failed'); return found; }
                options.index.reserve(port.binding); reserved = true; return undefined;
              });
              if (existing) return existing;
              await current();
              const id = manager.appendCustomEntry(COMPLETION_TYPE, marker);
              await current(); await syncSession(port.binding);
              await current();
              return await options.index.exclusive(async () => {
                const after = await options.index.evidence(port.binding);
                const proof = after.find(p => p.entryId === id);
                if (!proof || !isDeepStrictEqual(proof.marker, marker)) throw new ControllerError('session_failed');
                options.index.publish(port.binding); reserved = false;
                return proof;
              });
            } catch { if (reserved) options.index.invalidate(port.binding); throw new ControllerError('session_failed'); }
            finally { end(); }
          },
          async dispose() {
            if (disposeTask) return disposeTask;
            // Indefinite drain. Cleanup attempts are independent, fixed-error,
            // and always detach this handle so a failed cleanup cannot wedge chat.
            disposing = true;
            disposeTask = (async () => {
              let failed = false;
              try {
                try { await operation; } catch { failed = true; }
                try { await runtime.waitForIdle(); } catch { failed = true; }
                disposed = true;
                try { runtime.dispose(); } catch { failed = true; }
                try { await options.journal?.dispose(port.binding); } catch { failed = true; }
                if (failed) throw new ControllerError('session_failed');
              } finally {
                disposed = true;
                attaching.delete(port); if (active.get(chat) === port) active.delete(chat);
              }
            })();
            return disposeTask;
          }
        };
        attaching.set(port, async next => {
          await next.current();
          if (disposed || disposing) throw new ControllerError('session_failed');
          if (context && context !== next) throw new ControllerError('session_failed');
          if (next.work().event.event_id !== work.event.event_id || fingerprintEvent(next.work().event) !== fingerprintEvent(work.event)) throw new ControllerError('session_failed');
          context = next;
          await options.journal?.attach(port.binding, next);
        });
        active.set(chat, port); return port;
      } catch (error) {
        try { raw?.dispose(); }
        catch { /* Failed-open cleanup cannot replace the primary fixed error class. */ }
        if (journalBinding && options.journal) { try { await options.journal.dispose(journalBinding); } catch { /* fixed outer error */ } }
        if (error instanceof ControllerError) throw error;
        throw new ControllerError('session_failed');
      } finally { opening.delete(chat); }
    },
    async close() {
      if (closeTask) return closeTask;
      // Later lifecycle must drain scheduler/open operations before closing host.
      closingHost = true; closed = true;
      const sessions = [...active.values()]; active.clear();
      const openingFailed = opening.size > 0;
      closeTask = (async () => {
        const results = await Promise.allSettled(sessions.map(session => session.dispose()));
        if (openingFailed || results.some(result => result.status === 'rejected')) throw new ControllerError('session_failed');
      })();
      return closeTask;
    }
  };
  return host;
}
