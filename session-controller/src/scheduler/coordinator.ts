import { setTimeout as delay } from 'node:timers/promises';
import { checked, ControllerError, OpaqueIdSchema, type Fence, type WaitPort, type DrainPort, type IdSource } from '../domain.js';
import type { ControllerConfig } from '../config.js';
import type { RedisRepository } from '../redis/repository.js';
import { createRecovery } from './recovery.js';
import { runChatWorker, type WorkerOptions, type WorkerRepository } from './chat-worker.js';
export const timerWait: WaitPort = {
  async wait(ms, signal) {
    try { await delay(ms, undefined, { signal }); }
    catch { if (!signal.aborted) throw new ControllerError('internal_failed'); }
  }
};
export interface Coordinator extends DrainPort { start(): Promise<void>; failClosed(): void }
export interface CoordinatorOptions {
  scheduler: ControllerConfig['scheduler'];
  pollMs: number; ids: IdSource;
  worker: Omit<WorkerOptions, 'appCurrent' | 'leaseMs' | 'renewMs'>;
}
/** No top-level I/O, no stream-derived authorization, no backlog cap.
 * Stop prevents new claims; active work and app renewal drain indefinitely.
 */
export type CoordinatorRepository = WorkerRepository & Pick<RedisRepository, 'chats' | 'acquireApp' | 'renewApp' | 'releaseApp' | 'recoverPending' | 'claimNext'>;
export function createCoordinator(repository: CoordinatorRepository, options: CoordinatorOptions): Coordinator {
  const emit = (event: Parameters<CoordinatorOptions['worker']['diagnostics']['emit']>[0]) => {
    try { options.worker.diagnostics.emit(event); } catch { /* Diagnostics cannot change lifecycle disposition. */ }
  };
  const c = options.scheduler;
  if (!Number.isInteger(c.concurrency) || c.concurrency < 1 || c.concurrency > 64 ||
      !Number.isInteger(c.leaseMs) || c.leaseMs < 3000 || c.leaseMs > 300000 ||
      !Number.isInteger(c.renewMs) || c.renewMs < 100 || c.renewMs * 3 >= c.leaseMs ||
      !Number.isInteger(c.gapTimeoutMs) || c.gapTimeoutMs < 1000 || c.gapTimeoutMs > 3600000 ||
      !Number.isInteger(options.pollMs) || options.pollMs < 1 || options.pollMs > 5000) throw new ControllerError('config_invalid');
  const active = new Map<string, Promise<void>>();
  const recovery = createRecovery(repository);
  const stop = new AbortController(), heartbeatStop = new AbortController();
  let app: Fence | undefined, live = false, started = false, stopped = false, position = 0, lossReported = false;
  let startTask: Promise<void> | undefined, loop: Promise<void> = Promise.resolve(), heartbeat: Promise<void> = Promise.resolve();
  const lose = () => {
    if (lossReported) return;
    lossReported = true;
    live = false;
    stopped = true;
    stop.abort();
    heartbeatStop.abort();
    emit({ class: 'lease_lost' });
  };
  const coordinator: Coordinator = {
    start() {
      if (startTask) return startTask;
      if (stopped || started) return Promise.reject(new ControllerError('intake_closed'));
      started = true;
      startTask = (async () => {
        const id = checked(OpaqueIdSchema, options.ids.next(), 'config_invalid');
        app = await repository.acquireApp(id, c.leaseMs);
        if (!app) throw new ControllerError('lease_lost');
        live = true;
        heartbeat = (async () => {
          while (!heartbeatStop.signal.aborted) {
            await options.worker.wait.wait(c.renewMs, heartbeatStop.signal);
            if (heartbeatStop.signal.aborted) break;
            if (!app) { lose(); break; }
            const renewed = await repository.renewApp(app, c.leaseMs);
            if (!renewed) { lose(); break; }
            app = renewed;
          }
        })().catch(lose);
        loop = (async () => {
          while (!stopped && live && app) {
            // Round robin across configured canonical identities only.
            for (let i = 0; i < repository.chats.length && !stopped && live; i++) {
              if (active.size >= c.concurrency) break;
              const chat = repository.chats[position++ % repository.chats.length];
              if (!chat || active.has(chat)) continue;
              const owner = checked(OpaqueIdSchema, options.ids.next(), 'config_invalid');
              if (!await recovery(chat, app, owner)) continue;
              if (stopped || !live) break;
              const result = await repository.claimNext(chat, app, owner, c.leaseMs, c.gapTimeoutMs);
              if (result.status === 'lost') { lose(); break; }
              if (result.status !== 'claimed') continue;
              // Stop during claim still drains this granted lease without initiating a prompt.
              const task = stopped || !live ? Promise.resolve() : runChatWorker(repository, result.work, {
                ...options.worker, leaseMs: c.leaseMs, renewMs: c.renewMs, appCurrent: () => live
              });
              active.set(chat, task);
              void task.catch(() => emit({ class: 'internal_failed' }))
                .finally(() => { if (active.get(chat) === task) active.delete(chat); });
            }
            if (!stopped) await options.worker.wait.wait(options.pollMs, stop.signal);
          }
        })().catch(() => { lose(); });
      })();
      return startTask;
    },
    failClosed() { lose(); },
    stopScheduling() { stopped = true; stop.abort(); },
    async drain() {
      coordinator.stopScheduling();
      try { await startTask; } catch { /* No acquired app or already failed closed. */ }
      await loop;
      while (active.size) await Promise.allSettled([...active.values()]);
      heartbeatStop.abort(); await heartbeat;
      if (app && live) { try { await repository.releaseApp(app); } catch { emit({ class: 'store_unavailable' }); } }
      live = false;
    }
  };
  return coordinator;
}
