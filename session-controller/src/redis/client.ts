import { createClient } from 'redis';
import { ControllerError, type ReadinessPort } from '../domain.js';
import type { ControllerConfig } from '../config.js';
export interface CommandPort { sendCommand(args: readonly string[]): Promise<unknown> }
export interface RedisConnections extends ReadinessPort {
  readonly writer: CommandPort;
  readonly reader: CommandPort;
  /** Independent nonblocking reads only: never Lua, WAITAOF or blocking XREAD. */
  readonly mapping: CommandPort;
  serial<T>(operation: () => Promise<T>): Promise<T>;
  durable<T>(operation: () => Promise<T>): Promise<T>;
  close(): Promise<void>;
}
export function strings(reply: unknown): string[] {
  if (!Array.isArray(reply) || !reply.every(v => typeof v === 'string')) throw new ControllerError('store_unavailable');
  return reply as string[];
}
export async function requireBaseline(client: CommandPort): Promise<void> {
  const info = await client.sendCommand(['INFO', 'server']);
  const version = typeof info === 'string' ? /^redis_version:(\d+)\.(\d+)\.(\d+)\r?$/m.exec(info) : null;
  if (!version || Number(version[1]) < 7 || Number(version[1]) === 7 && Number(version[2]) < 2) throw new ControllerError('store_unavailable');
  for (const [key, expected] of [['appendonly', 'yes'], ['appendfsync', 'always'], ['maxmemory-policy', 'noeviction']] as const) {
    const result = strings(await client.sendCommand(['CONFIG', 'GET', key]));
    if (result.length !== 2 || result[0] !== key || result[1] !== expected) throw new ControllerError('store_unavailable');
  }
  const persistence = await client.sendCommand(['INFO', 'persistence']);
  if (typeof persistence !== 'string' || !/^aof_enabled:1\r?$/m.test(persistence) || !/^aof_last_write_status:ok\r?$/m.test(persistence)) throw new ControllerError('store_unavailable');
}
export async function connectRedis(config: ControllerConfig['redis']): Promise<RedisConnections> {
  const options = { url: config.url, RESP: 2 as const, disableOfflineQueue: true, socket: { reconnectStrategy: false as const, connectTimeout: 5000 } };
  const writer = createClient(options), reader = createClient(options), mapping = createClient(options);
  let healthy = true, closing = false, tail: Promise<unknown> = Promise.resolve(), closed: Promise<void> | undefined;
  const broken = () => { healthy = false; };
  for (const client of [writer, reader, mapping]) { client.on('error', broken); client.on('end', broken); }
  try {
    await writer.connect(); await requireBaseline(writer);
    await reader.connect();
    await mapping.connect(); await requireBaseline(mapping);
  } catch {
    for (const client of [mapping, reader, writer]) if (client.isOpen) client.destroy();
    throw new ControllerError('store_unavailable');
  }
  const ready = () => healthy && !closing && writer.isReady && reader.isReady && mapping.isReady;
  const serial = <T>(operation: () => Promise<T>): Promise<T> => {
    const result = tail.then(async () => { if (!ready()) throw new ControllerError('store_unavailable'); return operation(); });
    tail = result.catch(() => undefined); return result;
  };
  return {
    writer, reader, mapping, isReady: ready, serial,
    durable: operation => serial(async () => {
      try {
        // Fail closed if an operator changes the baseline after startup.
        await requireBaseline(writer);
        const result = await operation();
        const response = await writer.sendCommand(['WAITAOF', '1', '0', String(config.waitAofMs)]);
        if (!Array.isArray(response) || response.length !== 2 || typeof response[0] !== 'number' || response[0] < 1 || typeof response[1] !== 'number' || response[1] < 0) throw new ControllerError('store_unavailable');
        return result;
      } catch { healthy = false; throw new ControllerError('store_unavailable'); }
    }),
    close() {
      if (closed) return closed;
      closing = true;
      // Interrupt only blocking reads, never in-flight durable writes.
      if (reader.isOpen) reader.destroy();
      closed = tail.then(async () => {
        const closingClients: Promise<unknown>[] = [];
        if (mapping.isOpen) closingClients.push(mapping.close());
        if (writer.isOpen) closingClients.push(writer.close());
        const settled = await Promise.allSettled(closingClients);
        if (settled.some(result => result.status === 'rejected')) throw new ControllerError('store_unavailable');
      }).catch(() => {
        if (mapping.isOpen) mapping.destroy();
        if (writer.isOpen) writer.destroy();
      });
      return closed;
    }
  };
}
