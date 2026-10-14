import type { CanonicalChatId, Fence } from '../domain.js';
import type { RedisRepository } from '../redis/repository.js';
/** PEL cursors are hints only. A fresh instance rescans from 0-0 safely.
 * Each tick performs at most one bounded XAUTOCLAIM per idle chat. Empty
 * claimed IDs do not mean exhaustion: only next=0-0 ends a Redis scan.
 */
export function createRecovery(repository: Pick<RedisRepository, 'recoverPending'>, count = 32) {
  const cursors = new Map<CanonicalChatId, string>();
  return async (chat: CanonicalChatId, app: Fence, consumer: string): Promise<boolean> => {
    const r = await repository.recoverPending(chat, app, consumer, cursors.get(chat) ?? '0-0', count);
    if (r.status !== 'ok') return false;
    cursors.set(chat, r.next);
    return r.deleted === 0;
  };
}
