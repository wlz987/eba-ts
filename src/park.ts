import type { Reply } from "./job/reply.ts";

const CREDIT_LIMIT = 4096;

interface Waiter {
  readonly reply: Reply;
  readonly deadline: number | null;
  readonly owner: unknown;
}

function popWaiter(queue: Waiter[]): Waiter | null {
  while (queue.length > 0) {
    const waiter = queue.shift()!;
    if (!waiter.reply.isClosed) return waiter;
  }
  return null;
}

/**
 * Keyed waits: many Replies per key, one wake consumed by one live waiter.
 * Keys follow Map identity and deadlines are absolute milliseconds. A wake that
 * finds nobody is kept as a credit; a key keeps at most 4096 credits and extra
 * wakes are dropped. Wakes, expiries and cancellations return live Replies for
 * the caller to settle; a consumed credit leaves its Reply to the caller.
 * Requeues move between distinct keys; expiry scans the waiters. An owner
 * scopes cancellation: cancelOwner removes only its waiters, never another
 * owner's wait on the same key; credits stay key-scoped.
 */
export class Park<K, V> {
  private readonly waiters = new Map<K, Waiter[]>();
  private readonly credits = new Map<K, V[]>();

  wait(
    key: K,
    reply: Reply,
    deadlineMs: number | null = null,
    owner: unknown = null,
  ): V | null {
    const credits = this.credits.get(key);
    if (credits !== undefined && credits.length > 0) {
      const value = credits.shift() as V;
      if (credits.length === 0) this.credits.delete(key);
      return value;
    }
    const queue = this.waiters.get(key);
    const waiter: Waiter = { reply, deadline: deadlineMs, owner };
    if (queue === undefined) this.waiters.set(key, [waiter]);
    else queue.push(waiter);
    return null;
  }

  wake(key: K, value: V): Reply | null {
    const queue = this.waiters.get(key);
    const waiter = queue === undefined ? null : popWaiter(queue);
    this.drop(key);
    if (waiter !== null) return waiter.reply;
    const credits = this.credits.get(key) ?? [];
    this.credits.set(key, credits);
    if (credits.length < CREDIT_LIMIT) credits.push(value);
    return null;
  }

  take(key: K, count: number): Reply[] {
    if (count <= 0) return [];
    const taken: Reply[] = [];
    const queue = this.waiters.get(key);
    while (queue !== undefined && taken.length < count) {
      const waiter = popWaiter(queue);
      if (waiter === null) break;
      taken.push(waiter.reply);
    }
    this.drop(key);
    return taken;
  }

  moveWaiters(from: K, to: K, count: number): number {
    if (from === to || count <= 0) return 0;
    const source = this.waiters.get(from);
    if (source === undefined || source.length === 0) return 0;
    const target = this.waiters.get(to) ?? [];
    this.waiters.set(to, target);
    let moved = 0;
    while (moved < count) {
      const waiter = popWaiter(source);
      if (waiter === null) break;
      target.push(waiter);
      moved += 1;
    }
    if (source.length === 0) this.waiters.delete(from);
    this.drop(to);
    return moved;
  }

  requeue(from: K, to: K, wake: number, requeue: number): Reply[] {
    const woken = this.take(from, wake);
    this.moveWaiters(from, to, requeue);
    return woken;
  }

  expire(nowMs: number): Reply[] {
    const expired: Reply[] = [];
    for (const [key, queue] of [...this.waiters]) {
      const kept: Waiter[] = [];
      for (const waiter of queue) {
        if (waiter.deadline !== null && waiter.deadline <= nowMs) {
          if (!waiter.reply.isClosed) expired.push(waiter.reply);
        } else kept.push(waiter);
      }
      if (kept.length === 0) this.waiters.delete(key);
      else this.waiters.set(key, kept);
    }
    return expired;
  }

  cancel(key: K): Reply[] {
    this.credits.delete(key);
    const queue = this.waiters.get(key);
    this.waiters.delete(key);
    if (queue === undefined) return [];
    return queue
      .filter((waiter) => !waiter.reply.isClosed)
      .map((waiter) => waiter.reply);
  }

  cancelOwner(owner: unknown): Reply[] {
    const cancelled: Reply[] = [];
    for (const [key, queue] of [...this.waiters]) {
      const kept: Waiter[] = [];
      for (const waiter of queue) {
        if (waiter.owner === owner) {
          if (!waiter.reply.isClosed) cancelled.push(waiter.reply);
        } else kept.push(waiter);
      }
      if (kept.length === 0) this.waiters.delete(key);
      else this.waiters.set(key, kept);
    }
    return cancelled;
  }

  keys(): K[] {
    return [...this.waiters.keys()];
  }

  private drop(key: K): void {
    const queue = this.waiters.get(key);
    if (queue !== undefined && queue.length === 0) this.waiters.delete(key);
  }
}
