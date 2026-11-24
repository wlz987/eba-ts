

export class Queue<T = unknown> {
  capacity: number;
  items: T[] = [];
  constructor(capacity: number) {
    this.capacity = capacity;
  }
  get size(): number {
    return this.items.length;
  }
  get full(): boolean {
    return this.items.length >= this.capacity;
  }
  get readonly(): QueueView<T> {
    return this;
  }
  remove(predicate: (item: T) => boolean): number {
    const kept = this.items.filter((item) => !predicate(item));
    const removed = this.items.length - kept.length;
    this.items.length = 0;
    this.items.push(...kept);
    return removed;
  }

  push(item: T): boolean {
    if (this.full) return false;
    this.items.push(item);
    return true;
  }
  shift(): T | null {
    if (this.items.length === 0) return null;
    return this.items.shift()!;
  }
  shiftIf(predicate: (item: T) => boolean): T | null {
    for (let index = 0; index < this.items.length; index += 1) {
      const item = this.items[index]!;
      if (predicate(item)) {
        this.items.splice(index, 1);
        return item;
      }
    }
    return null;
  }
  [Symbol.iterator](): IterableIterator<T> {
    return this.items[Symbol.iterator]();
  }
}

/**
 * The read half of `Queue<T>`: bounds, size, items and iteration.
 *
 * `push`/`shift`/`shiftIf`/`remove` are simply absent — an oversight means a
 * missing method (a compile error at the call site), never a writable buffer.
 * Same shape and same reasoning as the `Map`/`Set`/`Array` views in
 */
export type QueueView<T> = Queue<T>;
