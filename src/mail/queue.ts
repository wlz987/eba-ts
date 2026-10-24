import { positiveInt } from "../time/numbers.ts";

export class Queue {
  readonly capacity: number;
  private items: unknown[] = [];
  constructor(capacity: number) {
    this.capacity = positiveInt(capacity, "capacity");
  }
  get size(): number {
    return this.items.length;
  }
  get full(): boolean {
    return this.items.length >= this.capacity;
  }
  remove(predicate: (item: unknown) => boolean): number {
    const kept = this.items.filter((item) => !predicate(item));
    const removed = this.items.length - kept.length;
    this.items = kept;
    return removed;
  }

  push(item: unknown): boolean {
    if (this.full) return false;
    this.items.push(item);
    return true;
  }
  shift(): unknown | null {
    if (this.items.length === 0) return null;
    return this.items.shift()!;
  }
  [Symbol.iterator](): IterableIterator<unknown> {
    return this.items[Symbol.iterator]();
  }
}
