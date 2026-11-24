import { ClockError } from "#src/core/errors.js";

/** Deadlines become heap keys and scheduler waits: only safe instants are legal. */
export function safeDeadline(value: number, name = "deadline_ms"): number {
  if (typeof value === "boolean" || !Number.isInteger(value) || value < 0) {
    throw new RangeError(`${name} must be a nonnegative integer`);
  }
  if (!Number.isSafeInteger(value)) {
    throw new RangeError(`${name} must be a safe integer`);
  }
  return value;
}

// --- clocks ----------------------------------------------------------------

export interface Clock {
  nowMs(): number;
  armsTimer?(): boolean;
}

export class MonotonicClock implements Clock {
  nowMs(): number {
    return Math.floor(performance.now());
  }
}

export class ManualClock implements Clock {
  #now: number;
  constructor(nowMs = 0) {
    this.#now = nowMs;
  }
  nowMs(): number {
    return this.#now;
  }
  armsTimer(): boolean {
    return false;
  }
  advance(ms: number): void {
    this.#now += ms;
  }
}

export class PinnedClock implements Clock {
  readonly #instant: number;
  constructor(nowMs: number) {
    this.#instant = nowMs;
  }
  nowMs(): number {
    return this.#instant;
  }
}

export function pinClock(nowMs: number): Clock {
  return new PinnedClock(nowMs);
}

export function readClock(clock: Clock): number {
  try {
    return clock.nowMs();
  } catch (error) {
    // `ClockError` and the reentrancy guard are already the exact text a caller
    // must see — wrapping the guard would change that text. Validation failures
    // are `ClockError`; the guard is not, so it passes through unchanged.
    if (error instanceof ClockError) throw error;
    if (error instanceof Error && error.message === "reentrant clock observation")
      throw error;
    throw new ClockError(String(error));
  }
}

// --- deadline heap ---------------------------------------------------------

type Item<T> = [at: number, sequence: number, itemId: T];

export const ELAPSED = 0;

/** Deadline heap ordered by (at, insertion-sequence). */
export class Deadlines<T = string> {
  #items: Item<T>[] = [];
  #positions = new Map<T, number>();
  #sequence = 0;

  at(itemId: T): number | null {
    const index = this.#positions.get(itemId);
    if (index === undefined) return null;
    return this.#items[index]![0];
  }

  get nextMs(): number | null {
    if (this.#items.length === 0) return null;
    return this.#items[0]![0];
  }

  add(itemId: T, at: number): void {
    if (this.#positions.has(itemId)) throw new Error("duplicate deadline");
    this.#sequence += 1;
    this.#positions.set(itemId, this.#items.length);
    this.#items.push([at, this.#sequence, itemId]);
    this.up(this.#items.length - 1);
  }

  remove(itemId: T): void {
    const index = this.#positions.get(itemId);
    if (index === undefined) return;
    this.#positions.delete(itemId);
    const last = this.#items.pop()!;
    if (index === this.#items.length) return;
    this.#items[index] = last;
    this.#positions.set(last[2], index);
    this.up(index);
    this.down(index);
  }

  peekDue(nowMs: number): T | null {
    if (this.#items.length === 0 || this.#items[0]![0] > nowMs) return null;
    return this.#items[0]![2];
  }

  private before(left: Item<T>, right: Item<T>): boolean {
    return left[0] < right[0] || (left[0] === right[0] && left[1] < right[1]);
  }

  private swap(left: number, right: number): void {
    const a = this.#items[left]!;
    const b = this.#items[right]!;
    this.#items[left] = b;
    this.#items[right] = a;
    this.#positions.set(a[2], right);
    this.#positions.set(b[2], left);
  }

  private up(index: number): void {
    while (index > 0) {
      const parent = (index - 1) >> 1;
      if (!this.before(this.#items[index]!, this.#items[parent]!)) return;
      this.swap(index, parent);
      index = parent;
    }
  }

  private down(index: number): void {
    const length = this.#items.length;
    while (2 * index + 1 < length) {
      let child = 2 * index + 1;
      if (
        child + 1 < length &&
        this.before(this.#items[child + 1]!, this.#items[child]!)
      ) {
        child += 1;
      }
      if (!this.before(this.#items[child]!, this.#items[index]!)) return;
      this.swap(index, child);
      index = child;
    }
  }
}

// --- schedule --------------------------------------------------------------

export class Schedule {
  readonly ready: boolean;
  readonly nextDeadlineMs: number | null;
  readonly isQuiescent: boolean;
  readonly nowMs: number | null;
  constructor(
    ready = false,
    nextDeadlineMs: number | null = null,
    isQuiescent = true,
    nowMs: number | null = null,
  ) {
    this.ready = ready;
    this.isQuiescent = isQuiescent;
    this.nextDeadlineMs = nextDeadlineMs;
    this.nowMs = nowMs === null ? null : nowMs;
  }

  waitMs(nowMs?: number | null): number | null {
    let instant = nowMs;
    if (instant === undefined || instant === null) {
      if (this.nowMs === null)
        throw new Error("schedule has no observed instant");
      instant = this.nowMs;
    }
    if (this.ready) return 0;
    if (this.nextDeadlineMs === null) return null;
    return Math.max(0, this.nextDeadlineMs - instant);
  }
}

// --- wake ------------------------------------------------------------------

/** Longest delay `setTimeout` can represent before Node clamps to 1 ms. */
export const MAX_TIMER_DELAY_MS = 2_147_483_647;

/** A one-shot scheduler wake: a flag plus its single waiter. */

