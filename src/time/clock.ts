import { ClockError } from "../errors.ts";
import { nonnegativeInt } from "./numbers.ts";

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
  private now: number;
  constructor(nowMs = 0) {
    this.now = nonnegativeInt(nowMs, "now_ms");
  }
  nowMs(): number {
    return this.now;
  }
  armsTimer(): boolean {
    return false;
  }
  advance(ms: number): void {
    const step = nonnegativeInt(ms, "advance_ms");
    this.now += step;
  }
}

export class PinnedClock implements Clock {
  private readonly instant: number;
  constructor(nowMs: number) {
    this.instant = nonnegativeInt(nowMs, "clock_ms");
  }
  nowMs(): number {
    return this.instant;
  }
}

export function pinClock(nowMs: number): Clock {
  return new PinnedClock(nowMs);
}

export function readClock(clock: Clock): number {
  try {
    return nonnegativeInt(clock.nowMs(), "clock_ms");
  } catch (error) {
    if (error instanceof ClockError) throw error;
    throw new ClockError(String(error));
  }
}
