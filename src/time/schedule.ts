import { nonnegativeInt } from "./numbers.ts";

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
    if (nextDeadlineMs !== null) nonnegativeInt(nextDeadlineMs, "deadline_ms");
    this.ready = ready;
    this.nextDeadlineMs = nextDeadlineMs;
    this.isQuiescent = isQuiescent;
    this.nowMs = nowMs === null ? null : nonnegativeInt(nowMs, "clock_ms");
  }
  merge(other: Schedule): Schedule {
    const deadlines = [this.nextDeadlineMs, other.nextDeadlineMs].filter(
      (item): item is number => item !== null,
    );
    return new Schedule(
      this.ready || other.ready,
      deadlines.length ? Math.min(...deadlines) : null,
      this.isQuiescent && other.isQuiescent,
    );
  }
  waitMs(nowMs?: number | null): number | null {
    let instant = nowMs;
    if (instant === undefined || instant === null) {
      if (this.nowMs === null)
        throw new Error("schedule has no observed instant");
      instant = this.nowMs;
    }
    nonnegativeInt(instant, "clock_ms");
    if (this.ready) return 0;
    if (this.nextDeadlineMs === null) return null;
    return Math.max(0, this.nextDeadlineMs - instant);
  }
}
