import {
  errPayload,
  errPayloadCode,
  hasResultShape,
} from "#src/core/envelope.js";

export interface LeaseOptions {
  readonly deadlineMs?: number | null;
  readonly budgetMs?: number | null;
}

export class Lease {
  /** Settlement state, pinned as ECMAScript `#` fields. */
  $body: Record<string, unknown> | null = null;
  #closed = false;
  #control = new AbortController();
  /** The two scheduling bounds, pinned as ECMAScript `#` fields. */
  readonly deadlineMs: number | null;
  readonly budgetMs: number | null;
  #spent = 0;
  /**
   * The job this lease was issued for; `null` for a caller-built one. `WeakRef`
   * keeps the job alive only while it lives and compares by identity.
   */
  $owner: WeakRef<object> | null = null;

  constructor(options: LeaseOptions = {}) {
    // Caller contract: safe nonnegative integers (README). `deadlineMs` reaches
    // the agenda heap through `Agenda.update`, which rejects an unsafe instant
    // before it can poison every host's timer.
    this.deadlineMs =
      options.deadlineMs == null
        ? null
        : options.deadlineMs;
    this.budgetMs =
      options.budgetMs == null
        ? null
        : options.budgetMs;
  }

  expired(nowMs: number): boolean {
    return this.deadlineMs !== null && this.deadlineMs <= nowMs;
  }

  get remainingMs(): number | null {
    return this.budgetMs === null ? null : this.budgetMs - this.#spent;
  }

  /** Charge work against the budget; false means the budget is exhausted. */
  consume(ms: number): boolean {
    this.#spent += ms;
    return this.budgetMs === null || this.#spent <= this.budgetMs;
  }

  get signal(): AbortSignal {
    return this.#control.signal;
  }

  settle(body: unknown): void {
    this.send(body);
  }

  send(body: unknown): void {
    if (this.#closed) throw new Error("lease closed");
    if (this.#control.signal.aborted) return;
    if (this.$body !== null) throw new Error("lease already settled");
    if (!hasResultShape(body)) throw new TypeError("invalid result body");
    // Take the body by value: a live object from the caller would still be
    // rewriteable after the answer was handed over.
    this.$body = { ...(body as Record<string, unknown>) };
  }

  fail(error: string, code?: number): void {
    this.send(
      code === undefined ? errPayload(error) : errPayloadCode(error, code),
    );
  }

  revoke(error = "cancelled"): boolean {
    return this.cancel(error);
  }

  cancel(error = "cancelled"): boolean {
    if (this.$body !== null || this.#closed) return false;
    this.$body = errPayload(error);
    this.abort();
    return true;
  }

  abort(): void {
    if (!this.#control.signal.aborted) this.#control.abort();
  }

  take(): Record<string, unknown> | null {
    return this.$body;
  }

  get isSettled(): boolean {
    return this.$body !== null;
  }

  get isClosed(): boolean {
    return this.#closed;
  }

  close(): void {
    this.#closed = true;
  }
}

export function bindLease(lease: Lease, job: object): void {
  lease.$owner = new WeakRef(job);
}

/** Was this lease issued for `job`? Total: a caller-built lease has no owner. */
export function leaseIssuedFor(lease: Lease, job: object): boolean {
  return (lease.$owner?.deref() ?? null) === job;
}

export function isLease(value: unknown): value is Lease {
  return (value instanceof Lease);
}
