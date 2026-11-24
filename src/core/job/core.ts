import { cancelJob } from "#src/core/job/cancel.js";
import { Envelope, isSealed } from "#src/core/envelope.js";
import { remove, readSignal } from "#src/core/mail/inbox.js";
import type { ActorId, EnvelopeId } from "#src/core/envelope.js";
import type { Receiver } from "#src/core/mail/inbox.js";
import { Queue } from "#src/core/mail/queue.js";
import type { QueueView } from "#src/core/mail/queue.js";
import { isLease, Lease } from "#src/core/job/lease.js";
import { isJob, Job } from "#src/core/job/job.js";
import { stateOf } from "#src/core/job/context.js";
import { Subscriber } from "#src/core/mail/subscriber.js";
import { parsePattern } from "#src/core/pattern.js";

import { LeaseRegistry } from "#src/core/job/lease_registry.js";
import { Interest, TIMER, WORK } from "#src/core/notify.js";
import { Mailbox } from "#src/core/job/mailbox.js";
import { RecordLog, MemorySink } from "#src/core/record.js";
import type { Record, RecordLogView, RecordSink } from "#src/core/record.js";
import type { EffectHost } from "#src/core/job/effect.js";
import { safeDeadline } from "#src/core/time.js";
import type { Clock } from "#src/core/time.js";

export type HostState = "running" | "draining" | "closing" | "closed";

export interface CoreOptions {
  actorId: ActorId | string;
  receiver: Receiver;
  patterns: readonly string[];
  requestTimeoutMs?: number | null;
  queueLimit: number;
  activeLimit: number;
  workBudget: number;
  mailboxLimit: number;
  makeJob: (root: Envelope) => Job;
  onRejected: (root: Envelope, reason: string) => unknown;
  recordSink?: RecordSink | undefined;
}

/** Enter a turn: sets the reentrancy guard. */
export function busy(core: Core): void {
  core.$busy = true;
}

/** Leave a turn: clears the reentrancy guard. */
export function idle(core: Core): void {
  core.$busy = false;
}

/** Retire the host; only `closeHost` reaches this, after its books are clean. */
export function closeCore(core: Core): void {
  core.$state = "closed";
}

export function stepPhase(core: Core): number {
  const ran = core.$phase;
            core.$phase = (ran + 1) % 4;
            return ran;
}

export function rejectRoot(core: Core): void {
  core.$rejectedCount += 1;
}

export class Core {
  /** The host's identity, bounds and wiring — pinned as ECMAScript `#` fields. */
  readonly makeJob: (root: Envelope) => Job;
  readonly onRejected: (root: Envelope, reason: string) => unknown;
  readonly activeLimit: number;
  readonly workBudget: number;
  readonly timeout: number | null;
  readonly #queue: Queue<Envelope>;
  readonly sub: Subscriber;
  readonly signal: ReturnType<typeof readSignal>;
  readonly mailbox: Mailbox;
  readonly jobs = new Map<EnvelopeId, Job>();
  readonly held = new Map<EnvelopeId, Lease>();
  readonly leases = new LeaseRegistry();
  readonly #records: RecordLog;
  mailboxWait: Interest | null = null;
  host: EffectHost | null = null;
  /**
   * The runtime's clock, attached when the host is admitted. Deferred
   * continuations (timer/barrier resumes) run outside a turn, so they must
   * read *this* clock rather than the per-turn pinned clock captured when the
   * hook was created — otherwise a resume that runs at t=500 still computes
   * deadlines from t=0 (an `ask(timeout)` made there would expire instantly).
   */
  clock: Clock | null = null;
  pendingWait: Promise<void> | null = null;
  #timers: Array<{ owner: EnvelopeId; deadline: number }> = [];
  #timerWaiters: Array<{ owner: EnvelopeId; deadline: number; resolve: () => void }> = [];
  $phase = 0;
  $busy = false;
  $state: HostState = "running";
  closeRequested = false;
  $rejectedCount = 0;
  readonly cancelPending = new Set<EnvelopeId>();
  #abandoned: Envelope[] | null = null;
  pending: Envelope | null = null;
  pendingJob: Job | null = null;
  rest: Array<[Envelope, Job | null]> = [];
  /** Outbound retry books, keyed by envelope id (not object identity). */
  readonly again = new Set<EnvelopeId>();
  readonly attempts = new Map<EnvelopeId, number>();
  fault: Error | null = null;
  deferFault: Error | null = null;
  #flushInFlight = false;

  /** Inbound roots waiting for adoption, exposed as a read-only view. */
  get queue(): QueueView<Envelope> {
    return this.#queue.readonly;
  }

  /**
   * Queue a root for adoption; `false` means the queue was full. Roots must be
   * sealed envelopes so the pump never adopts a forged header.
   */
  queuePush(root: Envelope): boolean {
    if (!isSealed(root))
      throw new TypeError("queuePush requires an Envelope");
    return this.#queue.push(root);
  }

  queueShift(): Envelope | null {
    return this.#queue.shift();
  }

  queueShiftIf(predicate: (root: Envelope) => boolean): Envelope | null {
    return this.#queue.shiftIf(predicate);
  }

  queueRemove(predicate: (root: Envelope) => boolean): number {
    return this.#queue.remove(predicate);
  }

  /** Active jobs, keyed by root envelope id */

  admitJob(rootId: EnvelopeId, job: Job): void {
    if (!isJob(job)) throw new TypeError("admitJob requires a Job");
    this.jobs.set(rootId, job);
  }

  /** Drop `job` from the book — but only if it is still the job under that id. */
  dropJob(rootId: EnvelopeId, job: Job): void {
    if (!isJob(job)) throw new TypeError("dropJob requires a Job");
    if (this.jobs.get(rootId) === job) this.jobs.delete(rootId);
  }

  /** Held replies, keyed by request id */

  holdReply(requestId: EnvelopeId, reply: Lease): void {
    if (!isLease(reply)) throw new TypeError("holdReply requires a Lease");
    this.held.set(requestId, reply);
  }

  takeHeld(requestId: EnvelopeId): Lease | null {
    const reply = this.held.get(requestId);
    if (reply === undefined) return null;
    this.held.delete(requestId);
    return reply;
  }

  releaseHeld(requestId: EnvelopeId): void {
    this.held.delete(requestId);
  }

  clearHeld(): void {
    this.held.clear();
  }

  /** The durable record log's **watermarks** */
  get records(): RecordLogView {
    return this.#records;
  }

  /**
   * Stage one fact in the durable log; `null` means the ring is full.
   *
   * It is called from `ctx.record`'s own hook (and its backpressure retry), so
   * the caller is the runtime's hook plumbing, not application code.
   */
  recordFact(envelope: Envelope, at: number): Record | null {
    return this.#records.record(envelope, at);
  }

  /**
   * Park a wait on the durable watermark reaching `seq`.
   *
   * The write half of `records`' read view: a barrier *moves* nothing itself,
   * but it is a promise that resolves only when the log has been flushed, so it
   * `ctx.barrier`'s hook, never from application code directly.
   */
  recordBarrier(seq: number): Promise<void> {
    return this.#records.barrier(seq);
  }

  armMailboxWait(interest: Interest): void {
    this.mailboxWait = interest;
  }

  takeMailboxWait(): Interest | null {
    const wait = this.mailboxWait;
    this.mailboxWait = null;
    return wait;
  }

  attachHost(host: EffectHost): void {
    this.host = host;
  }

  /**
   * Bind the runtime's clock at admission.
   *
   * Deferred continuations run outside a turn, so they must read the live clock
   * rather than the per-turn instant pinned when their hook was created.
   */
  attachClock(clock: Clock): void {
    this.clock = clock;
  }

  deferWait(wait: Promise<void>): void {
    this.pendingWait = wait;
  }

  /** Timers armed by jobs (owner → deadline). */
  get timers(): ReadonlyArray<{ owner: EnvelopeId; deadline: number }> {
    return this.#timers;
  }

  /**
   * The pending timer waiters (owner → deadline → resolve).
   *
   * Read-only view over a `#` array: a stray `core.timerWaiters = []` would only
   * shadow it with an own slot the class never reads, silently dropping every
   * armed timer's wake-up — the same failure `core.timers.length = 0` caused.
   */
  get timerWaiters(): ReadonlyArray<{
    owner: EnvelopeId;
    deadline: number;
    resolve: () => void;
  }> {
    return this.#timerWaiters;
  }

  get phase(): number {
    return this.$phase;
  }

  /** True while a turn is executing on this host (the reentrancy guard). */
  get busy(): boolean {
    return this.$busy;
  }

  get state(): HostState {
    return this.$state;
  }

  /** Start draining: no new roots are adopted, in-flight work finishes. */
  beginDrain(): void {
    if (this.$state === "running") this.$state = "draining";
  }

  takeCloseRequest(): boolean {
    if (!this.closeRequested) return false;
    this.closeRequested = false;
    return true;
  }

  get rejectedCount(): number {
    return this.$rejectedCount;
  }

  /** Requests whose cancellation is still pending (no live job to cancel yet). */

  /** The letters this host could not settle */
  get abandoned(): Envelope[] | null {
    return this.#abandoned === null ? null : [...this.#abandoned];
  }

  /**
   * Keep a letter this host could not settle (creating the list on demand).
   *
   * Branded: this list is what `close()` hands back to its caller, so the
   * letters leave the runtime and are re-published by whoever closed it.
   */
  keepAbandoned(envelope: Envelope): void {
    if (!isSealed(envelope))
      throw new TypeError("keepAbandoned requires an Envelope");
    if (this.#abandoned === null) this.#abandoned = [];
    this.#abandoned.push(envelope);
  }

  clearAbandoned(): void {
    this.#abandoned = null;
  }

  /** Parked letters beyond the single slot, in FIFO order */

  shiftRest(): [Envelope, Job | null] {
    return this.rest.shift()!;
  }

  takeRest(index: number): [Envelope, Job | null] {
    return this.rest.splice(index, 1)[0]!;
  }

  /** Put a claimed letter back at `index` (delivery failed; retry later). */
  insertRest(index: number, envelope: Envelope, job: Job | null): void {
    if (!isSealed(envelope))
      throw new TypeError("insertRest requires an Envelope");
    this.rest.splice(index, 0, [envelope, job]);
  }

  dropRestLetter(envelope: Envelope): void {
    const id = envelope.header.id;
    const kept = this.rest.filter(([held]) => held.header.id !== id);
    this.rest.length = 0;
    this.rest.push(...kept);
  }

  parkLetter(envelope: Envelope, job: Job | null): void {
    if (!isSealed(envelope))
      throw new TypeError("parkLetter requires an Envelope");
    if (this.pending === null) {
      this.pending = envelope;
      this.pendingJob = job;
    } else {
      this.rest.push([envelope, job]);
    }
  }

  takeParked(): [Envelope, Job | null] | null {
    if (this.pending === null) return null;
    const taken: [Envelope, Job | null] = [this.pending, this.pendingJob];
    this.pending = null;
    this.pendingJob = null;
    return taken;
  }

  restoreParked(envelope: Envelope, job: Job | null): void {
    if (!isSealed(envelope))
      throw new TypeError("restoreParked requires an Envelope");
    this.pending = envelope;
    this.pendingJob = job;
  }

  /**
   * Record one failed delivery attempt. `again` marks the envelope due for
   * exactly one retry — see `noteDeliveryFailure` for the state machine.
   */
  noteAttempt(envelopeId: EnvelopeId, count: number): void {
    this.attempts.set(envelopeId, count);
  }

  clearAttempt(envelopeId: EnvelopeId): void {
    this.attempts.delete(envelopeId);
  }

  markAgain(envelopeId: EnvelopeId): void {
    this.again.add(envelopeId);
  }

  clearAgain(envelopeId: EnvelopeId): void {
    this.again.delete(envelopeId);
  }

  releaseAttempts(): void {
    this.attempts.clear();
    this.again.clear();
  }

  /** Claim the record-sink fault; the runtime surfaces it as a thrown error. */
  takeFault(): Error | null {
    const fault = this.fault;
    this.fault = null;
    return fault;
  }

  takeDeferFault(): Error | null {
    const fault = this.deferFault;
    this.deferFault = null;
    return fault;
  }

  constructor(options: CoreOptions) {
    this.makeJob = options.makeJob;
    this.onRejected = options.onRejected;
    const patternText = [...options.patterns];
    for (const item of patternText) parsePattern(item);
    this.activeLimit = options.activeLimit;
    this.workBudget = options.workBudget;
    this.timeout =
      options.requestTimeoutMs == null
        ? null
        : options.requestTimeoutMs;
    const mailboxChecked = options.mailboxLimit;
    // Validate the queue bound under its own name ("queue_limit must be a
    // positive integer"), not the generic `capacity` the bare `Queue`
    // constructor would report.
    this.#queue = new Queue<Envelope>(options.queueLimit);
    this.sub = new Subscriber(options.actorId, options.receiver, patternText);
    this.signal = readSignal(this.sub.inbox);
    this.mailbox = new Mailbox(mailboxChecked, this.signal);
    this.#records = new RecordLog(options.recordSink ?? new MemorySink(), 64);
  }

  drainRecords(limit = this.#records.pending): number {
    return this.#records.drain(limit);
  }

  flushRecords(seq: number): Promise<void> {
    return this.#records.flush(seq);
  }

  /**
   * Per-turn record settlement: drain the pending ring into the sink, then
   * advance the durable watermark to the last appended seq. Barrier waiters
   * whose target has been reached resolve here, resuming any held post-effects.
   * Returns true when the ring was drained and/or the durable watermark advanced.
   */
  settleRecords(): boolean {
    // Retry a drained-but-unflushed watermark even when the ring is empty:
    // `drain` advances the ring head only after each append succeeds, so a
    // record can be out of the ring yet not durable if a prior flush failed.
    // `pending === 0` alone would short-circuit that retry and leave the
    // watermark (and any barrier on it) stalled forever. The re-drain is a
    // no-op on an empty ring and `flush` is idempotent, so this is safe.
    const pending = this.#records.pending;
    const target = this.#records.lastSeq;
    const durable = this.#records.durable;
    if (pending === 0 && target <= durable) return false;
    this.drainRecords();
    if (target > durable) {
      // Surface a sink flush failure to the scheduler: a failed `flush` aborts
      // the whole turn and the Drive fails over. The
      // old `void …catch(() => undefined)` swallowed the error so a persistently
      // failing sink left its barrier hanging with no visible fault. The
      // turn itself stays synchronous; the fault is armed here and surfaced
      // as a thrown error from `RuntimeAdvance.advance` on a later boundary.
      if (this.#flushInFlight) return true;
      this.#flushInFlight = true;
      void this.flushRecords(target)
        .catch((error: unknown) => {
          this.recordFault(error instanceof Error ? error : new Error(String(error)));
        })
        .finally(() => {
          this.#flushInFlight = false;
        });
      return true;
    }
    return false;
  }

  recordFault(error: Error): void {
    this.fault = error;
    this.signal.notify(WORK);
  }

  waitTimer(deadlineMs: number, owner: EnvelopeId): Promise<void> {
    // Effect landing is the choke point: a raw `Effect` literal can bypass the
    // `ctx.timer` builder, and a NaN here sinks to the heap root, making
    // `nextMs` NaN so no host timer ever fires again. This guards an internal
    // invariant, not an external argument (see README's contract).
    const deadline = safeDeadline(deadlineMs);
    return new Promise((resolve) => {
      this.#timerWaiters.push({ owner, deadline, resolve });
      this.#timers.push({ owner, deadline });
    });
  }

  /**
   * Retract every timer owned by `owner`. Called when a job is dropped
   * (finished, cancelled or closed): its continuation will never run, so both
   * the agenda deadline and the pending waiter are dead weight. The waiter's
   * promise is deliberately left unresolved — its `defer` callback already
   * guards on the owner still being live, and resolving it here would schedule
   * a resume for a job that is gone.
   */
  cancelTimers(owner: EnvelopeId): void {
    this.#retainTimers((timer) => timer.owner !== owner);
  }

  clearTimers(): void {
    this.#retainTimers(() => false);
  }

  /** Filter both timer arrays **in place**. */
  #retainTimers(keep: (timer: { owner: EnvelopeId }) => boolean): void {
    const timers = this.#timers.filter(keep);
    this.#timers.length = 0;
    this.#timers.push(...timers);
    const waiters = this.#timerWaiters.filter(keep);
    this.#timerWaiters.length = 0;
    this.#timerWaiters.push(...waiters);
  }

  takePendingWait(): Promise<void> | null {
    const wait = this.pendingWait;
    this.pendingWait = null;
    return wait;
  }

  defer(wait: Promise<void>, resume: () => void): void {
    void wait.then(
      () => {
        try {
          resume();
        } catch (error) {
          this.deferFault = error instanceof Error ? error : new Error(String(error));
          this.signal.notify(WORK);
          return;
        }
        this.signal.notify(WORK);
      },
      (error: unknown) => {
        this.deferFault = error instanceof Error ? error : new Error(String(error));
        this.signal.notify(WORK);
      },
    );
  }

  get nextTimerMs(): number | null {
    let next: number | null = null;
    for (const timer of this.#timers) {
      if (next === null || timer.deadline < next) next = timer.deadline;
    }
    return next;
  }

  /**
   * Earliest deadline across held leases; `null` when no held lease carries
   * one. Settled/closed leases are excluded so a finished lease does not keep
   * the agenda armed.
   */
  get nextHeldLeaseDeadlineMs(): number | null {
    let next: number | null = null;
    for (const lease of this.held.values()) {
      if (lease.isClosed || lease.isSettled) continue;
      if (lease.deadlineMs !== null) {
        if (next === null || lease.deadlineMs < next) next = lease.deadlineMs;
      }
    }
    return next;
  }

  fireDueTimers(nowMs: number): number {
    let due = 0;
    for (let index = this.#timerWaiters.length - 1; index >= 0; index -= 1) {
      const waiter = this.#timerWaiters[index]!;
      if (waiter.deadline <= nowMs) {
        this.#timerWaiters.splice(index, 1);
        waiter.resolve();
      }
    }
    for (let index = this.#timers.length - 1; index >= 0; index -= 1) {
      if (this.#timers[index]!.deadline <= nowMs) {
        this.#timers.splice(index, 1);
        due += 1;
      }
    }
    if (due > 0) {
      this.signal.notify(TIMER);
    }
    return due;
  }

  chargeLeases(deltaMs: number): void {
    let changed = false;
    for (const lease of this.held.values()) {
      if (lease.isClosed || lease.isSettled) continue;
      if (!lease.consume(deltaMs)) {
        lease.revoke("budget");
        changed = true;
      }
    }
    if (changed) this.signal.notify(WORK);
  }

  expireLeases(nowMs: number): void {
    let changed = false;
    for (const lease of this.held.values()) {
      if (lease.isClosed) continue;
      if (lease.expired(nowMs)) {
        lease.revoke("deadline");
        changed = true;
      }
    }
    if (changed) this.signal.notify(WORK);
  }

  takeCancelled(): EnvelopeId[] {
    return this.leases.takeCancelled();
  }

  revoke(requestIds: EnvelopeId[]): void {
    this.revokeQueued(requestIds);
    this.revokeHeld(requestIds);
    this.revokeActive(requestIds);
  }

  private revokeActive(requestIds: EnvelopeId[]): void {
    for (const requestId of requestIds) {
      const job = this.jobs.get(requestId);
      if (job === undefined) {
        this.cancelPending.delete(requestId);
        continue;
      }
      const state = stateOf(job);
      if (state.lifecycle !== "active" || state.held) {
        this.cancelPending.delete(requestId);
        continue;
      }
      if (!cancelJob(this, job)) this.cancelPending.add(requestId);
    }
  }

  takePendingCancels(): EnvelopeId[] {
    // `advance` drains this set twice per turn for every host, so the empty case
    // must not allocate: `[...set]` costs one array per call even when there is
    // nothing in it (measured: this method was 5% of the samples in a profile of
    // 20 000 asks, almost all of it the spread of an empty set).
    if (this.cancelPending.size === 0) return [];
    const found = [...this.cancelPending];
    this.cancelPending.clear();
    return found;
  }

  private revokeQueued(requestIds: EnvelopeId[]): void {
    if (requestIds.length === 0) return;
    const wanted = new Set(requestIds);
    this.queueRemove((item) => wanted.has(item.header.id));
    for (const requestId of requestIds) remove(this.sub.inbox, requestId);
  }

  private revokeHeld(requestIds: EnvelopeId[]): void {
    for (const requestId of requestIds) {
      const reply = this.held.get(requestId);
      if (reply === undefined || reply.isClosed || reply.take() !== null) continue;
      reply.revoke("cancelled");
    }
  }

  requestClose(): void {
    if (this.$state === "closed" || this.$state === "closing") return;
    this.closeRequested = true;
    this.signal.notify(WORK);
  }

  beginClose(): void {
    if (this.$state !== "closed") this.$state = "closing";
    if (this.#abandoned === null) this.#abandoned = [];
  }

  abandonedMut(): Envelope[] {
    this.beginClose();
    return this.#abandoned as Envelope[];
  }
}
