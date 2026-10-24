import { cancelJob } from "./cancel.ts";
import { Envelope } from "../envelope/envelope.ts";
import { remove } from "../mail/inbox.ts";
import type { ActorId, EnvelopeId } from "../envelope/ids.ts";
import type { Receiver } from "../mail/inbox.ts";
import { readSignal } from "../mail/inbox.ts";
import { Queue } from "../mail/queue.ts";
import { stateOf } from "./context.ts";
import { Subscriber } from "../mail/subscriber.ts";
import { parsePattern } from "../pattern/pattern.ts";
import type { Reply } from "./reply.ts";
import { nonnegativeInt, positiveInt } from "../time/numbers.ts";
import type { Job } from "./job.ts";
import { RequestBook } from "./book.ts";
import { Outbox } from "./outbox.ts";

export type HostState = "running" | "draining" | "closing" | "closed";

export interface CoreOptions {
  actorId: ActorId | string;
  receiver: Receiver;
  patterns: readonly string[];
  requestTimeoutMs?: number | null;
  queueLimit: number;
  activeLimit: number;
  workBudget: number;
  outboxLimit: number;
  makeJob: (root: Envelope) => Job;
  onRejected: (root: Envelope, reason: string) => unknown;
}

export class Core {
  readonly makeJob: (root: Envelope) => Job;
  readonly onRejected: (root: Envelope, reason: string) => unknown;
  readonly activeLimit: number;
  readonly workBudget: number;
  readonly timeout: number | null;
  readonly queue: Queue;
  readonly sub: Subscriber;
  readonly signal: ReturnType<typeof readSignal>;
  readonly outbox: Outbox;
  readonly jobs = new Map<EnvelopeId, Job>();
  readonly held = new Map<EnvelopeId, Reply>();
  readonly book = new RequestBook();
  phase = 0;
  busy = false;
  state: HostState = "running";
  rejectedCount = 0;
  cancelErrors = 0;
  lastCancelError: string | null = null;
  readonly cancelPending = new Set<EnvelopeId>();
  abandoned: Envelope[] | null = null;
  pending: Envelope | null = null;
  pendingJob: Job | null = null;
  rest: Array<[Envelope, Job | null]> = [];
  readonly again = new Set<Envelope>();
  readonly attempts = new Map<Envelope, number>();

  constructor(options: CoreOptions) {
    this.makeJob = options.makeJob;
    this.onRejected = options.onRejected;
    const patternText = [...options.patterns];
    for (const item of patternText) parsePattern(item);
    this.activeLimit = positiveInt(options.activeLimit, "active_limit");
    this.workBudget = positiveInt(options.workBudget, "work_budget");
    this.timeout =
      options.requestTimeoutMs == null
        ? null
        : nonnegativeInt(options.requestTimeoutMs, "request_timeout_ms");
    const outboxChecked = positiveInt(options.outboxLimit, "outbox_limit");
    this.queue = new Queue(options.queueLimit);
    this.sub = new Subscriber(options.actorId, options.receiver, patternText);
    this.signal = readSignal(this.sub.inbox);
    this.outbox = new Outbox(outboxChecked, this.signal);
  }

  takeCancelled(): EnvelopeId[] {
    return this.book.takeCancelled();
  }

  cancelJobs(requestIds: EnvelopeId[]): void {
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
    const found = [...this.cancelPending];
    this.cancelPending.clear();
    return found;
  }

  cancelQueued(requestIds: EnvelopeId[]): void {
    if (requestIds.length === 0) return;
    const wanted = new Set(requestIds);
    this.queue.remove((item) => item instanceof Envelope && wanted.has(item.header.id));
    for (const requestId of requestIds) remove(this.sub.inbox, requestId);
  }

  cancelHeld(requestIds: EnvelopeId[]): void {
    for (const requestId of requestIds) {
      const reply = this.held.get(requestId);
      if (reply === undefined || reply.isClosed || reply.take() !== null) continue;
      reply.fail("cancelled");
    }
  }

  beginClose(): Envelope[] {
    if (this.state !== "closed") this.state = "closing";
    if (this.abandoned === null) this.abandoned = [];
    return this.abandoned;
  }
}
