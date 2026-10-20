import type { Envelope } from "../envelope/envelope.ts";
import type { ActorId, EnvelopeId } from "../envelope/ids.ts";
import type { Receiver } from "../mail/inbox.ts";
import { readSignal } from "../mail/inbox.ts";
import { Queue } from "../mail/queue.ts";
import { Subscriber } from "../mail/subscriber.ts";
import { parsePattern } from "../pattern/pattern.ts";
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
  readonly book = new RequestBook();
  phase = 0;
  busy = false;
  state: HostState = "running";
  rejectedCount = 0;
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

  beginClose(): Envelope[] {
    if (this.state !== "closed") this.state = "closing";
    if (this.abandoned === null) this.abandoned = [];
    return this.abandoned;
  }
}
