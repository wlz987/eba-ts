import { Envelope } from "#src/core/envelope.js";
import type { ActorId, EnvelopeId } from "#src/core/envelope.js";
import type { Bus } from "#src/core/mail/bus.js";
import type { Clock } from "#src/core/time.js";
import type { Receiver } from "#src/core/mail/inbox.js";
import type { Job } from "#src/core/job/job.js";
import { Core } from "#src/core/job/core.js";
import type { RecordSink } from "#src/core/record.js";
import { HostView } from "#src/core/job/host_view.js";
import { closeHost } from "#src/core/job/close.js";

export interface HostOptions {
  actorId: ActorId | string;
  receiver: Receiver;
  patterns: readonly string[];
  requestTimeoutMs?: number | null;
  queueLimit?: number;
  activeLimit?: number;
  workBudget?: number;
  mailboxLimit?: number;
  recordSink?: RecordSink;
}

export class JobHost extends HostView {
  /** The host's books, pinned as an ECMAScript `#` field. */
  readonly $core: Core;

  get core(): Core {
    return this.$core;
  }

  constructor(options: HostOptions) {
    super();
    const hostRef = new WeakRef(this);
    const makeJob = (root: Envelope): Job => {
      const found = hostRef.deref();
      if (found === undefined) throw new Error("invalid JobHost");
      return found.makeJob(root);
    };
    const onRejected = (root: Envelope, reason: string): unknown => {
      const found = hostRef.deref();
      if (found === undefined) return undefined;
      return found.onRejected(root, reason);
    };
    this.$core = new Core({
      actorId: options.actorId,
      receiver: options.receiver,
      patterns: options.patterns,
      requestTimeoutMs: options.requestTimeoutMs ?? null,
      queueLimit: options.queueLimit ?? 512,
      activeLimit: options.activeLimit ?? 256,
      workBudget: options.workBudget ?? 64,
      mailboxLimit: options.mailboxLimit ?? 512,
      recordSink: options.recordSink,
      makeJob,
      onRejected,
    });
  }

  /**
   * Hand `$core` to the sibling accessor `coreOf` — see `hostRead` below.
   *
   */

  onStep(_clock: Clock): void {}
  close(bus: Bus): Envelope[] {
    const core = this.$core;
    if (core.signal.isBound)
      throw new Error("runtime owns host; use Runtime.close")
    const abandoned = closeHost(core, bus);
    core.clearAbandoned();
    return abandoned;
  }
}

/**
 * Hand a host's `$core` to the sibling modules that drive it.
 *
 * returns the live `Core` because the runtime mutates it, but the class no
 * longer hands out the symbol that reaches it.
 */
export function readCore(value: JobHost): Core {
  return value.$core;
}

export function coreOf(host: JobHost): Core {
  return readCore(host);
}

export function queued(
  queue: ReadonlyArray<Envelope>,
  envelopeId: EnvelopeId | string,
): Envelope | null {
  for (const item of queue) {
    if (item.header.id === envelopeId) return item;
  }
  return null;
}
