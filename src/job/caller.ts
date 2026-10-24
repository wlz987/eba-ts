import { Envelope } from "../envelope/envelope.ts";
import { makeEnvelope } from "../envelope/envelope.ts";
import type { IdGen } from "../envelope/idgen.ts";
import type { ActorId, EnvelopeId } from "../envelope/ids.ts";
import { Completion, errPayload } from "../envelope/result.ts";
import { splitTopic } from "../envelope/topic.ts";
import { nonnegativeInt } from "../time/numbers.ts";
import { BackpressureError, MailboxFull, REENTRANT } from "../errors.ts";
import { JobHost } from "./host.ts";
import { coreOf } from "./host.ts";
import { Job } from "./job.ts";
import type { Receiver } from "../mail/inbox.ts";
import { canEnqueue, enqueue, markRead, replyTopic } from "../mail/inbox.ts";
import type { Core } from "./core.ts";
import { Call, Wait, settle } from "./call.ts";
export { Call, settle, settled } from "./call.ts";

const ADMIT = "sys.admit";

export class Caller extends JobHost {
  readonly idGen: IdGen;
  constructor(receiver: Receiver, idGen: IdGen, activeLimit = 256, actorId = "caller") {
    super({
      actorId,
      receiver,
      patterns: [],
      activeLimit,
    });
    this.idGen = idGen;
  }

  override makeJob(root: Envelope): Job {
    return new Wait(root);
  }

  override onRejected(root: Envelope, reason: string): void {
    settle(root, errPayload(reason), root.header.id);
  }

  admit(options: {
    topic: string;
    payload: unknown;
    from: ActorId | string;
    timeoutMs?: number | null;
    pay?: ((completion: Completion) => void) | null;
  }): Envelope {
    const core = coreOf(this);
    if (core.busy) throw new Error(REENTRANT);
    const inbox = core.sub.inbox;
    if (inbox.isClosed) throw new Error("inbox closed");
    if (core.state === "draining") throw new Error("host draining");
    if (core.state !== "running") throw new Error("host closing");
    if (options.timeoutMs !== undefined && options.timeoutMs !== null) {
      nonnegativeInt(options.timeoutMs, "timeout_ms");
    }
    splitTopic(options.topic);
    if (this.crowded()) throw new BackpressureError("active_full");
    if (!canEnqueue(inbox, "request"))
      throw new MailboxFull("target inbox full");
    const request = makeEnvelope({
      topic: ADMIT,
      payload: new Call(
        options.topic,
        options.payload,
        options.timeoutMs ?? null,
        options.pay ?? null,
      ),
      from: options.from,
      idGen: this.idGen,
      kind: "request",
      replyTo: replyTopic(inbox),
    });
    if (!enqueue(inbox, request)) throw new MailboxFull("target inbox full");
    markRead(inbox);
    return request;
  }

  cancel(rootId: EnvelopeId | string): boolean {
    const core = coreOf(this);
    if (core.busy) throw new Error(REENTRANT);
    if (core.state === "closing" || core.state === "closed")
      throw new Error("host closing");
    const job = core.jobs.get(rootId as EnvelopeId) ?? null;
    if (job !== null) return end(this, core, job);
    return mark(this, rootId);
  }
}

function end(caller: Caller, core: Core, job: Job): boolean {
  const call = job.root.payload;
  if (!(call instanceof Call) || call.requestId === null) return false;
  const inbox = core.sub.inbox;
  if (inbox.isClosed) throw new Error("inbox closed");
  if (inbox.remaining() < 1) {
    markRead(inbox);
    throw new MailboxFull("target inbox full");
  }
  if (
    !enqueue(
      inbox,
      makeEnvelope({
        topic: replyTopic(inbox),
        payload: errPayload("cancelled"),
        from: core.sub.actorId,
        cause: job.root.header.cause,
        kind: "result",
        correlationId: call.requestId,
        idGen: caller.idGen,
      }),
    )
  ) {
    markRead(inbox);
    throw new MailboxFull("target inbox full");
  }
  markRead(inbox);
  return true;
}

function mark(caller: Caller, rootId: EnvelopeId | string): boolean {
  const found = caller.acceptedRoot(rootId);
  if (found === null || !(found.payload instanceof Call)) return false;
  found.payload.cancelled = true;
  return true;
}
