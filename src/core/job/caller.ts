import { Envelope, makeEnvelope, Completion, errPayload, splitTopic } from "#src/core/envelope.js";
import type { IdGen, ActorId, EnvelopeId } from "#src/core/envelope.js";

import { BackpressureError, MailboxFull, REENTRANT } from "#src/core/errors.js";
import { JobHost, coreOf } from "#src/core/job/host.js";
import { Job } from "#src/core/job/job.js";
import type { Receiver } from "#src/core/mail/inbox.js";
import { canEnqueue, enqueue, markRead, replyTopic } from "#src/core/mail/inbox.js";
import type { Core } from "#src/core/job/core.js";
import { Call, Wait, settle, settleCall } from "#src/core/job/call.js";
import { drop } from "#src/core/job/ending.js";
export { Call, settle } from "#src/core/job/call.js";

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

  revoke(rootId: EnvelopeId | string): boolean {
    const core = coreOf(this);
    if (core.busy) throw new Error(REENTRANT);
    if (core.state === "closing" || core.state === "closed")
      throw new Error("host closing");
    const job = core.jobs.get(rootId as EnvelopeId) ?? null;
    if (job !== null) return end(this, core, job);
    return mark(this, rootId);
  }
}

function end(_caller: Caller, core: Core, job: Job): boolean {
  const call = job.root.payload;
  if (!(call instanceof Call) || call.requestId === null) return false;
  core.leases.cancel(job.root.header.id);
  drop(core, job);
  markRead(core.sub.inbox);
  settleCall(call, new Completion(call.requestId, errPayload("cancelled")));
  return true;
}

function mark(caller: Caller, rootId: EnvelopeId | string): boolean {
  const found = caller.acceptedRoot(rootId);
  if (found === null || !(found.payload instanceof Call)) return false;
  found.payload.cancelled = true;
  return true;
}
