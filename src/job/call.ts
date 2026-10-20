import { Envelope } from "../envelope/envelope.ts";
import type { EnvelopeId } from "../envelope/ids.ts";
import { Completion, errPayload } from "../envelope/result.ts";
import { Job } from "./job.ts";
import type { CompletionLike, Context } from "./job.ts";

const STAGE = "reply";

export class Call {
  readonly topic: string;
  readonly payload: unknown;
  readonly timeoutMs: number | null;
  requestId: EnvelopeId | null = null;
  completion: Completion | null = null;
  cancelled = false;
  pay: ((completion: Completion) => void) | null;
  constructor(
    topic: string,
    payload: unknown,
    timeoutMs: number | null,
    pay: ((completion: Completion) => void) | null = null,
  ) {
    this.topic = topic;
    this.payload = payload;
    this.timeoutMs = timeoutMs;
    this.pay = pay;
  }
  complete(completion: Completion): void {
    if (this.completion !== null) return;
    this.completion = completion;
    const pay = this.pay;
    if (pay !== null) {
      this.pay = null;
      pay(completion);
    }
  }
}

export function settle(
  root: Envelope,
  body: Record<string, unknown>,
  requestId: EnvelopeId,
): void {
  const call = root.payload;
  if (!(call instanceof Call)) return;
  call.complete(new Completion(call.requestId ?? requestId, body));
}

export function settled(envelope: Envelope): Completion | null {
  const call = envelope.payload;
  if (!(call instanceof Call)) return null;
  return call.completion;
}

export class Wait extends Job {
  override begin(ctx: Context): void {
    const call = ctx.root.payload;
    if (!(call instanceof Call))
      throw new TypeError("admitted root has no call");
    if (call.cancelled) {
      this.onStageResult(
        ctx,
        STAGE,
        new Completion(ctx.root.header.id, errPayload("cancelled")),
      );
      return;
    }
    call.requestId = ctx.request({
      stage: STAGE,
      topic: call.topic,
      payload: call.payload,
      timeoutMs: call.timeoutMs,
    });
  }

  override onStageResult(
    ctx: Context,
    _stage: string,
    completion: CompletionLike,
  ): void {
    const call = ctx.root.payload;
    if (call instanceof Call) call.complete(completion as Completion);
  }

  override onFinished(body: Record<string, unknown> | null): void {
    if (body === null) return;
    settle(this.root, body, this.root.header.id);
  }
}

