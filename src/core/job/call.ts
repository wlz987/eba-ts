import { Envelope, Completion, errPayload } from "#src/core/envelope.js";
import type { EnvelopeId } from "#src/core/envelope.js";
import { Job } from "#src/core/job/job.js";
import type { CompletionLike, Context } from "#src/core/job/job.js";
import type { Effect } from "#src/core/job/effect.js";

const STAGE = "reply";

export class Call {
  /** The three routing inputs, pinned as ECMAScript `#` fields. */
  readonly topic: string;
  readonly payload: unknown;
  readonly timeoutMs: number | null;
  requestId: EnvelopeId | null = null;
  /** The one-shot completion behind `pending.ready`. */
  completion: Completion | null = null;
  cancelled = false;
  /** The settlement callback, pinned as an ECMAScript `#` field. */
  #pay: ((completion: Completion) => void) | null;
  constructor(
    topic: string,
    payload: unknown,
    timeoutMs: number | null,
    pay: ((completion: Completion) => void) | null = null,
  ) {
    this.topic = topic;
    this.payload = payload;
    this.timeoutMs = timeoutMs;
    this.#pay = pay;
  }
  /** The recorded completion, once settled. */
  $settle(completion: Completion): void {
    if (this.completion !== null) return;
    this.completion = completion;
    const pay = this.#pay;
    if (pay !== null) {
      this.#pay = null;
      pay(completion);
    }
  }
}

export function settleCall(value: Call, completion: Completion): void {
  value.$settle(completion);
}

export function settle(
  root: Envelope,
  body: Record<string, unknown>,
  requestId: EnvelopeId,
): void {
  const call = root.payload;
  if (!(call instanceof Call)) return;
  settleCall(call, new Completion(call.requestId ?? requestId, body));
}

export class Wait extends Job {
  override begin(ctx: Context): readonly Effect[] {
    const call = ctx.root.payload;
    if (!(call instanceof Call))
      throw new TypeError("admitted root has no call");
    if (call.cancelled) {
      settleCall(call, new Completion(ctx.root.header.id, errPayload("cancelled")));
      return [];
    }
    const ask = ctx.ask({
      stage: STAGE,
      topic: call.topic,
      payload: call.payload,
      timeoutMs: call.timeoutMs,
    });
    call.requestId = ask.intent.request.header.id;
    return [ask];
  }

  override onStageResult(
    ctx: Context,
    _stage: string,
    completion: CompletionLike,
  ): readonly Effect[] {
    const call = ctx.root.payload;
    if (call instanceof Call) settleCall(call, completion as Completion);
    return [];
  }

  override onFinished(body: Record<string, unknown> | null): void {
    if (body === null) return;
    settle(this.root, body, this.root.header.id);
  }
}

