import type { EnvelopeId } from "../../envelope/ids.ts";
import { makeEnvelope } from "../../envelope/envelope.ts";
import { BackpressureError } from "../../errors.ts";
import { hasResultShape } from "../../envelope/result.ts";
import { nonnegativeInt } from "../../time/numbers.ts";
import type { Context } from "../context.ts";
import { putReply, requireActive } from "../context.ts";
import type { Buffer } from "./buffer.ts";
import { Control, Intent } from "../context.ts";

export function requestIntent(
  buffer: Buffer,
  stage: string,
  topic: string,
  payload: unknown,
  timeout: number | null,
): EnvelopeId {
  if (buffer.control !== null || buffer.job.lifecycle !== "active") {
    throw new Error("job already finished");
  }
  if (buffer.pendingCount + buffer.requests.length >= buffer.job.maxInflight) {
    throw new BackpressureError("max_inflight");
  }
  if (buffer.idGen === null)
    throw new Error("Context is valid only during its callback");
  if (timeout !== null) timeout = nonnegativeInt(timeout, "timeout_ms");
  const request = makeEnvelope({
    topic,
    payload,
    from: buffer.actorId,
    cause: buffer.job.root.header.cause,
    kind: "request",
    replyTo: buffer.replyTo,
    idGen: buffer.idGen,
  });
  buffer.requests.push(new Intent(stage, request, timeout));
  return request.header.id;
}

export function finishControl(buffer: Buffer, body: unknown): void {
  if (buffer.control !== null) return;
  if (body !== null) {
    if (buffer.job.root.header.address.kind === "request") {
      putReply(buffer, body);
    } else if (!hasResultShape(body)) {
      throw new TypeError("invalid result body");
    }
  }
  buffer.control = new Control(
    typeof body === "object" && body !== null && !Array.isArray(body)
      ? (body as Record<string, unknown>)
      : null,
  );
}

export function mutate<T>(ctx: Context, action: (buffer: Buffer) => T): T {
  const buffer = requireActive(ctx);
  if (buffer.busy) throw new Error("reentrant Context mutation");
  buffer.busy = true;
  try {
    return action(buffer);
  } finally {
    buffer.busy = false;
  }
}
