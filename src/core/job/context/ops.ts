import { makeEnvelope, hasResultShape } from "#src/core/envelope.js";
import { BackpressureError } from "#src/core/errors.js";

import type { Context } from "#src/core/job/context.js";
import { putReply, requireActive, Control, Intent } from "#src/core/job/context.js";
import type { Buffer } from "#src/core/job/context/buffer.js";
import {
  beginMutation,
  bufferMint,
  endMutation,
  setControl,
} from "#src/core/job/context/buffer.js";
import { Lease, bindLease } from "#src/core/job/lease.js";

/**
 * 校验一个 ask 意图是否可提交。
 *
 * 纯 builder（`ctx.ask`）在建 Effect 时只会看到「当时」的 buffer；而 handler
 * 可以返回一整串 Effect（如 `[finish, ask]` 或两个 ask），真正的最终组合要到
 * `applyEffects` 按序落地才成形。因此这里在 apply 阶段再校一遍：此时
 * `buffer.requests` 已包含前面已 apply 的 ask，`control/hold/lifecycle` 也已
 * 反映前面 effect 的结果。这是保证最终组合合法的唯一权威校验点。
 */
export function assertAskAllowed(buffer: Buffer): void {
  if (buffer.hold !== null) throw new Error("job already held");
  if (buffer.control !== null || buffer.job.lifecycle !== "active") {
    throw new Error("job already finished");
  }
  if (buffer.pendingCount + buffer.requests.length >= buffer.job.maxInflight) {
    throw new BackpressureError("max_inflight");
  }
  if (buffer.idGen === null)
    throw new Error("Context is valid only during its callback");
}

/**
 * 校验 hold 是否可提交（与 `prepareHold` 同一组前置条件），用于 apply 阶段。
 * 纯 `ctx.holdEffect` 同理：builder 时看不到后续 effect，须在落地时复核，
 * 避免「hold 后仍有 finish/ask」这类组合静默丢请求。
 */
export function assertHoldAllowed(buffer: Buffer): void {
  if (buffer.hold !== null || buffer.job.held) throw new Error("job already held");
  if (buffer.control !== null || buffer.job.lifecycle !== "active")
    throw new Error("job already finished");
  if (buffer.job.root.header.address.kind !== "request")
    throw new TypeError("root is not a request");
  if (buffer.reply !== null || buffer.job.replied)
    throw new Error("root already replied");
  if (buffer.requests.length > 0 || buffer.pendingCount > 0)
    throw new Error("cannot hold with pending requests");
  if (buffer.idGen === null)
    throw new Error("Context is valid only during its callback");
}

export function buildIntent(
  buffer: Buffer,
  stage: string,
  topic: string,
  payload: unknown,
  timeout: number | null,
  extra = 0,
): Intent {
  if (buffer.hold !== null) throw new Error("job already held");
  if (buffer.control !== null || buffer.job.lifecycle !== "active") {
    throw new Error("job already finished");
  }
  if (buffer.pendingCount + buffer.requests.length + extra >= buffer.job.maxInflight) {
    throw new BackpressureError("max_inflight");
  }
  const request = makeEnvelope({
    topic,
    payload,
    from: buffer.actorId,
    cause: buffer.job.root.header.cause,
    kind: "request",
    replyTo: buffer.replyTo,
    idGen: buffer.idGen!,
  });
  return new Intent(stage, request, timeout, bufferMint(buffer));
}

/**
 * Install the control block for a `finish` effect.
 *
 * `body === undefined` is normalised to `null` **before** any validation, and
 * that is not cosmetic. `ctx.finish(body?: unknown)` advertises the argument as
 * optional, so `ctx.finish()` is the natural ts spelling of "finish without an
 */
export function finishControl(buffer: Buffer, body: unknown): void {
  const chosen = body === undefined ? null : body;
  if (buffer.hold !== null) throw new Error("job already held");
  if (buffer.control !== null) {
    // A silent finish already declared "no answer"; a later finish with a body
    // would contradict it. A repeated silent finish, or a finish repeating the
    // same body, stays a no-op.
    if (buffer.control.body === null && chosen !== null)
      throw new Error("job already finished");
    return;
  }
  if (chosen !== null) {
    if (buffer.job.root.header.address.kind === "request") {
      putReply(buffer, chosen);
    } else if (!hasResultShape(chosen)) {
      throw new TypeError("invalid result body");
    }
  }
  setControl(buffer, 
    new Control(
      // `Control`'s constructor takes the owned copy itself (`{
      // ...body }`), so this call site does not copy a second time: reverting a
      // copy here to a plain pass-through left all 964 arms green (measured), and
      // the guarantee belongs in the value type, where a directly-built
      // `new Control(live)` is sealed the same way.
      typeof chosen === "object" && chosen !== null && !Array.isArray(chosen)
        ? (chosen as Record<string, unknown>)
        : null,
    ),
  );
}

export function mutate<T>(ctx: Context, action: (buffer: Buffer) => T): T {
  const buffer = requireActive(ctx);
  if (buffer.busy) throw new Error("reentrant Context mutation");
  beginMutation(buffer);
  try {
    return action(buffer);
  } finally {
    endMutation(buffer);
  }
}

export function prepareHold(buffer: Buffer, options: import("#src/core/job/lease.js").LeaseOptions = {}): Lease {
  assertHoldAllowed(buffer);
  const lease = new Lease(options);
  // Only `Context::hold` stamps the owner; a caller-built `new Lease()` stays
  // unbound and is refused at apply time.
  bindLease(lease, buffer.job);
  return lease;
}
