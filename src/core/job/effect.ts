import type { Context, Intent } from "#src/core/job/context.js";
import type { Lease } from "#src/core/job/lease.js";
import { leaseIssuedFor } from "#src/core/job/lease.js";
import { bufferOf, putReply } from "#src/core/job/context.js";
import { pushRequest, setHold } from "#src/core/job/context/buffer.js";
import { markHeld } from "#src/core/job/job.js";
import { assertAskAllowed, assertHoldAllowed, finishControl } from "#src/core/job/context/ops.js";

/** Effect 字段表：kind → 载荷；mapped type 一次生成联合与具名别名。 */
interface EffectFields {
  ask: { readonly intent: Intent };
  reply: { readonly body: unknown };
  hold: { readonly lease: Lease };
  finish: { readonly body: unknown };
  settle: { readonly lease: Lease; readonly body: unknown };
  revoke: { readonly lease: Lease; readonly error?: string };
  tell: { readonly topic: string; readonly payload: unknown };
  record: { readonly topic: string; readonly payload: unknown };
  barrier: { readonly upTo?: number };
  timer: { readonly deadlineMs: number };
  close: { readonly reason?: string };
}

export type Effect = {
  readonly [K in keyof EffectFields]: { readonly kind: K } & EffectFields[K];
}[keyof EffectFields];

export type AskEffect = Extract<Effect, { kind: "ask" }>;
export type ReplyEffect = Extract<Effect, { kind: "reply" }>;
export type FinishEffect = Extract<Effect, { kind: "finish" }>;
export type TellEffect = Extract<Effect, { kind: "tell" }>;
export type RecordEffect = Extract<Effect, { kind: "record" }>;
export type BarrierEffect = Extract<Effect, { kind: "barrier" }>;
export type TimerEffect = Extract<Effect, { kind: "timer" }>;
export type CloseEffect = Extract<Effect, { kind: "close" }>;

/** Runtime capabilities that are not part of the Job's decision. */
export interface EffectHost {
  tell(topic: string, payload: unknown): void;
  record(topic: string, payload: unknown): void;
  barrier(upTo?: number): void;
  timer(deadlineMs: number): void;
  close(reason?: string): void;
}

/** Refuse a lease that `Context::hold` did not issue for this job. */
function assertLeaseIssued(buffer: ReturnType<typeof bufferOf>, lease: Lease): void {
  if (!leaseIssuedFor(lease, buffer.job)) {
    throw new Error("lease was not issued for this job");
  }
}

/** A settle/revoke may not write a slot that this turn already answered. */
function assertNoDeclaredAnswer(buffer: ReturnType<typeof bufferOf>): void {
  if (buffer.reply !== null) throw new Error("root already replied");
}

/** Normalize a hook's returned effect list to an array. */
export function toEffectArray(value: unknown): Effect[] | null {
  if (value === undefined || value === null) return null;
  if (Array.isArray(value)) return value as Effect[];
  return Array.from(value as Iterable<Effect>);
}

export function applyEffects(ctx: Context, effects: readonly Effect[]): void {
  const buffer = bufferOf(ctx);
  for (let index = 0; index < effects.length; index += 1) {
    const effect = effects[index]!;
    if ((effect.kind === "barrier" || effect.kind === "timer") && index + 1 < effects.length)
      throw new Error("barrier/timer must be the last effect");
    // `hold` must lead. `ctx.hold()` carries the same rule but cannot see an
    // effect staged through the pure builders (they return without pushing), nor
    // a literal returned from the hook — the order is only visible here. The
    // predicate is "a hold preceded by a *non*-hold", not `index !== 0`: two
    // holds is a different mistake and `assertHoldAllowed` names it `job already
    // held`, which an index rule would have masked.
    if (effect.kind === "hold" && index !== 0 && effects[index - 1]!.kind !== "hold")
      throw new Error("cannot hold after other effects");
    switch (effect.kind) {
      case "ask":
        // 落地阶段的权威校验：此时 buffer 已反映前面 effect 的结果（control/
        // hold/lifecycle/requests）。若前面已 finish 或 hold，或 maxInflight
        // 已被用尽，直接抛错——而不是把意图静默塞进 requests 后在 commitTo
        // 因 control 已置而丢弃。纯 builder 只能看到「当时」的 buffer，最终
        // 组合的合法性必须在这里定夺。
        assertAskAllowed(buffer);
        pushRequest(buffer, effect.intent);
        break;
      case "reply":
        putReply(buffer, effect.body);
        break;
      case "hold":
        assertHoldAllowed(buffer);
        assertLeaseIssued(buffer, effect.lease);
        setHold(buffer, effect.lease);
        markHeld(buffer.job, true);
        buffer.core.holdReply(buffer.job.root.header.id, effect.lease);
        break;
      case "finish":
        finishControl(buffer, effect.body);
        break;
      case "settle":
        assertNoDeclaredAnswer(buffer);
        assertLeaseIssued(buffer, effect.lease);
        effect.lease.settle(effect.body);
        break;
      case "revoke":
        assertNoDeclaredAnswer(buffer);
        assertLeaseIssued(buffer, effect.lease);
        effect.lease.revoke(effect.error);
        break;
      case "tell":
        host(buffer).tell(effect.topic, effect.payload);
        break;
      case "record":
        host(buffer).record(effect.topic, effect.payload);
        break;

      case "close":
        host(buffer).close(effect.reason);
        break;
      case "barrier":
        host(buffer).barrier(effect.upTo);
        break;
      case "timer":
        host(buffer).timer(effect.deadlineMs);
        break;
      default: {
        const unhandled: never = effect;
        throw new Error(`unsupported effect: ${String((unhandled as Effect).kind)}`);
      }
    }
  }
}

function host(buffer: { readonly host: EffectHost | null }): EffectHost {
  if (buffer.host === null) throw new Error("Effect host is not attached");
  return buffer.host;
}
