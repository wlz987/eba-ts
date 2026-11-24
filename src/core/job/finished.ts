import type { Core } from "#src/core/job/core.js";
import type { Effect } from "#src/core/job/effect.js";

/**
 * Apply the effects a finished Job may still emit (no asks/holds/replies).
 * `barrier`/`timer` are rejected: a finished job has no continuation to wake,
 * so accepting them would leave a stale wait on the host or silently do nothing.
 */
export function applyFinished(core: Core, effects: readonly Effect[]): void {
  const host = core.host;
  if (host === null) throw new Error("Effect host is not attached");
  for (const effect of effects) {
    switch (effect.kind) {
      case "tell": host.tell(effect.topic, effect.payload); break;
      case "record": host.record(effect.topic, effect.payload); break;
      case "close": host.close(effect.reason); break;
      // Every kind except tell/record/close is illegal here — list them all so
      // `default` is genuinely `never`, so adding a kind is a compile error.
      case "barrier":
      case "timer":
      case "ask":
      case "reply":
      case "hold":
      case "finish":
      case "settle":
      case "revoke":
        throw new Error(`unsupported onFinished effect: ${effect.kind}`);
      default: {
        const unhandled: never = effect;
        throw new Error(`unsupported onFinished effect: ${String((unhandled as Effect).kind)}`);
      }
    }
  }
}
