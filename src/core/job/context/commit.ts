import type { Control, Context, Intent } from "#src/core/job/context.js";
import type { Effect } from "#src/core/job/effect.js";
import type { Lease } from "#src/core/job/lease.js";
import { bufferOf } from "#src/core/job/context.js";
import { invalidate } from "#src/core/job/context/buffer.js";

/**
 * A sealed context snapshot: everything one hook decided, in one value.
 * `commitTo` submits each `requests` intent, answers with `reply`, installs
 * `control` and records `hold`.
 */
export class Commit {
  readonly requests: readonly Intent[];
  readonly reply: Record<string, unknown> | null;
  readonly control: Control | null;
  readonly hold: Lease | null;
  readonly effects: readonly Effect[];
  constructor(
    requests: Intent[],
    reply: Record<string, unknown> | null,
    control: Control | null,
    hold: Lease | null = null,
    effects: readonly Effect[] = [],
  ) {
    this.requests = requests;
    this.reply = reply === null ? null : { ...reply };
    this.control = control;
    this.hold = hold;
    this.effects = effects;
  }
}

export function sealContext(ctx: Context): Commit {
  const buffer = bufferOf(ctx);
  invalidate(buffer);
  // The copies live in `Commit`'s constructor (one place, so a directly-built
  // `Commit` is sealed the same way); here only the *arrays* need copying, so
  // freezing the container does not freeze a live buffer array the runtime
  // still owns.
  return new Commit(
    [...buffer.requests],
    buffer.reply,
    buffer.control,
    buffer.hold,
    ctx.takeEffects(),
  );
}
