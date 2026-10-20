import type { Envelope } from "../envelope/envelope.ts";
import type { EnvelopeId } from "../envelope/ids.ts";
import { positiveInt } from "../time/numbers.ts";
import type { Context, Control } from "./context.ts";
import { states, stateOf } from "./context.ts";
export { Commit, Context, Control, Intent, sealContext, stateOf } from "./context.ts";

export type Lifecycle = "new" | "active" | "delivering" | "finished";
export type Notice = "open" | "rejected" | "finished";

const edges = new Set<string>([
  "new active",
  "active delivering",
  "active finished",
  "delivering finished",
  "finished finished",
]);

export function become(state: State, lifecycle: Lifecycle): void {
  if (!edges.has(`${state.lifecycle} ${lifecycle}`)) {
    throw new Error(
      `illegal job transition: ${state.lifecycle} -> ${lifecycle}`,
    );
  }
  state.lifecycle = lifecycle;
}

export class State {
  readonly root: Envelope;
  readonly maxInflight: number;
  lifecycle: Lifecycle = "new";
  notice: Notice = "open";
  replied = false;
  replyBody: Record<string, unknown> | null = null;
  pendingControl: Control | null = null;
  constructor(root: Envelope, maxInflight: number) {
    this.root = root;
    this.maxInflight = maxInflight;
  }
}

export class Job {
  constructor(root: Envelope, maxInflight = 100) {
    states.set(this, new State(root, positiveInt(maxInflight, "max_inflight")));
  }
  get root(): Envelope {
    return stateOf(this).root;
  }
  get finished(): boolean {
    return stateOf(this).lifecycle === "finished";
  }
  begin(_ctx: Context): void {}
  onStageResult(
    _ctx: Context,
    _stage: string,
    _completion: CompletionLike,
  ): void {}
  onFinished(_body: Record<string, unknown> | null): void {}
}

export interface CompletionLike {
  readonly requestId: EnvelopeId;
  readonly body: Record<string, unknown> | null;
}
