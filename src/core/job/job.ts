import type { Envelope, EnvelopeId } from "#src/core/envelope.js";

import type { Context, Control } from "#src/core/job/context.js";
import type { Effect } from "#src/core/job/effect.js";
export { Commit, Context, Control, sealContext, stateOf } from "#src/core/job/context.js";

export type Lifecycle = "new" | "active" | "delivering" | "finished";
export type Notice = "open" | "rejected" | "finished";

function legal(from: Lifecycle, to: Lifecycle): boolean {
  if (from === "new") return to === "active";
  if (from === "active") return to === "delivering" || to === "finished";
  if (from === "delivering") return to === "finished";
  return to === "finished";
}

export function become(state: State, lifecycle: Lifecycle): void {
  const from = state.lifecycle;
  if (!legal(from, lifecycle)) {
    throw new Error(`illegal job transition: ${from} -> ${lifecycle}`);
  }
  state.$lifecycle = lifecycle;
}

export class State {
  readonly root: Envelope;
  readonly maxInflight: number;
  $lifecycle: Lifecycle = "new";
  $notice: Notice = "open";
  $replied = false;
  $held = false;
  $replyBody: Record<string, unknown> | null = null;
  $pendingControl: Control | null = null;

  constructor(root: Envelope, maxInflight: number) {
    this.root = root;
    this.maxInflight = maxInflight;
  }

  get lifecycle(): Lifecycle {
    return this.$lifecycle;
  }

  get notice(): Notice {
    return this.$notice;
  }

  get replied(): boolean {
    return this.$replied;
  }

  get held(): boolean {
    return this.$held;
  }

  get replyBody(): Record<string, unknown> | null {
    return this.$replyBody;
  }

  get pendingControl(): Control | null {
    return this.$pendingControl;
  }

  }

export class Job {
  /** The job's decision record, pinned as an ECMAScript `#` field. */
  readonly $state: State;

  constructor(root: Envelope, maxInflight = 100) {
    this.$state = new State(root, maxInflight);
  }

  get root(): Envelope {
    return this.$state.root;
  }
  get finished(): boolean {
    return this.$state.lifecycle === "finished";
  }
  begin(_ctx: Context): readonly Effect[] | void {}
  onStageResult(
    _ctx: Context,
    _stage: string,
    _completion: CompletionLike,
  ): readonly Effect[] | void {}
  onFinished(_body: Record<string, unknown> | null): readonly Effect[] | void {}
  onResume(_ctx: Context): readonly Effect[] | void {}
}

export interface CompletionLike {
  readonly requestId: EnvelopeId;
  readonly body: Record<string, unknown> | null;
}

export function setNotice(state: State, notice: Notice): void {
  state.$notice = notice;
}
export function markReplied(state: State): void {
  state.$replied = true;
}
export function markHeld(state: State, held: boolean): void {
  state.$held = held;
}
export function setReplyBody(state: State, body: Record<string, unknown> | null): void {
  state.$replyBody = body;
}
export function setPendingControl(state: State, control: Control | null): void {
  state.$pendingControl = control;
}
export function isJob(value: unknown): value is Job {
  return value instanceof Job;
}
export function readState(value: Job): State {
  return value.$state;
}
