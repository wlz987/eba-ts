import type { Bus } from "../../mail/bus.ts";
import type { Envelope } from "../../envelope/envelope.ts";
import { DeliveryError, combineFailures } from "../../errors.ts";
import type { Core } from "../core.ts";
import type { State } from "../job.ts";
import { Control, Job, stateOf } from "../job.ts";
import { holdOrSend, holdsLetter, leaveEnding, settleEnding } from "../execution.ts";

export function deliverEnding(
  core: Core,
  bus: Bus,
  job: Job,
  reply: Envelope | null,
  control: Control | null,
): void {
  const state = stateOf(job);
  if (reply !== null) {
    let accepted: boolean;
    try {
      accepted = holdOrSend(core, bus, job, reply);
    } catch (error) {
      if (error instanceof DeliveryError) {
        state.replied = true;
        leaveEnding(job, control);
      }
      throw error;
    }
    state.replied = true;
    if (!accepted) {
      leaveEnding(job, control);
      return;
    }
  }
  settleEnding(core, job, control);
}

export function deferEnding(
  core: Core,
  bus: Bus,
  job: Job,
  state: State,
  control: Control,
  reply: Envelope | null,
  error: Error,
): void {
  state.pendingControl = control;
  if (reply === null) return;
  try {
    holdOrSend(core, bus, job, reply);
    state.replied = true;
  } catch (deliveryError) {
    if (holdsLetter(core, reply)) {
      state.replied = true;
    }
    throw combineFailures(error, deliveryError);
  }
}

