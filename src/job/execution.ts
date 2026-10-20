import type { Bus } from "../mail/bus.ts";
import type { Envelope } from "../envelope/envelope.ts";
import type { IdGen } from "../envelope/idgen.ts";
import { combineFailures } from "../errors.ts";
import type { Core } from "./core.ts";
import { Matchmaker } from "../envelope/reply.ts";

export function invoke(callback: () => unknown): void {
  const returned = callback();
  if (returned instanceof Promise) {
    throw new TypeError("Job hooks must be synchronous and return None");
  }
  if (returned !== undefined)
    throw new TypeError("Job hooks must be synchronous and return None");
}

export function respond(
  core: Core,
  bus: Bus,
  idGen: IdGen,
  root: Envelope,
  body: unknown,
): void {
  holdOrSend(
    core,
    bus,
    null,
    new Matchmaker(root.header).reply(body, core.sub.actorId, idGen),
  );
}

export function observe(action: () => void, observer: () => void): void {
  try {
    action();
  } catch (error) {
    try {
      observer();
    } catch (secondary) {
      throw combineFailures(error, secondary);
    }
    throw error;
  }
  observer();
}

import { holdOrSend } from "./delivery.ts";
export { deadline, reject, silence } from "./execution/lifecycle.ts";
export {
  holdsLetter, holdOrSend, nextSendable, noteDeliveryFailure, noteDeliverySuccess, owns,
  raiseDeliveryFailure, releaseAttempts, releaseResult, retryDue,
} from "./delivery.ts";
export {
  leaveEnding, refused, releaseDeferred, releaseEnding, remove, seal, settleEnding,
  waitsForBook, waitsForLease,
} from "./ending.ts";
export { runHook, deliverEnding } from "./hook.ts";
export { adopt, keepUnread, resume } from "./resume.ts";
