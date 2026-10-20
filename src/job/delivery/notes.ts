import type { Envelope } from "../../envelope/envelope.ts";
import type { Core } from "../core.ts";

export function releaseAttempts(core: Core): void {
  core.attempts.clear();
  core.again.clear();
}

export function noteDeliverySuccess(core: Core, envelope: Envelope): void {
  core.attempts.delete(envelope);
  core.again.delete(envelope);
}

