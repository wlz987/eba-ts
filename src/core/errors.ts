import type { Envelope } from "#src/core/envelope.js";

/** 生成具名 Error 子类：name 固定，message 透传或取默认值。 */
function namedError(name: string, fallback?: string) {
  return class extends Error {
    constructor(message = fallback) {
      super(message);
      this.name = name;
    }
  };
}

export class BusError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "BusError";
  }
}
export class MailboxFull extends BusError {
  constructor(message: string) {
    super(message);
    this.name = "MailboxFull";
  }
}
export class InvalidTopic extends BusError {
  constructor(message: string) {
    super(message);
    this.name = "InvalidTopic";
  }
}
export class AmbiguousRoute extends BusError {
  constructor(message = "request matches multiple subscribers") {
    super(message);
    this.name = "AmbiguousRoute";
  }
}
export const BackpressureError = namedError("BackpressureError");
export class DeliveryError extends Error {
  readonly envelope: Envelope;
  constructor(envelope: Envelope, reason = "publication failed") {
    super(reason);
    this.name = "DeliveryError";
    this.envelope = envelope;
  }
}

export const MAILBOX_BUSY = "result mailbox full";
export class MailboxBusy extends DeliveryError {
  constructor(envelope: Envelope, reason = MAILBOX_BUSY) {
    super(envelope, reason);
    this.name = "MailboxBusy";
  }
}
export const REENTRANT = "reentrant host execution";
export class LeasePending extends Error {
  constructor() {
    super("lease release pending");
    this.name = "LeasePending";
  }
}
export const WakeError = namedError("WakeError");
export const ClockError = namedError("ClockError");
export const RecordSinkError = namedError("RecordSinkError");
export const SeqExhausted = namedError("SeqExhausted", "seq exhausted");

const SETTLED = new WeakSet<object>();
const isFinal = (error: unknown): boolean =>
  error instanceof DeliveryError ||
  error instanceof WakeError ||
  error instanceof ClockError;

export function noteSettled(error: Error): Error {
  if (!isFinal(error)) SETTLED.add(error);
  return error;
}
export function isSettled(error: unknown): boolean {
  return isFinal(error) || SETTLED.has(error as object);
}
export function toError(value: unknown): Error {
  return value instanceof Error ? value : new Error(String(value));
}
export function combineFailures(original: unknown, secondary: unknown): Error {
  const first = toError(original);
  const second = toError(secondary);
  const grouped = new AggregateError([first, second], "multiple failures");
  const chosen = second instanceof DeliveryError ? second : first;
  if (chosen instanceof DeliveryError) {
    const error = new (chosen.constructor as new (
      envelope: Envelope,
      reason: string,
    ) => DeliveryError)(chosen.envelope, chosen.message);
    error.cause = grouped;
    return error;
  }
  return grouped;
}
