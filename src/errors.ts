import type { Envelope } from "./envelope/envelope.ts";

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

export class BackpressureError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "BackpressureError";
  }
}

export class DeliveryError extends Error {
  readonly envelope: Envelope;
  constructor(envelope: Envelope, reason = "publication failed") {
    super(reason);
    this.name = "DeliveryError";
    this.envelope = envelope;
  }
}

export const OUTBOX_FULL = "result outbox full";

export class OutboxFull extends DeliveryError {
  constructor(envelope: Envelope, reason = OUTBOX_FULL) {
    super(envelope, reason);
    this.name = "OutboxFull";
  }
}
export const REENTRANT = "reentrant host execution";

export class LeasePending extends Error {
  constructor() {
    super("lease release pending");
    this.name = "LeasePending";
  }
}

export class WakeError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "WakeError";
  }
}

export class ClockError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "ClockError";
  }
}

const SETTLED = new WeakSet<object>();

export function noteSettled(error: Error): Error {
  if (
    error instanceof DeliveryError ||
    error instanceof WakeError ||
    error instanceof ClockError
  ) {
    return error;
  }
  return markSettled(error);
}

export function markSettled(error: Error): Error {
  SETTLED.add(error);
  return error;
}

export function isSettled(error: unknown): boolean {
  return SETTLED.has(error as object);
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
    ) => DeliveryError)(chosen.envelope, String(chosen));
    error.cause = grouped;
    return error;
  }
  return grouped;
}
