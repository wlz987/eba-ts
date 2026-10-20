import { EnvelopeBuildError } from "./errors.ts";
import type { ActorId, EnvelopeId } from "./ids.ts";
import { splitTopic } from "./topic.ts";

export type Kind = "event" | "request" | "result" | "diagnostic";

export interface EventAddress {
  readonly kind: "event";
}
export interface DiagnosticAddress {
  readonly kind: "diagnostic";
}
export interface RequestAddress {
  readonly kind: "request";
  readonly replyTo: string;
}
export interface ResultAddress {
  readonly kind: "result";
  readonly correlationId: EnvelopeId;
}

export type Address =
  EventAddress | DiagnosticAddress | RequestAddress | ResultAddress;

export function eventAddress(): EventAddress {
  return { kind: "event" };
}
export function diagnosticAddress(): DiagnosticAddress {
  return { kind: "diagnostic" };
}
export function requestAddress(replyTo: string): RequestAddress {
  return { kind: "request", replyTo };
}
export function resultAddress(correlationId: EnvelopeId): ResultAddress {
  return { kind: "result", correlationId };
}

export interface Header {
  readonly id: EnvelopeId;
  readonly topic: string;
  readonly from: ActorId;
  readonly cause: EnvelopeId;
  readonly address: Address;
}

export function kindOf(address: Address): Kind {
  return address.kind;
}

export function replyToOf(address: Address): string | undefined {
  return address.kind === "request" ? address.replyTo : undefined;
}

export function correlationIdOf(address: Address): EnvelopeId | undefined {
  return address.kind === "result" ? address.correlationId : undefined;
}

export function makeHeader(
  id: EnvelopeId,
  topic: string,
  from: ActorId,
  cause: EnvelopeId,
  address: Address,
): Header {
  const header: Header = { id, topic, from, cause, address };
  validateHeader(header);
  return header;
}

export function validateHeader(header: Header): void {
  if (typeof header.id !== "string" || !header.id) {
    throw new EnvelopeBuildError("id must be a nonempty string");
  }
  if (typeof header.cause !== "string" || !header.cause) {
    throw new EnvelopeBuildError("cause must be a nonempty string");
  }
  if (typeof header.from !== "string")
    throw new EnvelopeBuildError("from must be a string");
  splitTopic(header.topic);
  const address = header.address;
  if (address.kind === "request") {
    splitTopic(address.replyTo);
  } else if (address.kind === "result") {
    if (typeof address.correlationId !== "string" || !address.correlationId) {
      throw new EnvelopeBuildError("correlation_id must be nonempty");
    }
  } else if (address.kind !== "event" && address.kind !== "diagnostic") {
    throw new EnvelopeBuildError("invalid message kind or protocol address");
  }
}

export function copyAddress(address: Address): Address {
  if (address.kind === "request") return requestAddress(address.replyTo);
  if (address.kind === "result") return resultAddress(address.correlationId);
  if (address.kind === "diagnostic") return diagnosticAddress();
  if (address.kind === "event") return eventAddress();
  throw new EnvelopeBuildError("invalid message kind or protocol address");
}

export function isHeader(value: unknown): value is Header {
  return (
    typeof value === "object" &&
    value !== null &&
    "id" in value &&
    typeof (value as Header).id === "string"
  );
}

export function copyHeader(header: Header): Header {
  if (!isHeader(header)) throw new TypeError("expected Header");
  return makeHeader(
    header.id,
    header.topic,
    header.from,
    header.cause,
    copyAddress(header.address),
  );
}
