import type { Header } from "./header.ts";
import type { Kind } from "./header.ts";
import {
  copyHeader,
  diagnosticAddress,
  eventAddress,
  isHeader,
  makeHeader,
  requestAddress,
  resultAddress,
  validateHeader,
} from "./header.ts";
import type { ActorId, EnvelopeId } from "./ids.ts";
import { actorId, envelopeId } from "./ids.ts";
import { EnvelopeBuildError } from "./errors.ts";
import type { IdGen } from "./idgen.ts";

export interface EnvelopeLike {
  readonly header: Header;
  readonly payload: unknown;
}

export class Envelope {
  readonly header: Header;
  readonly payload: unknown;
  constructor(header: Header, payload: unknown) {
    if (!isHeader(header)) throw new TypeError("expected Header");
    validateHeader(header);
    this.header = header;
    this.payload = payload;
  }
}

export interface MakeEnvelopeOptions {
  topic: string;
  payload: unknown;
  from: ActorId | string;
  cause?: EnvelopeId | string;
  id?: EnvelopeId | string;
  idGen?: IdGen;
  kind?: Kind;
  replyTo?: string;
  correlationId?: EnvelopeId | string;
}

export function makeEnvelope(options: MakeEnvelopeOptions): Envelope {
  const assigned = assignId(options.id, options.idGen);
  const address = buildAddress(
    options.kind ?? "event",
    options.replyTo,
    options.correlationId,
  );
  const header = makeHeader(
    assigned,
    options.topic,
    actorFrom(options.from),
    options.cause == null ? assigned : causeOf(options.cause),
    address,
  );
  return new Envelope(header, options.payload);
}

export function snapshotEnvelope(envelope: unknown): Envelope {
  if (
    typeof envelope === "object" &&
    envelope !== null &&
    Object.getPrototypeOf(envelope) === Envelope.prototype
  ) {
    const own = envelope as Envelope;
    validateHeader(own.header);
    return own;
  }
  if (envelope instanceof Envelope) {
    return new Envelope(copyHeader(envelope.header), envelope.payload);
  }
  if (
    typeof envelope === "object" &&
    envelope !== null &&
    "header" in envelope &&
    "payload" in envelope
  ) {
    const raw = envelope as { header: unknown; payload: unknown };
    return new Envelope(copyHeader(raw.header as Header), raw.payload);
  }
  throw new TypeError("expected Envelope");
}

function assignId(
  id: EnvelopeId | string | undefined,
  idGen: IdGen | undefined,
): EnvelopeId {
  let assigned: string | undefined;
  if (id == null) {
    if (idGen === undefined)
      throw new EnvelopeBuildError("id_gen is required when id is omitted");
    assigned = idGen.nextEnvelopeId();
  } else {
    assigned = typeof id === "string" ? id : undefined;
  }
  if (typeof assigned !== "string" || !assigned) {
    throw new EnvelopeBuildError("id must be a nonempty string");
  }
  return envelopeId(assigned);
}

function causeOf(cause: EnvelopeId | string): EnvelopeId {
  if (typeof cause !== "string" || !cause) {
    throw new EnvelopeBuildError("cause must be a nonempty string");
  }
  return envelopeId(cause);
}

function actorFrom(sender: ActorId | string): ActorId {
  if (typeof sender !== "string")
    throw new EnvelopeBuildError("from must be a string");
  return actorId(sender);
}

function buildAddress(
  kind: Kind,
  replyTo: string | undefined,
  correlationId: EnvelopeId | string | undefined,
): Header["address"] {
  if (kind === "request") {
    if (correlationId !== undefined)
      throw new EnvelopeBuildError("request cannot have correlation_id");
    if (typeof replyTo !== "string")
      throw new EnvelopeBuildError("request requires reply_to");
    return requestAddress(replyTo);
  }
  if (kind === "result") {
    if (
      replyTo !== undefined ||
      typeof correlationId !== "string" ||
      !correlationId
    ) {
      throw new EnvelopeBuildError("result requires only correlation_id");
    }
    return resultAddress(envelopeId(correlationId));
  }
  if (kind === "event" || kind === "diagnostic") {
    if (replyTo !== undefined || correlationId !== undefined) {
      throw new EnvelopeBuildError("invalid message kind or protocol address");
    }
    return kind === "event" ? eventAddress() : diagnosticAddress();
  }
  throw new EnvelopeBuildError("invalid message kind or protocol address");
}
