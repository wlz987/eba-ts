import { randomUUID } from "node:crypto";
import { SeqExhausted } from "#src/core/errors.js";

export class EnvelopeBuildError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "EnvelopeBuildError";
  }
}

export type EnvelopeId = string & { readonly __brand: "EnvelopeId" };
export type ActorId = string & { readonly __brand: "ActorId" };

export function envelopeId(raw: string): EnvelopeId {
  return raw as EnvelopeId;
}

export function actorId(raw: string): ActorId {
  return raw as ActorId;
}

// --- topics ----------------------------------------------------------------

const MAX_TOPIC_CACHE = 512;
const topicCache = new Map<string, readonly string[]>();

export function splitTopic(topic: string): string[] {
  const cached = topicCache.get(topic);
  if (cached !== undefined) return cached as string[];
  const parts = topic.split(".");
  if (topicCache.size >= MAX_TOPIC_CACHE) topicCache.clear();
  topicCache.set(topic, parts);
  return parts;
}

// --- header ----------------------------------------------------------------

export const MAX_PAYLOAD_BYTES = 1024;

export function checkPayloadBytes(bytes: number): number {
  return bytes;
}

export type Kind = "event" | "request" | "result" | "diagnostic";

export type Address =
  | { readonly kind: "event" }
  | { readonly kind: "diagnostic" }
  | { readonly kind: "request"; readonly replyTo: string }
  | { readonly kind: "result"; readonly correlationId: EnvelopeId };

export type EventAddress = Extract<Address, { kind: "event" }>;
export type DiagnosticAddress = Extract<Address, { kind: "diagnostic" }>;
export type RequestAddress = Extract<Address, { kind: "request" }>;
export type ResultAddress = Extract<Address, { kind: "result" }>;

const EVENT_ADDRESS: EventAddress = { kind: "event" };
const DIAGNOSTIC_ADDRESS: DiagnosticAddress = { kind: "diagnostic" };

export function eventAddress(): EventAddress {
  return EVENT_ADDRESS;
}
export function diagnosticAddress(): DiagnosticAddress {
  return DIAGNOSTIC_ADDRESS;
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
  /** Payload schema version; 1 for unversioned mail. */
  readonly sv: number;
  /** Declared payload size in bytes, when the sender knows it. */
  readonly bytes?: number;
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
  sv = 1,
): Header {
  return { id, topic, from, cause, address, sv };
}

export function withSchemaVersion(header: Header, sv: number): Header {
  return { ...header, sv };
}

export function withPayloadBytes(header: Header, bytes: number): Header {
  return { ...header, bytes };
}

export function copyHeader(header: Header): Header {
  return header;
}

// --- envelope --------------------------------------------------------------

/** Opt-in identity gate: refuses an envelope whose `from` is not trusted. */
export interface TrustPolicy {
  allows(from: ActorId | string): boolean;
}

/** An immutable message: a validated, owned header plus an opaque payload. */
export class Envelope {
  readonly header: Header;
  readonly payload: unknown;
  readonly $seal = true;

  constructor(header: Header, payload: unknown) {
    this.header = header;
    this.payload = payload;
  }

  checkTrust(policy: TrustPolicy): void {
    if (!policy.allows(this.header.from)) {
      throw new EnvelopeBuildError("from is not trusted");
    }
  }

  withPayloadBytes(bytes: number): Envelope {
    return new Envelope(withPayloadBytes(this.header, bytes), this.payload);
  }

  withSchemaVersion(sv: number): Envelope {
    return new Envelope(withSchemaVersion(this.header, sv), this.payload);
  }
}

export function isSealed(value: unknown): value is Envelope {
  return (value instanceof Envelope);
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
  schemaVersion?: number;
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
    options.schemaVersion ?? 1,
  );
  return new Envelope(header, options.payload);
}

export function snapshotEnvelope(envelope: unknown): Envelope {
  return envelope as Envelope;
}

function assignId(
  id: EnvelopeId | string | undefined,
  idGen: IdGen | undefined,
): EnvelopeId {
  return id == null ? idGen!.nextEnvelopeId() : envelopeId(id);
}

function causeOf(cause: EnvelopeId | string): EnvelopeId {
  return envelopeId(cause);
}

function actorFrom(sender: ActorId | string): ActorId {
  return actorId(sender);
}

function buildAddress(
  kind: Kind,
  replyTo: string | undefined,
  correlationId: EnvelopeId | string | undefined,
): Header["address"] {
  if (kind === "request") return requestAddress(replyTo!);
  if (kind === "result") return resultAddress(envelopeId(correlationId!));
  return kind === "event" ? eventAddress() : diagnosticAddress();
}

// --- id generation ---------------------------------------------------------

export interface IdGen {
  nextEnvelopeId(): EnvelopeId;
}

export class SeqIdGen implements IdGen {
  #n: number | null;

  constructor(start = 1) {
    this.#n = start;
  }

  nextEnvelopeId(): EnvelopeId {
    if (this.#n === null) {
      throw new SeqExhausted("envelope id space exhausted");
    }
    const current = this.#n;
    this.#n = current + 1 > current ? current + 1 : null;
    return envelopeId(current.toString(16).padStart(32, "0"));
  }
}

export class UuidIdGen implements IdGen {
  nextEnvelopeId(): EnvelopeId {
    return envelopeId(randomUUID().replace(/-/g, ""));
  }
}

// --- reply matchmaker ------------------------------------------------------

export class Matchmaker {
  readonly topic: string;
  readonly cause: EnvelopeId;
  readonly correlationId: EnvelopeId;
  constructor(request: Header) {
    const address = request.address as RequestAddress;
    this.topic = address.replyTo;
    this.cause = request.cause;
    this.correlationId = request.id;
  }
  reply(body: unknown, from: ActorId | string, idGen: IdGen): Envelope {
    return makeEnvelope({
      topic: this.topic,
      payload: body as Record<string, unknown>,
      from,
      idGen,
      cause: this.cause,
      kind: "result",
      correlationId: this.correlationId,
    });
  }
}

// --- result bodies ---------------------------------------------------------

export class Completion {
  readonly requestId: EnvelopeId;
  readonly body: Record<string, unknown> | null;
  constructor(requestId: EnvelopeId, body: Record<string, unknown> | null) {
    this.requestId = requestId;
    this.body = body;
  }
  get isTimeout(): boolean {
    return this.body === null;
  }
}

export function okPayload(value: unknown): Record<string, unknown> {
  return { ok: true, value };
}

export function errPayload(
  error: string,
  detail?: Record<string, unknown>,
): Record<string, unknown> {
  return detail === undefined
    ? { ok: false, error }
    : { ok: false, error, ...detail };
}

export function errPayloadCode(
  error: string,
  code: number,
  detail?: Record<string, unknown>,
): Record<string, unknown> {
  return { ok: false, error, ...detail, code };
}

export function isResultOk(body: unknown): boolean {
  return (body as Record<string, unknown> | null)?.["ok"] === true;
}

export function isResultErr(body: unknown): boolean {
  return (body as Record<string, unknown> | null)?.["ok"] === false;
}

export function hasResultShape(body: unknown): body is Record<string, unknown> {
  return isResultOk(body) || isResultErr(body);
}

export function resultValue(body: Record<string, unknown>): unknown {
  return body["value"];
}

export function resultError(body: Record<string, unknown>): string {
  return body["error"] as string;
}

export function resultBody(envelope: Envelope): Record<string, unknown> | null {
  return envelope.header.address.kind === "result"
    ? (envelope.payload as Record<string, unknown>)
    : null;
}

export function resultRequestId(envelope: Envelope): EnvelopeId | null {
  if (envelope.header.address.kind !== "result") return null;
  return envelope.header.address.correlationId;
}
