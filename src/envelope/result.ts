import type { Envelope } from "./envelope.ts";
import type { EnvelopeId } from "./ids.ts";

const RESERVED = new Set(["ok", "error", "value"]);

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
  if (typeof error !== "string") throw new TypeError("error must be a string");
  const extra: Record<string, unknown> = detail ? { ...detail } : {};
  for (const key of Object.keys(extra)) {
    if (RESERVED.has(key))
      throw new RangeError(`reserved result key: ${JSON.stringify(key)}`);
  }
  return { ok: false, error, ...extra };
}

function asRecord(body: unknown): Record<string, unknown> | null {
  if (typeof body !== "object" || body === null || Array.isArray(body))
    return null;
  if (body instanceof String || ArrayBuffer.isView(body)) return null;
  return body as Record<string, unknown>;
}

export function isResultOk(body: unknown): boolean {
  const record = asRecord(body);
  return record !== null && record["ok"] === true && "value" in record;
}

export function isResultErr(body: unknown): boolean {
  const record = asRecord(body);
  return (
    record !== null &&
    record["ok"] === false &&
    typeof record["error"] === "string"
  );
}

export function hasResultShape(body: unknown): body is Record<string, unknown> {
  return isResultOk(body) || isResultErr(body);
}

export function resultValue(body: Record<string, unknown>): unknown {
  if (!isResultOk(body)) throw new TypeError("result body is not Ok");
  return body["value"];
}

export function resultError(body: Record<string, unknown>): string {
  if (!isResultErr(body)) throw new TypeError("result body is not Err");
  return body["error"] as string;
}

export function resultBody(envelope: Envelope): Record<string, unknown> | null {
  const payload = envelope.payload;
  if (envelope.header.address.kind === "result" && hasResultShape(payload)) {
    return asRecord(payload);
  }
  return null;
}

export function resultRequestId(envelope: Envelope): EnvelopeId | null {
  if (envelope.header.address.kind !== "result") return null;
  return envelope.header.address.correlationId;
}
