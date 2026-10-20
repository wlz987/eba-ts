import type { Envelope } from "./envelope.ts";
import { makeEnvelope } from "./envelope.ts";
import type { Header } from "./header.ts";
import { copyHeader } from "./header.ts";
import type { ActorId, EnvelopeId } from "./ids.ts";
import { hasResultShape } from "./result.ts";
import type { IdGen } from "./idgen.ts";

export class Matchmaker {
  private readonly topic: string;
  private readonly cause: EnvelopeId;
  private readonly correlationId: EnvelopeId;
  constructor(request: Header) {
    const header = copyHeader(request);
    const address = header.address;
    if (address.kind !== "request")
      throw new TypeError("Matchmaker requires a request");
    this.topic = address.replyTo;
    this.cause = header.cause;
    this.correlationId = header.id;
  }
  reply(body: unknown, from: ActorId | string, idGen: IdGen): Envelope {
    if (!hasResultShape(body)) throw new TypeError("invalid result body");
    return makeEnvelope({
      topic: this.topic,
      payload: body,
      from,
      idGen,
      cause: this.cause,
      kind: "result",
      correlationId: this.correlationId,
    });
  }
}
