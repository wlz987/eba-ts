import { randomUUID } from "node:crypto";
import type { EnvelopeId } from "./ids.ts";
import { envelopeId } from "./ids.ts";

export interface IdGen {
  nextEnvelopeId(): EnvelopeId;
}

export class SeqIdGen implements IdGen {
  private n: number;
  constructor(start = 1) {
    if (typeof start === "boolean" || !Number.isInteger(start)) {
      throw new TypeError("start must be an int");
    }
    if (start < 0) throw new RangeError("start must be nonnegative");
    this.n = start;
  }
  nextEnvelopeId(): EnvelopeId {
    const current = this.n;
    this.n = current + 1;
    return envelopeId(current.toString(16).padStart(32, "0"));
  }
}

export class UuidIdGen implements IdGen {
  nextEnvelopeId(): EnvelopeId {
    return envelopeId(randomUUID().replace(/-/g, ""));
  }
}
