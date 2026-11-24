import type { Envelope } from "#src/core/envelope.js";
import { hasResultShape } from "#src/core/envelope.js";
import { Job, readState } from "#src/core/job/job.js";
import type { State } from "#src/core/job/job.js";
import { Context, readBuffer } from "#src/core/job/context/context.js";
import type { Buffer } from "#src/core/job/context/buffer.js";
import { setReply } from "#src/core/job/context/buffer.js";


export class Intent {
  readonly stage: string;
  readonly request: Envelope;
  readonly timeout: number | null;
  /** Unforgeable context token: this intent belongs to one buffer. */
  readonly $mint: object;

  constructor(
    stage: string,
    request: Envelope,
    timeout: number | null,
    mint: object,
  ) {
    this.stage = stage;
    this.request = request;
    this.timeout = timeout;
    this.$mint = mint;
  }
}

export function mintOf(intent: Intent): object {
  return intent.$mint;
}

/** The control block a job finishes with: the body the caller will settle with. */
export class Control {
  readonly body: Record<string, unknown> | null;
  constructor(body: Record<string, unknown> | null) {
    this.body = body === null ? null : { ...body };
  }
}

export { Commit, sealContext } from "#src/core/job/context/commit.js";
export { Context };

export function stateOf(job: Job): State {
  return readState(job);
}

export function bufferOf(ctx: Context): Buffer {
  return readBuffer(ctx);
}

export function requireActive(ctx: Context): Buffer {
  const buffer = bufferOf(ctx);
  if (buffer.idGen === null) {
    throw new Error("Context is valid only during its callback");
  }
  return buffer;
}

export function putReply(buffer: Buffer, body: unknown): void {
  if (buffer.hold !== null) throw new Error("job already held");
  if (buffer.job.root.header.address.kind !== "request") {
    throw new TypeError("root is not a request");
  }
  if (buffer.reply !== null || buffer.job.replied) {
    throw new Error("root already replied");
  }
  // A settle in an earlier turn wrote the answer into the held lease; a later
  // reply/finish(body) must not overwrite it.
  if (
    buffer.core.held.get(buffer.job.root.header.id)?.isSettled === true
  ) {
    throw new Error("root already replied");
  }
  if (!hasResultShape(body)) {
    throw new TypeError("invalid result body");
  }
  // Stored **as handed over**: the *ownership* copy is taken at the two stations
  // that hand the value out (`sealContext`, `commitTo`), and a third copy here
  // is unobservable. What this function must keep is the gate — `hasResultShape`
  // rejects a body that is neither Ok nor Err before anything is stored.
  setReply(buffer, body as Record<string, unknown>);
}

