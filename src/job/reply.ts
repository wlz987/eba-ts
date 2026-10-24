import {
  errPayload,
  errPayloadCode,
  hasResultShape,
} from "../envelope/result.ts";

/** A held request's one-shot slot. The runtime settles it on the next advance. */
export class Reply {
  private body: Record<string, unknown> | null = null;
  private closed = false;

  send(body: unknown): void {
    if (this.closed) throw new Error("reply closed");
    if (this.body !== null) throw new Error("reply already settled");
    if (!hasResultShape(body)) throw new TypeError("invalid result body");
    this.body = body;
  }

  fail(error: string, code?: number): void {
    this.send(
      code === undefined ? errPayload(error) : errPayloadCode(error, code),
    );
  }

  take(): Record<string, unknown> | null {
    return this.body;
  }

  close(): void {
    this.closed = true;
  }
}
