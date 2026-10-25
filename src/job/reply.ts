import {
  errPayload,
  errPayloadCode,
  hasResultShape,
} from "../envelope/result.ts";

/** A held request's one-shot slot. The runtime settles it on the next advance. */
export class Reply {
  private body: Record<string, unknown> | null = null;
  private closed = false;
  private readonly control = new AbortController();

  /** Aborted when the request is cancelled or its host closes. */
  get signal(): AbortSignal {
    return this.control.signal;
  }

  send(body: unknown): void {
    if (this.closed) throw new Error("reply closed");
    if (this.control.signal.aborted) return;
    if (this.body !== null) throw new Error("reply already settled");
    if (!hasResultShape(body)) throw new TypeError("invalid result body");
    this.body = body;
  }

  fail(error: string, code?: number): void {
    this.send(
      code === undefined ? errPayload(error) : errPayloadCode(error, code),
    );
  }

  /** Settle as cancelled and abort the adapter. A late send is a no-op. */
  cancel(error = "cancelled"): boolean {
    if (this.body !== null || this.closed) return false;
    this.body = errPayload(error);
    this.abort();
    return true;
  }

  abort(): void {
    if (!this.control.signal.aborted) this.control.abort();
  }

  take(): Record<string, unknown> | null {
    return this.body;
  }

  get isClosed(): boolean {
    return this.closed;
  }

  close(): void {
    this.closed = true;
  }
}
