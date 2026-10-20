import type { Envelope } from "../../envelope/envelope.ts";
import type { AgendaLike } from "../../notify/signal.ts";
import { Signal } from "../../notify/signal.ts";
import { Interest } from "../../notify/watch.ts";
import type { Bus } from "../bus.ts";

export class Subscription {
  private release: (() => boolean) | null;
  constructor(release: () => boolean) {
    this.release = release;
  }
  active(): boolean {
    return this.release !== null;
  }
  disarm(): void {
    this.release = null;
  }
  close(): boolean {
    const release = this.release;
    if (release === null) return false;
    const released = release();
    this.release = null;
    return released;
  }
}

export class Offer extends Interest {
  readonly envelope: Envelope;
  readonly home: AgendaLike;
  private readonly source: Bus;
  constructor(source: Bus, envelope: Envelope, home: AgendaLike) {
    super(new Signal(), {}, envelope.header.topic);
    this.source = source;
    this.envelope = envelope;
    this.home = home;
  }
  override notify(): void {
    this.source.finishOffer(this);
  }
}

