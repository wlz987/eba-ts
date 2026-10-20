import type { Envelope } from "../envelope/envelope.ts";
import { READ } from "../notify/signal.ts";
import type { Inbox } from "./inbox.ts";
import { TOKEN, noteClose, stateOf } from "./inbox.ts";

export class Receiver {
  readonly inbox: Inbox;
  private active = true;
  private borrows = 0;
  constructor(inbox: Inbox, token: symbol) {
    if (token !== TOKEN) throw new Error("use Inbox.receiver()");
    this.inbox = inbox;
  }
  static claim(inbox: Inbox): Receiver {
    const state = stateOf(inbox);
    if (state.closed || state.claimed)
      throw new Error("inbox unavailable or already owned");
    state.claimed = true;
    return new Receiver(inbox, TOKEN);
  }
  get isActive(): boolean {
    return this.active;
  }
  borrow(): () => void {
    this.requireActive();
    this.borrows += 1;
    let released = false;
    return () => {
      if (!released) {
        this.borrows -= 1;
        released = true;
      }
    };
  }
  take(): Receiver {
    this.requireActive();
    if (this.borrows) throw new Error("receiver is borrowed");
    const transferred = new Receiver(this.inbox, TOKEN);
    this.active = false;
    return transferred;
  }
  tryRecv(): Envelope | null {
    this.requireActive();
    const state = stateOf(this.inbox);
    const full = state.items.length === this.inbox.capacity;
    const envelope = state.items.length ? state.items.shift()! : null;
    if (state.items.length === 0) state.read.clear(READ);
    if (envelope !== null && full) state.space.notify();
    return envelope;
  }
  close(): boolean {
    const state = stateOf(this.inbox);
    const started = this.active;
    if (started) {
      this.active = false;
      state.closed = true;
      noteClose();
      if (state.items.length > 0) {
        state.seized.push(...state.items);
        state.items.length = 0;
      }
      state.read.clear(READ);
      state.space.notify();
    } else if (!state.closed) {
      return false;
    }
    for (const owner of [...state.routes]) {
      try {
        owner.unsubscribeAll(this.inbox);
      } finally {
        state.routes.delete(owner);
      }
    }
    return started;
  }
  private requireActive(): void {
    if (!this.active) throw new Error("receiver transferred or closed");
  }
}

