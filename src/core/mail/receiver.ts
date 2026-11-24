import type { Envelope } from "#src/core/envelope.js";
import { READ } from "#src/core/notify.js";
import type { Inbox } from "#src/core/mail/inbox.js";
import { TOKEN, noteClose, stateOf } from "#src/core/mail/inbox.js";
import { migrateRead } from "#src/core/mail/inbox_ops.js";

export class Receiver {
  /** The inbox this receiver reads, pinned as an ECMAScript `#` field. */
  inbox: Inbox;
  /**
   * Liveness and borrow count. `#` fields keep a JS caller from flipping
   * `active` on a live receiver.
   */
  #active = true;
  #borrows = 0;
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
    return this.#active;
  }
  borrow(): () => void {
    this.#requireActive();
    this.#borrows += 1;
    let released = false;
    return () => {
      if (!released) {
        this.#borrows -= 1;
        released = true;
      }
    };
  }
  take(): Receiver {
    this.#requireActive();
    if (this.#borrows) throw new Error("receiver is borrowed");
    const transferred = new Receiver(this.inbox, TOKEN);
    this.#active = false;
    return transferred;
  }
  tryRecv(): Envelope | null {
    this.#requireActive();
    const state = stateOf(this.inbox);
    const head = state.items.length ? state.items[0]! : null;
    if (head === null) return null;
    // Peek first: a rejected migration must leave the head queued, so migrate
    // before popping.
    const envelope = migrateRead(this.inbox, head);
    const full = state.items.length === this.inbox.capacity;
    state.items.shift();
    if (state.items.length === 0) state.read.clear(READ);
    if (full) state.space.notify();
    return envelope;
  }
  close(): boolean {
    const state = stateOf(this.inbox);
    const started = this.#active;
    if (started) {
      this.#active = false;
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
  #requireActive(): void {
    if (!this.#active) throw new Error("receiver transferred or closed");
  }
}

