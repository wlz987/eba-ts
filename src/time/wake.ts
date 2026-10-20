import { nonnegativeInt } from "./numbers.ts";

export class Wake {
  private pending = false;
  private waiting = false;
  private resolver: (() => void) | null = null;
  private timer: ReturnType<typeof setTimeout> | null = null;

  notify(): void {
    this.pending = true;
    this.resolve();
  }

  take(): boolean {
    if (!this.pending) return false;
    this.pending = false;
    return true;
  }

  async wait(timeoutMs?: number): Promise<boolean> {
    if (timeoutMs !== undefined) nonnegativeInt(timeoutMs, "timeout_ms");
    if (this.waiting) throw new Error("Wake has one scheduler waiter");
    if (this.pending) {
      this.pending = false;
      return true;
    }
    if (timeoutMs === 0) return false;
    this.waiting = true;
    try {
      return await new Promise<boolean>((resolve) => {
        this.resolver = () => {
          this.pending = false;
          if (this.timer !== null) clearTimeout(this.timer);
          resolve(true);
        };
        if (timeoutMs !== undefined) {
          this.timer = setTimeout(() => {
            this.resolver = null;
            resolve(false);
          }, timeoutMs);
        }
      });
    } finally {
      this.waiting = false;
    }
  }

  private resolve(): void {
    const resolver = this.resolver;
    if (resolver !== null) {
      this.resolver = null;
      resolver();
    }
  }
}
