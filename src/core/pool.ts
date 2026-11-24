

export class PoolClosedError extends Error {
  constructor(message = "pool closed") {
    super(message);
    this.name = "PoolClosedError";
  }
}

export class PoolQueueFullError extends Error {
  constructor(message = "pool queue full") {
    super(message);
    this.name = "PoolQueueFullError";
  }
}

export class PoolAbortedError extends Error {
  constructor(message = "pool aborted") {
    super(message);
    this.name = "PoolAbortedError";
  }
}

export interface WorkerToken {
  complete(): void;
}

interface Job<T> {
  readonly task: (signal: AbortSignal) => Promise<T>;
  readonly signal: AbortSignal | undefined;
  readonly resolve: (value: T) => void;
  readonly reject: (reason: unknown) => void;
  detach?: () => void;
}

/** Bounded async worker pool: tasks start in submission order. */
export class WorkerPool {
  concurrency: number;
  maxQueue: number;
  #queue: Job<unknown>[] = [];
  #running = new Set<Promise<unknown>>();
  #controller = new AbortController();
  #active = 0;
  #closed = false;

  constructor(concurrency: number, maxQueue = 1024) {
    this.concurrency = concurrency;
    this.maxQueue = maxQueue;
  }

  get isClosed(): boolean {
    return this.#closed;
  }
  get queued(): number {
    return this.#queue.length;
  }

  run<T>(
    task: (signal: AbortSignal) => Promise<T>,
    signal?: AbortSignal,
  ): Promise<T> {
    if (this.#closed) return Promise.reject(new PoolClosedError());
    if (signal?.aborted === true) return Promise.reject(new PoolAbortedError());
    return new Promise<T>((resolve, reject) => {
      const job = { task, signal, resolve, reject } as Job<unknown>;
      if (signal !== undefined) {
        const onAbort = () => {
          const index = this.#queue.indexOf(job);
          if (index < 0) return;
          this.#queue.splice(index, 1);
          reject(new PoolAbortedError());
        };
        signal.addEventListener("abort", onAbort, { once: true });
        job.detach = () => signal.removeEventListener("abort", onAbort);
      }
      this.#queue.push(job);
      this.#pump();
      if (this.#queue.length > this.maxQueue) {
        const index = this.#queue.indexOf(job);
        if (index >= 0) {
          this.#queue.splice(index, 1);
          job.detach?.();
          reject(new PoolQueueFullError());
        }
      }
    });
  }

  /** Callback form: `token.complete()` releases the slot. */
  submit(
    task: (token: WorkerToken, signal: AbortSignal) => void,
    signal?: AbortSignal,
  ): Promise<void> {
    return this.run((internal) => new Promise<void>((resolve, reject) => {
      let done = false;
      const finish = (action: () => void): void => {
        if (done) return;
        done = true;
        internal.removeEventListener("abort", onAbort);
        action();
      };
      const onAbort = (): void => { finish(() => reject(new PoolAbortedError())); };
      if (internal.aborted) {
        finish(() => reject(new PoolAbortedError()));
        return;
      }
      internal.addEventListener("abort", onAbort, { once: true });
      try {
        task({ complete: () => { finish(resolve); } }, internal);
      } catch (error) {
        finish(() => reject(error));
      }
    }), signal);
  }

  /** Close with an optional budget; a stuck task fails with a timed-out error. */
  async close(timeoutMs?: number): Promise<void> {
    this.#closed = true;
    this.#controller.abort();
    for (const job of this.#queue.splice(0)) {
      job.detach?.();
      job.reject(new PoolAbortedError("pool closed"));
    }
    const running = [...this.#running];
    if (timeoutMs === undefined) {
      await Promise.allSettled(running);
      return;
    }
    const budget = timeoutMs;
    let timer: ReturnType<typeof setTimeout> | undefined;
    try {
      await Promise.race([
        Promise.allSettled(running),
        new Promise<never>((_resolve, reject) => {
          timer = setTimeout(
            () => reject(new PoolClosedError("pool close timed out")),
            budget,
          );
        }),
      ]);
    } finally {
      if (timer !== undefined) clearTimeout(timer);
    }
  }

  #pump(): void {
    while (!this.#closed && this.#active < this.concurrency && this.#queue.length > 0) {
      const job = this.#queue.shift()!;
      job.detach?.();
      if (job.signal?.aborted === true) {
        job.reject(new PoolAbortedError());
        continue;
      }
      const combined = job.signal === undefined
        ? this.#controller.signal
        : AbortSignal.any([this.#controller.signal, job.signal]);
      this.#active += 1;
      const promise = Promise.resolve()
        .then(() => job.task(combined))
        .then(job.resolve, job.reject)
        .finally(() => {
          this.#active -= 1;
          this.#pump();
        });
      this.#running.add(promise);
      void promise.finally(() => this.#running.delete(promise));
    }
  }
}
