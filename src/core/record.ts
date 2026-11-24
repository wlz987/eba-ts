import type { Envelope } from "#src/core/envelope.js";
import { SeqExhausted } from "#src/core/errors.js";

// --- record value ----------------------------------------------------------

export const RECORD_VERSION = 1;

export interface Record {
  readonly v: typeof RECORD_VERSION;
  readonly seq: number;
  readonly at: number;
  readonly envelope: Envelope;
}

// --- dedup -----------------------------------------------------------------

/** Process-local FIFO dedup window for at-least-once effects (C1). */
export class DedupWindow {
  #seen = new Map<string, true>();

  constructor(private readonly capacity: number) {}

  get size(): number {
    return this.#seen.size;
  }

  has(key: string): boolean {
    return this.#seen.has(key);
  }

  /** True if the key is new (and remembers it); false if already seen. */
  accept(key: string): boolean {
    if (this.#seen.has(key)) return false;
    this.#seen.set(key, true);
    if (this.#seen.size > this.capacity) {
      const oldest = this.#seen.keys().next();
      if (!oldest.done) this.#seen.delete(oldest.value);
    }
    return true;
  }

  clear(): void {
    this.#seen.clear();
  }
}

const encoder = new TextEncoder();

function utf8Length(text: string): number {
  return encoder.encode(text).length;
}

/** Stable key for `(from, envelope id)`; both ports count UTF-8 bytes. */
export function dedupKey(from: string, id: string): string {
  return `${utf8Length(from)}:${from}${utf8Length(id)}:${id}`;
}

// --- sequencer / ring ------------------------------------------------------

export class Sequencer {
  #next: number | null;
  #last: number;

  constructor(start = 1) {
    this.#next = start;
    this.#last = 0;
  }

  nextSeq(): number {
    if (this.#next === null) throw new SeqExhausted();
    const seq = this.#next;
    this.#next = seq + 1 > seq ? seq + 1 : null;
    this.#last = seq;
    return seq;
  }

  get current(): number {
    return this.#next ?? this.#last;
  }
  get lastSeq(): number {
    return this.#last;
  }
}

/** Bounded, preallocated ring: `tryPush` never allocates, `shift` is O(1). */
export class RecordRing {
  #slots: Array<Record | null>;
  #head = 0;
  #tail = 0;
  #count = 0;

  constructor(capacity: number) {
    this.#slots = new Array(capacity).fill(null);
  }

  get size(): number {
    return this.#count;
  }
  get capacity(): number {
    return this.#slots.length;
  }
  get isEmpty(): boolean {
    return this.#count === 0;
  }
  get isFull(): boolean {
    return this.#count === this.#slots.length;
  }

  tryPush(record: Record): boolean {
    if (this.isFull) return false;
    this.#slots[this.#tail] = record;
    this.#tail = (this.#tail + 1) % this.#slots.length;
    this.#count += 1;
    return true;
  }

  peek(): Record | null {
    if (this.isEmpty) return null;
    return this.#slots[this.#head]!;
  }

  shift(): Record | null {
    if (this.isEmpty) return null;
    const record = this.#slots[this.#head]!;
    this.#slots[this.#head] = null;
    this.#head = (this.#head + 1) % this.#slots.length;
    this.#count -= 1;
    return record;
  }

  drain(limit = this.#count): Record[] {
    const take = Math.min(limit, this.#count);
    const out: Record[] = new Array(take);
    for (let index = 0; index < take; index += 1) out[index] = this.shift()!;
    return out;
  }

  clear(): void {
    this.#slots.fill(null);
    this.#head = 0;
    this.#tail = 0;
    this.#count = 0;
  }
}

// --- sinks -----------------------------------------------------------------

export interface RecordSink {
  gap?: (from: number, to: number) => void;
  append(record: Record): void;
  flush(seq: number): Promise<void>;
  barrier(seq: number): Promise<void>;
  checkpoint(seq: number): number;
  readonly durable: number;
}

interface Waiter {
  readonly seq: number;
  readonly resolve: () => void;
}

export class MemorySink implements RecordSink {
  #log: Record[] = [];
  #flushed = 0;
  #waiters: Waiter[] = [];

  get durable(): number {
    return this.#flushed;
  }

  append(record: Record): void {
    const seq = record.seq;
    const at = record.at;
    const last = this.#log.length > 0 ? this.#log[this.#log.length - 1]!.seq : this.#flushed;
    if (seq <= last) throw new Error(`record out of order: ${seq} <= ${last}`);
    this.#log.push({ v: RECORD_VERSION, seq, at, envelope: record.envelope });
  }

  flush(seq: number): Promise<void> {
    const last = this.#log.length > 0 ? this.#log[this.#log.length - 1]!.seq : 0;
    const appended = Math.max(last, this.#flushed);
    if (seq > appended)
      throw new Error(`flush beyond appended: ${seq} > ${appended}`);
    if (seq > this.#flushed) {
      this.#flushed = seq;
      const ready = this.#waiters.filter((waiter) => waiter.seq <= seq);
      this.#waiters = this.#waiters.filter((waiter) => waiter.seq > seq);
      for (const waiter of ready) waiter.resolve();
    }
    return Promise.resolve();
  }

  barrier(seq: number): Promise<void> {
    const appended = Math.max(
      this.#flushed,
      this.#log.length > 0 ? this.#log[this.#log.length - 1]!.seq : 0,
    );
    if (seq > appended)
      return Promise.reject(
        new Error(`barrier beyond appended: ${seq} > ${appended}`),
      );
    if (seq <= this.#flushed) return Promise.resolve();
    return new Promise((resolve) => {
      this.#waiters.push({ seq, resolve });
    });
  }

  checkpoint(seq: number): number {
    if (seq > this.#flushed)
      throw new Error(`checkpoint before flush: ${seq} > ${this.#flushed}`);
    this.#log = this.#log.filter((record) => record.seq > seq);
    return seq;
  }

  records(): readonly Record[] {
    return this.#log.slice();
  }
}

/** Durable dedup sink: a repeated `(from, id)` fact never reaches the inner sink. */
export class DedupSink implements RecordSink {
  readonly #inner: RecordSink;
  readonly #seen: DedupWindow;

  constructor(inner: RecordSink, capacity = 1024) {
    this.#inner = inner;
    this.#seen = new DedupWindow(capacity);
  }

  get durable(): number {
    return this.#inner.durable;
  }

  append(record: Record): void {
    const key = dedupKey(record.envelope.header.from, record.envelope.header.id);
    if (this.#seen.has(key)) return;
    this.#inner.append(record);
    this.#seen.accept(key);
  }

  flush(seq: number): Promise<void> {
    return this.#inner.flush(seq);
  }
  barrier(seq: number): Promise<void> {
    return this.#inner.barrier(seq);
  }
  checkpoint(seq: number): number {
    return this.#inner.checkpoint(seq);
  }
  gap(from: number, to: number): void {
    this.#inner.gap?.(from, to);
  }
}

// --- log -------------------------------------------------------------------

/** Record production: seq + bounded ring on the scheduler side, sink on worker. */
export class RecordLog {
  #sequencer: Sequencer;
  #ring: RecordRing;
  #sink: RecordSink;
  #start: number;
  #gapReported = false;

  constructor(sink: RecordSink, capacity: number, start = 1) {
    this.#sink = sink;
    this.#ring = new RecordRing(capacity);
    this.#sequencer = new Sequencer(start);
    this.#start = start;
  }

  get pending(): number {
    return this.#ring.size;
  }
  get capacity(): number {
    return this.#ring.capacity;
  }
  get durable(): number {
    return this.#sink.durable;
  }
  get nextSeq(): number {
    return this.#sequencer.current;
  }
  get lastSeq(): number {
    return this.#sequencer.lastSeq;
  }

  record(envelope: Envelope, at: number): Record | null {
    if (!this.#gapReported) {
      this.#gapReported = true;
      if (this.#start > 1) this.#sink.gap?.(1, this.#start);
    }
    const stamp = at;
    if (this.#ring.isFull) return null;
    const record: Record = {
      v: RECORD_VERSION,
      seq: this.#sequencer.nextSeq(),
      at: stamp,
      envelope,
    };
    if (!this.#ring.tryPush(record)) return null;
    return record;
  }

  drain(limit = this.pending): number {
    const take = Math.min(limit, this.#ring.size);
    let moved = 0;
    for (let index = 0; index < take; index += 1) {
      const record = this.#ring.peek();
      if (record === null) break;
      this.#sink.append(record);
      this.#ring.shift();
      moved += 1;
    }
    return moved;
  }

  flush(seq: number): Promise<void> {
    return this.#sink.flush(seq);
  }

  barrier(seq: number): Promise<void> {
    const last = this.lastSeq;
    if (seq > last)
      throw new Error(`barrier target beyond recorded seq: ${seq} > ${last}`);
    this.drain();
    return this.#sink.barrier(seq);
  }

  checkpoint(seq: number): number {
    return this.#sink.checkpoint(seq);
  }

  checkpointDurable(): number {
    return this.#sink.checkpoint(this.#sink.durable);
  }
}

/** 只读水位视图：编译期限制 core.records 暴露面，无需运行时包装类。 */
export type RecordLogView = Pick<
  RecordLog,
  "pending" | "capacity" | "durable" | "nextSeq" | "lastSeq"
>;
