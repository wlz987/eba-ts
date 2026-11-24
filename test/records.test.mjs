import test from "node:test";
import assert from "node:assert/strict";
import {
  Bus,
  Inbox,
  SchemaRegistry,
  WorkerPool,
  PoolAbortedError,
  PoolClosedError,
  PoolQueueFullError,
  makeEnvelope,
  okPayload,
} from "../dist/index.js";
import {
  Deadlines,
  Schedule,
  safeDeadline,
  readClock,
  pinClock,
  MAX_TIMER_DELAY_MS,
} from "../dist/core/time.js";
import { LeaseRecords, UnreadableResult } from "../dist/core/rendezvous/records.js";
import { ClockError } from "../dist/core/errors.js";

const request = (id, topic = "pay.req", replyTo = "pay.res") =>
  makeEnvelope({
    id,
    topic,
    from: "client",
    kind: "request",
    replyTo,
    payload: { amount: 1 },
  });

// --- schema ---------------------------------------------------------------

test("schema: check validates, version default, register monotonic", () => {
  const s = new SchemaRegistry();
  assert.equal(s.size, 0);
  assert.equal(s.check("unknown", {}), 1);
  assert.equal(s.version("unknown"), 1);
  s.register("x", { version: 2, validate: (p) => (p?.ok ? null : "bad") });
  assert.equal(s.size, 1);
  assert.equal(s.check("x", { ok: true }), 2);
  assert.throws(() => s.check("x", {}), /invalid x@2: bad/);
  assert.throws(() => s.register("x", { version: 2, validate: () => null }), /already at version 2/);
  assert.throws(() => s.register("x", { version: 1, validate: () => null }), /already at version 2/);
  s.register("x", { version: 3, validate: () => null });
  assert.equal(s.version("x"), 3);
});

test("schema: migrate fail-closed on missing schema, future version, missing migrator", () => {
  const s = new SchemaRegistry();
  assert.throws(() => s.migrate("none", {}, 1), /no schema for none/);
  s.register("a", { version: 2, validate: () => null });
  assert.throws(() => s.migrate("a", {}, 3), /unsupported future schema version for a: 3/);
  assert.deepEqual(s.migrate("a", { v: 1 }, 2), { version: 2, payload: { v: 1 } });
  assert.throws(() => s.migrate("a", {}, 1), /no migration for a@1 -> 2/);
});

test("schema: migrate wraps throwing migrator and invalid migrated payload", () => {
  const s = new SchemaRegistry();
  s.register("boom", {
    version: 2,
    validate: () => null,
    migrate: () => { throw new Error("migrator exploded"); },
  });
  assert.throws(
    () => s.migrate("boom", {}, 1),
    (error) => error.message === "invalid boom@2 after migration: migrator exploded" && error.cause?.message === "migrator exploded",
  );
  s.register("bad", {
    version: 2,
    validate: (p) => (p === 1 ? null : "still bad"),
    migrate: () => 2,
  });
  assert.throws(() => s.migrate("bad", {}, 1), /invalid bad@2 after migration: still bad/);
});

test("schema: migrateEnvelope rewrites sv and is a no-op at current version", () => {
  const s = new SchemaRegistry();
  s.register("evt", {
    version: 2,
    validate: (p) => (p?.n === 2 ? null : "wrong"),
    migrate: (p, from) => ({ n: from + 1 }),
  });
  const old = makeEnvelope({ id: "m-1", topic: "evt", from: "a", payload: { n: 1 }, schemaVersion: 1 });
  const migrated = s.migrateEnvelope(old);
  assert.equal(migrated.header.sv, 2);
  assert.deepEqual(migrated.payload, { n: 2 });
  assert.equal(s.migrateEnvelope(old).header.sv, 2);
  const current = makeEnvelope({ id: "m-2", topic: "evt", from: "a", payload: { n: 2 }, schemaVersion: 2 });
  assert.equal(s.migrateEnvelope(current), current);
  const unregistered = makeEnvelope({ id: "m-3", topic: "other", from: "a", payload: {}, schemaVersion: 1 });
  assert.equal(s.migrateEnvelope(unregistered), unregistered);
});

// --- time -----------------------------------------------------------------

test("time: safeDeadline rejects boolean, fraction, negative and unsafe", () => {
  assert.equal(safeDeadline(0), 0);
  assert.equal(safeDeadline(12, "wait_ms"), 12);
  assert.throws(() => safeDeadline(true), /deadline_ms must be a nonnegative integer/);
  assert.throws(() => safeDeadline(1.5), /deadline_ms must be a nonnegative integer/);
  assert.throws(() => safeDeadline(-1), /deadline_ms must be a nonnegative integer/);
  assert.throws(() => safeDeadline(Number.MAX_SAFE_INTEGER + 1), /deadline_ms must be a safe integer/);
});

test("time: readClock wraps foreign failures but preserves ClockError and reentrancy", () => {
  assert.equal(readClock({ nowMs: () => 42 }), 42);
  assert.throws(() => readClock({ nowMs: () => { throw new ClockError("bad clock"); } }), ClockError);
  assert.throws(
    () => readClock({ nowMs: () => { throw new Error("reentrant clock observation"); } }),
    /reentrant clock observation/,
  );
  assert.throws(() => readClock({ nowMs: () => { throw "oops"; } }), ClockError);
  assert.equal(pinClock(7).nowMs(), 7);
  assert.equal(MAX_TIMER_DELAY_MS, 2_147_483_647);
});

test("time: deadlines heap orders by (deadline, insertion) and supports removal", () => {
  const d = new Deadlines();
  assert.equal(d.nextMs, null);
  assert.equal(d.at("a"), null);
  assert.equal(d.peekDue(10), null);
  d.add("late", 20);
  d.add("early", 5);
  d.add("tie1", 5);
  d.add("tie2", 5);
  assert.equal(d.nextMs, 5);
  assert.equal(d.at("tie1"), 5);
  assert.throws(() => d.add("early", 99), /duplicate deadline/);
  assert.equal(d.peekDue(4), null);
  assert.equal(d.peekDue(5), "early");
  d.remove("early");
  assert.equal(d.peekDue(5), "tie1");
  d.remove("tie2");
  d.remove("late");
  assert.equal(d.at("late"), null);
  d.remove("missing");
  d.remove("tie1");
  assert.equal(d.nextMs, null);
});

test("time: schedule waitMs uses observed instant, ready and quiescence", () => {
  assert.throws(() => new Schedule().waitMs(), /schedule has no observed instant/);
  const s = new Schedule(false, 100, false, 40);
  assert.equal(s.waitMs(), 60);
  assert.equal(s.waitMs(200), 0);
  assert.equal(new Schedule(true, 100, false, 40).waitMs(), 0);
  assert.equal(new Schedule(false, null, true, 40).waitMs(), null);
});

// --- lease records --------------------------------------------------------

test("lease records: submit, take result, release route and expire due", () => {
  const bus = new Bus();
  const inbox = new Inbox(16);
    const records = new LeaseRecords();
  const req = request("r-1");
  records.submit(bus, inbox, req, { kind: "first" }, 10);
  assert.equal(records.size, 1);
  assert.equal(records.nextDeadlineMs, 10);
  assert.equal(records.continuation("r-1").kind, "first");
  assert.deepEqual(records.ids(), ["r-1"]);
  assert.equal(records.continuation("absent"), null);
  assert.equal(records.unreleased, 0);
  assert.throws(() => records.submit(bus, inbox, req, {}, null), /duplicate request id: r-1/);
  const result = makeEnvelope({
    id: "res-1",
    topic: "pay.res",
    cause: req.header.id,
    correlationId: req.header.id,
    from: "srv",
    kind: "result",
    payload: okPayload({ paid: true }),
  });
  const claim = records.takeResult(result);
  assert.equal(claim.completion.requestId, "r-1");
  assert.equal(claim.completion.body.value.paid, true);
  assert.equal(claim.continuation.kind, "first");
  assert.equal(records.size, 0);
  assert.equal(records.nextDeadlineMs, null);
  assert.equal(records.takeResult(result), null);
  assert.equal(bus.subscriptionCount("pay.res", inbox), 0);
});

test("lease records: takeResult rejects unreadable and mismatched results", () => {
  const bus = new Bus();
  const inbox = new Inbox(16);
    const records = new LeaseRecords();
  records.submit(bus, inbox, request("r-2"), {}, null);
  assert.throws(() => records.takeResult({}), TypeError);
  assert.equal(
    records.takeResult(makeEnvelope({ id: "res-e", topic: "pay.res", from: "srv", kind: "event", payload: {} })),
    null,
  );
  assert.equal(
    records.takeResult(makeEnvelope({
      id: "res-x", topic: "other.res", correlationId: "r-2", cause: "r-2", from: "srv", kind: "result", payload: okPayload(1),
    })),
    null,
  );
  assert.equal(
    records.takeResult(makeEnvelope({
      id: "res-y", topic: "pay.res", correlationId: "r-2", cause: "wrong", from: "srv", kind: "result", payload: okPayload(1),
    })),
    null,
  );
  assert.throws(
    () => records.takeResult(makeEnvelope({
      id: "res-z", topic: "pay.res", correlationId: "r-2", cause: "r-2", from: "srv", kind: "result", payload: null,
    })),
    UnreadableResult,
  );
  assert.equal(records.size, 1);
});

test("lease records: elapseOpen, deadline expiry and recover", () => {
  const bus = new Bus();
  const inbox = new Inbox(16);
    const records = new LeaseRecords();
  records.submit(bus, inbox, request("r-3"), { tag: 3 }, null);
  records.elapseOpen();
  assert.equal(records.nextDeadlineMs, 0);
  assert.equal(records.expireDue(0).completion.body, null);
  assert.equal(records.size, 0);
  records.submit(bus, inbox, request("r-4"), { tag: 4 }, 50);
  assert.equal(records.expireDue(49), null);
  assert.equal(records.expireDue(50).continuation.tag, 4);
  records.recover();
});

test("lease records: closed inbox and non-request envelope rejected", () => {
  const bus = new Bus();
  const inbox = new Inbox(16);
  const records = new LeaseRecords();
  const event = makeEnvelope({ id: "e-1", topic: "pay.req", from: "client", kind: "event", payload: {} });
  assert.throws(
    () => records.submit(bus, inbox, event, {}, null),
    /expected a request with reply_to/,
  );
  const closed = new Inbox(16);
  closed.receiver().close();
  assert.throws(
    () => records.submit(bus, closed, request("r-6"), {}, null),
    /inbox closed/,
  );
});

// --- worker pool ----------------------------------------------------------

test("worker pool: queue bound, abort and close drain", async () => {
  const pool = new WorkerPool(1, 1);
  let release;
  const gate = new Promise((resolve) => { release = resolve; });
  const first = pool.run(async () => { await gate; return 1; });
  const second = pool.run(async () => 2);
  assert.equal(pool.queued, 1);
  await assert.rejects(() => pool.run(async () => 3), PoolQueueFullError);
  release();
  assert.deepEqual(await Promise.all([first, second]), [1, 2]);
  assert.equal(pool.queued, 0);
  await pool.close();
  assert.equal(pool.isClosed, true);
  await assert.rejects(() => pool.run(async () => 4), PoolClosedError);
});

test("worker pool: pre-aborted signal and queued abort", async () => {
  const pool = new WorkerPool(1, 4);
  const pre = AbortSignal.abort();
  await assert.rejects(() => pool.run(async () => 1, pre), PoolAbortedError);
  const controller = new AbortController();
  let release;
  const gate = new Promise((resolve) => { release = resolve; });
  const running = pool.run(async () => { await gate; return "ok"; });
  const queued = pool.run(async () => "never", controller.signal);
  controller.abort();
  await assert.rejects(() => queued, PoolAbortedError);
  release();
  assert.equal(await running, "ok");
  await pool.close();
});

test("worker pool: submit token completes once, task throw propagates", async () => {
  const pool = new WorkerPool(2, 2);
  await pool.submit((token) => token.complete());
  await assert.rejects(() => pool.submit(() => { throw new Error("task failed"); }), /task failed/);
  await pool.submit((token) => { token.complete(); token.complete(); });
  await pool.close();
});

test("worker pool: close with timeout rejects a stuck task", async () => {
  const pool = new WorkerPool(1, 1);
  let release;
  const gate = new Promise((resolve) => { release = resolve; });
  const stuck = pool.run(async () => { await gate; return 1; });
  await assert.rejects(() => pool.close(5), /pool close timed out/);
  release();
  await stuck;
  await pool.close();
});
