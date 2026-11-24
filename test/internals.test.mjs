import test from "node:test";
import assert from "node:assert/strict";
import {
  Bus,
  Inbox,
  Job,
  ManualClock,
  SeqIdGen,
  Lease,
  okPayload,
  makeEnvelope,
} from "../dist/index.js";
import { Core } from "../dist/core/job/core.js";
import { State } from "../dist/core/job/job.js";
import { Control } from "../dist/core/job/context.js";
import { Buffer } from "../dist/core/job/context/buffer.js";
import {
  bindLease,
  leaseIssuedFor,
  isLease,
} from "../dist/core/job/lease.js";
import { applyFinished } from "../dist/core/job/finished.js";
import { observe, invoke, replyFor } from "../dist/core/job/execution.js";
import {
  assertAskAllowed,
  assertHoldAllowed,
  finishControl,
  mutate,
  prepareHold,
} from "../dist/core/job/context/ops.js";
import { resolveHeld, closeHeld } from "../dist/core/job/held.js";

const idGen = () => new SeqIdGen(1);
const request = (id = "r1") => makeEnvelope({
  id, topic: "q", from: "caller", kind: "request", replyTo: "reply", payload: { n: 1 },
});
const makeCore = (overrides = {}) => new Core({
  actorId: "a",
  receiver: new Inbox(16).receiver(),
  patterns: ["q"],
  queueLimit: 8,
  activeLimit: 8,
  workBudget: 8,
  mailboxLimit: 8,
  makeJob: (root) => new Job(root),
  onRejected: () => {},
  ...overrides,
});
const makeBuffer = (core, state, options = {}) => {
  if (state.lifecycle === "new") state.$lifecycle = "active";
  return new Buffer(
  state,
  "a",
  "reply",
  options.idGen === undefined ? idGen() : options.idGen,
  options.pendingCount ?? 0,
  core,
  options.host ?? null,
  );
};

test("lease: ownership, abort, settle and close states", () => {
  const lease = new Lease({ deadlineMs: 0, budgetMs: 1 });
  const job = new Job(request());
  assert.equal(isLease(lease), true);
  assert.equal(isLease({}), false);
  assert.equal(leaseIssuedFor(lease, job), false);
  bindLease(lease, job);
  assert.equal(leaseIssuedFor(lease, job), true);
  assert.equal(leaseIssuedFor(lease, new Job(request())), false);
  lease.abort();
  lease.send(okPayload(1));
  assert.equal(lease.isSettled, false);
  const live = new Lease();
  live.close();
  assert.equal(live.isClosed, true);
  live.close();
  assert.equal(live.revoke(), false);
  assert.equal(live.cancel(), false);
  const other = new Lease();
  other.settle(okPayload({ v: 1 }));
  assert.throws(() => other.send(okPayload(2)), /lease already settled/);
  assert.equal(other.revoke(), false);
});

test("finished effects: host delegation and rejection of continuations", () => {
  const core = makeCore();
  assert.throws(() => applyFinished(core, []), /Effect host is not attached/);
  const seen = [];
  core.attachHost({
    tell: (...a) => seen.push(["tell", ...a]),
    record: (...a) => seen.push(["record", ...a]),
    barrier: () => {},
    timer: () => {},
    close: (reason) => seen.push(["close", reason]),
  });
  applyFinished(core, [
    { kind: "tell", topic: "t", payload: 1 },
    { kind: "record", topic: "r", payload: 2 },
    { kind: "close", reason: "bye" },
  ]);
  assert.deepEqual(seen, [["tell", "t", 1], ["record", "r", 2], ["close", "bye"]]);
  for (const kind of ["barrier", "timer", "ask", "reply", "hold", "finish", "settle", "revoke"]) {
    assert.throws(() => applyFinished(core, [{ kind }]), new RegExp(kind));
  }
});

test("execution: observe combines action and observer failures", () => {
  assert.equal(invoke(() => 42), 42);
  let observed = 0;
  observe(() => {}, () => { observed += 1; });
  assert.equal(observed, 1);
  const primary = new Error("primary");
  assert.throws(() => observe(() => { throw primary; }, () => {}), (e) => e === primary);
  let combined = null;
  try {
    observe(() => { throw primary; }, () => { throw new Error("secondary"); });
  } catch (error) {
    combined = error;
  }
  assert.equal(combined.name, "AggregateError");
  assert.equal(combined.errors.length, 2);
});

test("context ops: ask/hold preconditions and mutation guard", () => {
  const core = makeCore();
  const state = new State(request(), 2);
  const buffer = makeBuffer(core, state);
  assert.doesNotThrow(() => assertAskAllowed(buffer));
  buffer.$hold = new Lease();
  assert.throws(() => assertAskAllowed(buffer), /job already held/);
  buffer.$hold = null;
  buffer.$control = new Control({});
  assert.throws(() => assertAskAllowed(buffer), /job already finished/);
  buffer.$control = null;
  buffer.pendingCount = 2;
  assert.throws(() => assertAskAllowed(buffer), /max_inflight/);
  buffer.pendingCount = 0;
  buffer.$idGen = null;
  assert.throws(() => assertAskAllowed(buffer), /Context is valid/);
  buffer.$idGen = idGen();

  const heldState = new State(request(), 2);
  const heldBuffer = makeBuffer(core, heldState);
  assert.doesNotThrow(() => assertHoldAllowed(heldBuffer));
  heldState.$held = true;
  assert.throws(() => assertHoldAllowed(heldBuffer), /already held/);
  heldState.$held = false;
  heldBuffer.pendingCount = 1;
  assert.throws(() => assertHoldAllowed(heldBuffer), /cannot hold with pending requests/);
  heldBuffer.pendingCount = 0;
  heldBuffer.$idGen = null;
  assert.throws(() => assertHoldAllowed(heldBuffer), /Context is valid/);
  heldBuffer.$idGen = idGen();
  const eventBuffer = makeBuffer(core, new State(makeEnvelope({
    id: "e1", topic: "q", from: "a", payload: 1,
  }), 2));
  assert.throws(() => assertHoldAllowed(eventBuffer), TypeError);

  const ctx = { $buffer: buffer };
  assert.throws(() => mutate(ctx, () => mutate(ctx, () => {})), /reentrant Context mutation/);
  const lease = prepareHold(heldBuffer);
  assert.equal(leaseIssuedFor(lease, heldState), true);
});

test("context ops: finish normalization and conflict rules", () => {
  const core = makeCore();
  const buffer = makeBuffer(core, new State(request(), 2));
  finishControl(buffer, undefined);
  assert.equal(buffer.control.body, null);
  finishControl(buffer, undefined);
  assert.throws(() => finishControl(buffer, { ok: true }), /job already finished/);
  const event = makeBuffer(core, new State(makeEnvelope({
    id: "e2", topic: "q", from: "a", payload: 1,
  }), 2));
  assert.throws(() => finishControl(event, { nope: true }), TypeError);
  const req = makeBuffer(core, new State(request("r9"), 2));
  finishControl(req, okPayload({ v: 3 }));
  assert.equal(req.reply.value.v, 3);
});

test("core: queue, held and rest books", () => {
  const core = makeCore();
  const e1 = request("q1");
  const e2 = request("q2");
  assert.equal(core.queuePush(e1), true);
  assert.equal(core.queuePush(e2), true);
  for (let i = 3; i <= 8; i += 1) assert.equal(core.queuePush(request(`q${i}`)), true);
  assert.equal(core.queuePush(request("q9")), false);
  assert.equal(core.queue.items.length, 8);
  assert.equal(core.queueShift()?.header.id, "q1");
  assert.equal(core.queueRemove((e) => e.header.id === "q2"), 1);
  assert.equal(core.queueShiftIf((e) => e.header.id === "q3")?.header.id, "q3");
  assert.equal(core.queueShift()?.header.id, "q4");
  while (core.queueShift() !== null) { /* drain */ }
  assert.equal(core.queueShift(), null);

  const lease = new Lease();
  core.holdReply("h1", lease);
  assert.equal(core.takeHeld("h1"), lease);
  core.holdReply("h1", lease);
  core.releaseHeld("h1");
  assert.equal(core.takeHeld("h1"), null);
  core.holdReply("h2", lease);
  core.clearHeld();
  assert.equal(core.held.size, 0);

  core.insertRest(0, e1, null);
  assert.equal(core.shiftRest()[0], e1);
  core.insertRest(0, e2, null);
  assert.equal(core.takeRest(0)[0], e2);
  core.insertRest(0, e1, null);
  core.dropRestLetter(e1);
  assert.equal(core.rest.length, 0);
  core.parkLetter(e1, null);
  assert.equal(core.takeParked()[0], e1);
  core.restoreParked(e2, null);
  assert.equal(core.takeParked()[0], e2);
  assert.throws(() => core.restoreParked({}, null), TypeError);
  assert.throws(() => core.parkLetter({}, null), TypeError);
});

test("core: timers, leases, faults and close requests", async () => {
  const core = makeCore();
  core.attachClock(new ManualClock(0));
  assert.throws(() => core.waitTimer(Number.NaN, "t1"), /deadline_ms/);
  const wait = core.waitTimer(5, "t1");
  assert.equal(core.timers.length, 1);
  assert.equal(core.nextTimerMs, 5);
  assert.equal(core.fireDueTimers(4), 0);
  assert.equal(core.fireDueTimers(5), 1);
  await wait;
  core.cancelTimers("missing");
  core.clearTimers();
  assert.equal(core.timers.length, 0);

  const lease = new Lease({ deadlineMs: 10, budgetMs: 1 });
  core.holdReply("d1", lease);
  assert.equal(core.nextHeldLeaseDeadlineMs, 10);
  core.chargeLeases(1);
  assert.equal(lease.remainingMs, 0);
  core.chargeLeases(1);
  assert.equal(lease.take().error, "budget");
  core.holdReply("d2", new Lease({ deadlineMs: 10 }));
  core.expireLeases(10);
  assert.equal(core.takeHeld("d2")?.take().error, "deadline");

  const fault = new Error("sink");
  core.recordFault(fault);
  assert.equal(core.takeFault(), fault);
  core.defer(Promise.resolve(), () => {});
  await new Promise((resolve) => setImmediate(resolve));
  core.requestClose();
  assert.equal(core.takeCloseRequest(), true);
  assert.equal(core.takeCloseRequest(), false);
  core.beginDrain();
  assert.equal(core.state, "draining");
});

test("held books: stale lease is released, live lease resolves, close revokes", () => {
  const core = makeCore();
  const bus = new Bus();
  const stale = new Lease();
  stale.settle(okPayload({ v: 1 }));
  core.holdReply("stale", stale);
  assert.equal(resolveHeld(core, bus, idGen()), false);
  assert.equal(core.held.size, 0);

  const job = new Job(request("live"));
  const state = job.$state;
  state.$lifecycle = "active";
  state.$held = true;
  core.admitJob("live", job);
  const live = new Lease();
  live.settle(okPayload({ v: 2 }));
  core.holdReply("live", live);
  assert.equal(resolveHeld(core, bus, idGen()), true);
  assert.equal(state.held, false);
  assert.equal(resolveHeld(core, bus, idGen()), false);

  core.holdReply("closing", new Lease());
  closeHeld(core);
  assert.equal(core.held.size, 0);
  closeHeld(core);
});

test("execution: replyFor builds a sealed result envelope", () => {
  const core = makeCore();
  const reply = replyFor(request("rr"), okPayload({ v: 8 }), core, idGen());
  assert.equal(reply.header.topic, "reply");
  assert.equal(reply.header.cause, "rr");
  assert.equal(reply.header.address.kind, "result");
  assert.equal(reply.payload.value.v, 8);
});
