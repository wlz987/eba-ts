import test from "node:test";
import assert from "node:assert/strict";
import {
  Bus,
  Inbox,
  Caller,
  Drive,
  ManualClock,
  MonotonicClock,
  Runtime,
  SeqIdGen,
  Signal,
  Watchers,
  Interest,
} from "../dist/index.js";
import {
  WakeTicket,
  SpaceKick,
  Pending,
} from "../dist/core/runtime/signals.js";
import {
  Changes,
  Agenda,
  READ,
  TIMER,
  WORK,
  DELIVERY,
} from "../dist/core/notify.js";
import {
  driveWake,
  driveWaiters,
  driveStops,
  driveClosed,
  driveEnded,
  parkWaiter,
  claimWaiter,
  queueStop,
  retainStops,
  setPumping,
  setReleasing,
  markClosed,
  markEnded,
  setTask,
  clearTask,
  clearWaiters,
  armSpace,
  watchSpaceFor,
  clearSpaceKick,
  resetDrive,
} from "../dist/core/runtime/drive/base.js";
import { failWaiters } from "../dist/core/runtime/drive.js";
import { stateOf } from "../dist/core/mail/inbox.js";
import { MailboxFull } from "../dist/core/errors.js";

const makeRuntime = (clock = new ManualClock(0)) =>
  new Runtime(new Bus(), clock, new SeqIdGen(1));
const makeCaller = (rt, name = "caller") => {
  const caller = new Caller(new Inbox(64).receiver(), new SeqIdGen(100), 64, name);
  rt.add(caller);
  return caller;
};

// --- wake primitives -------------------------------------------------------

test("wake ticket: fire before/after arm, consume, disarm, expire", () => {
  const wake = new WakeTicket();
  let hits = 0;
  const waiter = () => { hits += 1; };
  wake.fire();
  assert.equal(wake.consume(), true);
  assert.equal(wake.consume(), false);
  wake.disarm(waiter);
  wake.expire(waiter);
  wake.arm(waiter);
  assert.equal(hits, 0);
  wake.fire();
  assert.equal(hits, 1);
  wake.disarm(waiter);
  wake.fire();
  assert.equal(hits, 1);
  wake.arm(waiter);
  wake.expire(waiter);
  assert.equal(hits, 3);
  const other = () => {};
  wake.arm(waiter);
  wake.disarm(other);
  wake.expire(other);
  assert.equal(hits, 4);
  wake.fire();
  assert.equal(hits, 5);
});

test("space kick and pending are thin one-shot handles", async () => {
  let freed = 0;
  const kick = new SpaceKick(() => { freed += 1; });
  kick.notify();
  assert.equal(freed, 1);
  const envelope = { header: { id: "e" } };
  const completion = { requestId: "e", body: { ok: true, value: 1 } };
  const pending = new Pending(envelope, Promise.resolve(completion));
  assert.equal(pending.envelope, envelope);
  assert.equal(await pending.ready, completion);
});

// --- notify ----------------------------------------------------------------

test("signal: flags, bind, notify/clear and detach", () => {
  const enqueued = [];
  const unqueued = [];
  const owner = {
    bindSignal() {},
    hasSignal: () => true,
    enqueue: (a) => enqueued.push(a),
    unqueue: (a) => unqueued.push(a),
    unbindSignal() {},
  };
  const s = new Signal();
  assert.equal(s.actor, null);
  assert.equal(s.isBound, false);
  assert.equal(s.handed, false);
  assert.equal(s.boundTo(owner), false);
  s.notify(READ);
  s.clear(READ);
  s.bind(owner, "a");
  assert.equal(s.actor, "a");
  assert.equal(s.isBound, true);
  assert.equal(s.handed, true);
  assert.equal(s.boundTo(owner), true);
  s.set(WORK, true);
  s.set(TIMER, true);
  assert.equal(s.has(WORK | TIMER), true);
  assert.deepEqual(enqueued, ["a", "a"]);
  s.clear(WORK);
  assert.deepEqual(unqueued, []);
  s.clear(TIMER);
  assert.deepEqual(unqueued, ["a"]);
  s.detach();
  assert.equal(s.handed, false);
  assert.equal(s.actor, null);
});

test("signal: rebind warns prior owner and replays pending flags", () => {
  const calls = [];
  const first = {
    bindSignal() {}, hasSignal: () => true,
    enqueue: (a) => calls.push(`first:enqueue:${a}`),
    unqueue: () => {}, unbindSignal: (a) => calls.push(`first:unbind:${a}`),
  };
  const second = {
    bindSignal() {}, hasSignal: () => true,
    enqueue: (a) => calls.push(`second:enqueue:${a}`),
    unqueue: () => {}, unbindSignal: () => {},
  };
  const s = new Signal();
  s.bind(first, "a");
  s.notify(DELIVERY);
  s.bind(second, "a");
  assert.deepEqual(calls, ["first:enqueue:a", "first:unbind:a", "second:enqueue:a"]);
});

test("watchers: onEmpty fires on remove and notify, errors propagate", () => {
  let empties = 0;
  const w = new Watchers(() => { empties += 1; });
  assert.equal(w.watched, 0);
  w.notify();
  const s = new Signal();
  const interest = new Interest(s, {}, "x");
  let notified = 0;
  interest.setOnNotify(() => { notified += 1; });
  w.watch(interest);
  assert.equal(w.watched, 1);
  assert.deepEqual(w.live(), [interest]);
  assert.equal(interest.$owners.size, 1);
  w.notify();
  assert.equal(notified, 1);
  assert.equal(w.watched, 0);
  assert.equal(interest.$owners.size, 0);
  assert.equal(empties, 1);
  w.watch(interest);
  w.remove(interest);
  w.remove(interest);
  assert.equal(empties, 2);
  const bad = new Interest(s, {}, "y");
  bad.setOnNotify(() => { throw new Error("boom"); });
  w.watch(interest);
  w.watch(bad);
  assert.throws(() => w.notify(), /boom/);
  interest.dispose();
  bad.dispose();
});

test("interest: default notify marks candidate and signals delivery", () => {
  const candidates = {};
  const interest = new Interest(new Signal(), candidates, "a.b");
  interest.notify();
  assert.equal(candidates["a.b"], null);
  interest.dispose();
});

test("changes: watch topic and notify matching pattern", () => {
  const changes = new Changes();
  const signal = new Signal();
  const candidates = {};
  const interest = new Interest(signal, candidates, "a.b");
  let hit = 0;
  interest.setOnNotify(() => { hit += 1; });
  changes.watch("a.b", interest);
  changes.notify({ wildcard: "none", literal: ["a", "b"] });
  assert.equal(hit, 1);
  changes.notify({ wildcard: "none", literal: ["x", "y"] });
  assert.equal(hit, 1);
  interest.dispose();
});

test("agenda: enqueue/pop, timers, wake faults and remove", () => {
  const agenda = new Agenda();
  assert.equal(agenda.pop(), null);
  let wakes = 0;
  agenda.bindWake(() => { wakes += 1; });
  const s = new Signal();
  agenda.bindSignal("a", new WeakRef(s));
  assert.equal(agenda.hasSignal("a"), true);
  s.bind(agenda, "a");
  s.notify(READ);
  assert.equal(agenda.readySize, 1);
  assert.equal(agenda.pop(), "a");
  assert.equal(agenda.pop(), null);
  agenda.update("a", 10, true, false);
  assert.equal(agenda.nextDeadlineMs, 10);
  agenda.update("a", 10, true, false);
  agenda.due(9);
  assert.equal(s.has(TIMER), false);
  agenda.due(10);
  assert.equal(s.has(TIMER), true);
  assert.equal(wakes, 2);
  const fault = agenda.takeFault();
  assert.equal(fault, null);
  agenda.bindWake(() => { throw new Error("wake failed"); });
  agenda.wakeFn();
  assert.match(agenda.takeFault().message, /wake failed/);
  agenda.bindWake(null);
  agenda.wakeFn();
  assert.equal(agenda.takeFault(), null);
  assert.equal(agenda.schedule(0).waitMs(0), 0);
  agenda.remove("a");
  assert.equal(agenda.hasSignal("a"), false);
  assert.equal(agenda.nextDeadlineMs, null);
  assert.equal(agenda.schedule(0).waitMs(0), null);
});

// --- drive state machine ---------------------------------------------------

test("drive: lifecycle, open, abandon, close", async () => {
  const rt = makeRuntime();
  const drive = new Drive(rt);
  const caller = makeCaller(rt);
  assert.equal(drive.running, false);
  assert.throws(
    () => drive.open(caller, { topic: "greet", payload: 1, from: "c" }),
    /drive is not running/,
  );
  await assert.rejects(() => drive.closeTimeout(-1), RangeError);
  await assert.rejects(() => drive.closeTimeout(1.5), RangeError);
  await drive.start();
  assert.equal(drive.running, true);
  await assert.rejects(() => drive.start(), /drive already running/);
  const second = new Drive(rt);
  await assert.rejects(() => second.start(), /runtime already has a drive/);
  const pending = drive.open(caller, { topic: "greet", payload: 1, from: "c" });
  assert.equal(pending.envelope.header.topic, "sys.admit");
  assert.equal(driveWaiters(drive).size, 1);
  drive.kick();
  drive.abandon(caller, pending.envelope);
  assert.equal(driveStops(drive).length, 1);
  await assert.rejects(() => pending.ready, /cancelled/);
  assert.equal(driveWaiters(drive).size, 0);
  await drive.close();
  assert.equal(drive.running, false);
  assert.equal(await drive.closeTimeout(100), "closed");
});

test("drive: open rejects a foreign caller and a closed drive rejects all", async () => {
  const rt = makeRuntime();
  const other = makeRuntime();
  const drive = new Drive(rt);
  const foreign = makeCaller(other, "foreign");
  await drive.start();
  assert.throws(
    () => drive.open(foreign, { topic: "x", payload: 1, from: "f" }),
    /caller belongs to another runtime/,
  );
  await drive.close();
  assert.throws(
    () => drive.open(foreign, { topic: "x", payload: 1, from: "f" }),
    /drive is not running/,
  );
  await other.close();
});

test("drive: closeTimeout returns timeout when loop never settles", async () => {
  const rt = makeRuntime();
  const drive = new Drive(rt);
  await drive.start();
  setTask(drive, new Promise(() => {}));
  assert.equal(await drive.closeTimeout(5), "timeout");
  clearTask(drive);
  await drive.close();
});

test("drive: helpers cover books, release and waiter failure", async () => {
  const rt = makeRuntime();
  const drive = new Drive(rt);
  const caller = makeCaller(rt);
  const waiter = { resolve() {}, reject(e) { rejected.push(e); }, done: false };
  const rejected = [];
  parkWaiter(drive, "k", waiter);
  assert.equal(claimWaiter(drive, "k"), waiter);
  assert.equal(claimWaiter(drive, "k"), undefined);
  parkWaiter(drive, "k2", waiter);
  failWaiters(drive, new Error("hard fault"));
  assert.equal(rejected.length, 1);
  assert.equal(driveWaiters(drive).size, 0);
  clearWaiters(drive);
  queueStop(drive, caller, "absent");
  assert.equal(driveStops(drive).length, 1);
  drive.$release();
  assert.equal(driveStops(drive).length, 0);
  const kept = [{ caller, rootId: "x" }];
  retainStops(drive, kept);
  assert.equal(driveStops(drive).length, 1);
  setPumping(drive, true);
  const wake = driveWake(drive);
  let wakes = 0;
  wake?.disarm(() => {});
  wake?.arm(() => { wakes += 1; });
  drive.kick();
  assert.equal(wakes, 0);
  setReleasing(drive, true);
  drive.kick();
  assert.equal(wakes, 0);
  setPumping(drive, false);
  setReleasing(drive, false);
  drive.kick();
  assert.equal(wakes, 1);
  const kick = new SpaceKick(() => { wakes += 1; });
  watchSpaceFor(drive, caller.inbox?.id ?? -1, kick);
  clearSpaceKick(drive, caller.inbox?.id ?? -1);
  markClosed(drive);
  assert.equal(driveClosed(drive), true);
  markEnded(drive, new Error("x"));
  assert.equal(driveEnded(drive), true);
  resetDrive(drive);
  assert.equal(driveClosed(drive), false);
  assert.equal(driveEnded(drive), false);
  armSpace(drive, caller);
  armSpace(drive, caller);
  await drive.close();
});

test("drive: release keeps a full stop, then drops it for a closing host", async () => {
  const rt = makeRuntime();
  const drive = new Drive(rt);
  const receiver = new Inbox(8).receiver();
  const caller = new Caller(receiver, new SeqIdGen(300), 8, "busy");
  rt.add(caller);
  await drive.start();
  caller.revoke = () => { throw new MailboxFull("target inbox full"); };
  queueStop(drive, caller, "r-1");
  drive.$release();
  assert.equal(driveStops(drive).length, 1);
  caller.revoke = () => { throw new Error("host rejected"); };
  assert.throws(() => drive.$release(), /host rejected/);
  assert.equal(driveStops(drive).length, 1);
  stateOf(caller.core.sub.inbox).closed = true;
  drive.$release();
  assert.equal(driveStops(drive).length, 0);
  await drive.close();
});

test("drive: pause uses a finite delay and disarms on wake", async () => {
  const drive = new Drive(makeRuntime(new MonotonicClock()));
  const p = drive.$pause(5);
  driveWake(drive)?.fire();
  await p;
  await drive.close();
});

test("drive: failWaiters skips already-settled waiters", async () => {
  const rt = makeRuntime();
  const drive = new Drive(rt);
  const seen = [];
  const done = { resolve() {}, reject() { seen.push("done"); }, done: true };
  const live = { resolve() {}, reject(e) { seen.push(e.message); }, done: false };
  parkWaiter(drive, "done", done);
  parkWaiter(drive, "live", live);
  failWaiters(drive, new Error("stopped"));
  assert.deepEqual(seen, ["stopped"]);
  assert.equal(driveWaiters(drive).size, 0);
  await drive.close();
});
