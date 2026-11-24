import test from "node:test";
import assert from "node:assert/strict";
import {
  Bus,
  Inbox,
  Job,
  JobHost,
  Runtime,
  ManualClock,
  SeqIdGen,
  SchemaRegistry,
  RecordLog,
  Lease,
  makeEnvelope,
  okPayload,
  AmbiguousRoute,
  BackpressureError,
  MailboxFull,
  EnvelopeBuildError,
} from "../dist/index.js";

const advance = (rt, times = 12) => {
  for (let i = 0; i < times; i += 1) rt.advance();
};
const settle = async (rt, times = 12) => {
  for (let i = 0; i < times; i += 1) {
    rt.advance();
    await new Promise((resolve) => setImmediate(resolve));
  }
};
const newRuntime = (bus = new Bus(), clock = new ManualClock(0)) =>
  new Runtime(bus, clock, new SeqIdGen(1));
class Host extends JobHost {
  constructor(actor, patterns, make, options = {}) {
    super({ actorId: actor, receiver: new Inbox(options.capacity ?? 64).receiver(), patterns, ...options });
    this.make = make;
  }
  makeJob(root) { return this.make(root); }
}
const finisher = (value) => (root) => new (class extends Job {
  begin(ctx) { ctx.finish(okPayload(value)); }
})(root);

// --- bus contract ----------------------------------------------------------

test("bus: publish with no route is a retryable miss; event fans out", () => {
  const bus = new Bus();
  const e = makeEnvelope({ id: "e0", topic: "a.b", from: "p", payload: 1 });
  assert.equal(bus.publish(e), false);
  const one = new Inbox(8);
  const two = new Inbox(8);
  const sub1 = bus.subscribe("a.*", one);
  const sub2 = bus.subscribe("a.b", two);
  assert.equal(bus.publish(e), true);
  assert.equal(bus.publish(e), false); // ingress dedup
  assert.equal(one.remaining(), 7);
  assert.equal(two.remaining(), 7);
  sub1.close();
  sub2.close();
});

test("bus: request route must be unambiguous", () => {
  const bus = new Bus();
  const one = new Inbox(8);
  const two = new Inbox(8);
  const s1 = bus.subscribe("q", one);
  const s2 = bus.subscribe("q", two);
  const req = makeEnvelope({ id: "r1", topic: "q", from: "p", kind: "request", replyTo: "reply", payload: 1 });
  assert.throws(() => bus.publish(req), AmbiguousRoute);
  s1.close();
  const done = bus.publish(req);
  assert.equal(done, true);
  assert.equal(bus.waiting(req.header.id), false);
  s2.close();
});

test("bus: full inbox rejects publish; tell skips it and reports drops", () => {
  const bus = new Bus();
  const full = new Inbox(1);
  const rx = full.receiver();
  const sub = bus.subscribe("f", full);
  const first = makeEnvelope({ id: "f1", topic: "f", from: "p", payload: 1 });
  const second = makeEnvelope({ id: "f2", topic: "f", from: "p", payload: 2 });
  assert.equal(bus.publish(first), true);
  assert.throws(() => bus.publish(second), MailboxFull);
  assert.equal(bus.tell(second), 1);
  assert.equal(rx.tryRecv().header.id, "f1");
  sub.close();
});

test("bus: trust policy rejects untrusted sender", () => {
  const bus = new Bus();
  bus.subscribe("t", new Inbox(8));
  bus.setTrustPolicy({ allows: (from) => from === "trusted" });
  const bad = makeEnvelope({ id: "t1", topic: "t", from: "evil", payload: 1 });
  assert.throws(() => bus.publish(bad), EnvelopeBuildError);
  assert.throws(() => bus.tell(bad), EnvelopeBuildError);
  const good = makeEnvelope({ id: "t2", topic: "t", from: "trusted", payload: 1 });
  assert.equal(bus.publish(good), true);
  bus.setTrustPolicy(null);
});

test("bus: ingress migrates envelope payload before delivery", () => {
  const schemas = new SchemaRegistry();
  schemas.register("m", {
    version: 2,
    validate: (p) => (p?.n === 2 ? null : "bad"),
    migrate: (p, from) => ({ n: from + 1 }),
  });
  const bus = new Bus(schemas);
  const inbox = new Inbox(8);
  const rx = inbox.receiver();
  const sub = bus.subscribe("m", inbox);
  bus.publish(makeEnvelope({ id: "m1", topic: "m", from: "p", payload: { n: 1 }, schemaVersion: 1 }));
  assert.equal(rx.tryRecv().header.sv, 2);
  sub.close();
});

test("bus: claim token is single-use and waits are plain bookkeeping", () => {
  const bus = new Bus();
  const owner = {};
  const token = bus.claim(owner);
  assert.throws(() => bus.claim({}), /bus already belongs to a runtime/);
  assert.equal(bus.holdWait("w1"), false);
  bus.release(token);
  assert.equal(bus.holdWait("w1"), true);
  assert.equal(bus.waiting("w1"), true);
  assert.equal(bus.waiting(undefined), false);
  bus.release(token);
  assert.equal(bus.releaseWait("w1"), true);
  assert.equal(bus.waiting("w1"), false);
});

// --- runtime contract ------------------------------------------------------

test("runtime: duplicate host and admission after close are rejected", () => {
  const rt = newRuntime();
  const host = new Host("dup", ["x"], (root) => finisher(1)(root));
  rt.add(host);
  assert.throws(() => rt.add(new Host("dup", ["y"], (root) => finisher(1)(root))), /duplicate host actor/);
  rt.close();
  assert.throws(() => rt.add(new Host("late", ["z"], (root) => finisher(1)(root))), /closed or reentrant runtime/);
  assert.throws(() => rt.tell("z", 1), /runtime is closed/);
});

test("runtime: askWith hands completion to pay and stats report host detail", async () => {
  const rt = newRuntime();
  rt.add(new Host("echo", ["echo"], (root) => finisher({ v: 9 })(root)));
  let completion = null;
  rt.askWith("echo", {}, (c) => { completion = c; }, "caller");
  advance(rt);
  assert.equal(completion.body.value.v, 9);
  const stats = rt.stats();
  assert.equal(stats.hosts, 2);
  assert.equal(typeof stats.hostsDetail.echo.active, "number");
  assert.equal(stats.active + stats.held + stats.queued, 0);
  rt.close();
});

test("record log: a failing sink propagates on append", () => {
  const faulty = {
    durable: 0,
    append() { throw new Error("sink down"); },
    flush: async () => {},
    barrier: async () => {},
    checkpoint: () => 0,
  };
  const log = new RecordLog(faulty, 4);
  const envelope = makeEnvelope({ id: "w1", topic: "w", from: "p", payload: 1 });
  assert.equal(log.record(envelope, 0)?.seq, 1);
  assert.throws(() => log.drain(), /sink down/);
});

test("runtime: max_inflight backpressure rejects an extra ask", () => {
  const rt = newRuntime();
  rt.add(new Host("flood", ["flood"], (root) => new (class extends Job {
    constructor(root) { super(root, 2); }
    begin(ctx) {
      ctx.request({ stage: "a", topic: "never", payload: 1 });
      ctx.request({ stage: "b", topic: "never", payload: 2 });
      ctx.request({ stage: "c", topic: "never", payload: 3 });
    }
  })(root)));
  rt.ask("flood", {}, "c");
  assert.throws(() => advance(rt, 3), BackpressureError);
  rt.close();
});

test("runtime: held lease can settle, and close revokes it", async () => {
  let held = null;
  const rt = newRuntime();
  rt.add(new Host("hold", ["hold"], (root) => new (class extends Job {
    begin(ctx) { held = ctx.hold({ deadlineMs: 100 }); }
    onResume(ctx) { ctx.finish(okPayload({ resumed: true })); }
    onFinished(body) { seen.push(body); }
  })(root)));
  const seen = [];
  const p = rt.ask("hold", {}, "c");
  advance(rt, 3);
  assert.equal(held instanceof Lease, true);
  assert.equal(held.remainingMs, null);
  assert.equal(held.expired(100), true);
  held.settle(okPayload({ async: true }));
  await settle(rt, 4);
  assert.equal((await p.ready).body.value.async, true);
  assert.equal(seen.length, 1);
  rt.close();
});

test("runtime: lease fail/cancel/close semantics", () => {
  const lease = new Lease({ budgetMs: 5, deadlineMs: 10 });
  assert.equal(lease.consume(3), true);
  assert.equal(lease.remainingMs, 2);
  assert.equal(lease.consume(3), false);
  assert.equal(lease.expired(9), false);
  assert.equal(lease.expired(10), true);
  lease.fail("nope", 42);
  assert.equal(lease.take().error, "nope");
  assert.equal(lease.cancel(), false);
  assert.equal(lease.isSettled, true);
  lease.abort();
  assert.equal(lease.signal.aborted, true);
  lease.close();
  assert.equal(lease.isClosed, true);
  assert.throws(() => lease.send(okPayload(1)), /lease closed/);
  assert.throws(() => new Lease().send({ nope: 1 }), TypeError);
});

test("runtime: close effect retires the runtime from a job", async () => {
  const rt = newRuntime();
  rt.add(new Host("stop", ["stop"], (root) => new (class extends Job {
    begin(ctx) { return [ctx.close("done")]; }
  })(root)));
  const p = rt.ask("stop", {}, "c");
  advance(rt);
  assert.equal((await p.ready).body.error, "job_silent");
  rt.close();
});

test("runtime: shutdown is idempotent and close returns abandoned roots", () => {
  const rt = newRuntime();
  rt.add(new Host("idle", ["idle"], (root) => finisher(1)(root)));
  rt.shutdown();
  rt.shutdown();
  assert.equal(rt.schedule().isQuiescent, true);
  assert.deepEqual(rt.close(), []);
  assert.deepEqual(rt.close(), []);
});

test("runtime: tell publishes a best-effort event to a subscriber host", async () => {
  const seen = [];
  const rt = newRuntime();
  rt.add(new Host("events", ["evt.*"], (root) => new (class extends Job {
    begin() { seen.push(root.header.topic); }
  })(root)));
  rt.tell("evt.hi", { n: 1 }, "src");
  advance(rt);
  assert.deepEqual(seen, ["evt.hi"]);
  rt.close();
});
