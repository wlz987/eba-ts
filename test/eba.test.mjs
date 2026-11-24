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
  UuidIdGen,
  okPayload,
  errPayload,
  errPayloadCode,
  isResultOk,
  isResultErr,
  resultValue,
  resultError,
  makeEnvelope,
  DedupWindow,
  DedupSink,
  RecordLog,
  MemorySink,
  dedupKey,
  Lease,
  WorkerPool,
  SchemaRegistry,
} from "../dist/index.js";

const advance = (runtime, times = 10) => {
  for (let i = 0; i < times; i += 1) runtime.advance();
};

// Deferred continuations run as promise callbacks: synchronous `advance` loops
// never let them fire, so tests that cross a barrier/timer must yield first.
const advanceAsync = async (runtime, times = 10) => {
  for (let i = 0; i < times; i += 1) {
    runtime.advance();
    await new Promise((resolve) => setImmediate(resolve));
  }
};

class Host extends JobHost {
  constructor(actorId, patterns, make) {
    super({ actorId, receiver: new Inbox(32).receiver(), patterns });
    this.make = make;
  }
  makeJob(root) {
    return this.make(root);
  }
}

test("envelope: id, cause, topic, result shape", () => {
  const ids = new SeqIdGen(1);
  const e = makeEnvelope({ topic: "a.b", payload: { x: 1 }, from: "me", kind: "event", idGen: ids });
  assert.equal(e.header.id, "00000000000000000000000000000001");
  assert.equal(e.header.cause, e.header.id);
  assert.equal(e.header.topic, "a.b");
  assert.equal(new UuidIdGen().nextEnvelopeId().length, 32);
  assert.ok(isResultOk(okPayload({ a: 1 })));
  assert.ok(!isResultErr(okPayload(1)));
  assert.equal(resultValue(okPayload({ a: 1 })).a, 1);
  assert.equal(resultError(errPayloadCode("y", 42)), "y");
  assert.equal(errPayload("z", { k: 1 }).k, 1);
  assert.deepEqual(okPayload(undefined), { ok: true, value: undefined });
});

test("dedup window: fifo and byte-length key", () => {
  const w = new DedupWindow(2);
  assert.equal(w.accept("a"), true);
  assert.equal(w.accept("a"), false);
  assert.equal(w.accept("b"), true);
  assert.equal(w.accept("c"), true);
  assert.equal(w.has("a"), false);
  assert.equal(dedupKey("ab:c", "d"), "4:ab:c1:d");
});

test("record log: dedup at sink, flush watermark and checkpoint", async () => {
  const sink = new MemorySink();
  const log = new RecordLog(new DedupSink(sink), 8);
  const envelope = makeEnvelope({ topic: "fact", payload: { n: 1 }, from: "me", kind: "event", idGen: new SeqIdGen(1) });
  assert.ok(log.record(envelope, 0));
  assert.ok(log.record(envelope, 0));
  assert.equal(log.drain(), 2);
  assert.equal(sink.records().length, 1);
  assert.equal(sink.durable, 0);
  await log.flush(1);
  await log.barrier(1);
  assert.equal(log.durable, 1);
  assert.throws(() => log.barrier(99), /barrier target beyond recorded seq/);
  assert.equal(log.checkpoint(1), 1);
  assert.equal(sink.records().length, 0);
});

test("schema registry: default version and monotonic upgrade", () => {
  const s = new SchemaRegistry();
  assert.equal(s.version("none"), 1);
  s.register("x", { version: 2, validate: () => null });
  assert.equal(s.version("x"), 2);
  assert.throws(() => s.register("x", { version: 2, validate: () => null }));
});

test("worker pool: bounded concurrency and close", async () => {
  const pool = new WorkerPool(2, 2);
  assert.equal(await pool.run(async () => 1), 1);
  assert.equal(await pool.run(async () => 2), 2);
  await pool.close();
  await assert.rejects(() => pool.run(async () => 3));
});

test("runtime ask: finish settles pending", async () => {
  const rt = new Runtime(new Bus(), new ManualClock(0), new SeqIdGen());
  rt.add(new Host("hello", ["greet"], (root) => new (class extends Job {
    begin(ctx) { ctx.finish(okPayload({ hi: true })); }
  })(root)));
  const p = rt.ask("greet", {}, "caller");
  advance(rt);
  const c = await p.ready;
  assert.equal(c.body.ok, true);
  assert.equal(c.body.value.hi, true);
  assert.equal(c.isTimeout, false);
  rt.close();
});

test("runtime: hook failure surfaces and silent job settles", async () => {
  {
    const rt = new Runtime(new Bus(), new ManualClock(0), new SeqIdGen());
    rt.add(new Host("boom", ["boom"], (root) => new (class extends Job {
      begin() { throw new Error("boom"); }
    })(root)));
    rt.ask("boom", {}, "c");
    assert.throws(() => advance(rt), /boom/);
    rt.close();
  }
  {
    const rt = new Runtime(new Bus(), new ManualClock(0), new SeqIdGen());
    rt.add(new Host("silent", ["silent"], (root) => new (class extends Job { begin() {} })(root)));
    const p = rt.ask("silent", {}, "c");
    advance(rt);
    const c = await p.ready;
    assert.equal(c.body.error, "job_silent");
    rt.close();
  }
});

test("runtime: timer resumes a held job", async () => {
  const rt = new Runtime(new Bus(), new ManualClock(0), new SeqIdGen());
  rt.add(new Host("timer", ["timer"], (root) => new (class extends Job {
    begin(ctx) { return [ctx.timer(1)]; }
    onResume(ctx) { ctx.finish(okPayload({ tick: true })); }
  })(root)));
  const p = rt.ask("timer", {}, "c");
  await advanceAsync(rt, 3);
  assert.equal(rt.schedule().nextDeadlineMs, 1);
  rt.clock.advance(5);
  await advanceAsync(rt, 3);
  const c = await p.ready;
  assert.equal(c.body.value.tick, true);
  rt.close();
});

test("runtime: bad timer deadline is rejected at effect landing", () => {
  for (const [deadline, expected] of [
    [Number.NaN, /deadline_ms must be a nonnegative integer/],
    [-1, /deadline_ms must be a nonnegative integer/],
    [1.5, /deadline_ms must be a nonnegative integer/],
    [2 ** 54, /deadline_ms must be a safe integer/],
  ]) {
    const rt = new Runtime(new Bus(), new ManualClock(0), new SeqIdGen());
    rt.add(new Host("bad", ["bad"], (root) => new (class extends Job {
      begin(ctx) { return [ctx.timer(deadline)]; }
    })(root)));
    rt.ask("bad", {}, "c");
    assert.throws(() => advance(rt), expected);
    rt.close();
  }
});

test("effects: hold must lead even for raw effect literals", () => {
  const rt = new Runtime(new Bus(), new ManualClock(0), new SeqIdGen());
  rt.add(new Host("order", ["order"], (root) => new (class extends Job {
    begin() {
      return [
        { kind: "finish", body: okPayload({}) },
        { kind: "hold", lease: new Lease() },
      ];
    }
  })(root)));
  rt.ask("order", {}, "c");
  assert.throws(() => advance(rt), /cannot hold after other effects/);
  rt.close();
});

test("runtime: record and barrier complete", async () => {
  const rt = new Runtime(new Bus(), new ManualClock(0), new SeqIdGen());
  rt.add(new Host("rec", ["rec"], (root) => new (class extends Job {
    begin(ctx) { return [ctx.record("fact", { n: 1 }), ctx.barrier()]; }
    onResume(ctx) { ctx.finish(okPayload({ r: 1 })); }
  })(root)));
  const p = rt.ask("rec", {}, "c");
  await advanceAsync(rt);
  const c = await p.ready;
  assert.equal(c.body.value.r, 1);
  rt.close();
});

test("runtime: close drains held jobs and settles pending", async () => {
  const rt = new Runtime(new Bus(), new ManualClock(0), new SeqIdGen());
  rt.add(new Host("held", ["held"], (root) => new (class extends Job {
    begin(ctx) { return [ctx.timer(1000)]; }
    onResume(ctx) { ctx.finish(okPayload({ tick: true })); }
  })(root)));
  const p = rt.ask("held", {}, "c");
  rt.advance();
  await new Promise((resolve) => setImmediate(resolve));
  const roots = rt.close();
  assert.equal(roots.length, 2);
  assert.equal(rt.schedule().isQuiescent, true);
  assert.deepEqual(rt.stats().active, 0);
  assert.equal((await p.ready).body.error, "host_draining");
});

test("bus: wildcard topic routing via host subscription", async () => {
  const rt = new Runtime(new Bus(), new ManualClock(0), new SeqIdGen());
  const make = (root) => new (class extends Job {
    begin(ctx) { ctx.finish(okPayload({ topic: ctx.root.header.topic })); }
  })(root);
  rt.add(new Host("glob", ["msg.*"], make));
  const p = rt.ask("msg.hello", {}, "c");
  advance(rt);
  const c = await p.ready;
  assert.equal(c.body.value.topic, "msg.hello");
  rt.close();
});

test("runtime: nested ask via request and onStageResult", async () => {
  const rt = new Runtime(new Bus(), new ManualClock(0), new SeqIdGen());
  rt.add(new Host("child", ["child"], (root) => new (class extends Job {
    begin(ctx) { ctx.finish(okPayload({ v: 7 })); }
  })(root)));
  rt.add(new Host("parent", ["parent"], (root) => new (class extends Job {
    begin(ctx) { ctx.request({ stage: "s", topic: "child", payload: {} }); }
    onStageResult(ctx, stage, completion) {
      ctx.finish(okPayload({ got: completion.body?.value?.v }));
    }
  })(root)));
  const p = rt.ask("parent", {}, "c");
  advance(rt, 30);
  const c = await Promise.race([p.ready, new Promise((r) => setTimeout(() => r(null), 200))]);
  assert.notEqual(c, null);
  assert.equal(c.body.value.got, 7);
  rt.close();
});

test("runtime: quiescent after shutdown and all timers fired", async () => {
  const rt = new Runtime(new Bus(), new ManualClock(0), new SeqIdGen());
  const seen = [];
  rt.add(new Host("s", ["s"], (root) => new (class extends Job {
    begin(ctx) { return [ctx.timer(1)]; }
    onResume(ctx) { seen.push(1); ctx.finish(okPayload({ ok: 1 })); }
  })(root)));
  const p = rt.ask("s", {}, "c");
  await advanceAsync(rt, 3);
  rt.clock.advance(5);
  await advanceAsync(rt, 3);
  assert.equal((await p.ready).body.value.ok, 1);
  assert.equal(seen.length, 1);
  rt.shutdown();
  assert.equal(rt.schedule().isQuiescent, true);
  rt.close();
});
