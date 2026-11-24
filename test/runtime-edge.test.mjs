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
  okPayload,
} from "../dist/index.js";

class Host extends JobHost {
  constructor(actor, options = {}) {
    super({ actorId: actor, receiver: new Inbox(16).receiver(), patterns: [actor], ...options });
  }
  makeJob(root) {
    return new (class extends Job {
      begin(ctx) { ctx.finish(okPayload({ actor: root.header.topic })); }
    })(root);
  }
}

const makeRuntime = () => new Runtime(new Bus(), new ManualClock(0), new SeqIdGen(1));
const advance = (rt, n = 8) => {
  for (let i = 0; i < n; i += 1) rt.advance();
};

test("runtime: targeted shutdown drains only the named host", () => {
  const rt = makeRuntime();
  rt.add(new Host("one"));
  rt.add(new Host("two"));
  rt.shutdown("one");
  assert.equal(rt.stats().hosts, 2);
  const one = rt.ask("one", {}, "c");
  const two = rt.ask("two", {}, "c");
  advance(rt);
  assert.equal(two.ready !== undefined, true);
  void one;
  rt.close();
});

class RecordingHost extends JobHost {
  constructor(actor, options) {
    super({ actorId: actor, receiver: new Inbox(16).receiver(), patterns: [actor], ...options });
  }
  makeJob(root) {
    return new (class extends Job {
      begin(ctx) { return [ctx.record("r", { n: 1 })]; }
    })(root);
  }
}

test("runtime: closeAsync surfaces an async sink rejection and stashes roots", async () => {
  const sink = {
    durable: 0,
    append() {},
    flush() { return Promise.reject(new Error("flush failed")); },
    barrier() { return Promise.resolve(); },
    checkpoint: () => 0,
  };
  const rt = makeRuntime();
  rt.add(new RecordingHost("writer", { recordSink: sink }));
  const pending = rt.ask("writer", {}, "c");
  advance(rt, 4);
  await assert.rejects(() => rt.closeAsync(), /flush failed/);
  assert.notEqual(await pending.ready, null);
  assert.doesNotThrow(() => rt.close());
});

test("runtime: closeAsync with a timeout rejects a stuck flush", async () => {
  const sink = {
    durable: 0,
    append() {},
    flush: () => new Promise(() => {}),
    barrier: () => new Promise(() => {}),
    checkpoint: () => 0,
  };
  const rt = makeRuntime();
  rt.add(new RecordingHost("stuck", { recordSink: sink }));
  rt.ask("stuck", {}, "c");
  advance(rt, 4);
  await assert.rejects(() => rt.closeAsync(undefined, 5), /closeAsync timed out/);
  await rt.closeAsync();
});
