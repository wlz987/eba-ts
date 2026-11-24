import test from "node:test";
import assert from "node:assert/strict";
import {
  Bus,
  Inbox,
  Job,
  ManualClock,
  SeqIdGen,
  makeEnvelope,
  okPayload,
} from "../dist/index.js";
import { Core } from "../dist/core/job/core.js";
import { adopt, resume } from "../dist/core/job/resume.js";
import { step, pump } from "../dist/core/job/pump/step.js";
import { DELIVERY } from "../dist/core/notify.js";
import { attach } from "../dist/core/job/pump.js";

const ids = () => new SeqIdGen(1);
const clock = () => new ManualClock(0);
const event = (id = "ev1") => makeEnvelope({ id, topic: "q", from: "p", payload: 1 });
const request = (id = "r1") => makeEnvelope({
  id, topic: "q", from: "p", kind: "request", replyTo: "reply", payload: 1,
});
const result = (id, cause) => makeEnvelope({
  id, topic: "reply", from: "w", kind: "result", payload: okPayload({ v: 1 }),
  correlationId: cause, cause,
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

test("adopt: duplicate id and a stale factory job are refused", () => {
  const bus = new Bus();
  const core = makeCore();
  const root = event("dup");
  core.admitJob("dup", new Job(root));
  assert.doesNotThrow(() => adopt(core, bus, clock(), ids(), root));
  assert.equal(core.rejectedCount, 1);

  const stale = new Job(event("fresh"));
  const broken = makeCore({ makeJob: () => stale });
  assert.throws(
    () => adopt(broken, bus, clock(), ids(), event("fresh")),
    /factory must return a fresh Job for this root/,
  );
  assert.equal(broken.rejectedCount, 1);
});

test("resume: unreadable and unknown results are kept as abandoned", () => {
  const bus = new Bus();
  const core = makeCore();
  core.leases.submit(bus, core.sub.inbox, request("known"), "owner", "stage", null);
  const unreadable = makeEnvelope({
    id: "bad", topic: "reply", from: "w", kind: "result",
    payload: null, correlationId: "known", cause: "known",
  });
  assert.doesNotThrow(() => resume(core, bus, clock(), ids(), unreadable));
  assert.equal(core.abandonedMut().length, 1);
  assert.doesNotThrow(() => resume(core, bus, clock(), ids(), 999999));
});

test("step: holding phases adopt queued events and resume results", () => {
  const bus = new Bus();
  const core = makeCore({
    makeJob: (root) => new (class extends Job {
      begin(ctx) { return [ctx.timer(1000)]; }
    })(root),
  });
  attach(core, bus);
  core.parkLetter(event("parked"), null);
  core.queuePush(event("h1"));
  assert.equal(step(core, bus, clock(), ids(), 2), "advanced");
  assert.equal(core.jobs.has("h1"), true);

  const waiting = makeCore();
  attach(waiting, bus);
  waiting.parkLetter(event("keep"), null);
  bus.subscribe("reply", waiting.sub.inbox);
  bus.publish(result("res1", "missing"));
  assert.equal(step(waiting, bus, clock(), ids(), 0), "advanced");
});

test("step: phase 3 flushes delivery and pump terminates on idle", () => {
  const bus = new Bus();
  const core = makeCore();
  core.signal.notify(DELIVERY);
  assert.equal(step(core, bus, clock(), ids(), 3), "idle");
  assert.equal(pump(core, bus, clock(), ids(), 8), false);
});

test("step: phase 0 dispatches an event, phase 1 resumes timers", () => {
  const bus = new Bus();
  const core = makeCore();
  attach(core, bus);
  bus.publish(event("direct"));
  core.signal.notify(1);
  assert.equal(step(core, bus, clock(), ids(), 0), "advanced");

  const timed = makeCore();
  timed.signal.notify(2);
  assert.equal(step(timed, bus, clock(), ids(), 1), "advanced");
});
