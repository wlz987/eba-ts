import test from "node:test";
import assert from "node:assert/strict";
import {
  Bus,
  Inbox,
  Signal,
  SchemaRegistry,
  makeEnvelope,
} from "../dist/index.js";
import { Interest } from "../dist/core/notify.js";
import { readyOrWatchOf } from "../dist/core/mail/bus.js";
import { stateOf } from "../dist/core/mail/inbox.js";

const note = (id = "m1") => makeEnvelope({ id, topic: "arm.topic", from: "p", payload: 1 });
const full = () => {
  const bus = new Bus();
  const inbox = new Inbox(1);
  const rx = inbox.receiver();
  const sub = bus.subscribe("arm.topic", inbox);
  bus.publish(note("seed"));
  return { bus, inbox, rx, sub };
};

test("bus arm: readyOrWatchOf watches a full target", () => {
  const { bus, rx, sub } = full();
  const interest = new Interest(new Signal(), {}, "arm.topic");
  assert.equal(readyOrWatchOf(bus, note(), interest), false);
  assert.ok(interest.$owners.size > 0);
  interest.dispose();
  rx.tryRecv();
  sub.close();
});

test("bus arm: deliver parks then lands when space frees", () => {
  const { bus, rx, sub } = full();
  const home = { actorId: "home" };
  bus.deliver(note("parked"), home);
  assert.equal(rx.tryRecv().header.id, "seed");
  assert.equal(rx.tryRecv()?.header.id, "parked");
  sub.close();
});

test("bus arm: a vanished route returns the letter to armed inboxes", () => {
  const { bus, rx, sub } = full();
  bus.deliver(note("seized"), { actorId: "home" });
  sub.close();
  rx.tryRecv();
  assert.equal(stateOf(rx.inbox).seized.length, 1);
  assert.equal(stateOf(rx.inbox).seized[0].header.id, "seized");
});

test("bus arm: abandonDeliveries returns only the named home's letters", async () => {
  const { bus } = full();
  const homeA = { actorId: "a" };
  const homeB = { actorId: "b" };
  bus.deliver(note("a1"), homeA);
  bus.deliver(note("b1"), homeB);
  await new Promise((resolve) => setImmediate(resolve));
  const abandoned = bus.abandonDeliveries(homeA);
  assert.deepEqual(abandoned.map((e) => e.header.id), ["a1"]);
  assert.deepEqual(bus.abandonDeliveries(homeA), []);
  await new Promise((resolve) => setImmediate(resolve));
  assert.deepEqual(bus.abandonDeliveries(homeB).map((e) => e.header.id), ["b1"]);
});

test("bus arm: deliver ignores an envelope that fails ingress migration", () => {
  const schemas = new SchemaRegistry();
  schemas.register("x", {
    version: 2,
    validate: () => null,
    migrate: () => { throw new Error("bad migration"); },
  });
  const bus = new Bus(schemas);
  bus.subscribe("x", new Inbox(4));
  const stale = makeEnvelope({ id: "stale", topic: "x", from: "p", payload: 1, schemaVersion: 1 });
  assert.doesNotThrow(() => bus.deliver(stale, { actorId: "home" }));
});
