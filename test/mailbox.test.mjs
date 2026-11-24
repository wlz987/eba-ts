import test from "node:test";
import assert from "node:assert/strict";
import {
  Bus,
  Inbox,
  Signal,
  Mailbox,
  makeEnvelope,
  MailboxBusy,
  DeliveryError,
} from "../dist/index.js";
import { setCandidate } from "../dist/core/notify.js";
import { mailboxCandidates, mailboxGroups } from "../dist/core/job/mailbox/base.js";

const note = (id, topic = "result.topic", extra = {}) => makeEnvelope({
  id, topic, from: "worker", kind: "result", payload: { ok: true, value: id }, ...extra,
});
const fullBus = () => {
  const bus = new Bus();
  const inbox = new Inbox(1);
  const rx = inbox.receiver();
  const sub = bus.subscribe("result.topic", inbox);
  bus.publish(note("seed"));
  return { bus, inbox, rx, sub };
};

test("mailbox: direct publish, parked group, bound and rearm/bury", () => {
  const bus = new Bus();
  const box = new Mailbox(3, new Signal());
  box.send(bus, note("direct"));
  assert.equal(box.count, 0);

  const { bus: busy, sub } = fullBus();
  const parked = new Mailbox(3, new Signal());
  parked.send(busy, note("a"));
  assert.equal(parked.count, 1);
  assert.equal(parked.contains(note("a")), true);
  parked.send(busy, note("b"));
  assert.equal(parked.count, 2);
  assert.equal(mailboxGroups(parked).get("result.topic").items.length, 2);
  parked.send(busy, note("c"));
  assert.equal(parked.count, 3);
  assert.throws(() => parked.send(busy, note("d")), MailboxBusy);

  parked.rearm(note("b"));
  assert.equal(mailboxCandidates(parked)["result.topic"], undefined);
  parked.rearm(note("a"));
  assert.equal(mailboxCandidates(parked)["result.topic"], null);
  parked.bury(note("b"));
  assert.equal(parked.count, 3);
  parked.bury(note("a"));
  assert.equal(parked.count, 2);
  parked.bury(note("c"));
  assert.equal(parked.count, 2);
  parked.bury(note("b"));
  assert.equal(parked.count, 1);
  parked.bury(note("c"));
  assert.equal(parked.count, 0);
  sub.close();
});

test("mailbox: flushOne idle, checked and advanced steps", () => {
  const { bus, sub } = fullBus();
  const box = new Mailbox(4, new Signal());
  assert.deepEqual(box.flushOne(bus), ["idle", null]);

  box.send(bus, note("p1"));
  assert.equal(box.count, 1);
  setCandidate(mailboxCandidates(box), "result.topic");
  assert.deepEqual(box.flushOne(bus), ["checked", null]);

  sub.close();
  const [step, envelope] = box.flushOne(bus);
  assert.equal(step, "advanced");
  assert.equal(envelope.header.id, "p1");
  assert.equal(box.count, 0);

  setCandidate(mailboxCandidates(box), "ghost.topic");
  assert.deepEqual(box.flushOne(bus), ["idle", null]);
  assert.equal(mailboxCandidates(box)["ghost.topic"], undefined);
});

test("mailbox: flushOne reports ambiguous and awaited publication", () => {
  const bus = new Bus();
  const a = bus.subscribe("dup", new Inbox(4));
  const b = bus.subscribe("dup", new Inbox(4));
  const box = new Mailbox(4, new Signal());
  const ambiguous = makeEnvelope({
    id: "amb", topic: "dup", from: "worker", kind: "request", replyTo: "r", payload: {},
  });
  assert.throws(() => box.send(bus, ambiguous), (e) =>
    e instanceof DeliveryError && e.message === "result publication failed");
  assert.deepEqual(box.flushOne(bus), ["idle", null]);
  a.close();
  b.close();

  const waiting = new Bus();
  waiting.holdWait("corr-1");
  const parked = new Mailbox(4, new Signal());
  assert.throws(() => parked.send(waiting, note("awaited", "no.route", {
    correlationId: "corr-1", cause: "corr-1",
  })), (e) => e instanceof DeliveryError && e.message === "result publication failed");
});

test("mailbox: send wraps foreign publish errors and drain empties books", () => {
  const bus = new Bus();
  bus.subscribe("result.topic", new Inbox(4));
  bus.setTrustPolicy({ allows: () => false });
  const box = new Mailbox(4, new Signal());
  assert.throws(() => box.send(bus, note("bad")), (e) =>
    e instanceof DeliveryError && e.message === "result publication failed");
  bus.setTrustPolicy(null);

  const { bus: busy, sub } = fullBus();
  const parked = new Mailbox(4, new Signal());
  parked.send(busy, note("z1"));
  parked.send(busy, note("z2"));
  const out = [];
  parked.drain(out);
  assert.deepEqual(out.map((e) => e.header.id), ["z1", "z2"]);
  assert.equal(parked.count, 0);
  assert.equal(parked.size, 0);
  assert.equal(parked.hasRoom(), true);
  sub.close();
});
