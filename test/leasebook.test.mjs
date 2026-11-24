import test from "node:test";
import assert from "node:assert/strict";
import {
  Bus,
  Inbox,
  makeEnvelope,
  EnvelopeBuildError,
} from "../dist/index.js";
import { LeaseRecords } from "../dist/core/rendezvous/records.js";
import { Subscription } from "../dist/core/mail/bus.js";
import {
  mine,
  matched,
  live,
  right,
} from "../dist/core/rendezvous/lease.js";

const request = (id = "req-1") => makeEnvelope({
  id, topic: "work", from: "client", kind: "request", replyTo: "reply", payload: { n: 1 },
});
const continuation = { tag: "continuation" };

test("rendezvous lease helpers: frame lookup, matching and ownership", () => {
  const bus = new Bus();
  const inbox = new Inbox(8);
  const sub = bus.subscribe("reply", inbox);
  const frame = [[inbox.id, sub]];
  assert.deepEqual(mine(inbox.id, frame), [sub]);
  assert.deepEqual(mine(999, frame), []);
  assert.equal(matched(sub, [sub]), sub);
  assert.equal(matched({}, [sub]), sub);
  assert.equal(matched({}, []), null);
  assert.equal(matched(new Subscription(() => {}), [sub]), sub);
  assert.equal(live(sub), sub);
  assert.throws(() => live({}), /lease does not carry its subscription/);
  assert.equal(right(inbox.id, sub, frame), sub);
  assert.throws(() => right(inbox.id, {}, []), /lease does not carry its subscription/);
  sub.close();
});

test("lease records: submit, claim, duplicate and deadline bookkeeping", () => {
  const bus = new Bus();
  const inbox = new Inbox(8);
  const records = new LeaseRecords();
  records.submit(bus, inbox, request(), continuation, 10);
  assert.equal(records.size, 1);
  assert.equal(records.continuation("req-1"), continuation);
  assert.deepEqual(records.ids(), ["req-1"]);
  assert.equal(records.nextDeadlineMs, 10);
  assert.throws(
    () => records.submit(bus, inbox, request(), continuation, 10),
    EnvelopeBuildError,
  );
  assert.throws(
    () => records.submit(bus, inbox, makeEnvelope({
      id: "bad", topic: "work", from: "client", payload: 1,
    }), continuation, 10),
    /expected a request with reply_to/,
  );
  const claim = records.expireDue(10);
  assert.equal(claim.continuation, continuation);
  assert.equal(claim.completion.body, null);
  assert.equal(records.size, 0);
  assert.equal(records.expireDue(10), null);
  records.elapseOpen();
  assert.equal(records.nextDeadlineMs, null);
});

test("lease records: undelivered request is immediately due", () => {
  const bus = new Bus();
  const inbox = new Inbox(8);
  const records = new LeaseRecords();
  records.submit(bus, inbox, request("lost"), continuation, null);
  assert.notEqual(records.nextDeadlineMs, null);
  assert.equal(records.expireDue(records.nextDeadlineMs).completion.isTimeout, true);
});

test("lease records: a stuck subscription is remembered then recovered", () => {
  const bus = new Bus();
  const inbox = new Inbox(8);
  const subscribe = bus.subscribe.bind(bus);
  let native = null;
  bus.subscribe = (topic, box) => {
    native = subscribe(topic, box);
    return {
      close() { throw new Error("stuck subscription"); },
    };
  };
  const records = new LeaseRecords();
  const req = request("stuck");
  records.submit(bus, inbox, req, continuation, 100);
  const reply = makeEnvelope({
    id: "res-1", topic: "reply", from: "worker", kind: "result",
    payload: { ok: true, value: 1 }, correlationId: "stuck", cause: "stuck",
  });
  const claim = records.takeResult(reply);
  assert.equal(claim.completion.body.value, 1);
  assert.equal(records.unreleased, 1);
  records.recover();
  assert.equal(records.unreleased, 1);
  native.close();
  records.recover();
  assert.equal(records.unreleased, 0);
});

test("lease records: unreadable results and mismatches", () => {
  const records = new LeaseRecords();
  assert.throws(() => records.takeResult(null), TypeError);
  assert.throws(() => records.takeResult({}), TypeError);
  assert.equal(records.takeResult(request("none")), null);
});
