import test from "node:test";
import assert from "node:assert/strict";
import {
  Bus,
  Inbox,
  Job,
  makeEnvelope,
  okPayload,
  DeliveryError,
} from "../dist/index.js";
import { Core } from "../dist/core/job/core.js";
import { Control } from "../dist/core/job/context.js";
import { setPendingControl } from "../dist/core/job/job.js";
import {
  noteDeliveryFailure,
  noteDeliverySuccess,
  raiseDeliveryFailure,
  holdOrSend,
  completeHeld,
  releaseAttempts,
} from "../dist/core/job/delivery.js";
import { park, holdsLetter, owns, parkedJob, unpark } from "../dist/core/job/delivery/parking.js";
import { nextSendable, retryDue, releaseResult } from "../dist/core/job/delivery/release.js";

const note = (id) => makeEnvelope({
  id, topic: "reply", from: "worker", kind: "result",
  payload: okPayload({ v: 1 }), correlationId: id, cause: id,
});
const makeCore = (options = {}) => new Core({
  actorId: "a",
  receiver: new Inbox(16).receiver(),
  patterns: ["q"],
  queueLimit: 8,
  activeLimit: 8,
  workBudget: 8,
  mailboxLimit: 2,
  makeJob: (root) => new Job(root),
  onRejected: () => {},
  ...options,
});
const fullBus = () => {
  const bus = new Bus();
  const inbox = new Inbox(1);
  const sub = bus.subscribe("reply", inbox);
  bus.publish(note("seed"));
  return { bus, sub };
};

test("delivery parking: identity by id and parked owner lookup", () => {
  const core = makeCore();
  const job = new Job(note("j1"));
  const letter = note("p1");
  park(core, job, letter);
  assert.equal(holdsLetter(core, note("p1")), true);
  assert.equal(owns(core, note("p1")), true);
  assert.equal(parkedJob(core, letter), job);
  core.parkLetter(note("p2"), null);
  assert.equal(nextSendable(core)[0], "pending");
  assert.equal(nextSendable(core)[0] === "rest", false);
  unpark(core, letter);
  assert.equal(owns(core, letter), false);
  assert.equal(nextSendable(core)[0], "rest");
});

test("delivery failure: first retry, second bury and completion", () => {
  const core = makeCore();
  const job = new Job(note("j2"));
  job.$state.$lifecycle = "active";
  setPendingControl(job.$state, new Control(okPayload({ v: 2 })));
  const letter = note("f1");
  park(core, job, letter);
  noteDeliveryFailure(core, letter);
  assert.equal(core.again.has("f1"), true);
  assert.equal(core.attempts.get("f1"), 1);
  noteDeliveryFailure(core, letter);
  assert.equal(core.again.has("f1"), false);
  assert.equal(core.attempts.get("f1"), 2);
  assert.equal(owns(core, letter), false);
  noteDeliveryFailure(core, letter);
  assert.equal(job.$state.pendingControl, null);
  noteDeliverySuccess(core, letter);
  assert.equal(core.attempts.has("f1"), false);
  assert.equal(releaseAttempts(core), undefined);
});

test("delivery failure: raiseDeliveryFailure combines secondary faults", () => {
  const core = makeCore();
  const letter = note("r1");
  core.parkLetter(letter, null);
  core.noteAttempt("r1", 5);
  core.markAgain("r1");
  const error = new DeliveryError(letter, "primary");
  let thrown = null;
  try {
    raiseDeliveryFailure(core, error);
  } catch (caught) {
    thrown = caught;
  }
  assert.equal(thrown, error);
});

test("delivery: holdOrSend parks on MailboxBusy and arms the wait", () => {
  const { bus } = fullBus();
  const core = makeCore({ mailboxLimit: 0 });
  const job = new Job(note("j3"));
  const result = holdOrSend(core, bus, job, note("busy"));
  assert.equal(result, false);
  assert.notEqual(core.mailboxWait, null);
  assert.equal(core.mailbox.count, 0);
  assert.equal(retryDue(core), false);
  core.takeMailboxWait()?.dispose();
});

test("delivery: retryDue and releaseResult drain parked letters", () => {
  const bus = new Bus();
  const inbox = new Inbox(8);
  const rx = inbox.receiver();
  const sub = bus.subscribe("reply", inbox);
  const core = makeCore();
  const job = new Job(note("j4"));
  core.parkLetter(note("out1"), job);
  core.parkLetter(note("out2"), null);
  assert.equal(retryDue(core), true);
  assert.equal(releaseResult(core, bus), true);
  assert.equal(rx.tryRecv().header.id, "out1");
  assert.equal(rx.tryRecv().header.id, "out2");
  assert.equal(releaseResult(core, bus), false);
  sub.close();
});

test("delivery: releaseResult restores a letter on publication failure", () => {
  const bus = new Bus();
  const core = makeCore();
  const letter = note("restore");
  core.parkLetter(letter, null);
  core.mailbox.limit = 0;
  assert.equal(releaseResult(core, bus), false);
  assert.equal(owns(core, letter), true);
});

test("delivery: completeHeld tolerates a missing job", () => {
  const core = makeCore();
  assert.doesNotThrow(() => completeHeld(core, null));
  const job = new Job(note("j5"));
  assert.doesNotThrow(() => completeHeld(core, job));
});
