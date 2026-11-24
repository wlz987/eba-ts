import test from "node:test";
import assert from "node:assert/strict";
import {
  BusError,
  MailboxFull,
  InvalidTopic,
  AmbiguousRoute,
  BackpressureError,
  DeliveryError,
  MailboxBusy,
  LeasePending,
  WakeError,
  ClockError,
  RecordSinkError,
  SeqExhausted,
  EnvelopeBuildError,
  MAX_PAYLOAD_BYTES,
  checkPayloadBytes,
  SeqIdGen,
  UuidIdGen,
  makeEnvelope,
  okPayload,
  errPayload,
  errPayloadCode,
  isResultOk,
  isResultErr,
  resultError,
  resultValue,
  isSealed,
  actorId,
  envelopeId,
} from "../dist/index.js";
import {
  splitTopic,
  makeHeader,
  withSchemaVersion,
  withPayloadBytes,
  copyHeader,
  eventAddress,
  diagnosticAddress,
  requestAddress,
  resultAddress,
  correlationIdOf,
} from "../dist/core/envelope.js";
import {
  MAILBOX_BUSY,
  REENTRANT,
  noteSettled,
  isSettled,
  toError,
  combineFailures,
} from "../dist/core/errors.js";

test("errors: every class carries its name and default message", () => {
  const envelope = makeEnvelope({ id: "x", topic: "t", from: "a", payload: 1 });
  const cases = [
    [new BusError("bus"), "BusError", "bus"],
    [new MailboxFull("full"), "MailboxFull", "full"],
    [new InvalidTopic("topic"), "InvalidTopic", "topic"],
    [new AmbiguousRoute(), "AmbiguousRoute", "request matches multiple subscribers"],
    [new BackpressureError("bp"), "BackpressureError", "bp"],
    [new DeliveryError(envelope), "DeliveryError", "publication failed"],
    [new MailboxBusy(envelope), "MailboxBusy", MAILBOX_BUSY],
    [new LeasePending(), "LeasePending", "lease release pending"],
    [new WakeError("wake"), "WakeError", "wake"],
    [new ClockError("clock"), "ClockError", "clock"],
    [new RecordSinkError("sink"), "RecordSinkError", "sink"],
    [new SeqExhausted(), "SeqExhausted", "seq exhausted"],
    [new EnvelopeBuildError("env"), "EnvelopeBuildError", "env"],
  ];
  for (const [error, name, message] of cases) {
    assert.equal(error.name, name);
    assert.equal(error.message, message);
  }
  assert.equal(new DeliveryError(envelope).envelope, envelope);
  assert.equal(REENTRANT, "reentrant host execution");
});

test("errors: settled markers, coercion and combined delivery failures", () => {
  const envelope = makeEnvelope({ id: "x", topic: "t", from: "a", payload: 1 });
  assert.equal(toError("bad").message, "bad");
  assert.equal(toError(new Error("keep")).message, "keep");
  const marked = noteSettled(new Error("settled"));
  assert.equal(isSettled(marked), true);
  assert.equal(isSettled(new WakeError("final")), true);
  assert.equal(isSettled(new ClockError("final")), true);
  assert.equal(isSettled(new DeliveryError(envelope)), true);
  const grouped = combineFailures("first", new Error("second"));
  assert.equal(grouped.name, "AggregateError");
  assert.equal(grouped.errors.length, 2);
  const delivery = combineFailures(new Error("primary"), new MailboxBusy(envelope));
  assert.equal(delivery.name, "MailboxBusy");
  assert.equal(delivery.cause.errors.length, 2);
  assert.deepEqual(combineFailures(new Error("a"), new Error("b")).errors.map((e) => e.message), ["a", "b"]);
});

test("envelope: header helpers, addresses and topic cache", () => {
  const id = envelopeId("id-1");
  const from = actorId("actor");
  const header = makeHeader(id, "a.b.c", from, id, requestAddress("reply"), 3);
  assert.equal(header.sv, 3);
  assert.equal(copyHeader(header), header);
  assert.equal(withSchemaVersion(header, 7).sv, 7);
  assert.equal(withPayloadBytes(header, 12).bytes, 12);
  assert.deepEqual(splitTopic("a.b.c"), ["a", "b", "c"]);
  assert.deepEqual(splitTopic("a.b.c"), ["a", "b", "c"]);
  assert.deepEqual(eventAddress(), { kind: "event" });
  assert.deepEqual(diagnosticAddress(), { kind: "diagnostic" });
  assert.deepEqual(requestAddress("r"), { kind: "request", replyTo: "r" });
  assert.deepEqual(resultAddress(id), { kind: "result", correlationId: id });
  assert.equal(correlationIdOf(resultAddress(id)), id);
  assert.equal(correlationIdOf(eventAddress()), undefined);
  assert.equal(MAX_PAYLOAD_BYTES, 1024);
  assert.equal(checkPayloadBytes(7), 7);
});

test("envelope: result shapes, trust check and immutable versions", () => {
  const ok = okPayload({ a: 1 });
  const err = errPayload("oops");
  assert.equal(isResultOk(ok), true);
  assert.equal(isResultErr(err), true);
  assert.equal(isResultErr(ok), false);
  assert.equal(resultValue(ok).a, 1);
  assert.equal(resultError(errPayloadCode("code", 42)), "code");
  const e = makeEnvelope({ id: "e1", topic: "t", from: "a", payload: ok });
  assert.equal(isSealed(e), true);
  assert.equal(isSealed({}), false);
  assert.equal(e.withSchemaVersion(4).header.sv, 4);
  assert.equal(e.withPayloadBytes(9).header.bytes, 9);
  assert.throws(() => e.checkTrust({ allows: () => false }), /from is not trusted/);
  e.checkTrust({ allows: (from) => from === "a" });
});

test("envelope: id generators exhaust and produce distinct ids", () => {
  const seq = new SeqIdGen(7);
  assert.equal(seq.nextEnvelopeId(), "00000000000000000000000000000007");
  const uuid = new UuidIdGen();
  assert.notEqual(uuid.nextEnvelopeId(), uuid.nextEnvelopeId());
  const capped = new SeqIdGen(Number.MAX_SAFE_INTEGER);
  let issued = 0;
  assert.throws(() => {
    for (let i = 0; i < 4; i += 1) { capped.nextEnvelopeId(); issued += 1; }
  }, SeqExhausted);
  assert.equal(issued, 2);
});
