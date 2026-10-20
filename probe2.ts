import { Bus, Inbox, Job, JobHost, ManualClock, Runtime, SeqIdGen, errPayload, makeEnvelope, okPayload } from "./src/index.ts";
import { Receiver, stateOf } from "./src/mail/inbox.ts";
import { copyHeader } from "./src/envelope/header.ts";
import { Records } from "./src/rendezvous/records.ts";
import { Schedule } from "./src/time/schedule.ts";
import { Deadlines } from "./src/time/deadlines.ts";
import { adopt, invoke } from "./src/job/execution.ts";
import { coreOf } from "./src/job/host.ts";
import { Subscriber } from "./src/mail/subscriber.ts";
import { ClockError } from "./src/errors.ts";

function frame(f: () => unknown): string {
  try { f(); } catch (e) {
    const line = String((e as Error).stack).split("\n").slice(1, 3).join(" || ");
    return `${(e as Error).constructor.name}: ${(e as Error).message} @ ${line}`;
  }
  return "NO REFUSAL";
}
console.log(1, frame(() => (Object.create(Inbox.prototype) as Inbox).isClosed));
console.log(2, frame(() => new Receiver(new Inbox(2), Symbol("x") as never)));
console.log(3, frame(() => { const b = new Inbox(2); b.receiver(); return b.receiver(); }));
const r = new Inbox(2).receiver(); r.take();
console.log(4, frame(() => r.tryRecv()));
const r2 = new Inbox(2).receiver(); const rel = r2.borrow();
console.log(5, frame(() => r2.take()));
const bad = { id: "1", topic: "a.b", from: "u", cause: "1", address: { kind: "carrier-pigeon" } };
console.log(6, frame(() => copyHeader(bad as never)));
const bus = new Bus(); const box = new Inbox(2); box.receiver().close();
const req = makeEnvelope({ topic: "a.b", payload: null, from: "u", id: "9", kind: "request", replyTo: "sys.reply.void" });
console.log(7, frame(() => new Records().submit(bus, box, req, {}, null)));
console.log(8, frame(() => new Schedule(false, 5, true).waitMs()));
console.log(9, frame(() => { const d = new Deadlines<string>(); d.add("x", 5); d.add("x", 9); }));
class Plain extends JobHost {}
console.log(10, frame(() => new Plain({ actorId: "bare", receiver: new Inbox(2).receiver(), patterns: ["a.b"] }).makeJob(req)));
class Boom { nowMs(): number { throw new Error("not a reading"); } }
const rt = new Runtime(new Bus(), new Boom() as never, new SeqIdGen());
console.log(11, frame(() => rt.advance()));
class Below { nowMs(): number { return -1; } }
const rt2 = new Runtime(new Bus(), new Below() as never, new SeqIdGen());
console.log(12, frame(() => rt2.advance()));
class Prying {
  inner = new ManualClock(); probed = 0; holder: { rt?: Runtime } = {};
  nowMs(): number { if (this.probed) return this.inner.nowMs(); this.probed = 1;
    try { (this.holder.rt as never as { now(): number }).now(); } catch (e) { this.holder.e = e as Error; }
    return this.inner.nowMs(); }
}
const p = new Prying(); const rt3 = new Runtime(new Bus(), p as never, new SeqIdGen()); p.holder.rt = rt3;
rt3.advance();
console.log(13, String(p.holder.e));
const b2 = new Inbox(4); const sub = new Subscriber("sub", b2.receiver(), ["a.b", "c.d"]);
sub.start(new Bus());
console.log(14, frame(() => sub.start(new Bus())));
const stub = { subscribe: () => { throw new Error("bus says no"); } };
const b3 = new Inbox(4); const sub2 = new Subscriber("s2", b3.receiver(), ["a.b"]);
console.log(15, frame(() => sub2.start(stub as never)));
const stuckLease = { active: () => false, close: () => { throw new Error("stuck lease"); } };
const b4 = new Inbox(4); const sub3 = new Subscriber("s3", b4.receiver(), ["a.b"]);
const stub2 = { subscribe: () => stuckLease };
console.log(16, frame(() => sub3.start(stub2 as never)));
console.log(17, frame(() => (sub3 as never as { release_(bus?: unknown): void }).release_(stub)));
