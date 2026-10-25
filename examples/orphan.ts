import { Bus, Inbox, Job, JobHost, ManualClock, Runtime, SeqIdGen } from "../src/index.ts";
import type { Context } from "../src/job/job.ts";
import type { Envelope } from "../src/envelope/envelope.ts";
import { Caller } from "../src/job/index.ts";
import { coreOf } from "../src/job/host.ts";
import { settled } from "../src/job/call.ts";

const finished: Array<Record<string, unknown> | null> = [];
const activeFinished: Array<Record<string, unknown> | null> = [];
const begun: number[] = [];
const begunCount = (): number => begun.length;
const finishedCount = (): number => finished.length;

let heldSignal: AbortSignal = new AbortController().signal;

class Hold extends Job {
  override begin(ctx: Context): void {
    begun.push(1);
    heldSignal = ctx.hold().signal;
  }

  override onFinished(body: Record<string, unknown> | null): void {
    finished.push(body);
  }
}

class HoldHost extends JobHost {
  override makeJob(root: Envelope): Job {
    return new Hold(root);
  }
}

class Active extends Job {
  override begin(ctx: Context): void {
    ctx.request({ stage: "inner", topic: "orphan.work", payload: { n: 3 } });
  }

  override onFinished(body: Record<string, unknown> | null): void {
    activeFinished.push(body);
  }
}

class ActiveHost extends JobHost {
  override makeJob(root: Envelope): Job {
    return new Active(root);
  }
}

class Bad extends Job {
  override begin(ctx: Context): void {
    ctx.request({ stage: "inner", topic: "orphan.work", payload: { n: 4 } });
  }

  override onFinished(_body: Record<string, unknown> | null): void {
    throw new Error("cleanup boom");
  }
}

class BadHost extends JobHost {
  override makeJob(root: Envelope): Job {
    return new Bad(root);
  }
}

const runtime = new Runtime(new Bus(), new ManualClock(), new SeqIdGen());
runtime.add(
  new HoldHost({
    actorId: "hold",
    receiver: new Inbox(8).receiver(),
    patterns: ["orphan.work"],
  }),
);
runtime.add(
  new ActiveHost({
    actorId: "active",
    receiver: new Inbox(8).receiver(),
    patterns: ["active"],
  }),
);
const badHost = new BadHost({
  actorId: "bad",
  receiver: new Inbox(8).receiver(),
  patterns: ["bad"],
});
runtime.add(badHost);
const caller = new Caller(new Inbox(8).receiver(), runtime.idGenRef);
runtime.add(caller);
const root = caller.admit({
  topic: "orphan.work",
  payload: { n: 1 },
  from: "orphan",
});
for (let index = 0; index < 12; index += 1) runtime.advance();
const cancelled = caller.cancel(root.header.id);
for (let index = 0; index < 12; index += 1) runtime.advance();

const completion = settled(root);
const body = completion?.body as { ok?: boolean; error?: string } | undefined;
if (!cancelled || body?.ok !== false || body.error !== "cancelled") {
  throw new Error(`orphan caller: ${cancelled} ${JSON.stringify(body)}`);
}
const holder = finished[0] as { ok?: boolean; error?: string } | null;
if (finished.length !== 1 || holder?.ok !== false || holder.error !== "cancelled") {
  throw new Error(`orphan holder: ${JSON.stringify(finished)}`);
}
if (!heldSignal.aborted) {
  throw new Error("orphan holder signal not aborted");
}

const before = begunCount();
const queued = caller.admit({
  topic: "orphan.work",
  payload: { n: 2 },
  from: "orphan",
});
runtime.advance();
const queuedCancelled = caller.cancel(queued.header.id);
for (let index = 0; index < 4; index += 1) runtime.advance();
const queuedBody = settled(queued)?.body as { error?: string } | null;
if (
  !queuedCancelled ||
  begunCount() !== before ||
  finished.length !== 1 ||
  queuedBody?.error !== "cancelled"
) {
  throw new Error(
    `orphan queued: begun=${begunCount()} finished=${JSON.stringify(finished)} body=${JSON.stringify(queuedBody)}`,
  );
}
const activeRoot = caller.admit({
  topic: "active",
  payload: {},
  from: "orphan",
});
for (let index = 0; index < 8; index += 1) runtime.advance();
const activeCancelled = caller.cancel(activeRoot.header.id);
for (let index = 0; index < 8; index += 1) runtime.advance();
const activeBody = settled(activeRoot)?.body as { error?: string } | null;
const activeHolder = activeFinished[0] as { error?: string } | null;
if (
  !activeCancelled ||
  finishedCount() !== 2 ||
  activeFinished.length !== 1 ||
  activeBody?.error !== "cancelled" ||
  activeHolder?.error !== "cancelled"
) {
  throw new Error(
    `orphan active: finished=${JSON.stringify(finished)} active=${JSON.stringify(activeFinished)} body=${JSON.stringify(activeBody)}`,
  );
}
const badRoot = caller.admit({ topic: "bad", payload: {}, from: "orphan" });
for (let index = 0; index < 8; index += 1) runtime.advance();
const badCancelled = caller.cancel(badRoot.header.id);
try {
  for (let index = 0; index < 8; index += 1) runtime.advance();
} catch (error) {
  throw new Error(`orphan bad hook escaped: ${String(error)}`);
}
const badStats = runtime.stats();
if (
  !badCancelled ||
  badStats.cancelErrors !== 1 ||
  badStats.lastCancelError !== "cleanup boom"
) {
  throw new Error(`orphan bad hook: ${JSON.stringify(badStats)}`);
}

const badCore = coreOf(badHost);
const originalCancel = badCore.book.cancel.bind(badCore.book);
let leaseCalls = 0;
badCore.book.cancel = (owner) => {
  leaseCalls += 1;
  if (leaseCalls === 1) throw new Error("lease");
  originalCancel(owner);
};
const leaseRoot = caller.admit({ topic: "bad", payload: {}, from: "orphan" });
for (let index = 0; index < 8; index += 1) runtime.advance();
const leaseCancelled = caller.cancel(leaseRoot.header.id);
for (let index = 0; index < 12; index += 1) runtime.advance();
const leaseStats = runtime.stats();
badCore.book.cancel = originalCancel;
if (
  !leaseCancelled ||
  leaseCalls !== 2 ||
  badCore.jobs.size !== 0 ||
  leaseStats.pendingCancels !== 0 ||
  leaseStats.active !== 0
) {
  throw new Error(`orphan lease: ${JSON.stringify(leaseStats)} calls=${leaseCalls}`);
}

const payRoot = caller.admit({ topic: "orphan.work", payload: { n: 5 }, from: "orphan" });
for (let index = 0; index < 4; index += 1) runtime.advance();
(payRoot.payload as { pay: ((completion: unknown) => void) | null }).pay = () => {
  throw new Error("pay boom");
};
try {
  caller.cancel(payRoot.header.id);
} catch (error) {
  if (String(error) !== "Error: pay boom") throw error;
}
if (runtime.stats().active !== 1) {
  throw new Error(`orphan pay caller leaked: ${JSON.stringify(runtime.stats())}`);
}
for (let index = 0; index < 4; index += 1) runtime.advance();
if (runtime.stats().active !== 0) {
  throw new Error(`orphan pay holder leaked: ${JSON.stringify(runtime.stats())}`);
}
console.log("orphan ok");
