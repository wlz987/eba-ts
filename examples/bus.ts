import {
  Bus,
  EnvelopeBuildError,
  Inbox,
  MailboxFull,
  makeEnvelope,
} from "../src/index.ts";
import type { Envelope } from "../src/index.ts";
import { pauseWork, watchSpace } from "../src/mail/inbox.ts";
import { Signal } from "../src/notify/signal.ts";
import { Interest } from "../src/notify/watch.ts";

function letter(topic: string, id: string): Envelope {
  return makeEnvelope({
    topic,
    payload: { sku: "sold-out" },
    from: "order",
    id,
  });
}

function main(): void {
  const bus = new Bus();
  const inventory = new Inbox(1);
  const audit = new Inbox(4);
  const overview = new Inbox(4);
  const boxes: Array<[string, ReturnType<Inbox["receiver"]>]> = [
    ["库存", inventory.receiver()],
    ["旁听", audit.receiver()],
    ["总览", overview.receiver()],
  ];
  bus.subscribe("inventory.*", inventory);
  bus.subscribe("inventory.reserve", audit);
  bus.subscribe("inventory.**", overview);

  const take = (): Array<[string, string]> => {
    const found: Array<[string, string]> = [];
    for (const [name, receiver] of boxes) {
      const item = receiver.tryRecv();
      if (item !== null) found.push([name, item.header.id]);
    }
    return found;
  };

  bus.publish(letter("inventory.reserve", "child-42"));
  console.log("inventory.reserve", take());
  bus.publish(letter("inventory.reserve.extra", "child-43"));
  console.log("inventory.reserve.extra", take());
  bus.publish(letter("inventory.reserve", "child-44"));
  try {
    bus.publish(letter("inventory.reserve", "child-45"));
  } catch (error) {
    if (error instanceof MailboxFull) console.log("库存已满", take());
    else throw error;
  }

  const full = new Inbox(1);
  const room = new Inbox(4);
  const fullSide = full.receiver();
  const roomSide = room.receiver();
  bus.subscribe("effort.x", full);
  bus.subscribe("effort.*", room);
  bus.publish(letter("effort.x", "e1"));
  roomSide.tryRecv();
  const skipped = bus.publishBestEffort(letter("effort.x", "e2"));
  console.log("best-effort", skipped, fullSide.tryRecv()?.header.id, roomSide.tryRecv()?.header.id);

  const latest = new Inbox(4);
  const latestSide = latest.receiver();
  bus.subscribe("latest.x", latest);
  bus.publish(letter("latest.x", "l1"));
  bus.publish(letter("latest.x", "l2"));
  const stale = bus.publishLatest(letter("latest.x", "l3"));
  if (stale !== 0 || latestSide.tryRecv()?.header.id !== "l3") {
    throw new Error("latest did not keep the newest");
  }
  if (latestSide.tryRecv() !== null) throw new Error("latest left a stale letter");

  try {
    bus.publishLatest(
      makeEnvelope({
        topic: "latest.x",
        payload: 1,
        from: "x",
        id: "req-1",
        kind: "request",
        replyTo: "sys.reply.i1",
      }),
    );
    throw new Error("publishLatest accepted a request");
  } catch (error) {
    if (!(error instanceof EnvelopeBuildError)) throw error;
  }

  const busy = new Inbox(1);
  const busySide = busy.receiver();
  bus.subscribe("latest.y.*", busy);
  bus.publish(letter("latest.y.a", "y1"));
  if (bus.publishLatest(letter("latest.y.a", "y2")) !== 0) {
    throw new Error("latest did not replace a full same-topic inbox");
  }
  if (busySide.tryRecv()?.header.id !== "y2") throw new Error("latest lost y2");
  bus.publish(letter("latest.y.a", "y3"));
  if (bus.publishLatest(letter("latest.y.b", "y4")) !== 1) {
    throw new Error("latest did not skip a full other-topic inbox");
  }
  if (busySide.tryRecv()?.header.id !== "y3") throw new Error("latest lost y3");

  const paused = new Inbox(2);
  const pausedSide = paused.receiver();
  bus.subscribe("latest.p", paused);
  bus.publish(letter("latest.p", "p1"));
  pauseWork(paused, true);
  if (bus.publishLatest(letter("latest.p", "p2")) !== 1) {
    throw new Error("latest did not skip a paused inbox");
  }
  pauseWork(paused, false);
  if (bus.publishLatest(letter("latest.p", "p3")) !== 0) {
    throw new Error("latest did not replace after unpause");
  }
  if (pausedSide.tryRecv()?.header.id !== "p3") throw new Error("latest lost p3");

  const candidates: Record<string, null> = {};
  const watched = new Inbox(2);
  bus.subscribe("latest.w", watched);
  const interest = new Interest(new Signal(), candidates, "latest.w");
  watchSpace(watched, interest);
  bus.publish(letter("latest.w", "w1"));
  bus.publish(letter("latest.w", "w2"));
  if (bus.publishLatest(letter("latest.w", "w3")) !== 0) {
    throw new Error("latest did not replace the full watched inbox");
  }
  if (!("latest.w" in candidates)) throw new Error("space watcher did not fire");
  console.log("latest ok");
}

main();
