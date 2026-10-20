import { Bus, Inbox, MailboxFull, makeEnvelope } from "../src/index.ts";
import type { Envelope } from "../src/index.ts";

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
}

main();
