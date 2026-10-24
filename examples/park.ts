import { Park, Reply, okPayload } from "../src/index.ts";

function check(ok: boolean, why: string): void {
  if (!ok) throw new Error(why);
}

function main(): void {
  const park = new Park<string, number>();

  const first = new Reply();
  const second = new Reply();
  check(park.wait("k", first) === null, "first parked");
  check(park.wait("k", second) === null, "second parked");
  check(park.wake("k", 7) === first, "fifo first");
  check(park.wake("k", 8) === second, "fifo second");
  check(park.wake("k", 9) === null, "credit kept");

  const credit = new Reply();
  check(park.wait("k", credit) === 9, "credit consumed");
  credit.send(okPayload(9));

  const left = new Reply();
  const right = new Reply();
  check(park.wait("move", left) === null, "move left");
  check(park.wait("move", right) === null, "move right");
  const woken = park.requeue("move", "next", 1, 1);
  check(woken.length === 1 && woken[0] === left, "requeue wake");
  check(park.wake("next", 1) === right, "requeue move");

  const late = new Reply();
  check(park.wait("deadline", late, 5) === null, "deadline parked");
  check(park.expire(4).length === 0, "early expire");
  const expired = park.expire(5);
  check(expired.length === 1 && expired[0] === late, "expire");

  const lost = new Reply();
  const found = new Reply();
  check(park.wait("cancel", lost) === null, "cancel lost");
  check(park.wait("cancel", found) === null, "cancel found");
  const cancelled = park.cancel("cancel");
  check(
    cancelled.length === 2 && cancelled[0] === lost && cancelled[1] === found,
    "cancel order",
  );
  const ownerA = new Reply();
  const ownerB = new Reply();
  check(park.wait("own", ownerA, null, 1) === null, "owner parked");
  check(park.wait("own", ownerB, null, 2) === null, "other owner parked");
  const owners = park.cancelOwner(1);
  check(owners.length === 1 && owners[0] === ownerA, "owner cancel");
  check(park.wake("own", 1) === ownerB, "other owner wakes");
  check(park.keys().length === 0, "keys empty");

  const capped = new Park<string, number>();
  for (let value = 0; value < 4097; value += 1) capped.wake("cap", value);
  for (let value = 0; value < 4096; value += 1) {
    check(capped.wait("cap", new Reply()) === value, "cap credit");
  }
  check(capped.wait("cap", new Reply()) === null, "cap drops newest");

  console.log("park ok");
}

main();
