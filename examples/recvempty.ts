import { Inbox } from "../src/index.ts";

const receiver = new Inbox(8).receiver();
const received = receiver.tryRecv();
if (received !== null) {
  throw new Error(`recvempty leaked: ${received}`);
}
console.log("recvempty:", received);
