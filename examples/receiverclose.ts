import { Inbox } from "../src/index.ts";

const inbox = new Inbox(8);
const receiver = inbox.receiver();
const first = receiver.close();
const second = receiver.close();
if (first !== true || second !== false || inbox.isClosed !== true) {
  throw new Error(`receiverclose leaked: ${first}/${second}/${inbox.isClosed}`);
}
console.log("receiverclose:", first, second, inbox.isClosed);
