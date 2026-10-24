import {
  Bus,
  Inbox,
  Job,
  JobHost,
  ManualClock,
  Runtime,
  SeqIdGen,
  errPayload,
  isResultOk,
  okPayload,
  resultError,
} from "../src/index.ts";
import { Caller } from "../src/job/index.ts";
import { Drive } from "../src/runtime/drive.ts";
import type { Envelope } from "../src/envelope/envelope.ts";
import type { CompletionLike, Context } from "../src/job/job.ts";

class OrderJob extends Job {
  override begin(ctx: Context): void {
    ctx.request({
      stage: "inventory",
      topic: "inventory.reserve",
      payload: ctx.root.payload,
    });
  }
  override onStageResult(
    ctx: Context,
    stage: string,
    completion: CompletionLike,
  ): void {
    if (completion.body === null || !isResultOk(completion.body)) {
      ctx.finish(
        completion.body ?? errPayload("dependency_timeout", { stage }),
      );
      return;
    }
    if (stage === "inventory") {
      ctx.request({
        stage: "payment",
        topic: "payment.charge",
        payload: ctx.root.payload,
      });
      return;
    }
    ctx.finish(
      okPayload({ sku: (ctx.root.payload as Record<string, unknown>)["sku"] }),
    );
  }
}

class ServiceJob extends Job {
  override begin(ctx: Context): void {
    const sku = (ctx.root.payload as Record<string, unknown>)["sku"];
    if (ctx.root.header.topic === "inventory.reserve" && sku === "sold-out") {
      ctx.finish(errPayload("out_of_stock"));
      return;
    }
    if (ctx.root.header.topic === "inventory.reserve") {
      ctx.finish(okPayload("reserved"));
      return;
    }
    ctx.finish(okPayload("paid"));
  }
}

class Orders extends JobHost {
  override makeJob(root: Envelope): Job {
    return new OrderJob(root);
  }
}

class Services extends JobHost {
  override makeJob(root: Envelope): Job {
    return new ServiceJob(root);
  }
}

async function main(): Promise<void> {
  const bus = new Bus();
  const runtime = new Runtime(bus, new ManualClock(), new SeqIdGen());
  runtime.add(
    new Orders({
      actorId: "orders",
      receiver: new Inbox(8).receiver(),
      patterns: ["order.submit"],
    }),
  );
  runtime.add(
    new Services({
      actorId: "services",
      receiver: new Inbox(8).receiver(),
      patterns: ["inventory.reserve", "payment.charge"],
    }),
  );
  const caller = new Caller(new Inbox(8).receiver(), runtime.idGenRef);
  const drive = new Drive(runtime);
  void drive.start();
  const first = drive.open(caller, {
    topic: "order.submit",
    payload: { sku: "A" },
    from: "caller.a",
  });
  const second = drive.open(caller, {
    topic: "order.submit",
    payload: { sku: "sold-out" },
    from: "caller.b",
  });
  console.log(
    "收信已返回",
    first.envelope.header.id,
    second.envelope.header.id,
  );
  const [left, right] = await Promise.all([first.ready, second.ready]);
  await drive.close();
  console.log("甲", left.body);
  console.log("乙", resultError(right.body!));
}

await main();
