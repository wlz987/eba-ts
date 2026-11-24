# eba-ts

进程内、单写者、合作式的 EBA 运行时。Node >= 24，ESM。

## 快速开始

```sh
npm install eba-ts
```

```js
import { Bus, Inbox, Job, JobHost, Runtime, ManualClock, SeqIdGen, okPayload }
  from "eba-ts";

class HelloJob extends Job {
  begin(ctx) { ctx.finish(okPayload({ hi: true })); }
}
class HelloHost extends JobHost {
  constructor() {
    super({ actorId: "hello", receiver: new Inbox(64).receiver(), patterns: ["greet"] });
  }
  makeJob(root) { return new HelloJob(root); }
}

const runtime = new Runtime(new Bus(), new ManualClock(0), new SeqIdGen());
runtime.add(new HelloHost());
const pending = runtime.ask("greet", {}, "caller");
for (let i = 0; i < 10; i++) runtime.advance();

const completion = await pending.ready; // 结算后才 resolve
console.log(completion.body);           // { ok: true, value: { hi: true } }
runtime.close();
```

`await pending.ready` 才算成功；手动推进时 `ManualClock` 不装真实定时器，时间只在 `clock.advance(ms)` 时变化。

## 命令

```sh
npm run check        # tsc --noEmit + eslint
npm test             # 构建 dist 后跑 104 个测试（12 个文件）
npm run conformance  # 只跑总线/运行时/effect 契约场景
npm run coverage     # 行覆盖门禁 ≥90%（实测 90.55%）
npm run build        # 产出发布包
```

## API

核心原语：`Lease` / `LeaseRegistry`（挂起续跑）、`SchemaRegistry`（schema 版本与迁移）、`RecordLog` / `MemorySink` / `DedupSink` / `DedupWindow`（记录去重）、`WorkerPool`（有界并发、取消、close）、`Bus` / `Inbox` / `Receiver`（路由背压）。

hook 返回 `ask / reply / hold / finish / settle / revoke / tell / record / barrier / timer / close` 之一；`applyEffects` / `applyFinished` 的 `switch` 带 `never` 臂，新增 effect 在编译期报错，无运行时校验。

源码用 `#src/*.js` 别名，构建时重写为相对 `.js`，发布包不依赖别名。

## 精简重构（零语义变更）

用 TypeScript 内置能力替换手写样板，不引入 runtime 依赖：`DedupWindow` 用 `Map` 保持 FIFO 淘汰、11 个 Effect 接口与 4 个 Address 接口改用 mapped type + `Extract`、`RecordLogView` 包装类改为 `Pick` 类型、简单错误类由 `namedError` 工厂生成、`pullKind`/`remove` 共用 `takeWhere`。净减约 82 行；104 测试、`npm run check`、覆盖率 90.55% 全绿，eslint warning 61 → 58。第三方库经审计不适用（行为被测试逐字锁定，引入反而增加依赖面）。

## 边界与约定

- **进程内、内存态**：无持久化、恢复或跨进程投递；`MemorySink` 仅用于测试与示例。
- **host 互信**：`from` 由发送者自填；`Bus.setTrustPolicy` 只做主题级准入，不是沙箱，不做租户隔离。
- **入参由调用方保证**：topic 格式、payload 可线上传播、hook 同步返回、schema 版本单调、时间为安全非负整数。运行时只守自身状态机不变量（effect 落地、租约归属、单写者、sink 序号/水位）；唯一例外是 `timer` 落地处的时间校验，防止污染调度堆。
- **单写者**：只有 `Runtime.advance()` 推进运行时。`ctx.timer` / `ctx.record` / `ctx.barrier` / `ctx.close` 是纯 builder，必须 `return [effect]` 或 `ctx.pushEffect(...)`；跨 `timer` / `barrier` 的续跑走 Promise 回调，手动推进需在两次 `advance()` 间让出事件循环。

## 测试

104 个测试 / 12 个文件；`npm run conformance` 单独跑契约场景。行覆盖门禁 90% 并接入 CI；测试直接读 `dist/`，所以 `npm test` 总会先构建。新增源码请同步补测试。

## 发布

`files` 只含 `dist/` + `README.md` + `CHANGELOG.md`（npm 自动附带 `LICENSE`）。`prepack` 在 `npm pack` / `npm publish` 前自动构建，然后 `npm login && npm publish`。变更记录见 `CHANGELOG.md`。
