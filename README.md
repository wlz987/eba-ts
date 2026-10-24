# eba-ts

进程内、单写者、合作式的 EBA 运行时。TypeScript，Node ≥ 24。

一次 request 交给每一台受理它的 Host 执行；调用方认领最先到的那份合法结果。副作用是否幂等，
由应用负责；超时只结清本地这一次等待。回调在返回前同步结束；`Envelope` 的 payload 在各 Job
之间共享、只读。一台运行时只有一位 `Caller`（actor id 固定为 `caller`），请求人身份由每次
调用的 `from` 承载。

## 骨架

`Runtime` 按轮次推进：`add` / `advance` / `close` 各占一次单写者窗口，一轮只读一次钟。每台
`JobHost` 有四相（receive / expire / adopt / deliver）与按台预算，预算只在不空转的格花。
`Drive` 把 `advance` 当异步循环跑，靠唤醒票与期限醒来，不轮询。`Bus` 路由信封，`Inbox`
暂存信件，`Records` 记等待、回复路由与租约，`Outbox` 按主题组押回信。

## 承诺

- 请求三条出口：合法结果、期限到点（`body === null`）、取消。
- 没设期限又没人接住的 request，期限就地记为已过（`ELAPSED`），下一步交回空结局；不会悬到
  永远。设了期限的请求仍按期限走。
- 回信同主题一组、按进入顺序一封一封发；发布失败重试一次，第二次埋没，等 `close` 交还。
  收件箱收不下是背压（`MailboxFull`），不是投递故障。
- `close` 返回未认领的信；半途失败停在 `closing`，最后一次 `close` 收齐。
- 错误结果可带一个域自定义的整数 `code`（`err_payload_code` / `result_code`）；EBA 只透传，不解释。
- 投递失败两次时，以 `delivery_failed` 结清它所回答的等待；钩子不答时补 `job_silent`。
- 读钟失败只结束这一轮，不扣住运行时：下一次 `advance` 仍能开。
- 服务端可在 `ctx.hold()` 扣住一个 request，稍后用 `Reply.send/fail` 写槽；`advance` 末尾结算。

## 与 eba-py 的结构差异（当前）

- 没有「谁在写」的运行时表：`Runtime` 直接持有 `coreOf(host)` 调模块函数，`JobHost` 上没有
  只属于轮次的动词；唯一露在外面的是 `JobHost.close(bus)`，它反过来拒绝已被收编的 Host。
  `Runtime.advance` 用 `signal.boundTo(agenda)` 拒绝别的运行时。
- `Bus` 是共享对象；覆写 `bus.subscribe` / `bus.publish` 是故障注入缝。
- `Envelope` 在入口做快照与校验；payload 若是可变对象，由调用方自己保证只读。
- 一只 `Wake` 只允许一个调度等待者，重复挂人直接抛错。
- 已结清的错误记在一只 `WeakSet` 里，泵据此决定是否停下；投递、唤醒、时钟三类天生不算新账。
- `WeakMap` / `WeakRef` 的回收时机不可确定观测，相关断言只能覆盖可观测的那一半。

## 命令

```sh
npm ci
npx tsc --noEmit          # 类型闸门
node examples/order.ts    # 两笔下单，一成一败
npm run example           # order 与 bus
```

四端例子在根工作区由 `make examples` 统一跑；本仓 CI 跑 `tsc --noEmit` 与 `examples/*.ts`。

## 边界

- 按 ADR-0010，仓内没有测试；回归靠类型闸门、例子与根工作区的端口一致性 audit。
- 撤走测试之前的差异对名账见 [`HISTORY.md`](HISTORY.md)。
