# minimq 0.0.2 版本设计总览

| 项目 | 内容 |
|------|------|
| 版本号 | 0.0.2-SNAPSHOT |
| 主题 | 拆地基：模块化、并发化、注册中心、消息元数据 |
| 状态 | 设计完成，待实现 |
| 设计日期 | 2026-11-18 |
| 基线提交 | `704c89b`（origin/dev-0.0.1） |
| 实施分支 | `dev-0.0.2` |
| 传输选型 | 0.0.2 线程池，Netty 推迟到 0.0.3（已确认） |

---

## 1. 为什么要有 0.0.2

0.0.1 已经打通了「Producer → Broker → Consumer」的拉模式链路，但代码是一个单模块平铺结构，存在四个结构性问题：

1. **客户端依赖服务端实现**：`Consumer`、`Producer`、`Client` 直接 import `top.sorie.minimq.broker.cmd.*`。
2. **协议与处理器耦合**：`MqCmd` 既是线上报文 DTO，又通过 `exec(Broker)` 承载服务端业务逻辑。
3. **Broker 串行处理**：accept 循环里同步处理会话，一个慢连接会阻塞所有客户端。
4. **消息模型太薄**：只有 `topic + content`，无法承载 msgId、headers、tags、时间戳等后续能力。

0.0.2 不追求新的消费能力（推送、持久化、延迟消息都不做），目标是把上述四条地基铺平，让 0.0.3 的 Netty / 推送 / 集群化有地方落。

---

## 2. 版本目标

| 编号 | 目标 | 验收方式 |
|------|------|----------|
| G1 | 拆成 Maven 多模块，依赖方向可强制校验 | `mvn dependency:tree` + enforcer |
| G2 | 协议 DTO 与 handler 解耦，客户端只依赖 common | 模块依赖 + 编译 |
| G3 | Broker 支持并发会话、资源上限、优雅停机 | 并发用例 + 慢连接用例 |
| G4 | 单机注册中心，broker 自动注册、客户端按 topic 发现 | 注册中心 E2E |
| G5 | 消息元数据与 fetch 分页 | 元数据往返 + 分页用例 |
| G6 | 文档、日志、配置、错误码等横切能力成型 | 文档齐全 + 全量回归 |

## 3. 明确不做

- 消息持久化（磁盘 / KV / DB）
- Broker 主动推送、长轮询
- 延迟消息、事务消息、顺序消息保证
- 注册中心集群与一致性复制
- 多副本、分区、消费组 rebalance
- Netty 传输

这些能力在 0.0.3 及之后版本逐步引入。

---

## 4. 里程碑地图

| 里程碑 | 主题 | 依赖 | 设计文档 | 预估 |
|--------|------|------|----------|------|
| M0 | 模块拆分 + 协议 DTO/handler 解耦 | - | [M0-modules.md](./M0-modules.md) | 0.5–1 天 |
| M1 | 协议增强 + 配置 / 日志 / 错误码底座 | M0 | [M1-protocol.md](./M1-protocol.md) | 1 天 |
| M2 | Broker 线程池、服务分层、存储接口 | M0, M1 | [M2-broker-concurrency.md](./M2-broker-concurrency.md) | 1–1.5 天 |
| M3 | 单机注册中心 + 服务发现 | M0, M1 | [M3-registry.md](./M3-registry.md) | 1.5–2 天 |
| M4 | 消息元数据 + fetch 分页 | M0, M1, M2 | [M4-metadata.md](./M4-metadata.md) | 1 天 |
| M5 | 文档、回归、版本发布 | M0–M4 | [M5-release.md](./M5-release.md) | 0.5 天 |

依赖关系：

```
M0 ──> M1 ──> M2 ──> M4
        │
        └────> M3 ──> M5
```

M2 与 M3 在 M1 完成后可以并行；M4 依赖 M2 抽出的存储接口，所以排在 M2 之后。

### 里程碑边界的调整说明

原始计划里 M0 只做「模块拆分」、M1 做「协议解耦」。但 `Consumer` / `Client` 对 `broker.cmd.*` 的依赖，正是 `exec(Broker)` 造成的：只要协议 DTO 还带服务端逻辑，client 模块就无法摆脱对 broker 的依赖。因此本设计把 **DTO 迁移 + `exec` 改为 handler** 纳入 M0 的必要范围，M1 聚焦协议层增强和横切底座。这样 M0 可以独立编译、独立验收，不需要临时违反依赖规则。

---

## 5. 全局设计决策

### D1 模块与依赖方向

固定为 6 个模块：

```
minimq-parent
├── minimq-common          协议 DTO、编解码、模型、错误码、配置、util
├── minimq-registry-api    注册中心 DTO + HTTP 客户端（broker 与 client 共用）
├── minimq-broker          服务端：server / handler / store
├── minimq-client          Producer / Consumer / Client / 发现逻辑
├── minimq-registry        注册中心服务端
└── minimq-example         demo + 跨模块 E2E
```

依赖方向：

```
common ← registry-api ← broker
                      ← client
                      ← registry
common ← broker / client / registry
example → broker / client / registry
client  ✗ broker
broker  ✗ client
```

`minimq-registry-api` 单独成模块，避免 broker 和 client 各写一份 HTTP 调用与 DTO。M0 先建空模块，M3 填内容。

### D2 协议兼容策略

0.0.2 的协议改动遵循「只加可选字段、不改既有字段语义」：

- `cmd` 数字保持不变：1 Publish、2 Subscribe、3 Unsubscribe、4 Fetch、5 Ack。
- JSON 字段名保持不变（`cmd` / `topic` / `msg` / `consumerId` / `msgIndex` / `code` / `message` / `data`）。
- 新字段（`version`、`requestId`、元数据）缺失时按默认值处理。
- **Broker 能解析 0.0.1 的请求**；但旧客户端解析 0.0.2 含元数据的应答时，可能因未知字段失败（0.0.1 的 `MqMsg` 未开启忽略未知字段）。因此 **0.0.2 不承诺跨版本混用**，客户端与 Broker 必须配套发布；README 与 Release 必须写明。

真正破坏兼容的变更（如 fetch 分页语义收紧）单独在 M4 文档里标注，并同步更新测试。

### D3 传输策略

- 0.0.2：`ServerSocket` + 有界线程池，保留短连接协议。
- 0.0.3：Netty + 长连接 + 心跳；因为 M0/M1 已把编解码和会话逻辑抽成独立类，Netty 只替换 `BrokerServer` 实现。

### D4 注册中心定位

- 只做**服务发现**，不做配置中心。
- 单机、内存存储、HTTP/JSON。
- 服务端实现 `RegistryStore` 接口，集群化时替换实现，API 不变。
- 客户端配置接受**注册中心地址列表**，为后续多节点留好入口。

### D5 模型分层

| 模型 | 归属 | 说明 |
|------|------|------|
| `MqMsg` | common | 客户端可见的消息模型，只放业务与传输元数据 |
| `StoredMessage` | broker | 服务端存储信封：offset、storeTime、brokerId + `MqMsg` |
| `BrokerInfo` | registry-api | 注册中心里的 broker 快照 |
| `CmdResult` | common | 统一应答信封 `{code, message, data}` |

客户端模型不掺 offset、重试次数等 broker 内部状态，避免后续加存储时互相污染。

### D6 不可变优先

- 值对象（`MqMsg`、`BrokerInfo`、配置、请求/应答 DTO）优先用 `record` 或不可变类 + builder。
- 集合字段构造时做防御性拷贝（`List.copyOf` / `Map.copyOf`），对外返回不可变视图。
- 业务状态（store 内部）通过「读快照、写加锁」保持线程安全，不把内部集合引用直接返回给调用方。

> Jackson 反序列化说明：Jackson 2.18 原生支持 record；如字段较多或需要默认值，使用 `@JsonCreator` + `@JsonProperty`。DTO 不需要无参构造时优先 record。

### D7 错误码

- 线上 `CmdResult.code` 仍是 `int`，保持兼容。
- 内部统一使用 `ErrorCode` 枚举：

| code | 常量 | 含义 |
|------|------|------|
| 0 | `SUCCESS` | 成功 |
| 1001 | `INVALID_ARGUMENT` | 参数缺失 / 非法 |
| 1002 | `UNKNOWN_CMD` | 未知指令类型 |
| 1003 | `TOPIC_NOT_FOUND` | topic 不存在 |
| 1004 | `NOT_SUBSCRIBED` | 未订阅该 topic |
| 1005 | `UNSUPPORTED_VERSION` | 协议版本不支持 |
| 2000 | `INTERNAL_ERROR` | 服务端内部错误 |
| 2001 | `SERVER_BUSY` | 线程池 / 连接数已满 |
| 2002 | `BROKER_UNAVAILABLE` | 连接 Broker 失败/超时（M3） |
| 3001 | `DISCOVERY_FAILED` | 注册中心查询失败且无缓存（M3） |
| 4001 | `REGISTRY_INVALID_ARGUMENT` | 注册中心参数非法（M3） |
| 4002 | `BROKER_NOT_FOUND` | 注册记录不存在/已过期（M3） |
| 4003 | `EPOCH_MISMATCH` | 心跳 epoch 落后（M3） |
| 5000 | `REGISTRY_INTERNAL_ERROR` | 注册中心内部错误（M3） |

`CmdResult.CODE_ERROR = -1` 保留一个版本作为兼容别名，新代码不再使用。

### D8 日志

- 统一 slf4j-api + logback-classic。
- 禁止新增 `System.out.println` / `System.err`（示例程序与测试输出除外）。
- 协议内容只在 DEBUG 打印，且必须截断（默认 256 字符），避免大消息刷盘。
- 每个会话、每条指令都能通过 `requestId` 串起来（M1 引入）。

### D9 配置

- `BrokerConfig` / `ClientConfig` / `RegistryConfig` 全部不可变对象，带 builder 与构造校验。
- 默认值集中在 `*Defaults` 常量类，不散落在业务代码里。
- 支持系统属性或环境变量覆盖（优先级：显式设置 > 环境变量 > 默认值），0.0.2 至少实现构造注入 + 默认值。

### D10 测试与质量门槛

- 每个模块自己的单测跟着模块走；跨模块链路测试放 `minimq-example/src/test`。
- 单元测试 + 集成测试 + 并发测试都必须有。
- 按 TDD 执行：先写失败用例（RED）→ 最小实现（GREEN）→ 重构（IMPROVE）。
- `mvn clean verify` 一键全绿；JaCoCo 行覆盖率门槛 **80%**，M5 发布前必须达标。
- 不允许提交 `Thread.sleep` 式的"等一会儿"测试；统一用轮询 + 超时或 `CountDownLatch`。

### D11 版本与分支

- 所有模块版本统一为 `0.0.2-SNAPSHOT`，由 parent 继承，子模块不重复写版本。
- 从 `dev-0.0.1` 切出 `dev-0.0.2`，每个里程碑一个可运行状态。
- 发布时打 `v0.0.2` tag；推远端前单独确认。

---

## 6. 验收总表（0.0.2 Definition of Done）

- [ ] `mvn clean verify` 在根目录一次通过，所有模块参与构建
- [ ] `minimq-client` 的依赖树里没有 `minimq-broker`，enforcer 能拦截误加
- [ ] 示例程序可以一条命令启动「注册中心 + Broker + Producer + Consumer」
- [ ] 50 producer / 50 consumer 并发压测无消息丢失、单 topic 内顺序正确
- [ ] 慢连接（连接后 10 秒不发送）不阻塞其他客户端
- [ ] Broker 停止时在 10 秒内完成优雅停机
- [ ] 客户端只配置注册中心地址即可完成发布与拉取
- [ ] 消息的 msgId / headers / tags / producedAt 能完整往返
- [ ] fetch 支持 offset + maxCount 分页，ack 后能增量拉取
- [ ] `docs/protocol.md`、`docs/architecture.md`、README quickstart 完成
- [ ] 日志里不再有业务代码的 `System.out/err`

---

## 7. 文档使用方式

1. 实现某个里程碑前，先读对应设计文档的「目标 / 目标结构 / 验收」。
2. 按文档里的「实施步骤」顺序做，每一步结束跑一次 `mvn -q -DskipTests compile`，避免一次性堆太多错误。
3. 完成后逐条勾选「验收标准」；不满足的先不进下一个里程碑。
4. 设计与实现出现偏差时，**先改文档再改代码**，保证文档始终是当前设计的真相。

## 8. 变更记录

| 日期 | 变更 | 说明 |
|------|------|------|
| 2026-11-18 | 初版 | 确定 6 模块、线程池优先、Netty 0.0.3、M0 范围调整 |
