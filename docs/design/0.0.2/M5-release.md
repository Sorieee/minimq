# M5 文档、回归与发布设计说明

| 项目 | 内容 |
|------|------|
| 里程碑 | M5 |
| 目标 | 文档齐全、全量回归、打 tag 发布 0.0.2 |
| 前置 | M0–M4 |
| 后续 | 0.0.3 Netty 与推送 |
| 状态 | 设计完成，待实现 |
| 设计日期 | 2026-11-18 |

---

## 1. 目标与非目标

### 目标

1. 让一个没读过代码的人，能按 README 在 5 分钟内跑通「注册中心 + Broker + Producer + Consumer」。
2. 固化协议文档：报文格式、指令字段、错误码、版本策略。
3. 固化架构文档：模块职责、依赖方向、线程模型、存储模型、后续演进点。
4. 完成 0.0.2 全量回归：单测、集成、并发、故障恢复、示例。
5. 打 `v0.0.2` tag，形成可回溯的版本节点。

### 非目标

- 不做部署脚本、Docker 镜像、K8s 清单（后续版本）
- 不做性能白皮书
- 不做 API 多语言 SDK

---

## 2. 文档交付清单

### 2.1 必须交付

| 文档 | 位置 | 内容 |
|------|------|------|
| README | `README.md` | 项目定位、模块列表、快速开始、示例命令 |
| 架构说明 | `docs/architecture.md` | 模块图、依赖规则、线程模型、存储模型、生命周期 |
| 协议说明 | `docs/protocol.md` | 线格式、5 种指令字段表、应答信封、错误码、版本兼容 |
| 注册中心 API | `docs/registry-api.md` | 端点、请求/响应示例、错误码、租约语义 |
| 运维说明 | `docs/operations.md` | 端口、配置项、日志、健康检查、已知限制 |
| 版本说明 | `CHANGELOG.md` 或 GitHub Release | 0.0.2 变更、破坏性变更、升级注意 |

设计与实现文档已经存在，可互相引用：

- [0.0.2 总览](./README.md)
- [M0 模块拆分](./M0-modules.md)
- [M1 协议增强](./M1-protocol.md)
- [M2 Broker 并发化](./M2-broker-concurrency.md)
- [M3 注册中心](./M3-registry.md)
- [M4 消息元数据](./M4-metadata.md)

### 2.2 README 结构建议

```markdown
# minimq

一个用于学习消息中间件原理的 Java 实现：Broker + Producer/Consumer + 注册中心。

## 模块
（6 个模块一句话说明）

## 快速开始
1. 构建：mvn clean package
2. 启动注册中心：java -cp ... top.sorie.minimq.registry.RegistryMain
3. 启动 Broker：java ... BrokerMain
4. 启动示例：java ... DemoMain

## 配置
（表格：端口、超时、线程数、注册中心地址）

## 协议
（链接到 docs/protocol.md）

## 当前能力 / 已知限制
（明确 0.0.2 不做推送、持久化、集群）

## 模块依赖图
```

要求：

- 所有命令可直接复制执行，不写"自行调整 classpath"。
- 端口写默认值，并说明如何改。
- 示例输出贴真实运行结果（截断到关键行）。
- 已知限制必须显式列出，避免误解为生产可用。

### 2.3 protocol.md 必备内容

1. 传输：TCP、短连接、一问一答、UTF-8、行分隔 + `</end>`。
2. 信封字段：`version`、`cmd`、`requestId`、`consumerId`、`topic`、`msg`、`offset`、`maxCount`、`msgIndex`。
3. 五种指令的字段表与示例。
4. `CmdResult`：`code`、`message`、`data`、`requestId`、`nextOffset`。
5. 错误码表（0 / 1001–1005 / 2000–2002 / 3001 / 4001–4003 / 5000）。
6. 版本策略：何时递增 `PROTOCOL_VERSION`；新增字段必须可选；未知字段必须忽略。
7. 兼容矩阵：0.0.1 客户端 × 0.0.2 Broker，0.0.2 客户端 × 0.0.1 Broker。
8. 一个手写 socket 的调试示例（netcat 或 Python 片段）。

### 2.4 architecture.md 必备内容

1. 模块图与依赖方向，附 enforcer 规则说明。
2. Broker 线程模型：accept 线程、工作线程池、连接上限、读超时。
3. 存储模型：`MessageStore` / `AckStore` / `SubscriptionStore`，内存实现与线程安全策略。
4. 注册中心：broker 注册/心跳/摘除，客户端发现与缓存。
5. 生命周期：Broker 与 Registry 的 start/stop/awaitTermination。
6. 请求全链路时序：`Producer.send → Client → Broker 会话 → handler → store → 应答`。
7. 后续演进：Netty、推送、持久化、集群的接入点。

---

## 3. 全量回归矩阵

### 3.1 构建与静态检查

| 项 | 命令 | 通过标准 |
|----|------|----------|
| 全量构建 | `mvn clean verify` | BUILD SUCCESS |
| 依赖方向 | `mvn dependency:tree` | client 无 broker；broker 无 client |
| 覆盖率 | `target/site/jacoco/index.html` | 行覆盖 ≥ 80%（发布门槛） |
| 禁止项 | 全局搜索 | 业务代码无 `System.out/err`；无硬编码端口 |
| 编译告警 | `mvn -Dmaven.compiler.showWarnings=true clean compile` | 无 unchecked 之外的明显告警 |

### 3.2 功能回归

| 编号 | 场景 | 预期 |
|------|------|------|
| R1 | 直连模式：发布 → 订阅 → 拉取 → ack | 全链路成功，顺序正确 |
| R2 | 注册中心模式：只配 registryUrls | 全链路成功 |
| R3 | 重复订阅 | 只保留一条记录 |
| R4 | 未知 topic fetch | 1003 |
| R5 | 非订阅者 fetch | 1004 |
| R6 | `strictFetch=false` | 非订阅者可拉 |
| R7 | 元数据往返 | msgId/tags/keys/headers/producedAt/producerId 全等 |
| R8 | fetch 分页 | offset/maxCount/nextOffset 正确 |
| R9 | ack offset / 旧 msgIndex | 都生效，进度单调 |
| R10 | 旧格式报文 | 发布/拉取/ack 成功 |
| R11 | Broker 启动时注册中心不可用 | Broker 正常服务；恢复后自动注册 |
| R12 | 注册中心重启 | Broker 自动重新注册 |
| R13 | Broker 正常停止 | 注册信息删除；客户端发现不到 |
| R14 | Broker 非优雅退出（kill） | TTL 后摘除 |
| R15 | 空闲连接超时 | 连接关闭，其他客户端正常 |

### 3.3 并发与故障

| 编号 | 场景 | 预期 |
|------|------|------|
| C1 | 32 producer × 50 条同 topic | 1600 条无丢失，offset 连续 |
| C2 | 50 并发短连接 | 全部收到应答，无 RST 泄漏 |
| C3 | 一个慢连接 + 正常客户端 | 正常客户端 1 秒内完成 |
| C4 | maxConnections 打满 | 返回 2001，服务可恢复 |
| C5 | worker 队列打满 | 返回 2001，不 OOM |
| C6 | 停机时有在途请求 | shutdownTimeout 内完成或关闭 |
| C7 | 连续 start/stop 100 次 | 端口可复用，无线程泄漏 |
| C8 | 大报文（> maxFrameBytes） | 拒绝并断开，其他连接不受影响 |

### 3.4 示例程序

```bash
# 终端 1
java -cp minimq-registry/target/... top.sorie.minimq.registry.RegistryMain --port=19901

# 终端 2
java -cp minimq-broker/target/... top.sorie.minimq.broker.BrokerMain \
  --port=19900 --registry=http://127.0.0.1:19901

# 终端 3
java -cp minimq-example/target/... top.sorie.minimq.example.DemoMain \
  --registry=http://127.0.0.1:19901 --topic=demo
```

预期输出：

```
[registry] started on 19901
[broker] started on 19900, registered as b-xxxx
[consumer] subscribed topic=demo
[producer] sent 3 messages
[consumer] fetched 3 messages, nextOffset=3
[consumer] acked offset=2
```

示例必须能在**全新 clone + 按 README 操作**的情况下跑通。

---

## 4. 版本与分支

### 4.1 版本号

- 所有模块统一 `0.0.2-SNAPSHOT` → 发布前改为 `0.0.2`。
- 发布提交后打 tag `v0.0.2`。
- 之后 `dev-0.0.3` 分支恢复 `0.0.3-SNAPSHOT`。

### 4.2 分支流程

```
dev-0.0.1 ──> dev-0.0.2 ──> 发布提交 ──> tag v0.0.2
                                  │
                                  └──> main（合并策略按仓库习惯）
```

规则：

- 每个里程碑完成且 `mvn clean verify` 通过后提交/合并，保证任意 commit 可构建。
- **push 到远端前必须单独确认**（全局规则）；本地 commit 可以直接做。
- 打 tag 与 push tag 同属远端操作，也要确认。

### 4.3 提交建议

M0–M4 每完成一个里程碑至少一个提交：

```
build: split project into multi-module layout
refactor: decouple protocol DTOs from broker handlers
feat: add protocol version, requestId and error codes
perf: process broker sessions with a bounded thread pool
feat: add single-node registry and broker discovery
feat: add message metadata and paginated fetch
docs: add architecture, protocol and operations guides
chore(release): bump version to 0.0.2
```

提交信息遵循 `type: description`，body 写清变更范围与兼容性影响。

---

## 5. 发布检查清单

### 5.1 代码

- [ ] `mvn clean verify` 全绿
- [ ] 无 `TODO` / `FIXME` 遗留在主流程（确认清单或转 issue）
- [ ] 无调试代码、无临时日志、无 `System.out/err`
- [ ] 无硬编码密钥/密码/内网地址
- [ ] 版本号已改为 `0.0.2`
- [ ] 依赖版本集中在 parent，无模块私自覆盖
- [ ] enforcer 规则生效
- [ ] JaCoCo 报告已生成，行覆盖率 ≥ 80%

### 5.2 文档

- [ ] README 快速开始可复现
- [ ] `docs/protocol.md` 与实现一致（字段、错误码、示例）
- [ ] `docs/architecture.md` 模块图与代码一致
- [ ] `docs/registry-api.md` 与 HTTP 实现一致
- [ ] `docs/operations.md` 配置项与 `*Config` 默认值一致
- [ ] CHANGELOG 列出破坏性变更（错误码、Producer 无参构造、strictFetch）
- [ ] 所有文档内部链接可用

### 5.3 发布

- [ ] 本地提交完成
- [ ] 已询问并获得推送确认
- [ ] 推送 `dev-0.0.2`
- [ ] 打 `v0.0.2` tag 并推送（再次确认）
- [ ] GitHub Release 说明从 CHANGELOG 复制
- [ ] 记录本次版本已知限制

---

## 6. 已知限制（必须写进 README 与 Release）

0.0.2 明确不具备：

1. 消息持久化：Broker 重启后消息、订阅、ack 全部丢失。
2. 推送：消费者仍需主动 `fetch`。
3. 延迟/优先级/TTL：字段已保存，但 Broker 不执行语义。
4. 集群：注册中心单点，Broker 单实例；无副本、无分区、无 rebalance。
5. 鉴权：注册中心与 Broker 均无认证，仅适合内网/本机学习。
6. 消息去重：msgId 只是标识，不保证幂等。
7. 生产级容量：内存存储无淘汰策略，长时间运行需关注内存。
8. 协议版本：客户端与 Broker 需同版本。新客户端不支持 0.0.1 Broker；0.0.1 客户端也不能读取 0.0.2 的元数据应答（未知字段会导致反序列化失败）。

---

## 7. 0.0.3 展望（仅记录，不实现）

用户已确认 Netty 放在 0.0.3。建议范围：

- `NettyBrokerServer implements BrokerServer`，长连接 + 心跳 + 空闲检测。
- 编解码复用 M1 的 `CommandCodec` / `CommandDecoder`，改为 Netty `ByteToMessageDecoder`。
- 推送/长轮询：订阅后由 Broker 主动投递，ack 驱动位点。
- 协议版本递增到 2（如果引入新的投递指令）。
- 保持 0.0.2 的模块与 store 接口不变。

0.0.3 的设计文档在本版本发布后再写，避免提前固化错误假设。

---

## 8. 验收标准（Definition of Done）

- [ ] 6 个文档全部存在，内容与代码一致
- [ ] 全新环境按 README 能跑通示例
- [ ] 回归矩阵 R1–R15、C1–C8 全部通过并记录结果
- [ ] `mvn clean verify` 与 enforcer 通过
- [ ] 版本号、CHANGELOG、tag 一致
- [ ] 已知限制在 README 与 Release 中显式声明
- [ ] 推送与 tag 操作均经过用户确认

---

## 9. 风险与应对

| 风险 | 应对 |
|------|------|
| 文档与实现漂移 | 每个里程碑结束更新对应章节；发布前对照检查清单 |
| 示例依赖 IDE/classpath 才能跑 | 提供 `exec-maven-plugin` 或明确 classpath 命令 |
| 回归场景太多执行不完 | 用脚本或 JUnit 标签分组；C 组可夜间跑 |
| 兼容矩阵遗漏 | 固定旧报文样例测试；README 写明版本匹配 |
| 发布后才发现破坏性变更 | CHANGELOG 在 M5 之前就开始记录，不最后补 |
