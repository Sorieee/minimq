# M3 单机注册中心设计说明

| 项目 | 内容 |
|------|------|
| 里程碑 | M3 |
| 目标 | 单机注册中心：broker 注册/心跳/摘除，客户端按 topic 发现 broker |
| 前置 | M0（模块）、M1（配置/日志/错误码） |
| 后续 | 0.0.4+ 注册中心集群 |
| 状态 | 设计完成，待实现 |
| 设计日期 | 2026-11-18 |

---

## 1. 目标与非目标

### 目标

1. 独立进程 `minimq-registry`，默认端口 `19901`，单机、内存存储。
2. Broker 启动后自动注册，定时心跳续租，停止时注销；注册中心不可用时 Broker 仍可服务。
3. 客户端只配置注册中心地址，即可按 topic 找到 Broker 并完成发布/拉取/订阅/ack。
4. 注册中心 API 与存储解耦，后续集群化只替换 `RegistryStore` 与网络复制层，不改客户端协议。
5. 提供健康检查、Broker 列表、topic 列表等运维接口。

### 非目标

- 注册中心集群、选主、数据复制（后续版本）
- 配置下发、限流、鉴权（后续版本）
- 持久化注册信息（重启即清空，靠 broker 重新注册恢复）
- 客户端侧复杂负载均衡（0.0.2 只做随机/轮询二选一）
- 跨机房、就近路由

---

## 2. 总体结构

```
┌──────────────┐    HTTP/JSON     ┌──────────────────┐
│   Broker     │ ───────────────> │  minimq-registry │
│  (启动/心跳)  │ <─────────────── │  (内存 RegistryStore)│
└──────┬───────┘                  └──────────────────┘
       │                                  ▲
       │ MQ 协议（TCP）                    │ HTTP/JSON（按 topic 查询）
       ▼                                  │
┌──────────────┐                          │
│  Producer /  │ ─────────────────────────┘
│  Consumer    │
└──────────────┘
```

- Broker 与注册中心之间：HTTP 注册 + 心跳。
- 客户端与注册中心之间：HTTP 查询 broker 列表。
- 客户端与 Broker 之间：原有 MQ 协议，不变。
- 注册中心不参与消息转发，只做目录。

### 2.1 模块划分

| 模块 | 内容 |
|------|------|
| `minimq-registry-api` | `BrokerInfo`、请求/响应 DTO、路径常量、`RegistryClient`（JDK HttpClient） |
| `minimq-registry` | `RegistryServer`、`RegistryService`、`RegistryStore`、`InMemoryRegistryStore`、HTTP 路由 |
| `minimq-broker` | 依赖 registry-api，启动注册与心跳 |
| `minimq-client` | 依赖 registry-api，按 topic 解析 broker |

`minimq-registry-api` 由 broker 与 client 共用，避免两份 HTTP 调用代码。

---

## 3. 数据模型

### 3.1 BrokerInfo

```java
package top.sorie.minimq.registry.api;

public record BrokerInfo(
        String brokerId,
        String host,              // 客户端可直连的地址（advertise host）
        int port,                 // 实际监听端口
        Set<String> topics,       // 当前提供的 topic
        int weight,               // 预留：0.0.2 不参与选择
        int protocolVersion,      // MQ 协议版本
        long epoch,               // 每次注册递增，用于识别旧心跳
        Instant registeredAt,
        Instant lastHeartbeatAt,
        Instant leaseExpireAt     // 超过此时间视为下线
) {
    public BrokerInfo {
        topics = topics == null ? Set.of() : Set.copyOf(topics);
    }

    public boolean isAlive(Instant now) {
        return leaseExpireAt != null && leaseExpireAt.isAfter(now);
    }
}
```

设计说明：

- `brokerId`：0.0.2 由 Broker 启动时生成 UUID；配置可显式指定，便于重启后保持身份。
- `host` 不是监听地址，而是**广告地址**：Broker 监听 `0.0.0.0` 时，注册的是 `IpUtil.getLocalIp()` 或显式配置。
- `port` 必须是 bind 之后的实际端口（支持测试用 0）。
- `topics` 随心跳更新，Broker 新增/删除 topic 时下一次心跳即可反映。
- `epoch`：重复注册时递增；心跳携带 epoch 时，旧 epoch 的心跳被拒绝，避免两个实例竞争同一个 brokerId。
- `leaseExpireAt`：由注册中心根据 TTL 计算，客户端也用这个字段过滤。

### 3.2 请求/响应 DTO

```java
public record RegisterRequest(
        String brokerId,          // 可空，空则注册中心生成
        String host,
        int port,
        Set<String> topics,
        int weight,
        int protocolVersion
) {}

public record RegisterResponse(
        String brokerId,
        long epoch,
        int ttlSeconds
) {}

public record HeartbeatRequest(
        String brokerId,
        long epoch,               // 0 表示不校验
        Set<String> topics        // 全量上报，覆盖旧值
) {}

public record HeartbeatResponse(
        int ttlSeconds,
        Instant serverTime
) {}
```

所有 record 使用 `@JsonInclude(NON_NULL)`，Jackson 2.18 原生支持 record；
如遇反序列化问题，加 `@JsonCreator` + `@JsonProperty`，不要退回可变 POJO。

---

## 4. HTTP API

### 4.1 通用约定

| 项 | 约定 |
|----|------|
| Base URL | `http://<registry-host>:19901` |
| Content-Type | `application/json; charset=utf-8` |
| 请求/响应体 | UTF-8 JSON |
| 应答信封 | 复用 `CmdResult`：`{"code":0,"message":"success","data":...}` |
| 请求体上限 | 64 KiB |
| 超时 | 服务端不做长任务；客户端连接 1s、读 2s |

错误码（`RegistryErrorCode`，放在 registry-api）：

| code | 常量 | HTTP | 含义 |
|------|------|------|------|
| 0 | `SUCCESS` | 200 | 成功 |
| 4001 | `INVALID_ARGUMENT` | 400 | 参数缺失/非法 |
| 4002 | `BROKER_NOT_FOUND` | 404 | brokerId 不存在或已过期 |
| 4003 | `EPOCH_MISMATCH` | 409 | 心跳 epoch 落后于当前注册 |
| 5000 | `INTERNAL_ERROR` | 500 | 服务端异常 |

客户端遇到 404 / `BROKER_NOT_FOUND` 应重新注册；遇到 409 应停止心跳并重新注册。

### 4.2 POST /api/brokers/register

请求：

```json
{
  "brokerId": "b-7f3c",
  "host": "192.168.1.10",
  "port": 19900,
  "topics": ["order", "pay"],
  "weight": 100,
  "protocolVersion": 1
}
```

响应：

```json
{"code":0,"message":"success","data":{"brokerId":"b-7f3c","epoch":3,"ttlSeconds":10}}
```

语义：

- `brokerId` 为空：生成 UUID 并返回。
- 已存在：覆盖 host/port/topics/weight/protocolVersion，`epoch++`，重置租约。
- `host` 空、`port` 不在 1–65535：返回 4001。
- `topics` 为空：允许（Broker 可能还没有 topic）。

### 4.3 POST /api/brokers/heartbeat

请求：

```json
{"brokerId":"b-7f3c","epoch":3,"topics":["order","pay","refund"]}
```

响应：

```json
{"code":0,"message":"success","data":{"ttlSeconds":10,"serverTime":"2026-11-18T10:00:00Z"}}
```

语义：

- 每次心跳刷新 `lastHeartbeatAt` 与 `leaseExpireAt = now + ttl`。
- `topics` 非空时全量覆盖；空集合表示"当前没有 topic"（也会覆盖）。
- `epoch` 非 0 且小于当前 epoch：返回 409，Broker 必须重新注册。
- brokerId 不存在 / 已过期：返回 404，Broker 重新注册。

### 4.4 DELETE /api/brokers/{brokerId}

语义：优雅下线，立即删除；不存在也返回成功（幂等）。
响应 `data` 为 `null`。

### 4.5 GET /api/brokers?topic=order

响应：

```json
{
  "code": 0,
  "message": "success",
  "data": [
    {"brokerId":"b-1","host":"10.0.0.1","port":19900,"topics":["order"],"weight":100,
     "protocolVersion":1,"epoch":1,"registeredAt":"...","lastHeartbeatAt":"...","leaseExpireAt":"..."}
  ]
}
```

语义：

- 只返回租约未过期的 broker。
- `topic` 为空时返回全部存活 broker。
- 结果顺序不保证；客户端自行选择。
- 没有可用 broker 时返回空数组，不报错（客户端据此返回发现失败）。

### 4.6 GET /api/topics

响应 `data` 为：

```json
{"order":2,"pay":1}
```

值：提供该 topic 的存活 broker 数量。用于运维排查。

### 4.7 GET /api/health

响应：

```json
{"code":0,"message":"success","data":{"status":"UP","brokers":2,"uptimeSeconds":120}}
```

---

## 5. 注册中心服务端设计

### 5.1 类结构

```
top.sorie.minimq.registry
├── RegistryServer.java          外观：start/stop/getPort/awaitTermination
├── RegistryService.java         业务：注册/心跳/注销/查询/过期清理
├── store/
│   ├── RegistryStore.java
│   └── InMemoryRegistryStore.java
├── http/
│   ├── RegistryHttpServer.java  JDK HttpServer 装配
│   ├── BrokerRegisterHandler.java
│   ├── BrokerHeartbeatHandler.java
│   ├── BrokerListHandler.java
│   ├── BrokerDeleteHandler.java
│   ├── TopicsHandler.java
│   └── HealthHandler.java
└── RegistryConfig.java          bindHost/port/ttl/sweep/workerSize
```

### 5.2 RegistryStore

```java
public interface RegistryStore {
    BrokerInfo register(RegisterRequest request, Instant now);
    boolean heartbeat(String brokerId, long epoch, Set<String> topics, Instant now);
    boolean deregister(String brokerId);
    List<BrokerInfo> findAliveByTopic(String topic, Instant now);
    List<BrokerInfo> findAlive(Instant now);
    Map<String, Integer> topicCounts(Instant now);
    int removeExpired(Instant now);
}
```

`InMemoryRegistryStore`：

- `ConcurrentHashMap<String, BrokerInfo>`；record 不可变，更新时整体替换。
- `AtomicLong epoch`，每个 broker 独立 epoch（放在 Map 里的 `epoch` 字段，用 `compute` 原子递增）。
- 注册：
  ```java
  brokers.compute(brokerId, (id, old) -> new BrokerInfo(
          id, host, port, topics, weight, protocolVersion,
          old == null ? 1 : old.epoch() + 1,
          old == null ? now : old.registeredAt(),
          now, now.plusSeconds(ttl)));
  ```
- 心跳：
  ```java
  brokers.computeIfPresent(brokerId, (id, old) -> {
      if (epoch > 0 && epoch < old.epoch()) {
          throw new EpochMismatchException();  // 由 service 转 4003
      }
      return new BrokerInfo(..., old.epoch(), old.registeredAt(), now, now.plusSeconds(ttl));
  });
  ```
  `compute` 里抛异常会中断更新，注意在 service 层先做检查或捕获后转换。
- 查询：`brokers.values().stream().filter(b -> b.isAlive(now))...`，结果做 `List.copyOf`。
- 过期清理：`removeExpired(now)` 用 `entrySet().removeIf` 或 `computeIfPresent` 判断 `!isAlive`。

### 5.3 过期清理

- 调度器每 1 秒执行一次 `removeExpired(now)`。
- TTL 默认 10 秒，心跳默认 3 秒；允许连续丢 2–3 次心跳。
- 清理线程 daemon，随 `RegistryServer.stop()` 关闭。
- 即使清理有延迟，查询时仍按 `isAlive` 过滤，不会返回过期 broker。

### 5.4 HTTP 装配

用 JDK 自带 `com.sun.net.httpserver.HttpServer`，不引 Spring：

```java
HttpServer server = HttpServer.create(new InetSocketAddress(bindHost, port), backlog);
server.createContext("/api/brokers", new BrokerHandler(service));
server.createContext("/api/topics", new TopicsHandler(service));
server.createContext("/api/health", new HealthHandler(service));
server.setExecutor(Executors.newFixedThreadPool(workerSize, namedDaemonFactory("mq-registry-http-")));
server.start();
```

- 端口传 0 时 `server.getAddress().getPort()` 取实际端口（测试用）。
- 每个 handler 先校验 method，再读 body，限制 64 KiB。
- JSON 解析失败返回 4001，不抛 500。
- 所有异常在 handler 顶层捕获，返回 5000 + 日志，避免连接悬挂。
- 关闭：`server.stop(0)` + executor shutdown。

### 5.5 RegistryConfig

| 字段 | 默认 | 说明 |
|------|------|------|
| `bindHost` | `0.0.0.0` | 监听地址 |
| `port` | 19901 | 0 表示系统分配 |
| `backlog` | 64 | HttpServer backlog |
| `workerSize` | 8 | HTTP 处理线程数 |
| `leaseTtl` | 10s | 租约时长 |
| `sweepInterval` | 1s | 过期清理间隔 |

> 安全提示：0.0.2 无鉴权，默认只应在开发/内网使用；不要把 19901 暴露到公网。

---

## 6. Broker 侧集成

### 6.1 RegistryClient（registry-api）

```java
public final class RegistryClient {
    private final HttpClient httpClient;      // connectTimeout 1s
    private final List<String> baseUrls;      // 支持多个，0.0.2 取第一个可用的
    private final Duration requestTimeout;    // 2s

    public RegisterResponse register(RegisterRequest request) throws RegistryException;
    public HeartbeatResponse heartbeat(HeartbeatRequest request) throws RegistryException;
    public void deregister(String brokerId);
    public List<BrokerInfo> brokersOf(String topic);
}
```

- 用 JDK `java.net.http.HttpClient`，不引第三方 HTTP 客户端。
- 非 2xx 或信封 `code != 0`：抛 `RegistryException(code, message)`，调用方按 code 处理。
- 多个 baseUrl：按顺序尝试，第一个成功即返回；全部失败抛异常（为集群预留）。
- `deregister` 失败只记日志，不抛（停机路径不能因注册中心挂掉而卡住）。

### 6.2 生命周期

```
start()
  ├── bind MQ 端口
  ├── 构造 BrokerService
  ├── registryClient.register(...)   // 启动注册，失败只告警
  ├── 启动心跳调度（每 3s）
  └── 启动 accept

stop()
  ├── 停止 accept
  ├── 取消心跳调度
  ├── registryClient.deregister(...) // best effort
  └── 等待线程退出
```

注册内容：

- `host`：`BrokerConfig.advertiseHost`，为空时用 `IpUtil.getLocalIp()`。
- `port`：实际绑定端口。
- `topics`：`messageStore.topics()` 的快照。
- `protocolVersion`：`MqCmd.PROTOCOL_VERSION`。
- `brokerId`：配置为空时启动生成 UUID。

心跳内容：

- 每次心跳带上当前 `messageStore.topics()`，让新 topic 自动出现在发现结果里。
- 心跳失败连续 N 次（默认 3）打一条 WARN，避免日志刷屏；恢复后打 INFO。
- 404 → 立即重新注册；409 → 停止当前心跳，重新注册（用新 epoch 继续）。

### 6.3 BrokerConfig 扩展

| 字段 | 默认 | 说明 |
|------|------|------|
| `registryUrls` | 空列表 | 空表示不使用注册中心 |
| `brokerId` | 空 | 空则启动生成 UUID |
| `advertiseHost` | 空 | 空则 `IpUtil.getLocalIp()` |
| `registryHeartbeatInterval` | 3s | 心跳间隔 |
| `registryRequestTimeout` | 2s | 单次 HTTP 超时 |

规则：

- `registryUrls` 非空时，URL 必须是 `http://host:port` 形式。
- 注册失败不影响 Broker 启动；`getStatus()` 仍为 RUNNING。
- 心跳线程 daemon，随 `stop()` 关闭。

---

## 7. 客户端服务发现

### 7.1 BrokerResolver

```java
public interface BrokerResolver {
    /** 解析 topic 对应的一个可用 broker；没有可用 broker 抛 DiscoveryException */
    BrokerEndpoint resolve(String topic);
}

public record BrokerEndpoint(String host, int port) {}
```

两种实现：

| 实现 | 场景 |
|------|------|
| `DirectBrokerResolver` | 配置了 `host:port`，永远返回它 |
| `RegistryBrokerResolver` | 配置了 `registryUrls`，按 topic 查询并缓存 |

`RegistryBrokerResolver` 细节：

- 缓存 `topic -> (List<BrokerEndpoint>, expireAt)`，TTL 默认 5 秒。
- 查询结果过滤 `leaseExpireAt > now`。
- 选择策略：0.0.2 用**随机**（`ThreadLocalRandom`），避免所有客户端集中到第一个。
- 缓存过期后同步刷新；刷新失败时，若缓存未超过 `staleTtl`（默认 30 秒），继续用旧列表并打 WARN；超过则抛 `DiscoveryException`。
- 解析失败映射为 `ErrorCode.DISCOVERY_FAILED`（新增 code 3001）。

### 7.2 ClientConfig 扩展

```java
public final class ClientConfig {
    // M1 已有
    private final String host;
    private final int port;
    private final List<String> registryUrls;
    private final int connectTimeoutMs;
    private final int readTimeoutMs;
    private final int maxFrameBytes;
    // M3 新增
    private final int discoveryCacheTtlMs;   // 默认 5000
    private final int discoveryStaleTtlMs;   // 默认 30000
}
```

规则：

- `registryUrls` 非空时优先走注册中心，`host/port` 可空。
- 两者都为空：构造失败。
- `registryUrls` 做 URL 格式校验。

### 7.3 Producer / Consumer 改动

```java
public class Producer {
    private final BrokerResolver resolver;
    private final ClientProvider clientProvider;  // 按 endpoint 缓存 Client

    public CmdResult sendMsg(String topic, String content) {
        return sendWithRetry(topic, cmd);
    }
}

public class Consumer {
    public CmdResult subscribe(String topic) { ... }
    public CmdResult fetch(String topic) { ... }
    public CmdResult ack(String topic, long offset) { ... }
}
```

`sendWithRetry`：

1. `resolver.resolve(topic)` 取 endpoint。
2. `clientProvider.get(endpoint).sendCmd(cmd)`。
3. 若返回连接失败类错误（`CmdResult` 只有 message，建议新增 `transportFailure` 标记或按 message 判断太脆弱）：
   - 方案：`Client.sendCmd` 在连接失败时返回 `CmdResult.error(ErrorCode.DISCOVERY_FAILED)` 或专用 `ErrorCode.BROKER_UNAVAILABLE`（2002）。
   - 收到 `BROKER_UNAVAILABLE`：调用 `resolver.invalidate(topic)`，换下一个 endpoint 重试一次。
4. 仍失败返回错误应答，不抛异常（保持 0.0.1 API 风格）。

新增错误码（M3 落地到 `ErrorCode`）：

| code | 常量 | 场景 |
|------|------|------|
| 2002 | `BROKER_UNAVAILABLE` | 连接 broker 失败/超时 |
| 3001 | `DISCOVERY_FAILED` | 注册中心查询失败且无可用缓存 |

> 0.0.2 的 `Client.sendCmd` 目前把连接失败也映射为 `CODE_ERROR`。M3 需要区分"协议错误"和"传输失败"，
> 建议在 `CmdResult` 增加 `boolean transportError`（不参与线上信封）或直接用上述错误码区分。

### 7.4 订阅与 ack 的跨 broker 语义

单机注册中心下通常只有一个 broker，问题不大，但要在文档里写清楚：

- 订阅关系和 ack 进度是 **broker 本地状态**，不是注册中心状态。
- 客户端切换到另一个 broker 后，订阅与 ack 不会自动迁移。
- 0.0.2 不做消费组与 rebalance；多 broker 场景下生产者可按 topic 分布，消费者行为属于"已知限制"。
- 0.0.4 集群化时再引入共享订阅/ack 存储或分区分配。

---

## 8. 注册中心重启与故障场景

| 场景 | 表现 | 处理 |
|------|------|------|
| Broker 启动时注册中心未启动 | 注册失败 WARN，Broker 正常服务 | 心跳周期内重试注册 |
| 运行中注册中心挂掉 | 心跳失败 WARN（限流日志） | Broker 继续服务；恢复后重新注册 |
| 注册中心重启（数据清空） | 心跳返回 404 | 立即重新注册 |
| Broker 非优雅退出 | 注册信息残留 | TTL 到期后清理；客户端按 lease 过滤 |
| 客户端查询时注册中心挂掉 | HTTP 异常 | 用未过期的本地缓存；超过 staleTtl 返回 3001 |
| 两个 broker 使用同一 brokerId | 心跳/注册 epoch 冲突 | 后注册者 epoch 更大；旧实例心跳 409 后重新注册（会覆盖，需运维保证 brokerId 唯一） |

建议：0.0.2 的 brokerId 默认 UUID，天然避免冲突；只有手工配置才可能重复。

---

## 9. 集群化预留

0.0.2 的实现必须保证以下扩展点存在，否则后续改造成本高：

1. **`RegistryStore` 接口**：集群版换成 `RaftRegistryStore` 或外部存储实现，HTTP 层不动。
2. **`RegistryClient` 支持多 baseUrl**：客户端配置可以直接填多个注册中心地址，按顺序 failover。
3. **`BrokerInfo.epoch`**：用于并发写冲突检测，后续复制时保留。
4. **API 无状态**：注册/心跳/查询都不依赖单机内存会话，便于任意节点处理。
5. **`LEASE_TTL` 与 `heartbeatInterval` 可配**：集群选举/复制延迟不同，参数需要可调。
6. **topic 全量上报**：避免增量同步协议，复制时整体覆盖更简单。

后续集群方案（不作为 0.0.2 任务）：

- 多注册中心节点互相复制 broker 表（Raft 或 gossip）。
- 客户端配置多个注册中心地址，读任意节点。
- 注册中心暴露 `GET /api/cluster` 返回节点与版本，便于排障。

---

## 10. 测试计划

### 10.1 store 单测

| 用例 | 断言 |
|------|------|
| register new | 返回 brokerId + epoch=1；TTL 正确 |
| register existing | epoch 递增；registeredAt 不变；字段覆盖 |
| heartbeat refresh | leaseExpireAt 延后；topics 更新 |
| heartbeat unknown | 返回 false / 404 |
| heartbeat stale epoch | 409 |
| deregister | 幂等；不存在也成功 |
| expire | 超过 TTL 后 `findAlive` 不返回 |
| findByTopic | 只返回存活且包含 topic 的 broker |
| topicCounts | 多 broker 共享 topic 时计数正确 |
| record immutability | 返回列表修改不影响 store |

### 10.2 HTTP API 集成测试

启动 `RegistryServer(0)`，用 JDK HttpClient 调用：

| 用例 | 断言 |
|------|------|
| register → list | 能查到；字段完整 |
| heartbeat → list | lease 刷新 |
| delete → list | 查不到 |
| 非法 JSON | 400 + code 4001 |
| 缺 host/port | 400 + 4001 |
| 未知 broker 心跳 | 404 + 4002 |
| 未知路径 | 404 |
| 错误 method | 405 |
| health | status UP，broker 数正确 |

### 10.3 Broker 注册测试

| 用例 | 断言 |
|------|------|
| Broker 启动 | 注册中心出现该 broker；topics 正确；port 是实际端口 |
| 新增 topic 后心跳 | 注册中心 topics 更新 |
| Broker 正常停止 | 记录被删除 |
| 注册中心未启动 | Broker 仍能 start；日志有 WARN；注册中心起来后自动注册 |
| 注册中心重启 | Broker 心跳 404 后重新注册 |

### 10.4 客户端发现测试

| 用例 | 断言 |
|------|------|
| 只配注册中心 | Producer 发布、Consumer 拉取成功 |
| 无可用 broker | 返回 3001，不抛异常 |
| 缓存 | 5 秒内重复发布只查询一次（可用 mock/count 验证） |
| broker 下线 | TTL 后解析失败 |
| 直连模式 | 不访问注册中心，行为与 0.0.1 一致 |

### 10.5 端到端

`minimq-example` 中启动 **Registry + Broker + Producer + Consumer**，跑完整链路：

- 注册 → 发布 100 条 → 订阅 → 拉取 → ack；
- 停 Broker → 客户端发现失败；
- 重启 Broker → 自动重新注册 → 恢复收发。

---

## 11. 验收标准

- [ ] `minimq-registry` 可独立启动，默认端口 19901，`/api/health` 返回 UP
- [ ] Broker 启动后 3 秒内出现在 `GET /api/brokers` 中
- [ ] 心跳间隔 3 秒、TTL 10 秒；停止心跳后 10–13 秒内被摘除
- [ ] Broker 正常停止后立即从列表消失
- [ ] 注册中心未启动时 Broker 仍可独立工作
- [ ] 客户端只配置注册中心即可完成发布/拉取/订阅/ack
- [ ] 注册中心重启后 Broker 自动重新注册
- [ ] 查询无可用 broker 时客户端返回 3001，不抛异常
- [ ] 注册中心 HTTP 层没有业务逻辑，全部委托 `RegistryService`
- [ ] `RegistryStore` 之外没有直接操作 broker 表的地方
- [ ] 所有 HTTP 错误返回统一信封，未知路径/方法也有明确状态码

---

## 12. 风险与应对

| 风险 | 影响 | 应对 |
|------|------|------|
| 注册中心成为单点 | 挂了无法发现新 broker | Broker 继续服务；客户端缓存兜底；后续集群化 |
| 心跳风暴 | 大量 broker 时 CPU 升高 | 默认 3s + 随机抖动（`±10%`）；TTL 可配 |
| 时钟不一致 | lease 判断偏差 | 全部使用注册中心时间；客户端用自己时间过滤只作近似 |
| brokerId 冲突 | 注册被覆盖 | 默认 UUID；配置重复时日志告警 |
| 无鉴权 | 任意客户端可注册假 broker | 文档标注仅内网；后续加 token/ACL |
| HTTP 客户端阻塞 | Broker 启动/心跳卡住 | 所有 HTTP 调用设超时；注册失败不阻塞启动 |
| 客户端缓存过期数据 | 连到已下线 broker | 连接失败触发 invalidate + 重试；stale 上限 30s |
| record + Jackson 兼容 | 反序列化失败 | 加明确的 `@JsonCreator` 测试；CI 覆盖 |

---

## 13. 实现顺序

1. `minimq-registry-api`：DTO、`RegistryErrorCode`、`RegistryClient`（先不接 broker）。
2. `minimq-registry`：`RegistryStore` + `InMemoryRegistryStore` + 单测。
3. HTTP 层 + API 集成测试。
4. Broker 注册/心跳/注销 + 测试。
5. 客户端 `BrokerResolver` + 发现 + 测试。
6. example 端到端 demo。
7. 故障场景测试（注册中心重启、broker 非优雅退出）。
