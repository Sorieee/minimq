# M2 Broker 并发化与服务分层设计说明

| 项目 | 内容 |
|------|------|
| 里程碑 | M2 |
| 目标 | accept 与业务处理分离、有界线程池、存储/订阅/ack 接口化、优雅停机 |
| 前置 | M0、M1 |
| 后续 | M4 元数据与 fetch 分页 |
| 状态 | 设计完成，待实现 |
| 设计日期 | 2026-11-18 |

---

## 1. 目标与非目标

### 目标

1. `start()` 只负责绑定端口与启动 accept 线程，返回后立即可用，不再阻塞调用线程。
2. 每个连接交给有界线程池处理，一个慢连接不阻塞其他客户端。
3. 连接数、队列、报文大小、读超时全部有上限，避免资源被一个客户端耗尽。
4. 抽出 `BrokerService` + `MessageStore` / `AckStore` / `SubscriptionStore`，`Broker` 只做外观与生命周期。
5. 支持优雅停机：停止接客 → 等待在途会话 → 超时强制关闭 → 释放端口。
6. 修复 0.0.1 遗留：`fetch` 返回内部 List 引用、ack key 字符串拼接、半条指令污染。
7. 保持线上协议与业务行为不变（仍是短连接、一问一答）。

### 非目标

- 不换 Netty（0.0.3）
- 不做持久化（存储仍是内存）
- 不做推送/长轮询
- 不做 fetch 分页（M4；M2 只把 store 的区间读取能力准备好）
- 不做连接复用/长连接（0.0.3 随 Netty 一起）

---

## 2. 现状问题

`src/main/java/top/sorie/minimq/broker/Broker.java`：

- `start()` 第 59–86 行在调用线程里做 accept 循环，`handleSession` 同步执行（第 110–129 行）。
- 一个连接只要不发送 `</end>`，就会一直占住 accept 线程，其他客户端全部排队。
- 会话没有 `SO_TIMEOUT`，空闲连接可以永久占用。
- 连接数、线程数、报文大小都没有上限。
- 业务状态（消息、订阅、ack）直接挂在 `Broker` 上，无法替换存储实现。
- `fetch`（第 183–185 行）直接返回 `topicMsgMap` 内部 `List`，调用方可以改到 Broker 状态。
- ack key 用 `consumerId + ":" + topic`（第 189 行）字符串拼接，topic 里含 `:` 时有歧义。

---

## 3. 目标结构

### 3.1 分层

```
          ┌──────────────────────────────┐
调用方 ──> │ Broker（外观 / 生命周期）      │
          └───────┬──────────────┬───────┘
                  │              │
          ┌───────▼──────┐  ┌────▼────────────┐
          │ BrokerServer │  │  BrokerService  │
          │  (传输接口)   │  │ (业务 / 三个 store)│
          └───────┬──────┘  └────┬────────────┘
                  │              │
       ┌──────────▼───────┐  ┌───▼──────────────┐
       │ TcpBrokerServer  │  │ MessageStore     │
       │  accept + 线程池  │  │ AckStore         │
       │  SessionHandler  │  │ SubscriptionStore│
       └──────────────────┘  └──────────────────┘
```

- `Broker`：对外 API（start/stop/getPort/getStatus/测试访问器），不写业务。
- `BrokerServer`：传输层接口，0.0.3 的 `NettyBrokerServer` 实现同一接口。
- `BrokerService`：实现 `BrokerContext`，handler 只依赖它。
- store 接口：内存实现可替换为磁盘/KV。

### 3.2 包结构

```
top.sorie.minimq.broker
├── Broker.java                    外观
├── BrokerContext.java             handler 依赖的接口
├── BrokerService.java             业务实现
├── config/BrokerConfig.java       （M1 引入，M2 扩展）
├── server/
│   ├── BrokerServer.java          传输接口
│   ├── TcpBrokerServer.java       socket + 线程池实现
│   └── SessionHandler.java        单连接读写
└── store/
    ├── MessageStore.java
    ├── AckStore.java
    ├── SubscriptionStore.java
    ├── StoredMessage.java
    └── memory/
        ├── InMemoryMessageStore.java
        ├── InMemoryAckStore.java
        └── InMemorySubscriptionStore.java
```

---

## 4. 核心接口

### 4.1 BrokerServer

```java
package top.sorie.minimq.broker.server;

public interface BrokerServer extends AutoCloseable {
    /** 绑定端口并启动 accept 线程；绑定失败抛 IOException，方法返回即可服务 */
    void start() throws IOException;

    /** 停止接客并关闭连接；幂等 */
    void stop();

    /** 等待服务线程与工作线程退出 */
    void awaitTermination(java.time.Duration timeout) throws InterruptedException;

    /** bind 后实际端口；未启动返回 0 */
    int getBoundPort();

    boolean isRunning();
}
```

`TcpBrokerServer` 在构造时接收 `BrokerConfig` 与 `HandlerRegistry`（或一个 `FrameHandler` 回调），
不直接依赖 `Broker`，保证 Netty 实现可替换：

```java
@FunctionalInterface
public interface FrameHandler {
    /** 处理一条已解码的指令，返回 JSON 应答（单行，不含换行） */
    String handle(String frame);
}
```

### 4.2 MessageStore

```java
package top.sorie.minimq.broker.store;

public interface MessageStore {
    /** 追加消息，返回该 topic 内的 offset（从 0 开始严格递增） */
    long append(String topic, MqMsg msg);

    boolean topicExists(String topic);

    /** 按 offset 区间读取；fromOffset 越界返回空列表，不抛异常 */
    List<StoredMessage> fetch(String topic, long fromOffset, int maxCount);

    /** topic 全部消息快照；不存在返回空列表 */
    List<MqMsg> snapshotMessages(String topic);

    /** topic 快照，顺序不保证 */
    Set<String> topics();

    /** 最新 offset；无消息返回 -1 */
    long latestOffset(String topic);
}
```

`StoredMessage`：

```java
public record StoredMessage(long offset, Instant storedAt, MqMsg msg) {
    public StoredMessage {
        if (offset < 0) {
            throw new IllegalArgumentException("offset must be >= 0");
        }
        if (msg == null) {
            throw new IllegalArgumentException("msg is required");
        }
    }
}
```

> `brokerId` 字段等 M3 之后再评估是否加入；它属于注册/集群语义，不是单机存储必需。

### 4.3 AckStore

```java
public interface AckStore {
    /** 记录 ack，返回当前生效的最大 offset（单调不减） */
    long ack(String consumerId, String topic, long offset);

    /** 已确认的最大 offset；从未 ack 返回 -1 */
    long ackedOffset(String consumerId, String topic);
}
```

`AckKey` 用 record 替代字符串拼接：

```java
public record AckKey(String consumerId, String topic) {
    public AckKey {
        Objects.requireNonNull(consumerId, "consumerId");
        Objects.requireNonNull(topic, "topic");
    }
}
```

### 4.4 SubscriptionStore

```java
public interface SubscriptionStore {
    void subscribe(String topic, String consumerId);
    boolean unsubscribe(String topic, String consumerId);
    boolean isSubscribed(String topic, String consumerId);
    List<String> subscribersOf(String topic);
}
```

### 4.5 BrokerService

```java
public final class BrokerService implements BrokerContext {
    private final MessageStore messageStore;
    private final AckStore ackStore;
    private final SubscriptionStore subscriptionStore;

    // 构造注入三个 store，默认用 InMemory 实现

    @Override public long publish(MqMsg msg) { ... }
    @Override public void subscribe(String topic, String consumerId) { ... }
    @Override public boolean unsubscribe(String topic, String consumerId) { ... }
    @Override public List<MqMsg> fetch(String topic) { ... }
    @Override public void ack(String consumerId, String topic, long offset) { ... }
    @Override public long ackedOffset(String consumerId, String topic) { ... }

    // 供 M4 使用，M2 先提供
    public List<StoredMessage> fetch(String topic, long fromOffset, int maxCount) { ... }
    public List<String> subscribersOf(String topic) { ... }
    public List<MqMsg> messagesOf(String topic) { ... }
}
```

`BrokerContext` 的 ack 参数从 `int` 改为 `long`（线上仍是 JSON 数字，兼容）。

### 4.6 Broker 外观

```java
public final class Broker implements AutoCloseable {
    public static final Integer STATUS_RUNNING = 1;
    public static final Integer STATUS_STOPPED = 0;

    private final BrokerConfig config;
    private final BrokerService service;
    private final HandlerRegistry handlerRegistry;
    private volatile BrokerServer server;
    private volatile Integer status = STATUS_STOPPED;

    public Broker(BrokerConfig config) { ... }

    // 兼容旧入口
    public Broker() { this(BrokerConfig.defaults()); }
    public Broker(int port) { this(BrokerConfig.defaults().toBuilder().port(port).build()); }

    public void start() { ... }          // 非阻塞
    public void stop() { ... }           // 幂等
    public void awaitTermination(Duration timeout) throws InterruptedException { ... }
    public int getPort() { ... }
    public Integer getStatus() { ... }

    // 测试/运维访问器，委托 BrokerService
    public List<MqMsg> msgsOf(String topic) { ... }
    public List<String> subscribersOf(String topic) { ... }
    public Integer ackedIndexOf(String consumerId, String topic) { ... }
}
```

**行为变化**：`start()` 不再阻塞。现有测试里 `new Thread(() -> broker.start())` 的写法会立即结束，
但 `stop()` + `join()` 仍能通过；建议测试直接调用 `broker.start()`，用 `awaitTermination` 或 `stop` 收尾。

`Broker` 不再实现 `BrokerContext`；handler 拿到的是 `BrokerService`。如果为兼容测试保留 `implements BrokerContext`，
则所有方法委托 `service`，二选一，但**不要让业务逻辑回到 Broker**。

---

## 5. 线程模型

### 5.1 线程组成

| 线程 | 数量 | 职责 | daemon |
|------|------|------|--------|
| `mq-broker-accept` | 1 | `accept()`、许可证检查、提交任务 | 是 |
| `mq-broker-worker-<n>` | 线程池 | 单连接会话：读指令 → 执行 → 写应答 | 是 |
| `mq-registry-heartbeat` | 1 | M3 引入，注册中心心跳 | 是 |

daemon 的原因：进程存活由调用方决定（`Main` 显式 `awaitTermination`），避免测试失败后 JVM 挂住。
生产化时可通过配置改为非 daemon，0.0.2 不做。

### 5.2 accept 循环

```java
while (running) {
    Socket socket = serverSocket.accept();
    if (!connectionPermits.tryAcquire()) {
        reject(socket, ErrorCode.SERVER_BUSY);
        continue;
    }
    try {
        socket.setSoTimeout(config.sessionReadTimeoutMs());
        socket.setTcpNoDelay(true);
        activeSockets.add(socket);
        executor.execute(() -> runSession(socket));
    } catch (RejectedExecutionException e) {
        connectionPermits.release();
        activeSockets.remove(socket);
        reject(socket, ErrorCode.SERVER_BUSY);
    }
}
```

关键点：

- `accept()` 自身抛 `SocketException` 表示 `stop()` 关闭了监听 socket，正常退出循环，不打印堆栈。
- 每个连接先拿 `Semaphore` 许可，拿不到立即回错误并关闭，避免无界堆积。
- 提交任务失败（队列满 / 已 shutdown）必须释放许可并关闭 socket，不能泄漏。
- `activeSockets` 用于停机时强制关闭仍阻塞在 `readLine` 的连接。

### 5.3 线程池参数

`BrokerConfig` 新增：

```java
workerSize             默认 max(16, CPU * 4)
workerQueueCapacity    默认 512
maxConnections         默认 1000
sessionReadTimeoutMs   默认 30000（M1 已有）
shutdownTimeout        默认 10s
```

实现用 `ThreadPoolExecutor`：

```java
new ThreadPoolExecutor(
        n, n, 0L, TimeUnit.MILLISECONDS,
        new ArrayBlockingQueue<>(queueCapacity),
        namedDaemonThreadFactory("mq-broker-worker-"),
        new ThreadPoolExecutor.AbortPolicy());
```

- 固定大小 + 有界队列，避免无界线程和 OOM。
- 拒绝策略用 `AbortPolicy`，由 accept 循环统一转成 `SERVER_BUSY` 应答，而不是 `CallerRunsPolicy`（会阻塞 accept）。
- 核心线程允许超时回收（`allowCoreThreadTimeOut(true)`）可选，默认关闭。

### 5.4 会话处理

```java
void runSession(Socket socket) {
    try (socket) {
        socket.setSoTimeout(config.sessionReadTimeoutMs());
        BufferedReader in = ...;
        PrintWriter out = ...;
        CommandDecoder decoder = new CommandDecoder(config.maxFrameBytes());
        String line;
        while ((line = in.readLine()) != null) {
            String frame = decoder.feed(line);
            if (frame == null) {
                continue;
            }
            String reply = frameHandler.handle(frame);
            out.println(reply);
            if (out.checkError()) {
                return;   // 写失败，结束会话
            }
        }
    } catch (SocketTimeoutException e) {
        LOG.debug("session idle timeout");
    } catch (FrameTooLargeException e) {
        writeBusyOrInvalid(socket, ErrorCode.INVALID_ARGUMENT);
    } catch (SocketException e) {
        LOG.debug("session closed: {}", e.getMessage());
    } catch (IOException e) {
        LOG.warn("session io error", e);
    } finally {
        decoder.reset();          // 丢弃半条指令
        activeSockets.remove(socket);
        connectionPermits.release();
    }
}
```

注意：

- `PrintWriter` 只写不 close（0.0.1 已踩过的坑）；`try (socket)` 负责关闭。
- `decoder.reset()` 在 finally，防止异常路径残留。
- 任何单会话异常都只结束该会话，不能冒到 accept 线程。
- 一条连接仍支持连续多条指令（0.0.1 行为保留）。

### 5.5 优雅停机

```java
public void stop() {
    if (status == STATUS_STOPPED) {
        return;
    }
    status = STATUS_STOPPING;       // 内部状态，对外仍可映射为 STOPPED
    closeServerSocket();            // accept 抛异常退出
    executor.shutdown();            // 不再接新任务
    try {
        if (!executor.awaitTermination(shutdownTimeout)) {
            activeSockets.forEach(closeQuietly);
            executor.shutdownNow();
        }
    } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        activeSockets.forEach(closeQuietly);
        executor.shutdownNow();
    } finally {
        activeSockets.forEach(closeQuietly);
        activeSockets.clear();
        status = STATUS_STOPPED;
        terminationLatch.countDown();
    }
}
```

语义：

- 先停止接客，再等待在途会话；超时才强制关闭。
- `stop()` 幂等，重复调用直接返回。
- `awaitTermination(timeout)` 等待 `terminationLatch`，供 `Main` 阻止进程退出。
- 端口释放由关闭 `ServerSocket` 保证，测试可立即重新绑定。

---

## 6. 存储实现

### 6.1 InMemoryMessageStore

```java
private final ConcurrentHashMap<String, TopicLog> topicLogs = new ConcurrentHashMap<>();

private static final class TopicLog {
    private final List<StoredMessage> messages = new ArrayList<>();
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
}
```

- `append`：写锁内 `messages.add(new StoredMessage(messages.size(), now, msg))`，返回 offset。
- `fetch`：读锁内校验 `fromOffset`，用 `subList` + `List.copyOf` 返回不可变快照。
- `snapshotMessages`：读锁内拷贝 `MqMsg` 列表，**绝不返回内部引用**。
- `topics`：返回 `Set.copyOf(topicLogs.keySet())`。
- `latestOffset`：读锁下 `size - 1`。

注意：`MqMsg` 在 M1 仍是可变 DTO；如果 M4 改为 record，store 可以直接存引用。
在 M4 之前，`append` 时至少做一次字段拷贝或要求调用方不再修改消息对象；0.0.2 约定「handler 之后不再改 MqMsg」。

### 6.2 InMemoryAckStore

```java
private final ConcurrentHashMap<AckKey, Long> acked = new ConcurrentHashMap<>();

public long ack(String consumerId, String topic, long offset) {
    AckKey key = new AckKey(consumerId, topic);
    return acked.merge(key, offset, Math::max);
}
```

### 6.3 InMemorySubscriptionStore

```java
private final ConcurrentHashMap<String, Set<String>> subscribers = new ConcurrentHashMap<>();

public void subscribe(String topic, String consumerId) {
    subscribers.computeIfAbsent(topic, k -> ConcurrentHashMap.newKeySet()).add(consumerId);
}
```

- 幂等：重复订阅天然只保留一个。
- `subscribersOf` 返回 `List.copyOf(set)`；**顺序不保证**，测试不要依赖多订阅者顺序。
- 现有测试断言 `List.of("c1")` 仍然成立（单元素）。

---

## 7. 配置扩展

`BrokerConfig` 在 M1 基础上增加：

| 字段 | 默认 | 说明 |
|------|------|------|
| `port` | 19900 | 0 表示系统分配 |
| `backlog` | 128 | bind backlog |
| `maxFrameBytes` | 1 MiB | 单条报文上限 |
| `workerSize` | max(16, CPU*4) | 工作线程数 |
| `workerQueueCapacity` | 512 | 等待队列 |
| `maxConnections` | 1000 | 并发会话上限 |
| `sessionReadTimeoutMs` | 30000 | 空闲连接超时 |
| `shutdownTimeout` | 10s | 优雅停机等待 |

校验规则：

- `port` ∈ [0, 65535]；`backlog > 0`；`maxFrameBytes ≥ 1024`
- `workerSize > 0`；`workerQueueCapacity ≥ 0`；`maxConnections > 0`
- `sessionReadTimeoutMs > 0`；`shutdownTimeout > 0`

提供 `BrokerConfig.defaults()` 与 `toBuilder()`（不可变对象修改后返回新实例，满足不可变原则）。

---

## 8. 对现有测试的影响（必须同步修改）

| 现有写法 | 问题 | 修改 |
|----------|------|------|
| `new Thread(() -> broker.start()).start()` | `start()` 改为非阻塞，线程立即结束 | 直接 `broker.start()` |
| `brokerThread.setUncaughtExceptionHandler` | 异常发生在内部 accept 线程 | 由 Broker 内部捕获并日志；测试改为断言 `getStatus()` / 端口 |
| `brokerThread.join()` | 无阻塞线程可 join | `broker.stop()` + `broker.awaitTermination(...)` |
| `new Broker(0)` | 保留便捷构造 | `BrokerConfig.builder().port(0).build()` 或继续用 `Broker(0)` |
| `broker.fetch("t")` 内部引用 | 返回快照 | 断言语义不变 |
| 测试连不上时轮询 `getPort()` | `start()` 返回时已 bind | 可保留轮询，无副作用 |
| `MqCmd.resolve(...).exec(broker)`（M0 后已删） | - | 改用 `HandlerRegistry.defaults().dispatch(cmd, brokerService)` |

测试基类建议：抽一个 `BrokerTestSupport`，负责启动 `Broker(0)`、等待端口、`@AfterEach` 停机，
后续 M3/M4 测试复用，避免每个测试重复 20 行样板。

---

## 9. 测试计划

### 9.1 store 单测

| 用例 | 断言 |
|------|------|
| append offsets | 同一 topic 从 0 严格递增；不同 topic 各自从 0 |
| fetch range | `fromOffset` / `maxCount` 边界；越界返回空 |
| snapshot isolation | 修改返回列表不影响 store；再次 fetch 数量不变 |
| latestOffset | 空 topic 返回 -1 |
| ack monotonic | 先 ack 5 再 ack 3，结果仍是 5 |
| ack key | 含冒号的 topic 与普通 topic 不串 |
| subscription idempotent | 重复订阅只有一个；退订后为空 |

### 9.2 并发测试

| 用例 | 说明 |
|------|------|
| `ConcurrentPublishTest` | 32 线程 × 50 条同 topic，断言总数 1600、每线程内部相对顺序正确 |
| `ConcurrentSubscribeAckTest` | 多 consumer 并发 subscribe/ack，无异常、ack 单调 |
| `SlowClientDoesNotBlockTest` | 一个 socket 连接后 3 秒不发数据，另一客户端 1 秒内完成 publish |
| `MaxConnectionsTest` | `maxConnections=2`，第 3 个连接收到 `SERVER_BUSY` 或被关闭 |
| `WorkerQueueFullTest` | 小队列 + 阻塞会话，触发拒绝后客户端收到 2001，服务仍可接受后续连接 |
| `IdleTimeoutTest` | 超过 `sessionReadTimeoutMs` 的连接被关闭，其他连接不受影响 |

### 9.3 生命周期测试

| 用例 | 断言 |
|------|------|
| start/stop/restart | stop 后端口可重新绑定；status 正确 |
| double start | 第二次抛 `IllegalStateException("Broker is already running")` |
| stop while idle | 1 秒内返回 |
| stop with in-flight | 请求完成或超时后关闭，`stop` 不超过 shutdownTimeout + 容差 |
| awaitTermination | 停机后立即返回 |

### 9.4 回归

- 0.0.1 的业务链路测试全部保留语义（发布下标、拉取顺序、订阅幂等、ack 单调、未知 topic 错误）。
- `mvn clean verify` 全绿。

---

## 10. 验收标准

- [ ] `Broker.start()` 返回时端口已绑定，不再阻塞
- [ ] 一个连接连上后不发送任何数据，不影响其他客户端在 1 秒内完成一次发布/拉取
- [ ] 32 线程并发发布 1600 条消息，无丢失、无重复 offset
- [ ] `maxConnections` / 队列满时返回 2001，服务不崩溃且能继续接客
- [ ] 空闲连接按 `sessionReadTimeoutMs` 被关闭
- [ ] `stop()` 幂等，停机后端口可立即重新绑定
- [ ] `fetch` 返回值修改不影响 Broker 内部状态
- [ ] `consumerId` / `topic` 含冒号时 ack 不串
- [ ] 业务代码（store/service/server）无 `synchronized` 包住 I/O
- [ ] Netty 接入只需新增 `BrokerServer` 实现，不改 handler/service/store

---

## 11. 风险与应对

| 风险 | 影响 | 应对 |
|------|------|------|
| 线程池参数不合理 | 高并发下拒绝或线程膨胀 | 固定大小 + 有界队列；参数进配置；压测验证 |
| 慢客户端占满线程 | 其他客户端排队 | 读超时 + 最大连接数 + 有界队列 |
| 停止时线程未退出 | 测试挂起 / 端口不释放 | `terminationLatch` + 强制关闭 activeSockets |
| `start()` 改为非阻塞破坏旧测试 | 编译/断言失败 | M2 内同步改测试基类，文档登记行为变化 |
| daemon 线程导致进程提前退出 | 示例程序跑不起来 | `Main` 显式 `awaitTermination`；文档写明 |
| 无界数据增长 | 内存 OOM | 0.0.2 明确不持久化、不淘汰；在文档中标注风险，后续版本引入容量上限 |
| `MqMsg` 可变被外部修改 | store 数据被篡改 | M4 改 record；M2 约定 handler 后不再修改并加 store 防御性拷贝 |

---

## 12. 与 0.0.1 的行为差异

| 差异 | 旧 | 新 |
|------|----|----|
| `start()` | 阻塞在 accept 循环 | 立即返回 |
| 并发模型 | 单线程串行 | accept + 线程池 |
| 空闲连接 | 永不超时 | 30s 默认超时 |
| 连接数 | 无上限 | 1000 默认上限 |
| 队列满 | 无此概念 | 返回 2001 |
| 停机 | 关闭监听 socket | 停止接客 + 等待 + 强制关闭 |
| `fetch` 返回 | 内部 List | 不可变快照 |
| ack key | 字符串拼接 | `AckKey` record |
