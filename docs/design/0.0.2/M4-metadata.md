# M4 消息元数据与 Fetch 分页设计说明

| 项目 | 内容 |
|------|------|
| 里程碑 | M4 |
| 目标 | 消息模型补元数据；fetch 支持 offset/maxCount 分页；ack 用 offset |
| 前置 | M0、M1、M2 |
| 后续 | 0.0.3 推送/长轮询、延迟消息 |
| 状态 | 设计完成，待实现 |
| 设计日期 | 2026-11-18 |

---

## 1. 目标与非目标

### 目标

1. `MqMsg` 从 `topic + content` 扩展为可承载 msgId、tags、keys、headers、contentType、producedAt、producerId 等元数据的不可变模型。
2. Broker 侧用 `StoredMessage` 保存 `offset + storedAt + brokerId + msg`，客户端模型不掺服务端状态。
3. `fetch` 支持 `offset` + `maxCount` 分页，响应通过 `CmdResult.nextOffset` 告知下一批位置。
4. `ack` 支持按 offset 确认，同时兼容旧的 `msgIndex` 字段。
5. fetch 校验订阅关系（可配置），修掉"非订阅者也能拉"的问题。
6. 保持线上兼容：新增字段全部可选，`MqMsg` 允许未知字段，旧客户端仍能工作。

### 非目标

- 消息持久化
- 按 tags/keys 过滤、路由（只存储和透传）
- 延迟消息、TTL 到期删除（只保存字段，不执行语义）
- 优先级调度
- 消费组、rebalance、消息重投
- 基于 msgId 的幂等去重（只提供字段，去重后续版本）
- 推送模式

---

## 2. 客户端消息模型

### 2.1 MqMsg

`common/model/MqMsg.java` 改为不可变 record：

```java
package top.sorie.minimq.common.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;

import java.time.Instant;
import java.util.Map;
import java.util.Set;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
public record MqMsg(
        String msgId,                // 客户端可传；空则由 Broker 生成
        String topic,                // 必填
        Set<String> tags,            // 可空；去重、不可变
        Set<String> keys,            // 可空；业务键
        String content,              // 必填（可为空字符串）
        String contentType,          // 默认 text/plain
        String charset,              // 默认 UTF-8
        Instant producedAt,          // 客户端时间；空则由 Broker 补
        String producerId,           // 生产者标识
        Map<String, String> headers, // 自定义头
        Long ttlMs,                  // 预留：0.0.2 只存不执行
        Long delayMs,                // 预留：0.0.2 只存不执行
        Integer priority             // 预留：0.0.2 只存不执行
) {
    public static final String DEFAULT_CONTENT_TYPE = "text/plain";
    public static final String DEFAULT_CHARSET = "UTF-8";

    public MqMsg {
        // 防御性拷贝 + 不可变集合
        tags = tags == null ? Set.of() : Set.copyOf(tags);
        keys = keys == null ? Set.of() : Set.copyOf(keys);
        headers = headers == null ? Map.of() : Map.copyOf(headers);

        if (contentType == null || contentType.isBlank()) {
            contentType = DEFAULT_CONTENT_TYPE;
        }
        if (charset == null || charset.isBlank()) {
            charset = DEFAULT_CHARSET;
        }
    }

    public static MqMsg of(String topic, String content) {
        return builder().topic(topic).content(content).build();
    }

    public static Builder builder() {
        return new Builder();
    }

    /** 可变 builder；build() 时统一校验，避免 record 里塞业务规则 */
    public static final class Builder {
        private String msgId;
        private String topic;
        private Set<String> tags = Set.of();
        private Set<String> keys = Set.of();
        private String content;
        private String contentType = DEFAULT_CONTENT_TYPE;
        private String charset = DEFAULT_CHARSET;
        private Instant producedAt;
        private String producerId;
        private Map<String, String> headers = Map.of();
        private Long ttlMs;
        private Long delayMs;
        private Integer priority;

        public Builder msgId(String msgId) { this.msgId = msgId; return this; }
        public Builder topic(String topic) { this.topic = topic; return this; }
        public Builder tags(Set<String> tags) { this.tags = tags; return this; }
        public Builder keys(Set<String> keys) { this.keys = keys; return this; }
        public Builder content(String content) { this.content = content; return this; }
        public Builder contentType(String contentType) { this.contentType = contentType; return this; }
        public Builder charset(String charset) { this.charset = charset; return this; }
        public Builder producedAt(Instant producedAt) { this.producedAt = producedAt; return this; }
        public Builder producerId(String producerId) { this.producerId = producerId; return this; }
        public Builder headers(Map<String, String> headers) { this.headers = headers; return this; }
        public Builder ttlMs(Long ttlMs) { this.ttlMs = ttlMs; return this; }
        public Builder delayMs(Long delayMs) { this.delayMs = delayMs; return this; }
        public Builder priority(Integer priority) { this.priority = priority; return this; }

        public MqMsg build() {
            return new MqMsg(msgId, topic, tags, keys, content, contentType, charset,
                    producedAt, producerId, headers, ttlMs, delayMs, priority);
        }
    }
}
```

设计说明：

- **不依赖 Lombok**，保持项目零注解处理器依赖。
- `record` 的紧凑构造器负责集合拷贝；`builder()` 负责字段校验（见 2.2）。
- `@JsonIgnoreProperties(ignoreUnknown = true)` 是兼容关键：旧客户端/新客户端互相多出字段时不报错。
- `@JsonInclude(NON_NULL)` 让空字段不落盘，报文更小；空集合仍会输出 `[]`，如果希望省略空集合，可改用 `NON_EMPTY`。0.0.2 用 `NON_NULL`，行为更可预测。
- `producedAt` 用 `Instant`，Jackson 2.18 需要 `JavaTimeModule` 才能正确序列化为 ISO-8601；否则会输出对象。**在 common 提供一个共享 ObjectMapper 工厂**：

```java
public final class MqJson {
    private static final ObjectMapper MAPPER = new ObjectMapper()
            .registerModule(new JavaTimeModule())
            .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    public static ObjectMapper mapper() { return MAPPER; }
}
```

`Client`、`Broker`、`CmdFactory`、`CommandCodec` 统一用 `MqJson.mapper()`，不要各自 new。
`jackson-datatype-jsr310` 加入 parent 的 dependencyManagement 与 common 依赖。

### 2.2 校验规则

`MqMsg` 的业务校验放在 `Builder.build()` 与 `InputValidator`，**record 构造器只做结构校验**：

| 字段 | 规则 | 失败 |
|------|------|------|
| topic | 1–128，`[A-Za-z0-9._-]`，首字符字母/数字 | `IllegalArgumentException`（客户端）/ `INVALID_ARGUMENT`（服务端） |
| content | 非 null，UTF-8 字节数 ≤ 1 MiB | 同上 |
| msgId | 可空；非空时 ≤ 64 字符 | 同上 |
| tags | ≤ 16 个，单个 ≤ 64 字符 | 同上 |
| keys | ≤ 16 个，单个 ≤ 128 字符 | 同上 |
| headers | ≤ 32 个；key ≤ 64，value ≤ 1024 | 同上 |
| ttlMs / delayMs | 可空；非空时 ≥ 0 | 同上 |
| priority | 可空；非空时 0–9 | 同上 |

服务端必须独立校验，不能信任客户端。客户端校验只是为了 fail fast。

### 2.3 Broker 侧存储信封

M2 已引入 `StoredMessage`，M4 增加 `brokerId`：

```java
public record StoredMessage(
        long offset,
        Instant storedAt,
        String brokerId,
        MqMsg msg
) {
    public StoredMessage {
        if (offset < 0) throw new IllegalArgumentException("offset must be >= 0");
        if (storedAt == null) throw new IllegalArgumentException("storedAt is required");
        if (msg == null) throw new IllegalArgumentException("msg is required");
    }
}
```

写入时 Broker 补全：

- `msgId` 为空：生成 `UUID.randomUUID().toString()`。
- `producedAt` 为空：填 `Instant.now()`。
- `contentType` / `charset` 为空：由 `MqMsg` 紧凑构造器补默认值。
- `brokerId`：来自 `BrokerConfig.brokerId`（M3 引入；未配置时启动生成的 UUID）。

因此 `MqMsg` 在存储后是不可变的：Broker 用「补全后的新实例」替换原对象，不修改传入对象。

---

## 3. 协议改动

### 3.1 Publish

请求体新增可选字段，旧报文 `{"cmd":1,"msg":{"topic":"t","content":"c"}}` 仍然合法：

```json
{
  "cmd": 1,
  "requestId": "…",
  "msg": {
    "msgId": "m-1",
    "topic": "order",
    "tags": ["vip"],
    "keys": ["order-1001"],
    "content": "{\"id\":1001}",
    "contentType": "application/json",
    "charset": "UTF-8",
    "producedAt": "2026-11-18T10:00:00Z",
    "producerId": "p-1",
    "headers": {"traceId": "abc"},
    "ttlMs": 60000,
    "delayMs": 0,
    "priority": 5
  }
}
```

Broker 行为：

- 校验元数据上限。
- 补全 `msgId` / `producedAt`。
- 存储 `StoredMessage`。
- 应答 `data` 仍然返回该消息的 offset（数字）。

### 3.2 Fetch

`FetchMsgCmd` 新增可选字段：

```json
{"cmd":4,"topic":"order","offset":10,"maxCount":100}
```

规则：

| 字段 | 缺省 | 说明 |
|------|------|------|
| `offset` | 0 | 从该 offset 开始（含） |
| `maxCount` | null | 为空返回从 offset 起的全部消息，兼容 0.0.1；非空必须 1–1000 |

响应保持 `data` 是消息数组（元素仍是 `MqMsg` JSON），新增 `nextOffset`：

```json
{
  "code": 0,
  "message": "success",
  "requestId": "…",
  "data": [
    {"msgId":"m-1","topic":"order","content":"a","tags":[],"headers":{},"contentType":"text/plain","charset":"UTF-8","producedAt":"..."},
    {"msgId":"m-2","topic":"order","content":"b",...}
  ],
  "nextOffset": 12
}
```

`CmdResult` 增加：

```java
/** 仅 fetch 使用：下一批的起始 offset；其他指令为 null */
private Long nextOffset;
public Long getNextOffset() { ... }
public void setNextOffset(Long nextOffset) { ... }
```

设计取舍：

- **不把 data 改成 `{messages, nextOffset}` 对象**。那样会破坏 0.0.1 客户端对 `data` 是数组的假设。
- 把分页信息放在应答信封的顶层字段，旧客户端忽略未知字段，新客户端读取 `nextOffset`。
- 消息元素仍是 `MqMsg`，不引入 `offset` 字段；客户端用「本批起点 offset + 数组下标」计算单条 offset。因为存储 offset 连续，等价于 `offset + i`。
- `nextOffset = offset + data.size()`；没有更多消息时 `nextOffset == offset` 或等于最新 offset + 1，客户端以「返回数量 < maxCount」判断结束。

### 3.3 Ack

`AckCmd` 新增可选 `offset`，保留旧 `msgIndex`：

```json
{"cmd":5,"topic":"order","consumerId":"c-1","offset":11}
```

兼容规则：

| 请求 | 行为 |
|------|------|
| 有 `offset` | 按 offset ack |
| 无 `offset`，有 `msgIndex` | 按 `msgIndex` ack（等价于 offset） |
| 两者都有 | 以 `offset` 为准 |
| 都缺失 | `INVALID_ARGUMENT` |

`consumerId` 在 M1 已改为必填；旧客户端如果不带，会收到 1001。这一点在 0.0.2 的变更说明里标注。

### 3.4 订阅校验

`FetchHandler` 增加：

```java
if (config.strictFetch() && !ctx.isSubscribed(topic, consumerId)) {
    return CmdResult.error(ErrorCode.NOT_SUBSCRIBED);
}
```

- `FetchMsgCmd` 需要增加 `consumerId` 字段；旧客户端不带时：
  - `strictFetch=true`（默认）：返回 1001（缺少 consumerId）。
  - `strictFetch=false`：允许匿名拉取，保持旧行为。
- `BrokerConfig.strictFetch` 默认 **true**，并在 0.0.2 变更说明中明确这是行为收紧。
- 消费者侧 `Consumer.fetch` 始终带自己的 `consumerId`。

> 为什么默认收紧：0.0.1 的"非订阅者也能拉"是已知漏洞；学习项目里趁协议兼容窗口一次修掉，
> 比拖到后面再改更安全。需要兼容旧行为时可显式配置 `strictFetch=false`。

### 3.5 协议版本

M4 的所有改动都是**新增可选字段**，不改变既有字段语义，因此：

- `PROTOCOL_VERSION` 仍为 1，不递增。
- `MqMsg` 必须允许未知字段，`CmdResult` 必须允许未知字段（已经是）。
- `version` 缺失时按 1 处理。

---

## 4. Broker 侧改动

### 4.1 Publisher 处理链

`PublishHandler`：

```java
public CmdResult handle(PublishCmd cmd, BrokerContext ctx) {
    MqMsg msg = cmd.getMsg();
    InputValidator.requireTopic(msg.topic());
    InputValidator.validateMessage(msg);          // 元数据上限
    long offset = ctx.publish(msg);
    return CmdResult.ok(offset);
}
```

`BrokerService.publish`：

```java
public long publish(MqMsg msg) {
    MqMsg normalized = normalize(msg);             // 补 msgId / producedAt
    return messageStore.append(normalized.topic(), normalized);
}
```

`normalize` 返回新实例，不修改入参：

```java
private MqMsg normalize(MqMsg msg) {
    boolean needId = StringUtils.isBlank(msg.msgId());
    boolean needTime = msg.producedAt() == null;
    if (!needId && !needTime) {
        return msg;
    }
    return MqMsg.builder()
            .msgId(needId ? UUID.randomUUID().toString() : msg.msgId())
            .topic(msg.topic())
            .tags(msg.tags())
            .keys(msg.keys())
            .content(msg.content())
            .contentType(msg.contentType())
            .charset(msg.charset())
            .producedAt(needTime ? Instant.now() : msg.producedAt())
            .producerId(msg.producerId())
            .headers(msg.headers())
            .ttlMs(msg.ttlMs())
            .delayMs(msg.delayMs())
            .priority(msg.priority())
            .build();
}
```

### 4.2 Fetch 处理链

`FetchHandler`：

```java
public CmdResult handle(FetchMsgCmd cmd, BrokerContext ctx) {
    InputValidator.requireTopic(cmd.getTopic());
    long offset = cmd.getOffset() == null ? 0 : cmd.getOffset();
    int maxCount = cmd.getMaxCount() == null ? Integer.MAX_VALUE : cmd.getMaxCount();
    if (offset < 0 || maxCount <= 0 || maxCount > MAX_FETCH_COUNT) {
        return CmdResult.error(ErrorCode.INVALID_ARGUMENT);
    }
    if (strictFetch && !ctx.isSubscribed(cmd.getTopic(), cmd.getConsumerId())) {
        return CmdResult.error(ErrorCode.NOT_SUBSCRIBED);
    }
    List<StoredMessage> stored = ctx.fetch(cmd.getTopic(), offset, maxCount);
    if (stored == null) {
        return CmdResult.error(ErrorCode.TOPIC_NOT_FOUND);
    }
    List<MqMsg> data = stored.stream().map(StoredMessage::msg).toList();
    CmdResult result = CmdResult.ok(data);
    result.setNextOffset(offset + data.size());
    return result;
}
```

`BrokerContext` 增加：

```java
boolean isSubscribed(String topic, String consumerId);
List<StoredMessage> fetch(String topic, long offset, int maxCount);
```

旧的 `List<MqMsg> fetch(String topic)` 保留为便捷方法，内部调用 `fetch(topic, 0, Integer.MAX_VALUE)` 并映射。

### 4.3 Ack 处理链

```java
long offset = cmd.getOffset() != null
        ? cmd.getOffset()
        : (cmd.getMsgIndex() == null ? -1 : cmd.getMsgIndex());
if (offset < 0) {
    return CmdResult.error(ErrorCode.INVALID_ARGUMENT);
}
ctx.ack(cmd.getConsumerId(), cmd.getTopic(), offset);
return CmdResult.ok(null);
```

`AckStore` 已是 `long`（M2）。

### 4.4 存储影响

- `InMemoryMessageStore.append` 返回自增 offset，`StoredMessage.storedAt` 由 store 生成。
- `fetch(topic, offset, maxCount)` 返回 `List.copyOf(sublist)`，不暴露内部结构。
- 元数据随 `MqMsg` 原样存储，不做二次序列化，不丢字段。

---

## 5. 客户端改动

### 5.1 Producer

```java
public CmdResult send(MqMsg msg) {
    InputValidator.validateMessage(msg);      // fail fast
    return doSend(msg.topic(), new PublishCmd(msg));
}

public CmdResult sendMsg(String topic, String content) {
    return send(MqMsg.of(topic, content));
}

public CmdResult send(String topic, String content, Map<String, String> headers) {
    return send(MqMsg.builder().topic(topic).content(content).headers(headers).build());
}
```

- `producerId` 在构造时确定（`this.id`），发送时若 `msg.producerId()` 为空则用生产者 id 补齐。
- `sendMsg(topic, content)` 保持兼容。

### 5.2 Consumer

```java
public CmdResult fetch(String topic) {
    return fetch(topic, 0, Integer.MAX_VALUE);
}

public CmdResult fetch(String topic, long offset, int maxCount) {
    FetchMsgCmd cmd = new FetchMsgCmd();
    cmd.setCmd(CmdType.FETCH_MSG);
    cmd.setTopic(topic);
    cmd.setConsumerId(id);
    cmd.setOffset(offset);
    cmd.setMaxCount(maxCount);
    return client.sendCmd(cmd);
}

public CmdResult ack(String topic, long offset) { ... }
```

可选便捷方法（不强制）：

```java
public List<MqMsg> fetchMessages(String topic, long offset, int maxCount) {
    CmdResult result = fetch(topic, offset, maxCount);
    if (result.getCode() != ErrorCode.SUCCESS.getCode()) {
        throw new IllegalStateException(result.getMessage());
    }
    // 用 MqJson.mapper() 把 data 转成 List<MqMsg>
}
```

- `ack(String topic, int msgIndex)` 旧签名保留并标记 `@Deprecated`，内部调用 `ack(topic, (long) msgIndex)`。
- 新增 `ack(String topic, long offset)`。
- 订阅关系在客户端不缓存，仍由 Broker 维护。

### 5.3 ObjectMapper 统一

- `Client`、`Consumer`、`Producer`、`CmdFactory`、`CommandCodec`、`Broker` 全部改用 `MqJson.mapper()`。
- 必须注册 `JavaTimeModule`，否则 `Instant` 序列化为 `{"seconds":...}` 对象。
- `FAIL_ON_UNKNOWN_PROPERTIES=false` 是兼容基线，不要再单独 new 默认 ObjectMapper。

---

## 6. 兼容性说明

### 6.1 0.0.1 → 0.0.2

| 场景 | 结果 |
|------|------|
| 旧 Publish 报文（无元数据） | 正常发布，Broker 补 msgId/producedAt |
| 旧 Fetch 报文（无 offset/maxCount） | 从 0 返回全部消息，data 仍是数组 |
| 旧 Ack 报文（只有 msgIndex） | 正常 ack |
| 旧 Client 解析新应答信封（多 nextOffset/requestId） | `CmdResult` 忽略未知字段，正常 |
| 旧 Client 解析新消息（多 msgId/tags/headers） | **失败**：0.0.1 的 `MqMsg` 未忽略未知字段，`convertValue` 会抛异常 |
| 非订阅者 fetch | **行为收紧**：默认返回 1004（可用配置关闭） |

> 结论修正：0.0.2 只保证 **Broker 能读旧请求**，不保证旧客户端能读新消息。
> 由于 0.0.2 Broker 会给消息补 `msgId` / `producedAt`，任何 fetch 应答都会带新字段，
> 所以实际使用中 **客户端与 Broker 必须同版本**。这一点必须写进 README 与 Release。

### 6.2 新客户端 → 旧 Broker（0.0.1）

- 新客户端发送元数据字段：旧 Broker 的 `MqMsg` 默认 Jackson 遇到未知字段会失败（0.0.1 没加 `@JsonIgnoreProperties`）。
- 结论：**不支持新客户端连 0.0.1 Broker**；0.0.2 客户端与 Broker 同版本发布。
- 文档与 README 必须写明"客户端与 Broker 版本需匹配"。

---

## 7. 测试计划

### 7.1 模型单测

| 用例 | 断言 |
|------|------|
| `MqMsg.of` | contentType/charset 默认值；集合为空而非 null |
| builder 校验 | topic 非法/content 超限/headers 超限抛 `IllegalArgumentException` |
| 不可变 | 修改传入的 Set/Map 不影响 msg；`tags()` 不可写 |
| Jackson 往返 | 全字段 + 空字段；`Instant` 输出 ISO-8601 |
| 未知字段 | 多一个 `foo` 字段仍能反序列化 |
| 旧报文 | `{"topic":"t","content":"c"}` 反序列化成功 |
| msgId/producedAt 补全 | BrokerService.normalize 返回新实例，原对象不变 |

### 7.2 协议 / handler 测试

| 用例 | 断言 |
|------|------|
| Publish 带全部元数据 | fetch 回来的字段一致 |
| Fetch offset/maxCount | 5 条消息，`maxCount=2` 依次返回 2/2/1，nextOffset 2/4/5 |
| Fetch offset 越界 | 返回空数组，nextOffset = offset |
| Fetch maxCount 非法 | 1001 |
| Fetch 非订阅者 | 1004 |
| strictFetch=false | 非订阅者可拉取 |
| Ack offset | 进度更新 |
| Ack 仅 msgIndex | 依然生效 |
| Ack 都缺失 | 1001 |
| 未知 topic | 1003 |

### 7.3 E2E

| 用例 | 断言 |
|------|------|
| 元数据全链路 | Producer 带 headers/tags/keys → Consumer 拉取后逐字段相等 |
| 分页消费 | 分页拉取 + 逐页 ack，消费完 nextOffset 不再前进 |
| 旧报文兼容 | 手写 0.0.1 格式 publish/fetch/ack，全链路成功 |
| 注册中心 + 元数据 | M3 的发现链路下元数据仍完整 |

### 7.4 性能基线（可选）

- 1 KiB 消息、1000 条、单 producer 顺序发送，记录 p99 延迟，作为后续版本对比基线。
- 大 headers（32 个 1 KiB value）不触发异常，报文大小符合预期。

---

## 8. 验收标准

- [ ] `MqMsg` 是 record，集合字段不可变，构造/Builder 校验完整
- [ ] msgId、tags、keys、headers、contentType、charset、producedAt、producerId 能完整往返
- [ ] Broker 为缺失 msgId/producedAt 的消息补全，且不修改入参
- [ ] `fetch(offset, maxCount)` 分页正确，`nextOffset` 正确
- [ ] 不传 offset/maxCount 时行为与 0.0.1 一致（从 0 全量）
- [ ] `ack(offset)` 与旧 `msgIndex` 都可用，offset 优先
- [ ] 默认非订阅者 fetch 返回 1004
- [ ] 旧报文（无元数据）仍能发布/拉取/ack
- [ ] `Instant` 序列化为 ISO-8601，所有组件共用 `MqJson.mapper()`
- [ ] `StoredMessage` 含 offset/storedAt/brokerId，客户端模型不含服务端字段

---

## 9. 风险与应对

| 风险 | 影响 | 应对 |
|------|------|------|
| record + Jackson 注解兼容问题 | 反序列化失败 | 全字段往返测试 + 旧报文测试；必要时加 `@JsonCreator` |
| `Instant` 未注册 JavaTimeModule | 时间变成对象 | 统一 `MqJson.mapper()`，禁用 `WRITE_DATES_AS_TIMESTAMPS` |
| 默认 `strictFetch=true` 破坏旧行为 | 旧 consumer 拉不到 | 配置开关 + 变更说明 + E2E 覆盖两种模式 |
| `maxCount` 缺省为无穷 | 大 topic 一次返回过多 | 0.0.2 为兼容保留；新客户端必须传 maxCount；后续版本加服务端硬上限 |
| 元数据膨胀 | 报文/内存变大 | 字段数量与长度上限；headers 建议只放追踪信息 |
| 消息模型改动范围大 | 测试全红 | 先改模型与单测，再改 handler，最后 E2E；按步骤提交 |
| producerId 可伪造 | 审计不可信 | 0.0.2 无鉴权，仅作标识；后续版本结合连接身份 |

---

## 10. 实现顺序

1. `MqMsg` record + builder + 校验 + 单测。
2. `MqJson` 共享 ObjectMapper + JavaTimeModule，替换各处 new ObjectMapper。
3. `StoredMessage` 加 brokerId，store/service 适配。
4. Publish 链路补全 msgId/producedAt + 元数据测试。
5. FetchCmd 加 offset/maxCount，handler/service/store 适配，nextOffset。
6. Ack 加 offset，兼容 msgIndex。
7. strictFetch + 订阅校验。
8. Producer/Consumer 便捷方法 + 旧签名 `@Deprecated`。
9. E2E 与旧报文兼容测试。
