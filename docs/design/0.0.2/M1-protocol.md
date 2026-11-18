# M1 协议增强与横切底座设计说明

| 项目 | 内容 |
|------|------|
| 里程碑 | M1 |
| 目标 | 协议可演进（version/requestId）、编解码收口、错误码/校验/配置/日志成型 |
| 前置 | M0（模块拆分与 DTO/handler 解耦） |
| 后续 | M2 Broker 并发化、M3 注册中心 |
| 状态 | 设计完成，待实现 |
| 设计日期 | 2026-11-18 |

---

## 1. 目标与非目标

### 目标

1. `cmd` 从裸 `Integer` 升级为 `CmdType` 枚举，线上仍是数字，类型安全。
2. 报文增加可选 `version` 与 `requestId`，为后续协议演进和链路排查做准备。
3. 编解码收口到 `common.protocol.CommandCodec` / `CommandDecoder`，Broker 不再自己拼 `</end>`。
4. 统一 `ErrorCode`，`CmdResult` 提供工厂方法，业务代码不再手写 `-1`。
5. 建立 `BrokerConfig` / `ClientConfig` 不可变配置，修掉 `Producer` 无参构造 NPE。
6. 入参校验集中化：topic、consumerId、消息大小、协议版本。
7. 引入 slf4j + logback，清掉业务代码里的 `System.out/err`。

### 非目标

- 不改传输模型（仍为短连接，M2 才加线程池）
- 不做消息元数据（M4）
- 不改 fetch 的语义与返回值（M4 才加分页）
- 不接注册中心（M3）

---

## 2. 协议设计

### 2.1 线格式

保持不变：

```
{"cmd":1,"msg":{"topic":"order","content":"hello"}}
</end>
```

- 一问一答，每条指令以独立行 `</end>` 结束。
- 请求与应答都是单行 JSON。
- `<start>` 前缀仍然兼容（0.0.1 的 `resolve` 支持，保留）。

### 2.2 CmdType 枚举

`common/protocol/CmdType.java`：

```java
package top.sorie.minimq.common.protocol;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

/** 指令类型；线上序列化为数字，保持与 0.0.1 兼容 */
public enum CmdType {
    PUBLISH(1),
    SUBSCRIBE(2),
    UNSUBSCRIBE(3),
    FETCH_MSG(4),
    ACK(5);

    private final int code;

    CmdType(int code) {
        this.code = code;
    }

    @JsonValue
    public int code() {
        return code;
    }

    @JsonCreator
    public static CmdType fromCode(int code) {
        for (CmdType type : values()) {
            if (type.code == code) {
                return type;
            }
        }
        throw new IllegalArgumentException("unknown cmd type: " + code);
    }
}
```

`MqCmd` 的字段改为：

```java
private CmdType cmd;
```

`CmdFactory` 的注册表改为 `EnumMap<CmdType, Class<? extends MqCmd>>`，读取 JSON 时：

```java
JsonNode cmdNode = root.get("cmd");
if (cmdNode == null || !cmdNode.canConvertToInt()) {
    throw new InvalidCmdException(...);
}
CmdType type = CmdType.fromCode(cmdNode.asInt());
```

兼容性检查：

- 序列化结果仍是 `"cmd":1`，不是 `"cmd":"PUBLISH"`。
- 测试里必须固定一条样例报文，断言序列化字符串（字段顺序可用 `ObjectMapper` 的 `ORDER_MAP_ENTRIES_BY_KEYS` 或直接比较 JSON 树）。

### 2.3 version

`MqCmd` 增加：

```java
public static final int PROTOCOL_VERSION = 1;

/** 报文协议版本，缺省视为 1 */
private Integer version = PROTOCOL_VERSION;
```

规则：

| 情况 | Broker 行为 |
|------|-------------|
| `version` 缺失 | 视为 1，正常处理 |
| `version == 1` | 正常处理 |
| `version > PROTOCOL_VERSION` | 返回 `UNSUPPORTED_VERSION`（1005），不执行 |
| `version <= 0` | 返回 `INVALID_ARGUMENT`（1001） |

只有在**破坏性**协议变更时才递增 `PROTOCOL_VERSION`；0.0.2 的加字段不递增。

### 2.4 requestId

`MqCmd` 增加：

```java
/** 客户端生成的请求标识，用于日志串联；旧客户端可为空 */
private String requestId;
```

规则：

- `Client.sendCmd` 在序列化前，如果 `cmd.getRequestId()` 为空，生成 `UUID.randomUUID().toString()`。
- Broker `dispatch` 执行完把 `requestId` 回填到 `CmdResult`。
- Broker 用 `MDC.put("requestId", ...)` 包住一次 dispatch，日志 pattern 里输出；`finally` 移除。
- `requestId` 只用于观测，不参与幂等（0.0.2 不做去重）。

`CmdResult` 增加对应字段：

```java
private String requestId;
public String getRequestId() { ... }
public void setRequestId(String requestId) { ... }
```

### 2.5 编解码收口

新增 `common/protocol/ProtocolConstants.java`：

```java
public final class ProtocolConstants {
    public static final String START_MARK = "<start>";
    public static final String END_MARK = "</end>";
    public static final int PROTOCOL_VERSION = 1;
    public static final int DEFAULT_MAX_FRAME_BYTES = 1024 * 1024; // 1 MiB
    private ProtocolConstants() {}
}
```

`CommandCodec`：

```java
public final class CommandCodec {
    private static final ObjectMapper MAPPER = new ObjectMapper();

    public static String encode(MqCmd cmd) {
        try {
            return MAPPER.writeValueAsString(cmd) + "\n" + ProtocolConstants.END_MARK;
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("serialize cmd error", e);
        }
    }

    public static MqCmd decode(String frame) {
        // 去空白、剥离 <start>/</end>，再交给 CmdFactory
    }
}
```

`MqCmd.toWireFormat()` / `MqCmd.resolve()` 保留为兼容入口，内部委托 `CommandCodec`，避免一次性改动所有调用方。

`CommandDecoder`（Broker 会话用，M2 会直接复用）：

```java
/** 增量式按行解码：喂一行，遇到 </end> 返回一条完整报文 */
public final class CommandDecoder {
    private final int maxFrameBytes;
    private final StringBuilder buffer = new StringBuilder();
    private int size;

    public CommandDecoder(int maxFrameBytes) { ... }

    /** @return 完整报文（不含 </end>），尚未结束返回 null */
    public String feed(String line) {
        // 累加前先按 UTF-8 字节数判断是否超限
        // 超限抛 FrameTooLargeException（或返回错误标记）
    }

    public void reset() { buffer.setLength(0); size = 0; }
}
```

要求：

- 大小限制按 **UTF-8 字节数**计算，不按字符数，避免中文消息绕过限制。
- 超限时 Broker 返回 `INVALID_ARGUMENT` 并关闭该连接（不能继续读，否则内存被打爆）。
- 会话异常结束时调用 `reset()`，避免半条指令污染下一个连接（0.0.1 的 `msg.setLength(0)` 逻辑迁到这里）。

### 2.6 错误码

`common/error/ErrorCode.java`：

```java
public enum ErrorCode {
    SUCCESS(0, "success"),
    INVALID_ARGUMENT(1001, "invalid argument"),
    UNKNOWN_CMD(1002, "unknown cmd type"),
    TOPIC_NOT_FOUND(1003, "topic not found"),
    NOT_SUBSCRIBED(1004, "not subscribed"),
    UNSUPPORTED_VERSION(1005, "unsupported protocol version"),
    INTERNAL_ERROR(2000, "internal server error"),
    SERVER_BUSY(2001, "server busy");

    private final int code;
    private final String message;
    // getters
}
```

`CmdResult`：

```java
public static CmdResult ok(Object data) {
    return new CmdResult(ErrorCode.SUCCESS.getCode(), "success", data);
}

public static CmdResult error(ErrorCode error) {
    return new CmdResult(error.getCode(), error.getMessage(), null);
}

public static CmdResult error(ErrorCode error, String detail) {
    return new CmdResult(error.getCode(), error.getMessage() + ": " + detail, null);
}
```

保留：

```java
public static final int CODE_SUCCESS = 0;

/** @deprecated 使用 ErrorCode；仅为兼容 0.0.1 测试保留一个版本 */
@Deprecated
public static final int CODE_ERROR = -1;
```

迁移要求：

- 新增代码只能 `CmdResult.ok` / `CmdResult.error`。
- 旧测试中的 `CODE_ERROR` 断言在 M1 一并改成具体错误码。
- `Broker.dispatch` 的兜底 500 改为 `error(INTERNAL_ERROR)`（code 2000，不再是 500）。

### 2.7 入参校验

新增 `common/validation/InputValidator.java`（或 `TopicValidator`，二选一，不要两套）：

```java
public final class InputValidator {
    private static final Pattern TOPIC_PATTERN = Pattern.compile("^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$");
    private static final int MAX_CONSUMER_ID_LENGTH = 128;

    public static void requireTopic(String topic) { ... }
    public static void requireConsumerId(String consumerId) { ... }
    public static void requireNotBlank(String value, String field) { ... }
    public static int utf8Length(String value) { ... }
}
```

规则：

- topic：1–128 字符，`[A-Za-z0-9._-]`，首字符必须是字母或数字。
- consumerId：1–128 字符，非空白。
- content：UTF-8 字节数 ≤ `maxFrameBytes - 协议开销`；实际由 `CommandDecoder` 兜底，handler 里再校验业务上限（默认 1 MiB）。
- 客户端在发送前也校验一次，避免无谓的网络往返（fail fast）。
- 校验失败统一 `INVALID_ARGUMENT`。

注意：0.0.1 的 `SubscribeCmd` / `UnsubscribeCmd` 在 topic 或 consumerId 为 null 时返回 `-1`；迁移后返回 `1001`，测试断言同步改。

---

## 3. 配置设计

### 3.1 ClientConfig

不可变类 + builder，构造时校验：

```java
public final class ClientConfig {
    private final String host;                 // 直连模式必填
    private final int port;
    private final List<String> registryUrls;   // M3 使用；0.0.2 可为空
    private final int connectTimeoutMs;        // 默认 2000
    private final int readTimeoutMs;           // 默认 10000
    private final int maxFrameBytes;           // 默认 1 MiB

    public static Builder builder() { ... }
    public static ClientConfig direct(String host, int port) { ... }
}
```

规则：

- `direct` 与 `registryUrls` 至少有一个有效。
- 超时、大小必须 > 0。
- 对外暴露 `List<String>` 时返回 `List.copyOf`。

### 3.2 BrokerConfig

M1 只放与协议/会话相关的字段，M2 再扩展线程池字段：

```java
public final class BrokerConfig {
    private final int port;                    // 0 = 系统分配
    private final int backlog;                 // 默认 128
    private final int maxFrameBytes;           // 默认 1 MiB
    private final int sessionReadTimeoutMs;    // 默认 30000

    public static Builder builder() { ... }
    public static BrokerConfig defaults() { ... }
}
```

`Broker` 构造改为 `Broker(BrokerConfig config)`，保留 `Broker()` 和 `Broker(int port)` 作为便捷入口（内部构造默认配置），减少测试改动。

### 3.3 Producer / Consumer 构造重构

当前问题：`Producer()` 不创建 `Client`，`sendMsg` 必 NPE（`common/Producer.java:22-26`）。

改造：

```java
public class Producer {
    private final String id;
    private final String name;
    private final String localIp;
    private final Client client;

    public Producer(String id, String name, String localIp, ClientConfig config) { ... }

    public Producer(ClientConfig config) {
        this(UUID.randomUUID().toString(), null, IpUtil.getLocalIp(), config);
    }

    @Deprecated
    public Producer(String id, String name, String localIp, String remoteIp, Integer port) {
        this(id, name, localIp, ClientConfig.direct(remoteIp, port));
    }
}
```

- **删除无参构造**：让"没配置地址"从运行期 NPE 变成编译期错误。
- 旧构造保留一个版本，标注 `@Deprecated`，内部委托新构造。
- 字段全部 `final`（不可变风格）。

`Consumer` 同理：

```java
public Consumer(String id, String name, String localIp, ClientConfig config)
public Consumer(String id, String name, ClientConfig config)  // localIp 取本机
public Consumer(ClientConfig config)                          // id/name 自动生成
```

旧的 `(..., String remoteIp, Integer port)` 构造保留并标记 `@Deprecated`。

`Client` 构造改为 `Client(ClientConfig)`，旧 `Client(String, Integer)` 保留并委托。

### 3.4 默认值集中管理

`common/config/MqDefaults.java` 或各配置类的常量：

```java
public final class MqDefaults {
    public static final int BROKER_PORT = 19900;
    public static final int BACKLOG = 128;
    public static final int CONNECT_TIMEOUT_MS = 2_000;
    public static final int READ_TIMEOUT_MS = 10_000;
    public static final int MAX_FRAME_BYTES = 1024 * 1024;
    public static final int SESSION_READ_TIMEOUT_MS = 30_000;
    private MqDefaults() {}
}
```

---

## 4. 日志设计

### 4.1 依赖与配置

parent 的 `dependencyManagement` 已管理 slf4j/logback（见 M0 文档 5.1）。
`minimq-broker` 与 `minimq-registry` 加 `logback-classic`（runtime scope），并各自提供 `src/main/resources/logback.xml`：

```xml
<configuration>
    <appender name="CONSOLE" class="ch.qos.logback.core.ConsoleAppender">
        <encoder>
            <pattern>%d{HH:mm:ss.SSS} %-5level [%thread] %logger{36} [%X{requestId}] - %msg%n</pattern>
        </encoder>
    </appender>
    <root level="INFO">
        <appender-ref ref="CONSOLE"/>
    </root>
</configuration>
```

### 4.2 日志约定

| 场景 | 级别 | 内容 |
|------|------|------|
| Broker 启动/停止、端口 | INFO | `broker started on port=...` |
| 收到/回复指令 | DEBUG | `cmd=... requestId=... bytes=...` |
| 参数错误、未知 topic | WARN | 不打印完整 payload |
| 客户端断开 | DEBUG | 连接地址 + 原因 |
| I/O 异常、内部异常 | ERROR | 带堆栈 |
| 协议内容 | DEBUG | 截断到 256 字符 |

实现细节：

- 每个类使用 `private static final Logger LOG = LoggerFactory.getLogger(Xxx.class);`
- Broker dispatch 中用 `MDC.put("requestId", requestId)`，`finally` 里 `MDC.remove`。
- 删除业务代码中所有 `System.out.println` / `System.err.println`。
- 测试里允许 `System.out`（尽量也换成日志）。

---

## 5. 变更文件清单

| 模块 | 文件 | 动作 |
|------|------|------|
| common | `protocol/CmdType.java` | 新增 |
| common | `protocol/ProtocolConstants.java` | 新增 |
| common | `protocol/CommandCodec.java` | 新增 |
| common | `protocol/CommandDecoder.java` | 新增 |
| common | `protocol/MqCmd.java` | 改字段类型、加 version/requestId、委托 codec |
| common | `protocol/CmdResult.java` | 加 requestId、错误码工厂 |
| common | `protocol/CmdFactory.java` | 注册表改 `EnumMap<CmdType, ...>` |
| common | `error/ErrorCode.java` | 新增 |
| common | `validation/InputValidator.java` | 新增 |
| common | `config/MqDefaults.java` | 新增 |
| common | `config/ClientConfig.java` | 新增 |
| common | `config/BrokerConfig.java` | 新增（M2 扩展） |
| broker | `Broker.java` | 构造用 BrokerConfig、用 CommandDecoder、requestId 回填、日志 |
| broker | `handler/*Handler.java` | 用 ErrorCode / InputValidator |
| client | `Client.java` | 用 ClientConfig、requestId、日志 |
| client | `Producer.java` | 构造重构、删无参构造、字段 final |
| client | `Consumer.java` | 构造重构、字段 final |
| broker | `resources/logback.xml` | 新增 |
| registry | `resources/logback.xml` | 新增（M3 使用） |
| 各模块 | 测试 | 错误码断言更新、codec/校验/config 新增用例 |

---

## 6. 测试计划

### 6.1 单元测试

| 用例 | 断言 |
|------|------|
| `CmdTypeTest` | 1–5 双向映射；未知 code 抛异常 |
| `MqCmdCodecTest` | 5 种指令 encode→decode 往返字段一致 |
| `MqCmdCodecTest` | 未知字段被忽略；缺失 version 视为 1 |
| `CommandDecoderTest` | 多行分片、正常结束、超限抛/返回错误、reset 后干净 |
| `InputValidatorTest` | topic 边界：1/128 字符、非法字符、null、空白 |
| `ClientConfigTest` | 缺 host/port、负超时、空 registry 列表均报错；集合不可变 |
| `BrokerConfigTest` | 端口 0 合法；负值非法 |
| `CmdResultTest` | `ok/error` 的 code 与 message；requestId 序列化 |

### 6.2 集成测试

| 用例 | 断言 |
|------|------|
| `ClientTest` | requestId 自动生成；Broker 应答回填同一 requestId |
| `ClientTest` | 超时/连接拒绝仍返回 `CmdResult`，不抛异常 |
| `BrokerCmdTest` | 参数缺失返回 1001；未知 cmd 返回 1002；未知 topic 返回 1003 |
| `ProducerConsumerE2ETest` | 业务链路行为与 0.0.1 一致 |
| `BrokerVersionTest` | `version=2` 返回 1005；`version=0` 返回 1001 |

### 6.3 兼容性测试

固定一份 0.0.1 报文样例（不含 version/requestId），断言：

- 0.0.2 Broker 能执行。
- 0.0.2 应答能被 0.0.1 的 `CmdResult` 反序列化（多出的 requestId 字段忽略）。

---

## 7. 验收标准

- [ ] `cmd` 线上仍然是数字 1–5，样例报文与 0.0.1 一致
- [ ] `requestId` 能从客户端传到 Broker 并回填到应答
- [ ] Broker 收到 `version=2` 时返回 1005，不执行指令
- [ ] 超过 `maxFrameBytes` 的报文被拒绝，连接关闭，内存不增长
- [ ] 业务代码再无 `System.out/err`
- [ ] `Producer` 无参构造已删除；`new Producer(config)` 可正常发送
- [ ] 旧构造（带 ip/port）仍可用，标注 `@Deprecated`
- [ ] 所有错误响应使用 `ErrorCode`，不再出现裸 `-1`
- [ ] `mvn clean verify` 全绿，新增测试覆盖 codec / 校验 / 配置

---

## 8. 风险与应对

| 风险 | 应对 |
|------|------|
| `CmdType` 枚举改变 Jackson 序列化格式 | 用 `@JsonValue`/`@JsonCreator`，加样例报文测试 |
| 错误码从 -1 改为 100x 破坏旧测试 | M1 内一次性改断言；`CODE_ERROR` 保留但不再使用 |
| 删除 `Producer()` 导致示例/测试编译失败 | 同步迁移到 `ClientConfig`，编译期暴露问题是有意为之 |
| MDC 使用不当造成线程池串号 | M2 引入线程池后，dispatch 的 finally 必须 `MDC.remove` |
| 解码器按字符计数导致中文绕过限制 | 明确按 UTF-8 字节数计算，并加中文超限测试 |

---

## 9. 与 0.0.1 的行为差异（需知会）

| 差异 | 旧行为 | 新行为 | 原因 |
|------|--------|--------|------|
| 参数错误码 | `-1` | `1001` | 错误分类 |
| 未知 topic | `-1` | `1003` | 同上 |
| 内部异常 | `500` | `2000` | 与服务端 HTTP 语义解耦 |
| `Producer()` | 可构造，`sendMsg` NPE | 编译期不可用 | 消除运行期地雷 |
| 超大报文 | 无限制 | 拒绝并断开 | 资源保护 |
