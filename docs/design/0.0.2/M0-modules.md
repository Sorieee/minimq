# M0 模块拆分与协议解耦设计说明

| 项目 | 内容 |
|------|------|
| 里程碑 | M0 |
| 目标 | 6 个 Maven 模块 + 协议 DTO 与 handler 解耦 |
| 前置 | 无 |
| 后续 | M1 协议增强 |
| 状态 | 设计完成，待实现 |
| 设计日期 | 2026-11-18 |

---

## 1. 目标与非目标

### 目标

1. 根工程改为 `packaging=pom` 的 parent，聚合 6 个模块。
2. 把协议报文类从 `broker.cmd` 迁到 `common.protocol`，并删除其中的 `exec(Broker)`。
3. 把指令执行逻辑迁到 `broker.handler`，用 `CmdHandler` + `HandlerRegistry` 分发。
4. 客户端（`Client` / `Producer` / `Consumer`）只依赖 `common` + `registry-api`，绝不依赖 `broker`。
5. 用 maven-enforcer 固化依赖方向，误加依赖时构建失败。
6. 现有 38 个测试全部迁移并通过，业务行为与 0.0.1 完全一致。

### 非目标

- 不引入线程池、不改变 accept 模型（M2）
- 不改变线上 JSON 字段（M1 才加 `version` / `requestId`）
- 不新增业务指令
- 不做存储接口抽象（M2）

---

## 2. 现状与问题

### 2.1 协议与处理器耦合

`src/main/java/top/sorie/minimq/broker/cmd/MqCmd.java:42`：

```java
public abstract CmdResult exec(Broker broker);
```

每个子类都 import `top.sorie.minimq.broker.Broker` 并在 `exec` 里写业务逻辑，例如
`broker/cmd/PublishCmd.java:23`、`broker/cmd/SubscribeCmd.java:16`、`broker/cmd/AckCmd.java:20`。

结果：协议报文类天然依赖服务端实现，无法下沉到 common。

### 2.2 客户端反向依赖服务端

- `src/main/java/top/sorie/minimq/common/Consumer.java:4-10`：import 了 6 个 `broker.cmd.*`
- `src/main/java/top/sorie/minimq/common/Producer.java:3-5`：import 了 `broker.cmd.*`
- `src/main/java/top/sorie/minimq/client/Client.java:5-6`：import 了 `MqCmd` 和 `CmdResult`

模块拆分后，这些依赖会直接违反「client 不依赖 broker」。

### 2.3 包归属不清晰

`Producer` / `Consumer` 放在 `common` 包里，但它们是客户端 API，不是公共协议。
`Client` 又单独在 `client` 包。拆分后应统一为 `top.sorie.minimq.client`。

### 2.4 其他已知问题（M0 不修，登记）

- `Producer` 无参构造不创建 `Client`，`sendMsg` 会 NPE（`common/Producer.java:22-26`）→ M1 用 `ClientConfig` 重做构造。
- `fetch` 返回内部 `List` 引用（`Broker.java:183-185`）→ M2 store 抽取时修。
- ack key 用字符串拼接（`Broker.java:189`）→ M2 改为值对象 key。

---

## 3. 目标模块结构

### 3.1 模块清单

| 模块 | 打包 | 职责 | 依赖 |
|------|------|------|------|
| `minimq-parent` | pom | 版本、依赖管理、插件、enforcer | - |
| `minimq-common` | jar | 协议 DTO、编解码、模型、错误码、配置、util | 仅第三方库 |
| `minimq-registry-api` | jar | 注册中心 DTO、路径常量、HTTP 客户端 | common |
| `minimq-broker` | jar | server / handler / store | common, registry-api |
| `minimq-client` | jar | Client / Producer / Consumer / 服务发现 | common, registry-api |
| `minimq-registry` | jar | 注册中心服务端 | common, registry-api |
| `minimq-example` | jar | 可运行 demo、跨模块 E2E | broker, client, registry |

> `minimq-registry-api` 在 M0 只建模块骨架（`pom.xml` + 空的 `src/main/java`），M3 再填内容。

### 3.2 依赖关系

```
                 ┌──────────────┐
                 │    common    │
                 └──────┬───────┘
                        │
                 ┌──────▼────────┐
                 │ registry-api  │
                 └───┬───────┬───┘
                     │       │
          ┌──────────▼─┐  ┌──▼─────────┐
          │   broker   │  │   client   │
          └────────────┘  └────────────┘
                     │       │
                 ┌───▼───────▼───┐
                 │    registry   │
                 └───────────────┘

example → broker + client + registry
```

### 3.3 目标包结构

```
minimq-common/src/main/java/top/sorie/minimq/common/
├── model/
│   └── MqMsg.java
├── protocol/
│   ├── MqCmd.java
│   ├── PublishCmd.java
│   ├── SubscribeCmd.java
│   ├── UnsubscribeCmd.java
│   ├── FetchMsgCmd.java
│   ├── AckCmd.java
│   ├── CmdFactory.java
│   └── CmdResult.java
└── util/
    └── IpUtil.java

minimq-broker/src/main/java/top/sorie/minimq/broker/
├── Broker.java
├── BrokerContext.java
└── handler/
    ├── CmdHandler.java
    ├── HandlerRegistry.java
    ├── PublishHandler.java
    ├── SubscribeHandler.java
    ├── UnsubscribeHandler.java
    ├── FetchHandler.java
    └── AckHandler.java

minimq-client/src/main/java/top/sorie/minimq/client/
├── Client.java
├── Producer.java
└── Consumer.java

minimq-registry/src/main/java/top/sorie/minimq/registry/
└── (M3 填充)

minimq-example/src/main/java/top/sorie/minimq/example/
└── (M0 可留空或放 DemoMain)
```

包名保持 `top.sorie.minimq.*` 不变，只调整子包；`Producer` / `Consumer` 从 `common` 迁到 `client`。

---

## 4. 协议 DTO / handler 解耦设计

### 4.1 改造前

```
Client ──> common.Consumer ──> broker.cmd.* (DTO + exec) ──> Broker
                                   ▲
                                   └── 同时是协议和业务
```

### 4.2 改造后

```
Client ──> client.Consumer ──> common.protocol.* (纯 DTO)
                                      │
Broker ──> HandlerRegistry ──> CmdHandler ──> BrokerContext (业务)
```

- 客户端只负责「构造 DTO → 序列化 → 发送」。
- Broker 只负责「反序列化 → 按 cmd 找 handler → 执行」。
- DTO 不带任何 `Broker` 依赖。

### 4.3 协议 DTO

`common/protocol/MqCmd.java`：删除 `exec`，删除 `Broker` import，只保留定界与序列化。

```java
package top.sorie.minimq.common.protocol;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang3.StringUtils;

@JsonIgnoreProperties(ignoreUnknown = true)
public abstract class MqCmd {
    public static final Integer CMD_PUBLISH = 1;
    public static final Integer CMD_SUBSCRIBE = 2;
    public static final Integer CMD_UNSUBSCRIBE = 3;
    public static final Integer CMD_FETCH_MSG = 4;
    public static final Integer CMD_ACK = 5;

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final String START_MARK = "<start>";
    private static final String END_MARK = "</end>";

    private Integer cmd;

    /** 仅做定界与反序列化；具体执行由 broker 侧 handler 负责 */
    public static MqCmd resolve(String msg) {
        if (StringUtils.isBlank(msg)) {
            throw new IllegalArgumentException("msg is blank");
        }
        String payload = msg.trim();
        if (payload.startsWith(START_MARK)) {
            payload = payload.substring(START_MARK.length());
        }
        if (payload.endsWith(END_MARK)) {
            payload = payload.substring(0, payload.length() - END_MARK.length());
        }
        return CmdFactory.create(payload);
    }

    public String toWireFormat() {
        try {
            return MAPPER.writeValueAsString(this) + "\n" + END_MARK;
        } catch (Exception e) {
            throw new IllegalStateException("serialize cmd error: " + e.getMessage(), e);
        }
    }

    public Integer getCmd() { return cmd; }
    public void setCmd(Integer cmd) { this.cmd = cmd; }
}
```

各子类只保留字段 + getter/setter + 便捷构造。例如 `PublishCmd`：

```java
package top.sorie.minimq.common.protocol;

@JsonIgnoreProperties(ignoreUnknown = true)
public class PublishCmd extends MqCmd {
    private MqMsg msg;

    public PublishCmd() {
        setCmd(CMD_PUBLISH);
    }

    public PublishCmd(String topic, String content) {
        setCmd(CMD_PUBLISH);
        this.msg = new MqMsg(topic, content);
    }

    public MqMsg getMsg() { return msg; }
    public void setMsg(MqMsg msg) { this.msg = msg; }
}
```

`CmdFactory` 原样迁到 `common.protocol`，只改 import，注册表仍是 `Map<Integer, Class<? extends MqCmd>>`。

### 4.4 Broker 侧 handler

`CmdHandler`（带类型，避免每个 handler 手写强制转换）：

```java
package top.sorie.minimq.broker.handler;

import top.sorie.minimq.broker.BrokerContext;
import top.sorie.minimq.common.protocol.CmdResult;
import top.sorie.minimq.common.protocol.MqCmd;

public interface CmdHandler<T extends MqCmd> {
    Class<T> cmdType();

    CmdResult handle(T cmd, BrokerContext ctx);
}
```

`HandlerRegistry`：

```java
package top.sorie.minimq.broker.handler;

import java.util.HashMap;
import java.util.Map;
import top.sorie.minimq.broker.BrokerContext;
import top.sorie.minimq.common.protocol.CmdResult;
import top.sorie.minimq.common.protocol.MqCmd;

public final class HandlerRegistry {
    private final Map<Integer, CmdHandler<?>> handlers = new HashMap<>();

    public <T extends MqCmd> void register(int cmdType, CmdHandler<T> handler) {
        if (handlers.putIfAbsent(cmdType, handler) != null) {
            throw new IllegalStateException("handler already registered: " + cmdType);
        }
    }

    public CmdResult dispatch(MqCmd cmd, BrokerContext ctx) {
        CmdHandler<?> handler = handlers.get(cmd.getCmd());
        if (handler == null) {
            return new CmdResult(CmdResult.CODE_ERROR, "unknown cmd type: " + cmd.getCmd(), null);
        }
        return dispatchTyped(handler, cmd, ctx);
    }

    @SuppressWarnings("unchecked")
    private <T extends MqCmd> CmdResult dispatchTyped(
            CmdHandler<T> handler, MqCmd cmd, BrokerContext ctx) {
        if (!handler.cmdType().isInstance(cmd)) {
            throw new IllegalArgumentException(
                    "cmd type mismatch: expect " + handler.cmdType().getSimpleName()
                            + " but got " + cmd.getClass().getSimpleName());
        }
        return handler.handle((T) cmd, ctx);
    }

    /** M0 的默认注册表；M3/M4 新增指令时在这里追加 */
    public static HandlerRegistry defaults() {
        HandlerRegistry registry = new HandlerRegistry();
        registry.register(MqCmd.CMD_PUBLISH, new PublishHandler());
        registry.register(MqCmd.CMD_SUBSCRIBE, new SubscribeHandler());
        registry.register(MqCmd.CMD_UNSUBSCRIBE, new UnsubscribeHandler());
        registry.register(MqCmd.CMD_FETCH_MSG, new FetchHandler());
        registry.register(MqCmd.CMD_ACK, new AckHandler());
        return registry;
    }
}
```

`PublishHandler` 示例（其余四个照搬原 `exec` 内容）：

```java
package top.sorie.minimq.broker.handler;

import top.sorie.minimq.broker.BrokerContext;
import top.sorie.minimq.common.protocol.CmdResult;
import top.sorie.minimq.common.protocol.PublishCmd;

public final class PublishHandler implements CmdHandler<PublishCmd> {
    @Override
    public Class<PublishCmd> cmdType() {
        return PublishCmd.class;
    }

    @Override
    public CmdResult handle(PublishCmd cmd, BrokerContext ctx) {
        if (cmd.getMsg() == null || cmd.getMsg().getTopic() == null) {
            return new CmdResult(CmdResult.CODE_ERROR, "msg and topic are required", null);
        }
        int index = ctx.publish(cmd.getMsg());
        return new CmdResult(CmdResult.CODE_SUCCESS, "success", index);
    }
}
```

### 4.5 BrokerContext

```java
package top.sorie.minimq.broker;

import java.util.List;
import top.sorie.minimq.common.model.MqMsg;

/** handler 可用的服务端能力；M0 由 Broker 实现，M2 迁到 BrokerService */
public interface BrokerContext {
    int publish(MqMsg msg);

    void subscribe(String topic, String consumerId);

    boolean unsubscribe(String topic, String consumerId);

    List<MqMsg> fetch(String topic);

    void ack(String consumerId, String topic, int msgIndex);

    Integer ackedIndexOf(String consumerId, String topic);
}
```

`Broker` 在 M0 声明 `implements BrokerContext`，方法体保持不变（业务行为零变化）。
`Broker` 增加一个 `HandlerRegistry` 字段，在构造时 `HandlerRegistry.defaults()`。

> 类型演进提示：M0/M1 的 `publish` 返回 `int`、`ack` 入参是 `int`；M2 会统一改为 `long`（线上仍是 JSON 数字，调用方无感），M4 的 fetch 分页也基于 `long` offset。实现 M0 时不需要提前改类型。

`Broker.dispatch` 改为：

```java
private String dispatch(String rawCmd, ObjectMapper mapper) {
    try {
        MqCmd cmd = MqCmd.resolve(rawCmd);
        CmdResult result = handlerRegistry.dispatch(cmd, this);
        return mapper.writeValueAsString(result);
    } catch (Exception e) {
        // 保持原有 500 兜底逻辑
        ...
    }
}
```

### 4.6 为什么不在 common 里保留 `exec`

如果保留 `exec(Broker)` 并让 common 依赖 broker，会形成 `common → broker` 反向依赖，client 又依赖 common，最终 client 仍然间接依赖 broker，模块化目标失败。因此 `exec` 必须离开 DTO。

---

## 5. pom 设计

### 5.1 parent pom

根 `pom.xml` 改为：

```xml
<project xmlns="http://maven.apache.org/POM/4.0.0"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 https://maven.apache.org/xsd/maven-4.0.0.xsd">
    <modelVersion>4.0.0</modelVersion>

    <groupId>top.sorie.minimq</groupId>
    <artifactId>minimq-parent</artifactId>
    <version>0.0.2-SNAPSHOT</version>
    <packaging>pom</packaging>

    <modules>
        <module>minimq-common</module>
        <module>minimq-registry-api</module>
        <module>minimq-broker</module>
        <module>minimq-client</module>
        <module>minimq-registry</module>
        <module>minimq-example</module>
    </modules>

    <properties>
        <maven.compiler.release>21</maven.compiler.release>
        <project.build.sourceEncoding>UTF-8</project.build.sourceEncoding>
        <jackson.version>2.18.3</jackson.version>
        <commons-lang3.version>3.17.0</commons-lang3.version>
        <commons-io.version>2.18.0</commons-io.version>
        <junit.version>5.10.2</junit.version>
        <slf4j.version>2.0.13</slf4j.version>
        <logback.version>1.5.6</logback.version>
        <jacoco.version>0.8.12</jacoco.version>
        <enforcer.version>3.5.0</enforcer.version>
        <surefire.version>3.2.5</surefire.version>
    </properties>

    <dependencyManagement>
        <dependencies>
            <dependency>
                <groupId>top.sorie.minimq</groupId>
                <artifactId>minimq-common</artifactId>
                <version>${project.version}</version>
            </dependency>
            <dependency>
                <groupId>top.sorie.minimq</groupId>
                <artifactId>minimq-registry-api</artifactId>
                <version>${project.version}</version>
            </dependency>
            <dependency>
                <groupId>top.sorie.minimq</groupId>
                <artifactId>minimq-broker</artifactId>
                <version>${project.version}</version>
            </dependency>
            <dependency>
                <groupId>top.sorie.minimq</groupId>
                <artifactId>minimq-client</artifactId>
                <version>${project.version}</version>
            </dependency>

            <dependency>
                <groupId>com.fasterxml.jackson.core</groupId>
                <artifactId>jackson-databind</artifactId>
                <version>${jackson.version}</version>
            </dependency>
            <dependency>
                <groupId>org.apache.commons</groupId>
                <artifactId>commons-lang3</artifactId>
                <version>${commons-lang3.version}</version>
            </dependency>
            <dependency>
                <groupId>commons-io</groupId>
                <artifactId>commons-io</artifactId>
                <version>${commons-io.version}</version>
            </dependency>
            <dependency>
                <groupId>org.slf4j</groupId>
                <artifactId>slf4j-api</artifactId>
                <version>${slf4j.version}</version>
            </dependency>
            <dependency>
                <groupId>ch.qos.logback</groupId>
                <artifactId>logback-classic</artifactId>
                <version>${logback.version}</version>
            </dependency>
            <dependency>
                <groupId>org.junit.jupiter</groupId>
                <artifactId>junit-jupiter</artifactId>
                <version>${junit.version}</version>
                <scope>test</scope>
            </dependency>
        </dependencies>
    </dependencyManagement>

    <build>
        <pluginManagement>
            <plugins>
                <plugin>
                    <groupId>org.apache.maven.plugins</groupId>
                    <artifactId>maven-surefire-plugin</artifactId>
                    <version>${surefire.version}</version>
                </plugin>
                <plugin>
                    <groupId>org.jacoco</groupId>
                    <artifactId>jacoco-maven-plugin</artifactId>
                    <version>${jacoco.version}</version>
                </plugin>
                <plugin>
                    <groupId>org.apache.maven.plugins</groupId>
                    <artifactId>maven-enforcer-plugin</artifactId>
                    <version>${enforcer.version}</version>
                </plugin>
            </plugins>
        </pluginManagement>

        <plugins>
            <plugin>
                <groupId>org.jacoco</groupId>
                <artifactId>jacoco-maven-plugin</artifactId>
                <executions>
                    <execution>
                        <id>prepare-agent</id>
                        <goals><goal>prepare-agent</goal></goals>
                    </execution>
                    <execution>
                        <id>report</id>
                        <phase>verify</phase>
                        <goals><goal>report</goal></goals>
                    </execution>
                </executions>
            </plugin>
        </plugins>
    </build>
</project>
```

要点：

- 子模块通过 `<parent>` 继承版本，不重复写 `<version>`。
- 用 `maven.compiler.release=21` 替代 source/target。
- JaCoCo 放在 parent 的 `<build><plugins>`，所有模块自动生效。
- 不要使用 `${revision}`（需要 flatten 插件，0.0.2 不值得引入）。

### 5.2 子模块 pom 模板

以 `minimq-client/pom.xml` 为例：

```xml
<project xmlns="http://maven.apache.org/POM/4.0.0"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 https://maven.apache.org/xsd/maven-4.0.0.xsd">
    <modelVersion>4.0.0</modelVersion>

    <parent>
        <groupId>top.sorie.minimq</groupId>
        <artifactId>minimq-parent</artifactId>
        <version>0.0.2-SNAPSHOT</version>
        <relativePath>../pom.xml</relativePath>
    </parent>

    <artifactId>minimq-client</artifactId>

    <dependencies>
        <dependency>
            <groupId>top.sorie.minimq</groupId>
            <artifactId>minimq-common</artifactId>
        </dependency>
        <dependency>
            <groupId>top.sorie.minimq</groupId>
            <artifactId>minimq-registry-api</artifactId>
        </dependency>
        <dependency>
            <groupId>org.junit.jupiter</groupId>
            <artifactId>junit-jupiter</artifactId>
            <scope>test</scope>
        </dependency>
    </dependencies>
</project>
```

其他模块：

- `minimq-common`：jackson-databind、commons-lang3、commons-io、slf4j-api、junit（test）
- `minimq-registry-api`：common、jackson-databind、slf4j-api、junit（test）
- `minimq-broker`：common、registry-api、slf4j-api、logback-classic（runtime）、junit（test）
- `minimq-registry`：common、registry-api、slf4j-api、logback-classic（runtime）、junit（test）
- `minimq-example`：broker、client、registry、logback-classic、junit（test，可选）

### 5.3 enforcer：禁止反向依赖

在 `minimq-client/pom.xml` 的 `<build>` 里：

```xml
<build>
    <plugins>
        <plugin>
            <groupId>org.apache.maven.plugins</groupId>
            <artifactId>maven-enforcer-plugin</artifactId>
            <executions>
                <execution>
                    <id>ban-broker-dependency</id>
                    <goals><goal>enforce</goal></goals>
                    <configuration>
                        <rules>
                            <bannedDependencies>
                                <includes>
                                    <include>top.sorie.minimq:minimq-broker</include>
                                </includes>
                                <searchTransitive>true</searchTransitive>
                            </bannedDependencies>
                        </rules>
                    </configuration>
                </execution>
            </executions>
        </plugin>
    </plugins>
</build>
```

在 `minimq-broker/pom.xml` 里对称禁止 `top.sorie.minimq:minimq-client`。

注意：`bannedDependencies` 的语义是「匹配 `excludes` 且不匹配 `includes` 的依赖被禁止」——
**`excludes` 是禁止名单，`includes` 是豁免名单**（写反了规则会永远通过，实测踩过）。
这里统一用 `<excludes>` 精确点名要禁止的坐标。
另外实测 Maven 3.9 下 `validate` 阶段依赖图尚未解析、规则拦不住，
因此两个模块的 enforcer 执行绑定到 **`verify`** 阶段（负向测试已验证可拦截）。

---

## 6. 文件迁移映射

### 6.1 主代码

| 当前路径 | 目标路径 | 改动 |
|----------|----------|------|
| `common/MqMsg.java` | `minimq-common/.../common/model/MqMsg.java` | 包名改 `common.model` |
| `common/util/IpUtil.java` | `minimq-common/.../common/util/IpUtil.java` | 不变 |
| `broker/cmd/MqCmd.java` | `minimq-common/.../common/protocol/MqCmd.java` | 删 `exec`、删 Broker import |
| `broker/cmd/PublishCmd.java` | `common/protocol/PublishCmd.java` | 删 `exec` |
| `broker/cmd/SubscribeCmd.java` | `common/protocol/SubscribeCmd.java` | 删 `exec` |
| `broker/cmd/UnsubscribeCmd.java` | `common/protocol/UnsubscribeCmd.java` | 删 `exec` |
| `broker/cmd/FetchMsgCmd.java` | `common/protocol/FetchMsgCmd.java` | 删 `exec` |
| `broker/cmd/AckCmd.java` | `common/protocol/AckCmd.java` | 删 `exec` |
| `broker/cmd/CmdFactory.java` | `common/protocol/CmdFactory.java` | 包名改 |
| `broker/cmd/result/CmdResult.java` | `common/protocol/CmdResult.java` | 包名改 |
| `broker/Broker.java` | `minimq-broker/.../broker/Broker.java` | 实现 `BrokerContext`，用 `HandlerRegistry` 分发 |
| `client/Client.java` | `minimq-client/.../client/Client.java` | import 改 `common.protocol` |
| `common/Producer.java` | `minimq-client/.../client/Producer.java` | 包名改 `client`，import 调整 |
| `common/Consumer.java` | `minimq-client/.../client/Consumer.java` | 包名改 `client`，import 调整 |
| `Main.java` | `minimq-example/.../example/DemoMain.java` | 重命名/搬迁 |

新增：

| 文件 | 说明 |
|------|------|
| `broker/BrokerContext.java` | handler 依赖的上下文接口 |
| `broker/handler/CmdHandler.java` | 指令处理器接口 |
| `broker/handler/HandlerRegistry.java` | 类型 → handler 注册表 |
| `broker/handler/PublishHandler.java` 等 5 个 | 从原 `exec` 搬来的逻辑 |

### 6.2 测试

| 当前测试 | 目标模块 | 备注 |
|----------|----------|------|
| `broker/BrokerCmdTest.java` | `minimq-broker` | 直接测 Broker |
| `broker/cmd/CmdFactoryTest.java` | `minimq-common` | 只测 DTO 工厂 |
| `broker/cmd/MqCmdTest.java` | `minimq-common` | 只测定界/序列化 |
| `client/ClientTest.java` | `minimq-client` | 起 Broker 需要依赖 broker，见下 |
| `common/ConsumerTest.java` | `minimq-client` | 同上 |
| `common/util/IpUtilTest.java` | `minimq-common` | 无变化 |
| `ProducerConsumerE2ETest.java` | `minimq-example` | 跨 broker + client |

**测试依赖的处理**：`ClientTest` / `ConsumerTest` 需要启动真实 Broker，但 `minimq-client` 不能依赖 `minimq-broker`（连 test scope 也不建议，否则 enforcer 会拦）。

两种做法，推荐第二种：

1. 把这两个测试移到 `minimq-example`。
2. 在 `minimq-client` 里用**测试替身**（一个极简的 `FakeBroker`，只实现 socket 收发和 JSON 应答），保持 client 模块自洽；真正的全链路测试放 `minimq-example`。

推荐方案 2：
- `minimq-client` 测试：`ClientTest`（用 `FakeBroker` 验证协议收发、超时、错误应答）、`ConsumerTest`（同）。
- `minimq-example` 测试：`ProducerConsumerE2ETest` + 新增「真实 Broker + 真实 Consumer」的集成测试。

如果时间紧，0.0.2 可以先用方案 1，M5 再补 client 模块自洽测试；但要在文档里登记。

---

## 7. 实施步骤（按顺序）

### 步骤 0：准备

```bash
git checkout dev-0.0.1
git pull
git checkout -b dev-0.0.2
```

### 步骤 1：建 parent 与模块骨架

1. 备份/移动现有 `src` 到临时目录（或直接用 `git mv`）。
2. 按第 5 节写根 `pom.xml`（`packaging=pom`）。
3. 创建 6 个模块目录，各写一个最小 `pom.xml`。
4. `minimq-example` 的 `pom.xml` 依赖 broker/client/registry。

此步结束时 `mvn -q validate` 应通过（还没有源码）。

### 步骤 2：迁移 common

1. `git mv src/main/java/top/sorie/minimq/common/MqMsg.java minimq-common/src/main/java/top/sorie/minimq/common/model/MqMsg.java`
2. `git mv src/main/java/top/sorie/minimq/common/util minimq-common/src/main/java/top/sorie/minimq/common/util`
3. 迁移协议类到 `common/protocol`，改包名，删除 `exec` 和 `Broker` import。
4. `CmdFactory` / `CmdResult` 跟着迁移。
5. `mvn -q -pl minimq-common -am test` 通过。

### 步骤 3：迁移 broker

1. `Broker.java` 搬到 `minimq-broker`。
2. 新建 `BrokerContext`、`CmdHandler`、`HandlerRegistry`、5 个 handler。
3. 把每个 `exec` 的方法体复制到对应 handler，参数从 `Broker broker` 改成 `BrokerContext ctx`。
4. `Broker implements BrokerContext`，加 `HandlerRegistry` 字段，`dispatch` 改为调用 registry。
5. 删除旧 `broker/cmd` 目录。
6. `mvn -q -pl minimq-broker -am test` 通过（先迁移 broker 测试）。

### 步骤 4：迁移 client

1. `Client.java` 搬到 `minimq-client`，import 改成 `common.protocol.*`。
2. `Producer` / `Consumer` 搬到 `top.sorie.minimq.client`，import 调整。
3. 迁移/改造 client 测试。
4. `mvn -q -pl minimq-client -am test` 通过。

### 步骤 5：example 与 E2E

1. 空 `Main` 搬到 `minimq-example`。
2. `ProducerConsumerE2ETest` 搬到 example，import 改为新包名。
3. `mvn -q clean verify` 全绿。

### 步骤 6：enforcer 与依赖检查

1. 在 client / broker 加 enforcer 规则。
2. 跑 `mvn -q verify`。
3. 临时在 client 里加一个 `minimq-broker` 依赖，确认构建失败，再删掉。

### 步骤 7：提交

建议提交拆分（每个都能编译最好；做不到就合并成一个）：

```
build: split project into multi-module layout
refactor(common): move protocol DTOs out of broker.cmd
refactor(broker): replace Cmd.exec with CmdHandler registry
refactor(client): move Producer/Consumer into client module
test: migrate tests into owning modules
build: enforce module dependency direction
```

---

## 8. 验收标准

全部满足才算 M0 完成：

- [ ] `mvn -q clean verify` 根目录一次通过
- [ ] 6 个模块都出现在 reactor 里，`minimq-registry-api` 有 pom
- [ ] `mvn -pl minimq-client dependency:tree` 输出中没有 `minimq-broker`
- [ ] `mvn -pl minimq-broker dependency:tree` 输出中没有 `minimq-client`
- [ ] `common` 里没有任何 `top.sorie.minimq.broker.*` import
- [ ] `consumer.subscribe/fetch/ack/unsubscribe` 与 `producer.sendMsg` 行为与 0.0.1 一致
- [ ] 原有 38 个用例全部通过（迁移后数量可增减，但断言语义保留）
- [ ] 临时给 client 加 broker 依赖时 enforcer 构建失败
- [ ] 协议线上 JSON 与 0.0.1 完全一致（可用测试固定一份样例报文）

---

## 9. 风险与回滚

| 风险 | 影响 | 应对 |
|------|------|------|
| 包名变更导致大范围 import 修改 | 编译错误多 | 按模块顺序迁移，每步 `-am` 编译 |
| Jackson 对改包后的类反序列化失败 | 运行期错误 | 类名不进 JSON，只有字段名重要；补一份协议往返测试 |
| client 测试需要真实 Broker | 依赖规则冲突 | 用 FakeBroker 或把集成测试移到 example |
| 一次性搬太多导致 review 困难 | 难以定位问题 | 按第 7 节顺序、按模块提交 |
| 老客户端连新 Broker | 兼容性 | M0 不改字段，保持完全兼容 |

回滚：M0 全部在 `dev-0.0.2` 分支，出问题直接切回 `dev-0.0.1`；模块拆分不涉及线上部署，成本低。

---

## 10. 自查清单

- [ ] 每个新增类 < 200 行，方法 < 50 行，嵌套 ≤ 4 层
- [ ] 没有 `System.out/err`（测试与 example 除外）
- [ ] 没有 hardcode 端口/超时（统一进配置，M1 完成）
- [ ] DTO 不含服务端字段
- [ ] 集合字段不直接暴露内部引用
- [ ] 所有新类有 JavaDoc，说明职责与线程安全语义
