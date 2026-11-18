package top.sorie.minimq.broker.handler;

import top.sorie.minimq.broker.BrokerContext;
import top.sorie.minimq.common.protocol.CmdResult;
import top.sorie.minimq.common.protocol.MqCmd;

import java.util.HashMap;
import java.util.Map;

/**
 * 指令类型到处理器的注册表。
 *
 * <p>协议解析在 common 模块完成，本类只负责按 cmd 类型找到对应 handler 并执行。
 * 新增指令时在 {@link #defaults()} 中追加注册即可，Broker 无需改动。</p>
 */
public final class HandlerRegistry {

    private final Map<Integer, CmdHandler<?>> handlers = new HashMap<>();

    public <T extends MqCmd> void register(int cmdType, CmdHandler<T> handler) {
        if (handlers.putIfAbsent(cmdType, handler) != null) {
            throw new IllegalStateException("handler already registered: " + cmdType);
        }
    }

    /** 执行一条已解析的指令；未知类型返回错误应答而不是抛异常 */
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

    /** 0.0.2 的默认注册表；新增指令类型时在这里追加 */
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
