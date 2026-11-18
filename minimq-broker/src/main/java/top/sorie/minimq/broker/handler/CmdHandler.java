package top.sorie.minimq.broker.handler;

import top.sorie.minimq.broker.BrokerContext;
import top.sorie.minimq.common.protocol.CmdResult;
import top.sorie.minimq.common.protocol.MqCmd;

/**
 * 单条指令的处理器。
 *
 * <p>泛型参数让每个实现只处理一种协议报文；{@link HandlerRegistry} 在分发前做类型校验，
 * 避免实现类里手写强制转换。</p>
 *
 * @param <T> 该处理器负责的协议报文类型
 */
public interface CmdHandler<T extends MqCmd> {

    Class<T> cmdType();

    CmdResult handle(T cmd, BrokerContext ctx);
}
