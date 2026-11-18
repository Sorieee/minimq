package top.sorie.minimq.broker.handler;

import top.sorie.minimq.broker.BrokerContext;
import top.sorie.minimq.common.protocol.CmdResult;
import top.sorie.minimq.common.protocol.SubscribeCmd;

/**
 * 处理订阅指令：topic 与 consumerId 均必填，重复订阅由 BrokerContext 保证幂等。
 */
public final class SubscribeHandler implements CmdHandler<SubscribeCmd> {

    @Override
    public Class<SubscribeCmd> cmdType() {
        return SubscribeCmd.class;
    }

    @Override
    public CmdResult handle(SubscribeCmd cmd, BrokerContext ctx) {
        if (cmd.getTopic() == null || cmd.getConsumerId() == null) {
            return new CmdResult(CmdResult.CODE_ERROR, "topic and consumerId are required", null);
        }
        ctx.subscribe(cmd.getTopic(), cmd.getConsumerId());
        return new CmdResult(CmdResult.CODE_SUCCESS, "success", null);
    }
}
