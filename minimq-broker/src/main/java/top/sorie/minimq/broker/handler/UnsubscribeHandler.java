package top.sorie.minimq.broker.handler;

import top.sorie.minimq.broker.BrokerContext;
import top.sorie.minimq.common.protocol.CmdResult;
import top.sorie.minimq.common.protocol.UnsubscribeCmd;

/**
 * 处理退订指令：未订阅过时返回错误应答。
 */
public final class UnsubscribeHandler implements CmdHandler<UnsubscribeCmd> {

    @Override
    public Class<UnsubscribeCmd> cmdType() {
        return UnsubscribeCmd.class;
    }

    @Override
    public CmdResult handle(UnsubscribeCmd cmd, BrokerContext ctx) {
        if (cmd.getTopic() == null || cmd.getConsumerId() == null) {
            return new CmdResult(CmdResult.CODE_ERROR, "topic and consumerId are required", null);
        }
        boolean removed = ctx.unsubscribe(cmd.getTopic(), cmd.getConsumerId());
        if (!removed) {
            return new CmdResult(CmdResult.CODE_ERROR, "subscription not found", null);
        }
        return new CmdResult(CmdResult.CODE_SUCCESS, "success", null);
    }
}
