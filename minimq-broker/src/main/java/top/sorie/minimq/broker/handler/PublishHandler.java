package top.sorie.minimq.broker.handler;

import top.sorie.minimq.broker.BrokerContext;
import top.sorie.minimq.common.protocol.CmdResult;
import top.sorie.minimq.common.protocol.PublishCmd;

/**
 * 处理发布指令：校验消息后写入 topic，data 返回消息下标。
 */
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
