package top.sorie.minimq.broker.handler;

import top.sorie.minimq.broker.BrokerContext;
import top.sorie.minimq.common.protocol.CmdResult;
import top.sorie.minimq.common.protocol.FetchMsgCmd;

import java.util.List;

/**
 * 处理拉取指令：topic 不存在时返回错误应答，避免与"没有消息"混淆。
 */
public final class FetchHandler implements CmdHandler<FetchMsgCmd> {

    @Override
    public Class<FetchMsgCmd> cmdType() {
        return FetchMsgCmd.class;
    }

    @Override
    public CmdResult handle(FetchMsgCmd cmd, BrokerContext ctx) {
        if (cmd.getTopic() == null) {
            return new CmdResult(CmdResult.CODE_ERROR, "topic is required", null);
        }
        List<?> msgs = ctx.fetch(cmd.getTopic());
        if (msgs == null) {
            return new CmdResult(CmdResult.CODE_ERROR, "topic not found: " + cmd.getTopic(), null);
        }
        return new CmdResult(CmdResult.CODE_SUCCESS, "success", msgs);
    }
}
