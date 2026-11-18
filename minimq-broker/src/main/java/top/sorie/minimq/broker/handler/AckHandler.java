package top.sorie.minimq.broker.handler;

import top.sorie.minimq.broker.BrokerContext;
import top.sorie.minimq.common.protocol.AckCmd;
import top.sorie.minimq.common.protocol.CmdResult;

/**
 * 处理确认指令：记录 consumer 在 topic 上的最大已确认下标。
 */
public final class AckHandler implements CmdHandler<AckCmd> {

    /** 当前协议尚未强制携带 consumerId 时的兜底标识，M1 会改为必填 */
    public static final String DEFAULT_CONSUMER = "c-default";

    @Override
    public Class<AckCmd> cmdType() {
        return AckCmd.class;
    }

    @Override
    public CmdResult handle(AckCmd cmd, BrokerContext ctx) {
        if (cmd.getTopic() == null || cmd.getMsgIndex() == null) {
            return new CmdResult(CmdResult.CODE_ERROR, "topic and msgIndex are required", null);
        }
        String consumer = cmd.getConsumerId() != null ? cmd.getConsumerId() : DEFAULT_CONSUMER;
        ctx.ack(consumer, cmd.getTopic(), cmd.getMsgIndex());
        return new CmdResult(CmdResult.CODE_SUCCESS, "success", null);
    }
}
