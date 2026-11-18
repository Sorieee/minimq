package top.sorie.minimq.broker.cmd;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import top.sorie.minimq.broker.Broker;
import top.sorie.minimq.broker.cmd.result.CmdResult;
import top.sorie.minimq.common.MqMsg;

/**
 * 发布消息指令：cmd=1
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class PublishCmd extends MqCmd {
    private MqMsg msg;

    public PublishCmd() {
        this.setCmd(CMD_PUBLISH);
    }
    public PublishCmd(String topic, String msg) {
        this.setCmd(CMD_PUBLISH);
        this.msg = new MqMsg(topic, msg);
    }
    @Override
    public CmdResult exec(Broker broker) {
        if (msg == null || msg.getTopic() == null) {
            return new CmdResult(CmdResult.CODE_ERROR, "msg and topic are required", null);
        }
        int index = broker.publish(msg);
        return new CmdResult(CmdResult.CODE_SUCCESS, "success", index);
    }

    public MqMsg getMsg() {
        return msg;
    }

    public void setMsg(MqMsg msg) {
        this.msg = msg;
    }
}
