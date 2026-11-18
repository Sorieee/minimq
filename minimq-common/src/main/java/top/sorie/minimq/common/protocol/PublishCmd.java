package top.sorie.minimq.common.protocol;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import top.sorie.minimq.common.model.MqMsg;

/**
 * 发布消息指令：cmd=1
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class PublishCmd extends MqCmd {
    private MqMsg msg;

    public PublishCmd() {
        this.setCmd(CMD_PUBLISH);
    }

    public PublishCmd(String topic, String content) {
        this.setCmd(CMD_PUBLISH);
        this.msg = new MqMsg(topic, content);
    }

    public MqMsg getMsg() {
        return msg;
    }

    public void setMsg(MqMsg msg) {
        this.msg = msg;
    }
}
