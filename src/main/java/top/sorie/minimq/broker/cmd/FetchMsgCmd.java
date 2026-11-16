package top.sorie.minimq.broker.cmd;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import top.sorie.minimq.broker.Broker;
import top.sorie.minimq.broker.cmd.result.CmdResult;

import java.util.List;

/**
 * 拉取消息指令：cmd=4
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class FetchMsgCmd extends MqCmd {
    private String topic;

    @Override
    public CmdResult exec(Broker broker) {
        if (topic == null) {
            return new CmdResult(CmdResult.CODE_ERROR, "topic is required", null);
        }
        List<?> msgs = broker.fetch(topic);
        if (msgs == null) {
            return new CmdResult(CmdResult.CODE_ERROR, "topic not found: " + topic, null);
        }
        return new CmdResult(CmdResult.CODE_SUCCESS, "success", msgs);
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }
}
