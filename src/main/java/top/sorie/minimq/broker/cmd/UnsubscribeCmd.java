package top.sorie.minimq.broker.cmd;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import top.sorie.minimq.broker.Broker;
import top.sorie.minimq.broker.cmd.result.CmdResult;

/**
 * 退订指令：cmd=3
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class UnsubscribeCmd extends MqCmd {
    private String topic;
    private String consumerId;

    @Override
    public CmdResult exec(Broker broker) {
        if (topic == null || consumerId == null) {
            return new CmdResult(CmdResult.CODE_ERROR, "topic and consumerId are required", null);
        }
        boolean removed = broker.unsubscribe(topic, consumerId);
        if (!removed) {
            return new CmdResult(CmdResult.CODE_ERROR, "subscription not found", null);
        }
        return new CmdResult(CmdResult.CODE_SUCCESS, "success", null);
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public String getConsumerId() {
        return consumerId;
    }

    public void setConsumerId(String consumerId) {
        this.consumerId = consumerId;
    }
}
