package top.sorie.minimq.broker.cmd;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import top.sorie.minimq.broker.Broker;
import top.sorie.minimq.broker.cmd.result.CmdResult;

/**
 * 订阅指令：cmd=2
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class SubscribeCmd extends MqCmd {
    private String topic;
    private String consumerId;

    @Override
    public CmdResult exec(Broker broker) {
        if (topic == null || consumerId == null) {
            return new CmdResult(CmdResult.CODE_ERROR, "topic and consumerId are required", null);
        }
        broker.subscribe(topic, consumerId);
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
