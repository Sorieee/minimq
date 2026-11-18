package top.sorie.minimq.common.protocol;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

/**
 * 订阅指令：cmd=2
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class SubscribeCmd extends MqCmd {
    private String topic;
    private String consumerId;

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
