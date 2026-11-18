package top.sorie.minimq.common.protocol;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

/**
 * 退订指令：cmd=3
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class UnsubscribeCmd extends MqCmd {
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
