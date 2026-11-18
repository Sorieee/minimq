package top.sorie.minimq.common.protocol;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

/**
 * 消息确认指令：cmd=5
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class AckCmd extends MqCmd {
    private String topic;
    private Integer msgIndex;
    private String consumerId;

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public Integer getMsgIndex() {
        return msgIndex;
    }

    public void setMsgIndex(Integer msgIndex) {
        this.msgIndex = msgIndex;
    }

    public String getConsumerId() {
        return consumerId;
    }

    public void setConsumerId(String consumerId) {
        this.consumerId = consumerId;
    }
}
