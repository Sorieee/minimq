package top.sorie.minimq.common.protocol;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

/**
 * 拉取消息指令：cmd=4
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class FetchMsgCmd extends MqCmd {
    private String topic;

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }
}
