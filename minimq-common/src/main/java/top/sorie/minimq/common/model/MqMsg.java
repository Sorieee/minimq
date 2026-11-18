package top.sorie.minimq.common.model;

/**
 * mq 消息
 */
public class MqMsg {
    private String topic;
    private String content;

    public MqMsg(String topic, String content) {
        this.topic = topic;
        this.content = content;
    }

    public MqMsg() {
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public String getContent() {
        return content;
    }

    public void setContent(String content) {
        this.content = content;
    }
}
