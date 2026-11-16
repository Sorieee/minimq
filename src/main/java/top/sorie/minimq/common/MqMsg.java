package top.sorie.minimq.common;

/**
 * mq消息
 */
public class MqMsg {
    private String topic;
    private String content;

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
