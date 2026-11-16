package top.sorie.minimq.broker.cmd;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import top.sorie.minimq.broker.Broker;
import top.sorie.minimq.broker.cmd.result.CmdResult;

/**
 * 消息确认指令：cmd=5
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class AckCmd extends MqCmd {
    /** 测试默认值：当前协议尚未带 consumerId 字段时的兜底标识 */
    public static final String DEFAULT_CONSUMER = "c-default";

    private String topic;
    private Integer msgIndex;
    private String consumerId;

    @Override
    public CmdResult exec(Broker broker) {
        if (topic == null || msgIndex == null) {
            return new CmdResult(CmdResult.CODE_ERROR, "topic and msgIndex are required", null);
        }
        String consumer = consumerId != null ? consumerId : DEFAULT_CONSUMER;
        broker.ack(consumer, topic, msgIndex);
        return new CmdResult(CmdResult.CODE_SUCCESS, "success", null);
    }

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
