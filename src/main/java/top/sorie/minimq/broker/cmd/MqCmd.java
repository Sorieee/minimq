package top.sorie.minimq.broker.cmd;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang3.StringUtils;
import top.sorie.minimq.broker.Broker;
import top.sorie.minimq.broker.cmd.result.CmdResult;

/**
 * 消息指令抽象基类：只负责协议定界（<start> ... </end>），
 * 具体由哪个子类承载由 {@link CmdFactory} 按 cmd 类型决定。
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public abstract class MqCmd {
    public static final Integer CMD_PUBLISH = 1;
    public static final Integer CMD_SUBSCRIBE = 2;
    public static final Integer CMD_UNSUBSCRIBE = 3;
    public static final Integer CMD_FETCH_MSG = 4;
    public static final Integer CMD_ACK = 5;

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final String START_MARK = "<start>";
    private static final String END_MARK = "</end>";

    private Integer cmd;

    public static MqCmd resolve(String msg) {
        if (StringUtils.isBlank(msg)) {
            throw new RuntimeException("msg is blank");
        }
        msg = msg.trim();
        if (msg.startsWith(START_MARK)) {
            msg = msg.substring(START_MARK.length());
        }
        if (msg.endsWith(END_MARK)) {
            msg = msg.substring(0, msg.length() - END_MARK.length());
        }
        return CmdFactory.create(msg);
    }

    /** 执行指令；指令通过 broker 参数读写服务端状态，自身保持无副作用可测 */
    public abstract CmdResult exec(Broker broker);

    /**
     * 序列化为线上协议格式：JSON + 结束符，与 {@link #resolve} 对称。
     * 结束符独立成行，Broker 逐行读到它才认为一条指令结束。
     */
    public String toWireFormat() {
        try {
            return MAPPER.writeValueAsString(this) + "\n" + END_MARK;
        } catch (Exception e) {
            throw new IllegalStateException("serialize cmd error: " + e.getMessage(), e);
        }
    }

    public Integer getCmd() {
        return cmd;
    }

    public void setCmd(Integer cmd) {
        this.cmd = cmd;
    }
}
