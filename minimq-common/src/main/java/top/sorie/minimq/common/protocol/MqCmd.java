package top.sorie.minimq.common.protocol;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang3.StringUtils;

/**
 * 消息指令协议基类：只承载线上报文字段与定界（{@code <start> ... </end>}），
 * 不包含任何服务端逻辑；具体执行由 broker 侧 {@code CmdHandler} 负责。
 *
 * <p>具体子类由 {@link CmdFactory} 按 cmd 类型实例化。</p>
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

    /**
     * 解析一段完整报文（可带 {@code <start>} / {@code </end>} 定界符），返回对应子类。
     * 只做协议解析，不执行指令。
     */
    public static MqCmd resolve(String msg) {
        if (StringUtils.isBlank(msg)) {
            throw new IllegalArgumentException("msg is blank");
        }
        String payload = msg.trim();
        if (payload.startsWith(START_MARK)) {
            payload = payload.substring(START_MARK.length());
        }
        if (payload.endsWith(END_MARK)) {
            payload = payload.substring(0, payload.length() - END_MARK.length());
        }
        return CmdFactory.create(payload);
    }

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
