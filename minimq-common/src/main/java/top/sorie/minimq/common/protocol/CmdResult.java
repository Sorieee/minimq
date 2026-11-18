package top.sorie.minimq.common.protocol;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

/**
 * 指令执行结果应答。带无参构造 + setter：客户端要把 Broker 的 JSON 应答反序列化回本类。
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class CmdResult {
    public static final int CODE_SUCCESS = 0;
    public static final int CODE_ERROR = -1;

    private int code;
    private String message;
    private Object data;

    public CmdResult() {
    }

    public CmdResult(int code, String message, Object data) {
        this.code = code;
        this.message = message;
        this.data = data;
    }

    public int getCode() {
        return code;
    }

    public void setCode(int code) {
        this.code = code;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public Object getData() {
        return data;
    }

    public void setData(Object data) {
        this.data = data;
    }
}
