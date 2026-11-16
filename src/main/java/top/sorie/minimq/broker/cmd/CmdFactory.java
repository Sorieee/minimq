package top.sorie.minimq.broker.cmd;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.HashMap;
import java.util.Map;

/**
 * 指令工厂：按 JSON 里的 cmd 类型生产对应的 MqCmd 子类实例。
 * 新增指令类型只需在这里注册（或调用 {@link #register}），解析与执行无需改动。
 */
public final class CmdFactory {
    private static final ObjectMapper MAPPER = new ObjectMapper();

    /** cmd 类型 -> 子类，注册表即工厂的产品目录 */
    private static final Map<Integer, Class<? extends MqCmd>> REGISTRY = new HashMap<>(Map.ofEntries(
            Map.entry(MqCmd.CMD_PUBLISH, PublishCmd.class),
            Map.entry(MqCmd.CMD_SUBSCRIBE, SubscribeCmd.class),
            Map.entry(MqCmd.CMD_UNSUBSCRIBE, UnsubscribeCmd.class),
            Map.entry(MqCmd.CMD_FETCH_MSG, FetchMsgCmd.class),
            Map.entry(MqCmd.CMD_ACK, AckCmd.class)
    ));

    private CmdFactory() {
    }

    /** 解析一段已去掉定界符的 JSON，按 cmd 类型实例化对应子类 */
    public static MqCmd create(String json) {
        JsonNode root;
        try {
            root = MAPPER.readTree(json);
        } catch (Exception e) {
            throw new IllegalArgumentException("parse msg error: " + e.getMessage(), e);
        }
        JsonNode cmdNode = root.get("cmd");
        if (cmdNode == null || !cmdNode.canConvertToInt()) {
            throw new IllegalArgumentException("msg has no valid cmd field: " + json);
        }
        return create(cmdNode.asInt(), json);
    }

    /** 注册新指令类型；重复注册视为编程错误直接抛出 */
    public static void register(int cmdType, Class<? extends MqCmd> clazz) {
        Class<?> prev = REGISTRY.put(cmdType, clazz);
        if (prev != null) {
            throw new IllegalStateException("cmd type already registered: " + cmdType);
        }
    }

    private static MqCmd create(int cmdType, String json) {
        Class<? extends MqCmd> clazz = REGISTRY.get(cmdType);
        if (clazz == null) {
            throw new IllegalArgumentException("unknown cmd type: " + cmdType);
        }
        try {
            return MAPPER.treeToValue(MAPPER.readTree(json), clazz);
        } catch (Exception e) {
            throw new IllegalArgumentException(
                    "parse msg to " + clazz.getSimpleName() + " error: " + e.getMessage(), e);
        }
    }
}
