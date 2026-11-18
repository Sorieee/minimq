package top.sorie.minimq.client;

import org.junit.jupiter.api.Test;
import top.sorie.minimq.common.protocol.CmdResult;
import top.sorie.minimq.common.protocol.MqCmd;
import top.sorie.minimq.common.protocol.PublishCmd;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Client 单测：构造校验、非法指令、连接失败的降级行为。
 * 真实 Broker 的收发链路测试放在 minimq-example（ClientIntegrationTest）。
 */
class ClientTest {

    @Test
    void constructorShouldRejectInvalidArguments() {
        assertThrows(IllegalArgumentException.class, () -> new Client(null, 19900));
        assertThrows(IllegalArgumentException.class, () -> new Client(" ", 19900));
        assertThrows(IllegalArgumentException.class, () -> new Client("127.0.0.1", null));
        assertThrows(IllegalArgumentException.class, () -> new Client("127.0.0.1", 0));
    }

    @Test
    void sendCmdShouldReturnErrorForNullCmd() {
        CmdResult result = new Client("127.0.0.1", 19999).sendCmd(null);

        assertEquals(CmdResult.CODE_ERROR, result.getCode());
        assertNull(result.getData());
    }

    @Test
    void sendCmdShouldReturnErrorWhenCmdTypeMissing() {
        MqCmd cmd = new MqCmd() {
        };

        CmdResult result = new Client("127.0.0.1", 19999).sendCmd(cmd);

        assertEquals(CmdResult.CODE_ERROR, result.getCode());
        assertTrue(result.getMessage().contains("required"), result.getMessage());
    }

    @Test
    void sendCmdShouldReturnErrorResultWhenBrokerUnreachable() {
        // 用一个确定无人监听的端口验证"连接失败也返回 CmdResult 而非抛异常"
        Client deadClient = new Client("127.0.0.1", 20199);

        CmdResult result = deadClient.sendCmd(new PublishCmd("t1", "hello"));

        assertEquals(CmdResult.CODE_ERROR, result.getCode());
        assertTrue(result.getMessage().contains("connect") || result.getMessage().contains("Connection"),
                "错误信息应说明连接失败: " + result.getMessage());
        assertNull(result.getData());
    }
}
