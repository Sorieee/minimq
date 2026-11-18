package top.sorie.minimq.common.protocol;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * MqCmd 协议定界解析的单测：只验证解析，不涉及服务端执行。
 */
class MqCmdTest {

    @Test
    void resolveShouldStripEndMarkAndCreatePublishCmd() {
        MqCmd cmd = MqCmd.resolve("{\"cmd\":1,\"msg\":{\"topic\":\"t1\",\"content\":\"hi\"}}</end>");
        assertInstanceOf(PublishCmd.class, cmd);
        assertEquals(MqCmd.CMD_PUBLISH, cmd.getCmd());
    }

    @Test
    void resolveShouldStripStartMark() {
        MqCmd cmd = MqCmd.resolve("<start>{\"cmd\":1,\"msg\":{\"topic\":\"t1\",\"content\":\"hi\"}}</end>");
        assertInstanceOf(PublishCmd.class, cmd);
    }

    @Test
    void resolveShouldThrowOnBlankMsg() {
        assertThrows(IllegalArgumentException.class, () -> MqCmd.resolve("  "));
    }

    @Test
    void resolveShouldThrowOnInvalidJson() {
        assertThrows(IllegalArgumentException.class, () -> MqCmd.resolve("{not json}"));
    }
}
