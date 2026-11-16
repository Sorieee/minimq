package top.sorie.minimq.broker.cmd;

import org.junit.jupiter.api.Test;
import top.sorie.minimq.broker.Broker;
import top.sorie.minimq.broker.cmd.result.CmdResult;
import top.sorie.minimq.common.MqMsg;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * MqCmd 协议定界解析的单测（完整 JSON 由 CmdFactory 解析并实例化子类）
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
    void execShouldStoreMsgAndReturnSuccess() {
        Broker broker = new Broker();
        CmdResult result = MqCmd.resolve("{\"cmd\":1,\"msg\":{\"topic\":\"t1\",\"content\":\"hi\"}}").exec(broker);

        assertEquals(CmdResult.CODE_SUCCESS, result.getCode());
        MqMsg stored = broker.msgsOf("t1").get(0);
        assertEquals("hi", stored.getContent());
    }

    @Test
    void resolveShouldThrowOnBlankMsg() {
        assertThrows(RuntimeException.class, () -> MqCmd.resolve("  "));
    }

    @Test
    void resolveShouldThrowOnInvalidJson() {
        assertThrows(RuntimeException.class, () -> MqCmd.resolve("{not json}"));
    }
}
