package top.sorie.minimq.common.protocol;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * CmdFactory 工厂测试：按 JSON 里的 cmd 类型生产对应子类实例。
 */
class CmdFactoryTest {

    @Test
    void shouldCreatePublishCmdWithType1() {
        MqCmd cmd = CmdFactory.create("{\"cmd\":1,\"msg\":{\"topic\":\"t1\",\"content\":\"hello\"}}");
        PublishCmd publish = assertInstanceOf(PublishCmd.class, cmd);
        assertEquals(MqCmd.CMD_PUBLISH, cmd.getCmd());
        assertEquals("t1", publish.getMsg().getTopic());
        assertEquals("hello", publish.getMsg().getContent());
    }

    @Test
    void shouldCreateEachCmdType() {
        assertInstanceOf(SubscribeCmd.class,
                CmdFactory.create("{\"cmd\":2,\"topic\":\"t1\",\"consumerId\":\"c1\"}"));
        assertInstanceOf(UnsubscribeCmd.class,
                CmdFactory.create("{\"cmd\":3,\"topic\":\"t1\",\"consumerId\":\"c1\"}"));
        assertInstanceOf(FetchMsgCmd.class,
                CmdFactory.create("{\"cmd\":4,\"topic\":\"t1\"}"));
        assertInstanceOf(AckCmd.class,
                CmdFactory.create("{\"cmd\":5,\"topic\":\"t1\",\"msgIndex\":0}"));
    }

    @Test
    void shouldThrowWhenUnknownCmdType() {
        assertThrows(IllegalArgumentException.class,
                () -> CmdFactory.create("{\"cmd\":99}"));
    }

    @Test
    void shouldThrowWhenCmdFieldMissing() {
        assertThrows(IllegalArgumentException.class,
                () -> CmdFactory.create("{\"topic\":\"t1\"}"));
    }
}
