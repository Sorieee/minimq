package top.sorie.minimq.broker.cmd;

import org.junit.jupiter.api.Test;
import top.sorie.minimq.broker.Broker;
import top.sorie.minimq.broker.cmd.result.CmdResult;
import top.sorie.minimq.common.MqMsg;

import java.util.List;

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

    @Test
    void publishCmdExecShouldStoreMsgInBroker() {
        Broker broker = new Broker();
        MqCmd cmd = CmdFactory.create("{\"cmd\":1,\"msg\":{\"topic\":\"t1\",\"content\":\"hello\"}}");

        CmdResult result = cmd.exec(broker);

        assertEquals(CmdResult.CODE_SUCCESS, result.getCode());
        List<MqMsg> msgs = broker.fetch("t1");
        assertEquals(1, msgs.size());
        assertEquals("hello", msgs.get(0).getContent());
    }

    @Test
    void fetchCmdExecShouldReturnStoredMsgs() {
        Broker broker = new Broker();
        CmdFactory.create("{\"cmd\":1,\"msg\":{\"topic\":\"t1\",\"content\":\"hello\"}}").exec(broker);

        CmdResult result = CmdFactory.create("{\"cmd\":4,\"topic\":\"t1\"}").exec(broker);

        assertEquals(CmdResult.CODE_SUCCESS, result.getCode());
        @SuppressWarnings("unchecked")
        List<MqMsg> data = (List<MqMsg>) result.getData();
        assertEquals(1, data.size());
        assertEquals("hello", data.get(0).getContent());
    }

    @Test
    void subscribeAndUnsubscribeShouldChangeBrokerSubscribers() {
        Broker broker = new Broker();

        CmdFactory.create("{\"cmd\":2,\"topic\":\"t1\",\"consumerId\":\"c1\"}").exec(broker);
        assertEquals(List.of("c1"), broker.subscribersOf("t1"));

        CmdFactory.create("{\"cmd\":3,\"topic\":\"t1\",\"consumerId\":\"c1\"}").exec(broker);
        assertEquals(List.of(), broker.subscribersOf("t1"));
    }

    @Test
    void ackCmdExecShouldRecordAckInBroker() {
        Broker broker = new Broker();

        CmdResult result = CmdFactory.create("{\"cmd\":5,\"topic\":\"t1\",\"msgIndex\":2}").exec(broker);

        assertEquals(CmdResult.CODE_SUCCESS, result.getCode());
        assertEquals(2, broker.ackedIndexOf("c-default", "t1"));
    }

    @Test
    void fetchCmdExecShouldFailWhenTopicMissing() {
        Broker broker = new Broker();
        CmdResult result = CmdFactory.create("{\"cmd\":4,\"topic\":\"no-such\"}").exec(broker);
        assertEquals(CmdResult.CODE_ERROR, result.getCode());
    }
}
