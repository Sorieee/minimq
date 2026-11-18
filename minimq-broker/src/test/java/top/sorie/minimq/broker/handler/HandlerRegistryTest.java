package top.sorie.minimq.broker.handler;

import org.junit.jupiter.api.Test;
import top.sorie.minimq.broker.Broker;
import top.sorie.minimq.common.model.MqMsg;
import top.sorie.minimq.common.protocol.CmdFactory;
import top.sorie.minimq.common.protocol.CmdResult;
import top.sorie.minimq.common.protocol.MqCmd;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * HandlerRegistry 与各 CmdHandler 的单测：验证从原 MqCmd.exec 搬来的业务行为保持一致。
 */
class HandlerRegistryTest {

    private final Broker broker = new Broker();
    private final HandlerRegistry registry = HandlerRegistry.defaults();

    private CmdResult dispatch(String json) {
        return registry.dispatch(CmdFactory.create(json), broker);
    }

    @Test
    void publishShouldStoreMsgAndReturnIndex() {
        CmdResult result = dispatch("{\"cmd\":1,\"msg\":{\"topic\":\"t1\",\"content\":\"hi\"}}");

        assertEquals(CmdResult.CODE_SUCCESS, result.getCode());
        assertEquals(0, ((Number) result.getData()).intValue());
        assertEquals("hi", broker.msgsOf("t1").get(0).getContent());
    }

    @Test
    void fetchShouldReturnStoredMsgs() {
        dispatch("{\"cmd\":1,\"msg\":{\"topic\":\"t1\",\"content\":\"hello\"}}");

        CmdResult result = dispatch("{\"cmd\":4,\"topic\":\"t1\"}");

        assertEquals(CmdResult.CODE_SUCCESS, result.getCode());
        List<?> data = assertInstanceOf(List.class, result.getData());
        assertEquals(1, data.size());
        assertEquals("hello", ((MqMsg) data.get(0)).getContent());
    }

    @Test
    void fetchUnknownTopicShouldReturnError() {
        CmdResult result = dispatch("{\"cmd\":4,\"topic\":\"no-such\"}");

        assertEquals(CmdResult.CODE_ERROR, result.getCode());
        assertTrue(result.getMessage().contains("not found"), result.getMessage());
        assertNull(result.getData());
    }

    @Test
    void subscribeAndUnsubscribeShouldChangeSubscribers() {
        dispatch("{\"cmd\":2,\"topic\":\"t1\",\"consumerId\":\"c1\"}");
        assertEquals(List.of("c1"), broker.subscribersOf("t1"));

        CmdResult result = dispatch("{\"cmd\":3,\"topic\":\"t1\",\"consumerId\":\"c1\"}");
        assertEquals(CmdResult.CODE_SUCCESS, result.getCode());
        assertTrue(broker.subscribersOf("t1").isEmpty());
    }

    @Test
    void unsubscribeWithoutSubscriptionShouldReturnError() {
        CmdResult result = dispatch("{\"cmd\":3,\"topic\":\"t1\",\"consumerId\":\"c1\"}");

        assertEquals(CmdResult.CODE_ERROR, result.getCode());
        assertTrue(result.getMessage().contains("not found"), result.getMessage());
    }

    @Test
    void ackShouldUseDefaultConsumerWhenMissing() {
        CmdResult result = dispatch("{\"cmd\":5,\"topic\":\"t1\",\"msgIndex\":2}");

        assertEquals(CmdResult.CODE_SUCCESS, result.getCode());
        assertEquals(2, broker.ackedIndexOf(AckHandler.DEFAULT_CONSUMER, "t1"));
    }

    @Test
    void ackWithConsumerIdShouldRecordProgress() {
        dispatch("{\"cmd\":5,\"topic\":\"t1\",\"msgIndex\":3,\"consumerId\":\"c1\"}");

        assertEquals(3, broker.ackedIndexOf("c1", "t1"));
    }

    @Test
    void missingRequiredFieldsShouldReturnError() {
        assertEquals(CmdResult.CODE_ERROR, dispatch("{\"cmd\":1}").getCode());
        assertEquals(CmdResult.CODE_ERROR, dispatch("{\"cmd\":2,\"topic\":\"t1\"}").getCode());
        assertEquals(CmdResult.CODE_ERROR, dispatch("{\"cmd\":4}").getCode());
        assertEquals(CmdResult.CODE_ERROR, dispatch("{\"cmd\":5,\"topic\":\"t1\"}").getCode());
    }

    @Test
    void unknownCmdTypeShouldReturnError() {
        MqCmd unknown = new MqCmd() {
        };
        unknown.setCmd(99);

        CmdResult result = registry.dispatch(unknown, broker);

        assertEquals(CmdResult.CODE_ERROR, result.getCode());
        assertTrue(result.getMessage().contains("unknown cmd type"), result.getMessage());
    }

    @Test
    void cmdTypeMismatchShouldThrow() {
        HandlerRegistry broken = new HandlerRegistry();
        broken.register(MqCmd.CMD_ACK, new PublishHandler());

        MqCmd ack = CmdFactory.create("{\"cmd\":5,\"topic\":\"t1\",\"msgIndex\":0}");
        assertThrows(IllegalArgumentException.class, () -> broken.dispatch(ack, broker));
    }
}
