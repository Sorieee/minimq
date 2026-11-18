package top.sorie.minimq.common;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import top.sorie.minimq.broker.Broker;
import top.sorie.minimq.broker.cmd.MqCmd;
import top.sorie.minimq.broker.cmd.PublishCmd;
import top.sorie.minimq.broker.cmd.result.CmdResult;
import top.sorie.minimq.client.Client;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Consumer 集成测试：真实连接 Broker（端口 0 由系统分配），覆盖订阅 / 拉取 / ack / 退订与异常分支。
 */
class ConsumerTest {

    private static final long CONNECT_TIMEOUT_MS = 5000;
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private Broker broker;
    private Thread brokerThread;
    /** 后台线程的异常 JUnit 感知不到，这里显式接住，@AfterEach 统一断言 */
    private volatile Throwable brokerUncaught;
    private int port;
    /** 测试侧用独立 Client 扮演生产者 */
    private Client producerClient;

    @BeforeEach
    void startBroker() throws InterruptedException {
        brokerUncaught = null;
        broker = new Broker(0);
        brokerThread = new Thread(() -> broker.start(), "broker-consumer-test");
        brokerThread.setDaemon(true);
        brokerThread.setUncaughtExceptionHandler((t, e) -> brokerUncaught = e);
        brokerThread.start();
        port = awaitBoundPort();
        producerClient = new Client("127.0.0.1", port);
    }

    /** 等 Broker bind 完成拿到系统分配的实际端口 */
    private int awaitBoundPort() throws InterruptedException {
        long deadline = System.currentTimeMillis() + CONNECT_TIMEOUT_MS;
        while (System.currentTimeMillis() < deadline) {
            int bound = broker.getPort();
            if (bound > 0) {
                return bound;
            }
            Thread.sleep(10);
        }
        fail("Broker 未在 " + CONNECT_TIMEOUT_MS + "ms 内完成端口绑定");
        return 0; // 不可达
    }

    @AfterEach
    void stopBroker() throws InterruptedException {
        broker.stop();
        brokerThread.join(CONNECT_TIMEOUT_MS);
        assertNull(brokerUncaught, "broker 线程不应有未捕获异常: " + brokerUncaught);
    }

    private Consumer newConsumer() {
        return new Consumer("c-consumer", "consumer-1", "127.0.0.1", port);
    }

    /** 通过 Broker 写入一条消息，返回其下标 */
    private int publish(String topic, String content) {
        PublishCmd cmd = new PublishCmd(topic, content);
        cmd.setCmd(MqCmd.CMD_PUBLISH);
        CmdResult result = producerClient.sendCmd(cmd);
        assertEquals(CmdResult.CODE_SUCCESS, result.getCode(), "预置消息发布失败: " + result.getMessage());
        return ((Number) result.getData()).intValue();
    }

    @Test
    void subscribeShouldRegisterConsumerIdOnBroker() {
        Consumer consumer = newConsumer();

        CmdResult result = consumer.subscribe("t1");

        assertEquals(CmdResult.CODE_SUCCESS, result.getCode());
        assertEquals(List.of(consumer.getId()), broker.subscribersOf("t1"));
    }

    @Test
    void subscribeTwiceShouldKeepSingleSubscriberRecord() {
        Consumer consumer = newConsumer();

        assertEquals(CmdResult.CODE_SUCCESS, consumer.subscribe("t1").getCode());
        assertEquals(CmdResult.CODE_SUCCESS, consumer.subscribe("t1").getCode());

        assertEquals(1, broker.subscribersOf("t1").size(), "重复订阅不应产生重复记录");
    }

    @Test
    void fetchShouldReturnAllPublishedMessagesInOrder() {
        Consumer consumer = newConsumer();
        assertEquals(CmdResult.CODE_SUCCESS, consumer.subscribe("t1").getCode());
        publish("t1", "first");
        publish("t1", "second");

        CmdResult result = consumer.fetch("t1");

        assertEquals(CmdResult.CODE_SUCCESS, result.getCode());
        List<?> data = assertInstanceOf(List.class, result.getData());
        assertEquals(2, data.size());
        assertEquals("first", MAPPER.convertValue(data.get(0), MqMsg.class).getContent());
        assertEquals("second", MAPPER.convertValue(data.get(1), MqMsg.class).getContent());
    }

    @Test
    void fetchUnknownTopicShouldReturnErrorResult() {
        Consumer consumer = newConsumer();

        CmdResult result = consumer.fetch("no-such-topic");

        assertEquals(CmdResult.CODE_ERROR, result.getCode());
        assertTrue(result.getMessage().contains("not found"), "应说明 topic 不存在: " + result.getMessage());
        assertNull(result.getData());
    }

    @Test
    void ackShouldKeepMaxIndexForConsumer() {
        Consumer consumer = newConsumer();
        int first = publish("t1", "first");
        int second = publish("t1", "second");

        assertEquals(CmdResult.CODE_SUCCESS, consumer.ack("t1", second).getCode());
        assertEquals(second, broker.ackedIndexOf(consumer.getId(), "t1"));

        // 回退 ack 不生效：进度只增不减
        assertEquals(CmdResult.CODE_SUCCESS, consumer.ack("t1", first).getCode());
        assertEquals(second, broker.ackedIndexOf(consumer.getId(), "t1"));
    }

    @Test
    void ackWithoutSubscribingShouldStillSucceedAndRecordProgress() {
        Consumer consumer = newConsumer();

        CmdResult result = consumer.ack("t2", 0);

        assertEquals(CmdResult.CODE_SUCCESS, result.getCode());
        assertEquals(0, broker.ackedIndexOf(consumer.getId(), "t2"));
    }

    @Test
    void unsubscribeShouldRemoveSubscriberAndSecondCallShouldFail() {
        Consumer consumer = newConsumer();
        assertEquals(CmdResult.CODE_SUCCESS, consumer.subscribe("t1").getCode());

        assertEquals(CmdResult.CODE_SUCCESS, consumer.unsubscribe("t1").getCode());
        assertTrue(broker.subscribersOf("t1").isEmpty(), "退订后不应再有订阅者");

        CmdResult second = consumer.unsubscribe("t1");
        assertEquals(CmdResult.CODE_ERROR, second.getCode());
        assertTrue(second.getMessage().contains("not found"), "应说明订阅不存在: " + second.getMessage());
    }

    @Test
    void subscribeWithNullTopicShouldReturnErrorResult() {
        Consumer consumer = newConsumer();

        CmdResult result = consumer.subscribe(null);

        assertEquals(CmdResult.CODE_ERROR, result.getCode());
        assertTrue(result.getMessage().contains("topic"), "应说明 topic 必填: " + result.getMessage());
    }

    @Test
    void constructorShouldGenerateIdNameAndLocalIp() {
        Consumer consumer = new Consumer("127.0.0.1", port);

        assertNotNull(consumer.getId());
        assertFalse(consumer.getId().isBlank());
        assertEquals(consumer.getId(), consumer.getName(), "未指定 name 时应回退为 id");
        assertFalse(consumer.getLocalIp().isBlank());
    }

    @Test
    void constructorShouldFallbackNameToIdWhenBlank() {
        Consumer consumer = new Consumer("c1", "  ", "127.0.0.1", port);

        assertEquals("c1", consumer.getId());
        assertEquals("c1", consumer.getName());
    }

    @Test
    void constructorShouldRejectInvalidArguments() {
        assertThrows(IllegalArgumentException.class, () -> new Consumer("  ", port));
        assertThrows(IllegalArgumentException.class, () -> new Consumer(null, port));
        assertThrows(IllegalArgumentException.class, () -> new Consumer("127.0.0.1", 0));
        assertThrows(IllegalArgumentException.class, () -> new Consumer("127.0.0.1", null));
        assertThrows(IllegalArgumentException.class,
                () -> new Consumer("c1", "n1", "  ", "127.0.0.1", port));
    }
}
