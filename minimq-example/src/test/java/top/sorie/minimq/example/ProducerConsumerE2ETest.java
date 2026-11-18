package top.sorie.minimq.example;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import top.sorie.minimq.broker.Broker;
import top.sorie.minimq.client.Consumer;
import top.sorie.minimq.client.Producer;
import top.sorie.minimq.common.model.MqMsg;
import top.sorie.minimq.common.protocol.CmdResult;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * 端到端链路测试：Producer 发布 -> Broker 中转存储 -> Consumer 订阅 / 拉取 / ack。
 *
 * <p>验证链路是"拉模式"：订阅只登记消费者，消息要由 {@link Consumer#fetch(String)} 主动拉取，
 * Broker 不会主动推送。</p>
 */
class ProducerConsumerE2ETest {

    private static final long CONNECT_TIMEOUT_MS = 5000;
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private Broker broker;
    private Thread brokerThread;
    private volatile Throwable brokerUncaught;
    private int port;

    @BeforeEach
    void startBroker() throws InterruptedException {
        brokerUncaught = null;
        broker = new Broker(0);
        brokerThread = new Thread(() -> broker.start(), "broker-e2e-test");
        brokerThread.setDaemon(true);
        brokerThread.setUncaughtExceptionHandler((t, e) -> brokerUncaught = e);
        brokerThread.start();
        port = awaitBoundPort();
    }

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

    @Test
    void producerPublish_consumerSubscribeFetchAck_shouldWorkEndToEnd() {
        Producer producer = new Producer("p1", "producer-1", "127.0.0.1", "127.0.0.1", port);
        Consumer consumer = new Consumer("c1", "consumer-1", "127.0.0.1", port);

        // 1. 订阅
        assertEquals(CmdResult.CODE_SUCCESS, consumer.subscribe("order").getCode());
        assertEquals(List.of("c1"), broker.subscribersOf("order"));

        // 2. 生产者发布三条消息，Broker 按顺序存进 topic
        assertEquals(0, ((Number) producer.sendMsg("order", "m1").getData()).intValue());
        assertEquals(1, ((Number) producer.sendMsg("order", "m2").getData()).intValue());
        assertEquals(2, ((Number) producer.sendMsg("order", "m3").getData()).intValue());

        // 3. 消费者拉取：拿到全部消息且顺序一致
        CmdResult fetched = consumer.fetch("order");
        assertEquals(CmdResult.CODE_SUCCESS, fetched.getCode());
        List<?> data = assertInstanceOf(List.class, fetched.getData());
        assertEquals(3, data.size());
        assertEquals(List.of("m1", "m2", "m3"), contentsOf(data));

        // 4. ack 最后一条，Broker 记录消费进度
        assertEquals(CmdResult.CODE_SUCCESS, consumer.ack("order", 2).getCode());
        assertEquals(2, broker.ackedIndexOf("c1", "order"));
    }

    @Test
    void publishBeforeSubscribe_shouldStillBeFetchable() {
        Producer producer = new Producer("p1", "producer-1", "127.0.0.1", "127.0.0.1", port);
        Consumer consumer = new Consumer("c1", "consumer-1", "127.0.0.1", port);

        // Broker 按 topic 存储，与"谁先连上"无关：先发布后订阅也能拉到
        producer.sendMsg("late", "before-subscribe");
        assertEquals(CmdResult.CODE_SUCCESS, consumer.subscribe("late").getCode());

        CmdResult fetched = consumer.fetch("late");
        assertEquals(CmdResult.CODE_SUCCESS, fetched.getCode());
        assertEquals(List.of("before-subscribe"),
                contentsOf(assertInstanceOf(List.class, fetched.getData())));
    }

    @Test
    void fetchByNonSubscriber_shouldStillReturnMessages() {
        Producer producer = new Producer("p1", "producer-1", "127.0.0.1", "127.0.0.1", port);
        Consumer stranger = new Consumer("c-other", "stranger", "127.0.0.1", port);

        producer.sendMsg("open", "hello");

        // 当前实现里 fetch 不校验订阅关系，非订阅者也能拉到（后续要收紧则改 Broker/FetchMsgCmd）
        CmdResult fetched = stranger.fetch("open");
        assertEquals(CmdResult.CODE_SUCCESS, fetched.getCode());
        assertEquals(List.of("hello"),
                contentsOf(assertInstanceOf(List.class, fetched.getData())));
        assertTrue(broker.subscribersOf("open").isEmpty(), "非订阅者拉取不应登记订阅关系");
    }

    private static List<String> contentsOf(List<?> data) {
        return data.stream()
                .map(node -> MAPPER.convertValue(node, MqMsg.class).getContent())
                .toList();
    }
}
