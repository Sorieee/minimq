package top.sorie.minimq.broker;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import top.sorie.minimq.broker.cmd.MqCmd;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.Socket;
import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Broker cmd 链路集成测试：真实 socket 连接，验证 "发送指令 + </end> 结束符 -> 收到 JSON 应答"。
 *
 * Broker 是 accept 循环：一条连接可连续多问一答，客户端断开后还能接受新连接；stop() 显式停机。
 */
class BrokerCmdTest {

    private static final long CONNECT_TIMEOUT_MS = 5000;

    private Broker broker;
    private Thread brokerThread;
    /** 后台线程的异常 JUnit 感知不到，这里显式接住，@AfterEach 统一断言 */
    private volatile Throwable brokerUncaught;
    private int port;

    @BeforeEach
    void startBroker() throws InterruptedException {
        brokerUncaught = null;
        // 端口传 0 由系统分配，避免固定端口被残留进程占用
        broker = new Broker(0);
        brokerThread = new Thread(() -> broker.start(), "broker-test");
        brokerThread.setDaemon(true);
        brokerThread.setUncaughtExceptionHandler((t, e) -> brokerUncaught = e);
        brokerThread.start();
        port = awaitBoundPort();
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
        // stop() 关闭监听 socket 后 accept 抛 SocketException，线程应退出
        brokerThread.join(CONNECT_TIMEOUT_MS);
        assertFalse(brokerThread.isAlive(), "stop() 后 broker 线程应退出");
        assertEquals(Broker.STATUS_STOPPED, broker.getStatus(), "停止后 status 应复位为 STOPPED");
        assertNull(brokerUncaught, "broker 线程不应有未捕获异常: " + brokerUncaught);
    }

    /** 完整的发布指令 payload（协议要求 msg 字段） */
    private static final String PUBLISH_PAYLOAD =
            "{\"cmd\":" + MqCmd.CMD_PUBLISH + ",\"msg\":{\"topic\":\"t1\",\"content\":\"hello\"}}";

    @Test
    void shouldReplySuccessJsonWhenCmdEndsWithEndMark() throws Exception {
        try (Socket client = connectWithRetry()) {
            String reply = sendCmd(client, PUBLISH_PAYLOAD);

            assertNotNull(reply, "应收到一行 JSON 应答");
            JsonNode node = new ObjectMapper().readTree(reply);
            assertEquals(0, node.get("code").asInt());
            assertEquals("success", node.get("message").asText());
            // data 为该消息在 topic 里的下标
            assertEquals(0, node.get("data").asInt());
        }
    }

    @Test
    void shouldReplyEachCmdOnSameConnection() throws Exception {
        try (Socket client = connectWithRetry()) {
            String reply1 = sendCmd(client, PUBLISH_PAYLOAD);
            String reply2 = sendCmd(client, PUBLISH_PAYLOAD);

            assertNotNull(reply1, "第一条指令应收到应答");
            assertNotNull(reply2, "同一连接上的第二条指令也应收到应答");
            assertEquals(0, new ObjectMapper().readTree(reply2).get("code").asInt());
            assertEquals(1, new ObjectMapper().readTree(reply2).get("data").asInt());
        }
    }

    @Test
    void shouldAcceptNewSessionAfterClientDisconnect() throws Exception {
        try (Socket first = connectWithRetry()) {
            sendCmd(first, PUBLISH_PAYLOAD);
        }
        // 第一个客户端断开后，accept 循环继续接客：新连接仍能拿到应答
        try (Socket second = connectWithRetry()) {
            String reply = sendCmd(second, PUBLISH_PAYLOAD);
            assertNotNull(reply, "客户端断开后的新会话应仍能收到应答");
            assertEquals(0, new ObjectMapper().readTree(reply).get("code").asInt());
        }
    }

    @Test
    void shouldThrowAlreadyRunningWhenStartedTwice() throws Exception {
        // 等 accept 循环进入监听后再从本线程调 start()：应直接被拒，不再占用 socket
        try (Socket probe = connectWithRetry()) {
            assertEquals(Broker.STATUS_RUNNING, broker.getStatus(), "运行中 status 应为 RUNNING");
        }
        RuntimeException e = assertThrows(RuntimeException.class, broker::start);
        assertEquals("Broker is already running", e.getMessage());
    }

    /** 连接重试：端口就绪前连接被拒不占用 accept 槽位 */
    private Socket connectWithRetry() throws InterruptedException {
        long deadline = System.currentTimeMillis() + CONNECT_TIMEOUT_MS;
        while (System.currentTimeMillis() < deadline) {
            try {
                return new Socket("127.0.0.1", port);
            } catch (IOException e) {
                Thread.sleep(50);
            }
        }
        fail("Broker 未在 " + CONNECT_TIMEOUT_MS + "ms 内监听端口 " + port);
        return null; // 不可达
    }

    /** 发送一条指令（payload + 结束符 </end> 各占一行），读回一行应答 */
    private String sendCmd(Socket client, String payload) throws IOException {
        BufferedWriter out = new BufferedWriter(
                new OutputStreamWriter(client.getOutputStream(), StandardCharsets.UTF_8));
        out.write(payload);
        out.newLine();
        out.write("</end>");
        out.newLine();
        out.flush();

        BufferedReader in = new BufferedReader(
                new InputStreamReader(client.getInputStream(), StandardCharsets.UTF_8));
        return in.readLine();
    }
}
