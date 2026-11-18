package top.sorie.minimq.example;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import top.sorie.minimq.broker.Broker;
import top.sorie.minimq.client.Client;
import top.sorie.minimq.common.model.MqMsg;
import top.sorie.minimq.common.protocol.CmdResult;
import top.sorie.minimq.common.protocol.FetchMsgCmd;
import top.sorie.minimq.common.protocol.MqCmd;
import top.sorie.minimq.common.protocol.PublishCmd;
import top.sorie.minimq.common.protocol.SubscribeCmd;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Client.sendCmd 集成测试：真实连接 Broker，发送 MqCmd、收回 CmdResult。
 */
class ClientIntegrationTest {

    private static final long CONNECT_TIMEOUT_MS = 5000;

    private Broker broker;
    private Thread brokerThread;
    private volatile Throwable brokerUncaught;
    private Client client;
    private int port;

    @BeforeEach
    void startBroker() throws InterruptedException {
        brokerUncaught = null;
        // 端口传 0 由系统分配，避免固定端口被残留进程占用
        broker = new Broker(0);
        brokerThread = new Thread(() -> broker.start(), "broker-client-test");
        brokerThread.setDaemon(true);
        brokerThread.setUncaughtExceptionHandler((t, e) -> brokerUncaught = e);
        brokerThread.start();
        port = awaitBoundPort();
        client = new Client("127.0.0.1", port);
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

    @Test
    void sendCmdShouldReturnParsedCmdResultOnPublish() {
        PublishCmd cmd = new PublishCmd();
        cmd.setCmd(MqCmd.CMD_PUBLISH);
        MqMsg msg = new MqMsg();
        msg.setTopic("t1");
        msg.setContent("hello");
        cmd.setMsg(msg);

        CmdResult result = client.sendCmd(cmd);

        assertEquals(CmdResult.CODE_SUCCESS, result.getCode());
        assertEquals(0, ((Number) result.getData()).intValue());
    }

    @Test
    void sendCmdShouldRoundTripSubscribeAndFetch() {
        SubscribeCmd subscribe = new SubscribeCmd();
        subscribe.setCmd(MqCmd.CMD_SUBSCRIBE);
        subscribe.setTopic("t1");
        subscribe.setConsumerId("c1");
        assertEquals(CmdResult.CODE_SUCCESS, client.sendCmd(subscribe).getCode());

        PublishCmd publish = new PublishCmd();
        publish.setCmd(MqCmd.CMD_PUBLISH);
        MqMsg msg = new MqMsg();
        msg.setTopic("t1");
        msg.setContent("hello");
        publish.setMsg(msg);
        client.sendCmd(publish);

        FetchMsgCmd fetch = new FetchMsgCmd();
        fetch.setCmd(MqCmd.CMD_FETCH_MSG);
        fetch.setTopic("t1");
        CmdResult result = client.sendCmd(fetch);

        assertEquals(CmdResult.CODE_SUCCESS, result.getCode());
        List<?> msgs = assertInstanceOf(List.class, result.getData());
        assertEquals(1, msgs.size());
        MqMsg stored = new ObjectMapper().convertValue(msgs.get(0), MqMsg.class);
        assertEquals("hello", stored.getContent());
    }
}
