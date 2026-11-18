package top.sorie.minimq.broker;

import com.fasterxml.jackson.databind.ObjectMapper;
import top.sorie.minimq.broker.handler.HandlerRegistry;
import top.sorie.minimq.common.model.MqMsg;
import top.sorie.minimq.common.protocol.CmdResult;
import top.sorie.minimq.common.protocol.MqCmd;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

public class Broker implements BrokerContext {
    /** 本机 9000 被前端 dev server 占用，改用 19900 */
    public static final int PORT = 19900;
    private static final String END_MARK = "</end>";
    /** topic -> 已发布消息（按发布顺序） */
    private final ConcurrentHashMap<String, List<MqMsg>> topicMsgMap;
    /** topic -> 订阅者 id */
    private final ConcurrentHashMap<String, List<String>> topicSubscribers;
    /** consumerId:topic -> 已 ack 的最大消息下标 */
    private final ConcurrentHashMap<String, Integer> ackIndexMap;
    /** cmd 类型 -> 处理器；协议解析在 common，执行在这里 */
    private final HandlerRegistry handlerRegistry;
    private final int port;
    /** bind 后实际使用的端口；构造传 0 时由系统分配，0 表示尚未 bind */
    private volatile int boundPort;
    private volatile Integer status;
    private volatile ServerSocket serverSocket;
    public static final Integer STATUS_RUNNING = 1;
    public static final Integer STATUS_STOPPED = 0;

    public Broker() {
        this(PORT);
    }

    /** @param port 监听端口；传 0 表示由系统分配空闲端口，之后用 {@link #getPort()} 取实际值 */
    public Broker(int port) {
        this.port = port;
        this.boundPort = 0;
        this.topicMsgMap = new ConcurrentHashMap<>();
        this.topicSubscribers = new ConcurrentHashMap<>();
        this.ackIndexMap = new ConcurrentHashMap<>();
        this.handlerRegistry = HandlerRegistry.defaults();
        this.status = STATUS_STOPPED;
    }

    public void start() {
        if (STATUS_RUNNING.equals(this.status)) {
            throw new RuntimeException("Broker is already running");
        }
        StringBuilder msg = new StringBuilder();
        try (ServerSocket serverSocket = createServerSocket(port)) {
            this.serverSocket = serverSocket;
            this.boundPort = serverSocket.getLocalPort();
            this.status = STATUS_RUNNING;
            // 循环 accept：Client.sendCmd 是短连接（每条指令一个连接），必须支持会话结束后继续接客
            while (STATUS_RUNNING.equals(this.status)) {
                try (Socket clientSocket = serverSocket.accept()) {
                    // 单条会话内的 I/O 异常（客户端 RST 等）只结束该会话，不能终止 accept 循环
                    try {
                        handleSession(clientSocket, msg);
                    } catch (SocketException e) {
                        System.out.println("Broker 会话结束（客户端异常断开）: " + e.getMessage());
                    } finally {
                        msg.setLength(0); // 会话中断时丢弃半条指令，避免污染下一条连接
                    }
                }
            }
        } catch (SocketException e) {
            // stop() 关闭 serverSocket：accept 抛异常，正常退出，不打堆栈
            System.out.println("Broker 停止监听: " + e.getMessage());
        } catch (IOException e) {
            System.err.println("Broker I/O 错误: " + e.getMessage());
            e.printStackTrace();
        } finally {
            this.status = STATUS_STOPPED;
            this.serverSocket = null;
        }
    }

    /** 停止 Broker：置 STOPPED 并关闭监听 socket，start 的 accept 循环随即退出 */
    public void stop() {
        this.status = STATUS_STOPPED;
        ServerSocket ss = this.serverSocket;
        if (ss != null && !ss.isClosed()) {
            try {
                ss.close();
            } catch (IOException ignored) {
                // 关闭失败不影响停止语义
            }
        }
    }

    /** 建监听 socket：先开 SO_REUSEADDR 再 bind；backlog 50 应对测试短连接风暴 */
    private ServerSocket createServerSocket(int port) throws IOException {
        ServerSocket serverSocket = new ServerSocket();
        serverSocket.setReuseAddress(true);
        serverSocket.bind(new InetSocketAddress(port), 50);
        return serverSocket;
    }

    /** 处理一条客户端会话：循环读取指令并应答，客户端断开时 readLine 返回 null 结束 */
    private void handleSession(Socket clientSocket, StringBuilder msg) throws IOException {
        BufferedReader in = new BufferedReader(
                new InputStreamReader(clientSocket.getInputStream(), StandardCharsets.UTF_8)
        );
        // writer 每条连接只建一次；回复后只写不 close —— close 会连 socket 一起关掉，
        // 下一次 readLine 就会抛 SocketException: Socket closed
        PrintWriter out = new PrintWriter(
                new OutputStreamWriter(clientSocket.getOutputStream(), StandardCharsets.UTF_8),
                true
        );
        ObjectMapper mapper = new ObjectMapper();
        String inputLine;
        while ((inputLine = in.readLine()) != null) {
            msg.append(inputLine);
            if (END_MARK.equals(inputLine)) {
                out.println(dispatch(msg.toString(), mapper));
                msg.setLength(0); // 复位缓冲，支持同一连接上的下一条指令
            }
        }
    }

    /** 解析并执行一条完整指令，返回 JSON 应答；单条指令失败返回 500 应答，不终结会话 */
    private String dispatch(String rawCmd, ObjectMapper mapper) {
        try {
            MqCmd cmd = MqCmd.resolve(rawCmd);
            CmdResult result = handlerRegistry.dispatch(cmd, this);
            return mapper.writeValueAsString(result);
        } catch (Exception e) {
            System.err.println("处理指令失败: " + e.getMessage());
            e.printStackTrace();
            try {
                return mapper.writeValueAsString(new CmdResult(500, "Internal Server Error", null));
            } catch (IOException ex) {
                // CmdResult 自身序列化失败时兜底固定应答
                return "{\"code\":500,\"message\":\"Internal Server Error\",\"data\":null}";
            }
        }
    }

    // ---------- 供各 MqCmd 子类调用的状态操作 ----------

    /** 发布消息，返回其在 topic 里的下标 */
    public int publish(MqMsg msg) {
        List<MqMsg> msgs = topicMsgMap.computeIfAbsent(msg.getTopic(), k -> new ArrayList<>());
        synchronized (msgs) {
            msgs.add(msg);
            return msgs.size() - 1;
        }
    }

    /** 订阅：同一 consumer 重复订阅同一 topic 只记一次 */
    public void subscribe(String topic, String consumerId) {
        List<String> subscribers =
                topicSubscribers.computeIfAbsent(topic, k -> new ArrayList<>());
        synchronized (subscribers) {
            if (!subscribers.contains(consumerId)) {
                subscribers.add(consumerId);
            }
        }
    }

    /** 退订；未订阅过返回 false */
    public boolean unsubscribe(String topic, String consumerId) {
        List<String> subscribers = topicSubscribers.get(topic);
        if (subscribers == null) {
            return false;
        }
        synchronized (subscribers) {
            return subscribers.remove(consumerId);
        }
    }

    /** 拉取 topic 的全部消息；topic 不存在返回 null */
    public List<MqMsg> fetch(String topic) {
        return topicMsgMap.get(topic);
    }

    /** 记录确认进度（每个 consumer:topic 只保留最大下标） */
    public void ack(String consumerId, String topic, int msgIndex) {
        ackIndexMap.merge(consumerId + ":" + topic, msgIndex, Math::max);
    }

    public Integer ackedIndexOf(String consumerId, String topic) {
        return ackIndexMap.get(consumerId + ":" + topic);
    }

    /** topic 的订阅者快照（测试与后续推送用） */
    public List<String> subscribersOf(String topic) {
        List<String> subscribers = topicSubscribers.get(topic);
        if (subscribers == null) {
            return new ArrayList<>();
        }
        synchronized (subscribers) {
            return new ArrayList<>(subscribers);
        }
    }

    /** topic 的消息快照；topic 不存在返回空列表 */
    public List<MqMsg> msgsOf(String topic) {
        List<MqMsg> msgs = topicMsgMap.get(topic);
        return msgs == null ? new ArrayList<>() : new ArrayList<>(msgs);
    }

    public Integer getStatus() {
        return status;
    }

    /** 实际监听端口；构造传 0 时需等 start() bind 完成后才有值，0 表示尚未就绪 */
    public int getPort() {
        return boundPort;
    }
}
