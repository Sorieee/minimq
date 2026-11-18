package top.sorie.minimq.client;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang3.StringUtils;
import top.sorie.minimq.common.protocol.CmdResult;
import top.sorie.minimq.common.protocol.MqCmd;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.nio.charset.StandardCharsets;

/**
 * MQ 客户端：每次 sendCmd 建一条短连接，把 MqCmd 序列化发给 Broker 并收回 CmdResult。
 * 显式 UTF-8，避免 Windows 默认 GBK 造成乱码。
 */
public class Client {
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final int READ_TIMEOUT_MS = 10_000;

    private final String ip;
    private final Integer port;

    public Client(String ip, Integer port) {
        if (StringUtils.isBlank(ip) || port == null || port <= 0) {
            throw new IllegalArgumentException("ip and port are required");
        }
        this.ip = ip;
        this.port = port;
    }

    /**
     * 发送一条指令并返回 Broker 的应答。
     * 连接失败 / 应答缺失 / 应答解析失败都返回 {@link CmdResult#CODE_ERROR} 应答，不抛异常，
     * 调用方只需判断 code。
     */
    public CmdResult sendCmd(MqCmd cmd) {
        if (cmd == null || cmd.getCmd() == null) {
            return new CmdResult(CmdResult.CODE_ERROR, "cmd and cmd type are required", null);
        }
        try (Socket socket = new Socket(ip, port)) {
            socket.setSoTimeout(READ_TIMEOUT_MS);
            PrintWriter out = new PrintWriter(
                    new java.io.OutputStreamWriter(socket.getOutputStream(), StandardCharsets.UTF_8), true);
            BufferedReader in = new BufferedReader(
                    new InputStreamReader(socket.getInputStream(), StandardCharsets.UTF_8));

            // toWireFormat 已含 JSON 行 + </end> 行
            out.println(cmd.toWireFormat());

            String reply = in.readLine();
            if (StringUtils.isBlank(reply)) {
                return new CmdResult(CmdResult.CODE_ERROR, "no reply from broker", null);
            }
            return MAPPER.readValue(reply, CmdResult.class);
        } catch (IOException e) {
            // 连接被拒 / 超时 / 读写失败：统一作为错误应答返回
            return new CmdResult(CmdResult.CODE_ERROR, "send cmd failed: " + e.getMessage(), null);
        } catch (Exception e) {
            return new CmdResult(CmdResult.CODE_ERROR, "parse reply failed: " + e.getMessage(), null);
        }
    }

    public String getIp() {
        return ip;
    }

    public Integer getPort() {
        return port;
    }
}
