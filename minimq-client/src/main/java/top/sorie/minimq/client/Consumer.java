package top.sorie.minimq.client;

import org.apache.commons.lang3.StringUtils;
import top.sorie.minimq.common.protocol.AckCmd;
import top.sorie.minimq.common.protocol.CmdResult;
import top.sorie.minimq.common.protocol.FetchMsgCmd;
import top.sorie.minimq.common.protocol.MqCmd;
import top.sorie.minimq.common.protocol.SubscribeCmd;
import top.sorie.minimq.common.protocol.UnsubscribeCmd;
import top.sorie.minimq.common.util.IpUtil;

import java.util.UUID;

/**
 * MQ 消费者：用自身 id 作 consumerId，对 topic 做订阅 / 拉取 / 回执 / 退订。
 *
 * <p>业务方法统一返回 {@link CmdResult}：网络或协议错误通过 code 表达而不抛异常，
 * 调用方判断 code 即可（只有构造参数非法才抛 IllegalArgumentException）。
 * 与 {@link Producer} 一样，每条指令一条短连接，由 {@link Client} 负责收发。</p>
 */
public class Consumer {
    private final String id;
    private final String name;
    private final String localIp;
    private final Client client;

    public Consumer(String id, String name, String localIp, String remoteIp, Integer port) {
        if (StringUtils.isBlank(id)) {
            throw new IllegalArgumentException("id is required");
        }
        if (StringUtils.isBlank(localIp)) {
            throw new IllegalArgumentException("localIp is required");
        }
        this.id = id;
        // name 未指定时与 id 保持一致
        this.name = StringUtils.isBlank(name) ? id : name;
        this.localIp = localIp;
        this.client = new Client(remoteIp, port);
    }

    /** 指定 id/name 与远端地址，localIp 取本机地址 */
    public Consumer(String id, String name, String remoteIp, Integer port) {
        this(id, name, IpUtil.getLocalIp(), remoteIp, port);
    }

    /** id/name 自动生成，localIp 取本机地址 */
    public Consumer(String remoteIp, Integer port) {
        this(UUID.randomUUID().toString(), null, IpUtil.getLocalIp(), remoteIp, port);
    }

    /** 订阅 topic；同一 consumer 重复订阅只记一次 */
    public CmdResult subscribe(String topic) {
        SubscribeCmd cmd = new SubscribeCmd();
        cmd.setCmd(MqCmd.CMD_SUBSCRIBE);
        cmd.setTopic(topic);
        cmd.setConsumerId(id);
        return client.sendCmd(cmd);
    }

    /** 退订 topic；未订阅过时 Broker 返回错误应答 */
    public CmdResult unsubscribe(String topic) {
        UnsubscribeCmd cmd = new UnsubscribeCmd();
        cmd.setCmd(MqCmd.CMD_UNSUBSCRIBE);
        cmd.setTopic(topic);
        cmd.setConsumerId(id);
        return client.sendCmd(cmd);
    }

    /**
     * 拉取 topic 下的全部消息。
     * 成功时 {@code result.getData()} 为消息列表；topic 不存在时返回错误应答而不是空列表，
     * 避免"没有消息"和"topic 不存在"被混为一谈。
     */
    public CmdResult fetch(String topic) {
        FetchMsgCmd cmd = new FetchMsgCmd();
        cmd.setCmd(MqCmd.CMD_FETCH_MSG);
        cmd.setTopic(topic);
        return client.sendCmd(cmd);
    }

    /** 确认已消费到 msgIndex（同一 consumer:topic 只保留最大下标） */
    public CmdResult ack(String topic, int msgIndex) {
        AckCmd cmd = new AckCmd();
        cmd.setCmd(MqCmd.CMD_ACK);
        cmd.setTopic(topic);
        cmd.setMsgIndex(msgIndex);
        cmd.setConsumerId(id);
        return client.sendCmd(cmd);
    }

    public String getId() {
        return id;
    }

    public String getName() {
        return name;
    }

    public String getLocalIp() {
        return localIp;
    }
}
