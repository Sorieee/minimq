package top.sorie.minimq.common;

import top.sorie.minimq.broker.cmd.MqCmd;
import top.sorie.minimq.broker.cmd.PublishCmd;
import top.sorie.minimq.broker.cmd.result.CmdResult;
import top.sorie.minimq.client.Client;
import top.sorie.minimq.common.util.IpUtil;

import java.util.UUID;

public class Producer {
    private String id;
    private String name;
    private String localIp;
    private Client client;
    public Producer(String id, String name, String localIp, String remoteIp, Integer port) {
        this.id = id;
        this.name = name;
        this.localIp = localIp;
        this.client = new Client(remoteIp, port);
    }
    public Producer() {
        this.id = UUID.randomUUID().toString();
        this.name = this.id;
        this.localIp = IpUtil.getLocalIp();
    }
    public CmdResult sendMsg(String topic, String msg) {
        return this.client.sendCmd(new PublishCmd(topic, msg));
    }
}
