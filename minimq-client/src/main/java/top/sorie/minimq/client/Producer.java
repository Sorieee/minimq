package top.sorie.minimq.client;

import top.sorie.minimq.common.protocol.CmdResult;
import top.sorie.minimq.common.protocol.PublishCmd;
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

    /** 未指定远端地址的构造入口；M1 引入 ClientConfig 后会改为必须显式配置 */
    public Producer() {
        this.id = UUID.randomUUID().toString();
        this.name = this.id;
        this.localIp = IpUtil.getLocalIp();
    }

    public CmdResult sendMsg(String topic, String msg) {
        return this.client.sendCmd(new PublishCmd(topic, msg));
    }
}
