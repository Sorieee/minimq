package top.sorie.minimq.broker;

import top.sorie.minimq.common.model.MqMsg;

import java.util.List;

/**
 * 指令处理器可用的服务端能力。
 *
 * <p>M0 由 {@link Broker} 实现，保证业务行为与 0.0.1 完全一致；
 * M2 会把实现迁到独立的 BrokerService，handler 不需要感知。</p>
 */
public interface BrokerContext {

    /** 发布消息，返回其在 topic 内的下标 */
    int publish(MqMsg msg);

    /** 订阅：同一 consumer 重复订阅同一 topic 只记一次 */
    void subscribe(String topic, String consumerId);

    /** 退订；未订阅过返回 false */
    boolean unsubscribe(String topic, String consumerId);

    /** 拉取 topic 的全部消息；topic 不存在返回 null */
    List<MqMsg> fetch(String topic);

    /** 记录确认进度（同一 consumer:topic 只保留最大下标） */
    void ack(String consumerId, String topic, int msgIndex);

    /** 已确认的最大下标；从未确认返回 null */
    Integer ackedIndexOf(String consumerId, String topic);
}
