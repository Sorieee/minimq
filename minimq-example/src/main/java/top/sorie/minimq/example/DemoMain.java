package top.sorie.minimq.example;

import top.sorie.minimq.broker.Broker;

/**
 * 最小可运行示例：启动一个 Broker 并保持进程存活，客户端可用
 * {@code Producer}/{@code Consumer} 连接 127.0.0.1:19900 体验发布与拉取。
 */
public final class DemoMain {

    private DemoMain() {
    }

    public static void main(String[] args) throws InterruptedException {
        Broker broker = new Broker();
        Thread brokerThread = new Thread(broker::start, "demo-broker");
        brokerThread.start();
        Runtime.getRuntime().addShutdownHook(new Thread(broker::stop, "demo-shutdown"));

        System.out.println("minimq broker listening on port " + Broker.PORT);
        brokerThread.join();
    }
}
