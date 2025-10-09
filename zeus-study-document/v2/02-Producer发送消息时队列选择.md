## 核心逻辑

```java
private SendResult sendDefaultImpl(
        Message msg,
        final CommunicationMode communicationMode,
        final SendCallback sendCallback,
        final long timeout
) throws MQClientException, RemotingException, MQBrokerException, InterruptedException {

    // 1. 验证消息的合法性
    checkMessage(msg);

    // 2. 通过 Topic 名称获取 Topic 路由信息
    TopicPublishInfo topicPublishInfo = tryToFindTopicPublishInfo(msg.getTopic());

    // 3. 在同步发送的情况下，获取消息发送失败时的重试次数, 异步默认为 1，即不重试, 同步默认为 2
    int timesTotal = getRetryTimesWhenSendFailedWhenSync(communicationMode);

    // 4. 获取发送的队列, 根据路由信息和上次发送的 Broker 名称 (上次这个 Broker 发送失败，进行排除) 选择一个消息队列
    MessageQueue mqSelected = selectOneMessageQueue(topicPublishInfo, lastBrokerName);

    // 5. 进行消息发送
    SendResult sendResult = sendKernelImpl();

    // 6. 如果是同步发送且发送失败，则进行重试
    retryIfSyncAndSendFailed(timesTotal, communicationMode, sendResult);

    // 7. 返回发送结果, 异步发送返回 null, 同步发送返回 SendResult
    return isSync() ? sendResult : null;
}
```

这段伪代码的核心

1. TopicPublishInfo 的数据如何获取
2. 发送的队列如何选择

## TopicPublishInfo 的数据如何获取

```java
public class DefaultMQProducerImpl {

    // key: Topic 名称, value: Topic 路由信息
    private final ConcurrentMap<String, TopicPublishInfo> topicPublishInfoTable = new ConcurrentHashMap<String, TopicPublishInfo>();

    private MQClientInstance mQClientFactory;

}
```

在 DefaultMQProducerImpl 维护了一份 Topic 对应的 Topic 路由信息的缓存,
维护的地方有 2 个
> 1. 发送消息时, 如果缓存中没有对应的 Topic 路由信息, 则会从 NameServer 获取 (requestCode: GET_ROUTEINFO_BY_TOPIC = 105)
     org.apache.rocketmq.client.impl.producer.DefaultMQProducerImpl.tryToFindTopicPublishInfo
> 2. 内部的属性 MQClientInstance 的 start 方法，会启动一个 30s 的定时器, 定时调用
     org.apache.rocketmq.client.impl.MQClientAPIImpl.getTopicRouteInfoFromNameServer(java.lang.String, long) 更新这个缓存

TopicPublishInfo 的格式如下 (broker-lcn-1 和 broker-lcn-2 都是主 Broker, 没有从节点)

```json 
{
  "haveTopicRouterInfo": true,
  "messageQueueList": [
    {
      "brokerName": "broker-lcn-1",
      "queueId": 0,
      "topic": "TopicTest"
    },
    {
      "brokerName": "broker-lcn-1",
      "queueId": 1,
      "topic": "TopicTest"
    },
    {
      "brokerName": "broker-lcn-1",
      "queueId": 2,
      "topic": "TopicTest"
    },
    {
      "brokerName": "broker-lcn-1",
      "queueId": 3,
      "topic": "TopicTest"
    },
    {
      "brokerName": "broker-lcn-2",
      "queueId": 0,
      "topic": "TopicTest"
    },
    {
      "brokerName": "broker-lcn-2",
      "queueId": 1,
      "topic": "TopicTest"
    },
    {
      "brokerName": "broker-lcn-2",
      "queueId": 2,
      "topic": "TopicTest"
    },
    {
      "brokerName": "broker-lcn-2",
      "queueId": 3,
      "topic": "TopicTest"
    }
  ],
  "orderTopic": false,
  "sendWhichQueue": {},
  "topicRouteData": {
    "brokerDatas": [
      {
        "brokerAddrs": {
          "0": "192.168.2.10:10911"
        },
        "brokerName": "broker-lcn-1",
        "cluster": "LcnCluster"
      },
      {
        "brokerAddrs": {
          "0": "192.168.2.11:10911"
        },
        "brokerName": "broker-lcn-2",
        "cluster": "LcnCluster"
      }
    ],
    "filterServerTable": {},
    "queueDatas": [
      {
        "brokerName": "broker-lcn-1",
        "perm": 7,
        "readQueueNums": 4,
        "topicSysFlag": 0,
        "writeQueueNums": 4
      },
      {
        "brokerName": "broker-lcn-2",
        "perm": 7,
        "readQueueNums": 4,
        "topicSysFlag": 0,
        "writeQueueNums": 4
      }
    ]
  }
}
```

## 发送的队列如何选择

```java
public class DefaultMQProducerImpl {
    private MQFaultStrategy mqFaultStrategy = new MQFaultStrategy();
}
```

在 DefaultMQProducerImpl 有一个 MQFaultStrategy 实例, 用于确定 Producer 的队列选择
默认就是按照当前 Topic 下所有的队列进行轮询选择, 但是如果启用了延迟故障功能, 则会避开那些**队列的 Broker**高延迟或者有故障,
直接选择下一个，直到找到一个可用

```java
public class MQFaultStrategy {

    /**
     * 是否启用延迟故障功能
     * 对于高延迟的、有故障的 broker，都会保存下来，并有一个不可用时间段，发送消息的时会临时避开这个 broker
     * 没启用的情况, 顺着已有的队列循环, 会存在上次使用了 broker-a 的队列 1,下一次使用到 broker-a 的队列 2
     * 可以通过 producer.setSendLatencyFaultEnable(true); 来启用
     */
    private boolean sendLatencyFaultEnable = false;

    public MessageQueue selectOneMessageQueue(final TopicPublishInfo tpInfo, final String lastBrokerName) {

        // 如果启用了延迟故障功能
        if (this.sendLatencyFaultEnable) {
            try {
                // 获取本次发送消息的队列的索引
                int index = tpInfo.getSendWhichQueue().incrementAndGet();

                // 循环所有的队列, 直到找到一个队列的 Broker 是可用的， 返回这个队列
                for (int i = 0; i < tpInfo.getMessageQueueList().size(); i++) {
                    int pos = Math.abs(index++) % tpInfo.getMessageQueueList().size();
                    if (pos < 0)
                        pos = 0;
                    MessageQueue mq = tpInfo.getMessageQueueList().get(pos);
                    // 这个 Broker 是可用的
                    if (latencyFaultTolerance.isAvailable(mq.getBrokerName()))
                        return mq;
                }

                // 如果所有的 Broker 都不可用, 则在所有不可用的 Broker 中选择一个，按照下面的规则获取
                // 1. 选择一个延迟最小的 Broker, 如果延迟时间都相同, 则取再次可用时间距离当前时间最近的 Broker
                final String notBestBroker = latencyFaultTolerance.pickOneAtLeast();
                int writeQueueNums = tpInfo.getQueueIdByBroker(notBestBroker);
                if (writeQueueNums > 0) {
                    final MessageQueue mq = tpInfo.selectOneMessageQueue();
                    if (notBestBroker != null) {
                        return new MessageQueue(mq.getTopic(), notBestBroker, tpInfo.getSendWhichQueue().incrementAndGet() % writeQueueNums);
                    } else {
                        return mq;
                    }
                } else {
                    latencyFaultTolerance.remove(notBestBroker);
                }
            } catch (Exception e) {
                log.error("Error occurred when selecting message queue", e);
            }
            // 兜底，直接调用 TopicPublishInfo 的 selectOneMessageQueue 方法获取一个
            return tpInfo.selectOneMessageQueue();
        }

        // 直接调用 TopicPublishInfo 的 selectOneMessageQueue 方法
        return tpInfo.selectOneMessageQueue(lastBrokerName);
    }
}
```

TopicPublishInfo 的队列选择逻辑

```java
public class TopicPublishInfo {

    // 当前线程发送消息时选择的队列索引，使用 ThreadLocal 以支持多线程环境下的负载均衡
    private volatile ThreadLocalIndex sendWhichQueue = new ThreadLocalIndex();

    public MessageQueue selectOneMessageQueue(final String lastBrokerName) {
        if (lastBrokerName == null) {
            // 获取上次选择的队列的下一个位置的队列
            return selectOneMessageQueue();
        }
        for (int i = 0; i < this.messageQueueList.size(); i++) {
            // 循环选择下一个队列，直到找到一个不属于上次使用的 Broker 的队列
            MessageQueue mq = selectOneMessageQueue();
            if (!mq.getBrokerName().equals(lastBrokerName)) {
                return mq;
            }
        }
        // 兜底操作
        return selectOneMessageQueue();
    }

    // 循环选择一个消息队列, 
    public MessageQueue selectOneMessageQueue() {
        // sendWhichQueue + 1, 从上次选择的位置的下一个位置开始选择，如果到了末尾则从头开始
        int index = this.sendWhichQueue.incrementAndGet();
        int pos = Math.abs(index) % this.messageQueueList.size();
        if (pos < 0)
            pos = 0;
        return this.messageQueueList.get(pos);
    }
}
```

### Broker 的延迟和可用，如何维护的

在调用 DefaultMQProducerImpl.sendKernelImpl 方法发送消息后, 会更新这个 Broker 的延迟和可用状态

```java
private SendResult sendDefaultImpl() {

    try {
        // 开始发送的时间戳
        long beginTimestampPrev = System.currentTimeMillis();

        // 5. 进行消息发送   
        SendResult sendResult = sendKernelImpl();
        // 更新这个 Broker 的延迟和可用状态
        // 最后一个参数, 表示是否是隔离的 Broker, 发送成功则不是隔离的 Broker
        // updateFaultItem 方法直接就是调用 org.apache.rocketmq.client.latency.MQFaultStrategy.updateFaultItem 方法
        this.updateFaultItem(mq.getBrokerName(), System.currentTimeMillis() - beginTimestampPrev, false);
    } catch (Exception e) {
        // 更新这个 Broker 的延迟和可用状态
        this.updateFaultItem(mq.getBrokerName(), System.currentTimeMillis() - beginTimestampPrev, false);
    }
}
```

```java
public class MQFaultStrategy {

    // 延迟的时间
    private long[] latencyMax = {50L, 100L, 550L, 1000L, 2000L, 3000L, 15000L};
    // 不可用的时间
    private long[] notAvailableDuration = {0L, 0L, 30000L, 60000L, 120000L, 180000L, 600000L};

    // Broker 的状态，是否可用，延迟时间和下次可用时间的封装对象
    private final LatencyFaultTolerance<String> latencyFaultTolerance = new LatencyFaultToleranceImpl();

    public void updateFaultItem(final String brokerName, final long currentLatency, boolean isolation) {
        // 如果启用了延迟故障功能
        if (this.sendLatencyFaultEnable) {
            // 如果这个是隔离的 Broker, 则使用 30 秒的不可用时间, 否则根据当前的延迟时间计算不可用时间
            long duration = computeNotAvailableDuration(isolation ? 30000 : currentLatency);
            // 更新这个 Broker 的延迟和不可用时间到 latencyFaultTolerance 中 (下次可用时间 = 当前时间 + duration)
            this.latencyFaultTolerance.updateFaultItem(brokerName, currentLatency, duration);
        }
    }

    private long computeNotAvailableDuration(final long currentLatency) {
        // 根据当前的延迟时间，找到第一个大于配置的延迟时间的位置，返回对应的位置的不可用时间
        for (int i = latencyMax.length - 1; i >= 0; i--) {
            if (currentLatency >= latencyMax[i])
                return this.notAvailableDuration[i];
        }
        return 0;
    }
}
```

## Producer 发送消息到指定队列

可以调用
org.apache.rocketmq.client.producer.DefaultMQProducer.send(
org.apache.rocketmq.common.message.Message,
org.apache.rocketmq.client.producer.MessageQueueSelector,
java.lang.Object
)

核心是自定义 MessageQueueSelector 接口  (参数3, 作为参数给到 MessageQueueSelector.select 方法)

```java
public interface MessageQueueSelector {
    MessageQueue select(final List<MessageQueue> mqs, final Message msg, final Object arg);
}
```

选择队列的核心逻辑如下

```java
private SendResult sendSelectImpl(
        Message msg,
        MessageQueueSelector selector,
        Object arg,
        final CommunicationMode communicationMode,
        final SendCallback sendCallback, final long timeout
) throws MQClientException, RemotingException, MQBrokerException, InterruptedException {

    // 1. 验证消息的合法性
    checkMessage(msg);

    // 2. 通过 Topic 名称获取 Topic 路由信息
    TopicPublishInfo topicPublishInfo = tryToFindTopicPublishInfo(msg.getTopic());

    // 3. 从 Topic 路由信息中获取所有的消息队列
    List<MessageQueue> messageQueueList = parseMessageQueuesFromTopicPublishInfo(topicPublishInfo);

    // 4. 通过自定义的 MessageQueueSelector 选择一个消息队列, 然后通过 queueWithNamespace 方法添加命名空间前缀
    MessageQueue mq = queueWithNamespace(selector.select(messageQueueList, userMessage, arg));

    // 5. 进行消息发送
    SendResult sendResult = sendKernelImpl();
}
```

逻辑差不多

## 极端情况

因为 Topic 路由信息是有缓存的
> Broker 创建后, 会立即同步给所有的 NameServer, 同时会定时上报
> Producer 会在请求时，没有对应的 Topic 路由信息, 则会从 NameServer 获取, 进行缓存 + 定时更新

那么可能存在 Topic 已经在 Broker 创建了, 同时在同步 NameServer 中， Producer 发送消息时，本身缓存没有，从 NameServer
获取时，NameServer 还没有同步过来这个 Topic 的路由信息
==> Producer 重试

Topic 从 Broker 删除了, 但是 Producer 定时更新还没有更新到, 发送消息到这个 Topic
==> 消息最终时 Broker 处理， 这时 Broker 判断到没有这个 Topic, 会返回 Topic 不存在的错误 (没有开启自动创建 Topic
的情况下)
