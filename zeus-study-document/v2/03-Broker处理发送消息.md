# Broker 处理发送消息

## Broker 收到请求的入口

org.apache.rocketmq.remoting.netty.NettyRemotingServer.NettyServerHandler.channelRead0

## Broker 处理发送消息请求的入口

SendMessageProcessor.processRequest
> parseRequestHeader 获取请求头
> asyncSendMessage 处理消息
> io.netty.channel.ChannelOutboundInvoker.writeAndFlush(java.lang.Object) 相应处理结果

## asyncSendMessage 处理消息的过程

### 1. preSend

检查

1. Broker 启动时可以设置一个时间戳，表示这个时间点之后才会处理消息, 判断当前时间是否在这个时间点之后
2. Broker 是否有写权限, 从节点没有写权限
3. Topic 格式校验, 非空，长度，合法字符，是否为系统默认的 Topic （SCHEDULE_TOPIC_XXXX, RMQ_SYS_TRANS_HALF_TOPIC)
4. Topic 是否在 org.apache.rocketmq.broker.topic.TopicConfigManager.topicConfigTable 中存在

> 4.1 如果不存在，且允许自动创建 Topic，则创建 Topic
> 4.2 如果不存在不允许自动创建 Topic，但是这个 Topic 以 %RETRY% 开头, 则创建这个 Topic (读写队列各 1 个)

5. 发送的队列 ID 是否大于当前 Topic 的读写队列中的最大值

### 2. handleRetryAndDLQ

重试队列的处理
如果 Topic 不是 %RETRY% 开头的重试队列, 进入下一步

1. 从 Topic 的名称中截取 %RETRY% 之后的字符串, 作为消费者组名称
2. 从 org.apache.rocketmq.broker.subscription.SubscriptionGroupManager.subscriptionGroupTable
   中获取这个消费者组的配置信息 (如果配置了允许自动创建消费者组或者这个组以 CID_RMQ_SYS_, 则自动创建)
3. 从消费者组配置中获取最大重试次数(默认 16 次), 从请求头获取这个消息的重试次数, 如果重试的次数没有大于最大的重试次数,
   进入下一步

> 3.1 如果重试次数大于最大重试次数
> 3.2 %DLQ%+消费者组名称 作为新的 Topic 名称, 创建这个新的 Topic (读写队列各 1 个)
> 3.3 从 99999999 随机一个数作为队列 ID
> 3.4 将消息的 Topic 和 队列 ID 设置为前二步的新值, 进入下一步

### 3. MessageExtBrokerInner msgInner 将消息转为 MessageExtBrokerInner, 如果需要修改一下消息的一些属性

如果消息的属性中包含 TRAN_MSG， 且属性值为 true, 表示为事务消息

非事务消息 ---> org.apache.rocketmq.store.DefaultMessageStore.asyncPutMessage
事务消息 ---> org.apache.rocketmq.broker.transaction.queue.TransactionalMessageServiceImpl.asyncPrepareMessage

### DefaultMessageStore.asyncPutMessage

1. checkStoreStatus() 检查存储状态 (broker 准备关闭, 从节点无法保存消息， 当前磁盘不可写了, PageCache 繁忙)

2. checkMessage() 检查消息是否合法 (topic 长度， 自定义属性字符串长度)

3. org.apache.rocketmq.store.CommitLog.asyncPutMessage 消息写入

4. 更新统计信息，相应处理结果

#### org.apache.rocketmq.store.CommitLog.asyncPutMessage 消息写入

1. 给 MessageExtBrokerInner 设置当前的时间戳， crc 值
2. 非事务消息 或 事务提交消息 同时设置了延迟级别(定时消息)
   > 修正延迟级别 (不能超过 Broker 配置的最大延迟级别), 根据延迟级别 - 1 得到存储的队列 id, 将原本的 topic 和队列 ID 存放到消息的属性中
   > 修改消息的 topic 为 SCHEDULE_TOPIC_XXXX, 队列 ID 为上一步得到的存储队列 ID
3. 从 ThreadLocal 中获取对应的 PutMessageThreadLocal 对象, 把 MessageExtBrokerInner 的内容转为 转为 ByteBuf 放到里面的 MessageExtEncoder.ByteBuf 中 
4. 获取最新的代表 commitLog 的 MappedFile 对象 (获取不到或文件满了，重新创建一个), 调用其 appendMessage 方法将消息写入 commitLog
5. 提交一个刷盘请求
6. 提交一个主从同步请求

## 涉及到文件的操作如下

1. 写入 CommitLog
2. 更新 CheckPoint
3. 同步从节点
4. 写入 consumeQueue
5. 更新 CheckPoint
6. 写入 indexFile (如果有 key)
7. 更新 CheckPoint
