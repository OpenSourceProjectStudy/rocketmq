**RocketMQ 客户端再平衡详解**

- RocketMQ 的 Push 模式如何精准调整消费队列分布？
- 再平衡过程涉及哪些线程、哪些数据结构？
- 什么时候触发？怎样保证负载均衡又不丢消息？
- 自定义分配策略时需要注意什么？

下面我们将沿着一条“消息队列被重新分配”的故事线，逐段拆解其实现细节。

---

## 再平衡的参与者

- `RebalanceService`：定时唤醒的后台线程，继承自 `ServiceThread`（client/src/main/java/org/apache/rocketmq/client/impl/consumer/RebalanceService.java:24）。
- `MQClientInstance`：客户端容器，集中管理所有消费者，并在唤醒时遍历调用每个消费者的再平衡入口（client/src/main/java/org/apache/rocketmq/client/impl/factory/MQClientInstance.java:998）。
- `DefaultMQPushConsumerImpl`：Push 消费实现类，持有真正干活的 `RebalancePushImpl`（client/src/main/java/org/apache/rocketmq/client/impl/consumer/DefaultMQPushConsumerImpl.java:641）。
- `RebalanceImpl` / `RebalancePushImpl`：再平衡核心，负责计算新分配集合、维护本地 `ProcessQueue`（client/src/main/java/org/apache/rocketmq/client/impl/consumer/RebalanceImpl.java:235）。
- `AllocateMessageQueueStrategy` 及其默认实现 `AllocateMessageQueueAveragely`：决定队列在消费者间的分布（client/src/main/java/org/apache/rocketmq/client/consumer/rebalance/AllocateMessageQueueAveragely.java:39）。
- `ProcessQueue`、`PullRequest` 与 `PullMessageService`：分别承载本地消费窗口、一次拉取动作和真正的异步拉取线程（client/src/main/java/org/apache/rocketmq/client/impl/consumer/RebalanceImpl.java:401, client/src/main/java/org/apache/rocketmq/client/impl/consumer/PullMessageService.java:30）。

---

## 再平衡何时触发？

- **定时触发**：`RebalanceService` 默认每 20 秒唤醒一次，可通过系统属性 `rocketmq.client.rebalance.waitInterval` 调整（client/src/main/java/org/apache/rocketmq/client/impl/consumer/RebalanceService.java:25,39）。
- **事件驱动**：路由元数据变化、消费者增删等情况下，调用 `MQClientInstance.rebalanceImmediately()` 立刻触发一次（client/src/main/java/org/apache/rocketmq/client/impl/factory/MQClientInstance.java:994）。

定时与实时结合，保证既不过于频繁造成抖动，也能在拓扑突变时快速收敛。

---

## 主流程概览

1. `RebalanceService` 唤醒后调用 `MQClientInstance.doRebalance()`。
2. 客户端工厂遍历所有 `MQConsumerInner`，逐个执行 `doRebalance()`。
3. `DefaultMQPushConsumerImpl#doRebalance` 如果未暂停，委托给 `RebalancePushImpl#doRebalance`（client/src/main/java/org/apache/rocketmq/client/impl/consumer/DefaultMQPushConsumerImpl.java:1072）。
4. `RebalanceImpl#doRebalance`：
   - 逐个主题处理；
   - 根据消息模型选择广播或集群分配逻辑；
   - 集群模式下，通过分配策略计算本机应该持有的队列集合；
   - 更新本地 `ProcessQueue`，生成新 `PullRequest`。
5. `RebalancePushImpl#dispatchPullRequest` 将拉取请求交给 `DefaultMQPushConsumerImpl`，后者再投递到 `PullMessageService` 执行线程池（client/src/main/java/org/apache/rocketmq/client/impl/consumer/RebalancePushImpl.java:230, client/src/main/java/org/apache/rocketmq/client/impl/consumer/DefaultMQPushConsumerImpl.java:525）。

---

## 每个主题的计算细节

### 1. 获取最新视图

- `topicSubscribeInfoTable`：NameServer 返回的最新队列列表，按主题维护。
- `subscriptionInner`：客户端本地订阅信息，包含过滤表达式、版本等。

两者均保存在 `RebalanceImpl` 内部线程安全的 `ConcurrentMap` 中（client/src/main/java/org/apache/rocketmq/client/impl/consumer/RebalanceImpl.java:55,66）。

### 2. 广播 vs 集群

- **广播**：无需计算，直接使用全部队列；关键是确保新旧差集正确更新（client/src/main/java/org/apache/rocketmq/client/impl/consumer/RebalanceImpl.java:259-277）。
- **集群**：取出同组消费者 ID 列表 `cidAll`，以及主题所有队列 `mqAll`，二者排序后交给策略类（client/src/main/java/org/apache/rocketmq/client/impl/consumer/RebalanceImpl.java:279-320）。

### 3. 队列分配策略

默认 `AllocateMessageQueueAveragely`，核心逻辑：

```java
int index = cidAll.indexOf(currentCID);
int mod = mqAll.size() % cidAll.size();
int averageSize = mqAll.size() > cidAll.size()
    ? (mod > 0 && index < mod ? mqAll.size() / cidAll.size() + 1 : mqAll.size() / cidAll.size())
    : 1;
int startIndex = (mod > 0 && index < mod) ? index * averageSize : index * averageSize + mod;
int range = Math.min(averageSize, mqAll.size() - startIndex);
```

随后取 `mqAll` 中从 `startIndex` 开始的 `range` 个连续队列，形成本客户端的分配结果（client/src/main/java/org/apache/rocketmq/client/consumer/rebalance/AllocateMessageQueueAveragely.java:41-70）。

### 4. 更新本地 ProcessQueue

`updateProcessQueueTableInRebalance()` 负责差异化更新（client/src/main/java/org/apache/rocketmq/client/impl/consumer/RebalanceImpl.java:355-444）：

- **移除已不属于本客户端的队列**：标记 `ProcessQueue` 为 dropped，调用 `removeUnnecessaryMessageQueue()` 清理，并从 `processQueueTable` 中删除。
- **处理拉取超时**：若 `ProcessQueue#isPullExpired()` 为真，按消费类型采取修复措施，避免因为长期无拉取造成堆积。
- **新增队列**：
  - 顺序消费场景先尝试向 Broker 加锁（client/src/main/java/org/apache/rocketmq/client/impl/consumer/RebalanceImpl.java:404）。
  - 清理旧 offset（避免残留）。
  - 新建 `ProcessQueue`，设置锁状态。
  - 计算起始 offset：依据 `consumeFromWhere` 和真实存储偏移（client/src/main/java/org/apache/rocketmq/client/impl/consumer/RebalancePushImpl.java:153-227）。
  - 插入 `processQueueTable`。
  - 构造 `PullRequest`，填入消费组、队列、起始 offset、ProcessQueue。

### 5. 派发拉取请求

- `RebalancePushImpl#dispatchPullRequest` 简单地遍历新请求，调用 `DefaultMQPushConsumerImpl.executePullRequestImmediately`。
- 后者将任务丢给 `PullMessageService` 的阻塞队列，最终由后台线程串行处理拉取，保证再平衡后立刻开始从新队列抓取数据（client/src/main/java/org/apache/rocketmq/client/impl/consumer/DefaultMQPushConsumerImpl.java:525, client/src/main/java/org/apache/rocketmq/client/impl/consumer/PullMessageService.java:59-107）。

---

## 顺序消费的额外步骤

顺序消费要求同一队列同一时刻只被一个实例消费。`RebalanceImpl#lock()` 会：

1. 通过 `MQClientInstance.findBrokerAddressInSubscribe()` 找到当前 Broker 地址；
2. 发送 `lockBatchMQ` 请求；
3. Broker 返回已被锁定的队列集合，客户端将成功结果写入 `ProcessQueue`（client/src/main/java/org/apache/rocketmq/client/impl/consumer/RebalanceImpl.java:152-177）。

若锁定失败，则跳过该队列等待下一轮，保持严格顺序语义。

---

## 动态阈值调整

当队列数变化时，Push consumer 会在 `messageQueueChanged()` 中根据当前持有队列数量重新分摊 `pullThresholdForQueue`、`pullThresholdSizeForQueue`，确保总流控能力大致恒定，防止新增队列导致单队列吞吐过低或总体超限（client/src/main/java/org/apache/rocketmq/client/impl/consumer/RebalancePushImpl.java:50-77）。

---

## 确保 Offset 正确

- 新队列分配时根据策略与历史 offset 计算起点，若首次消费则取 `maxOffset`、`minOffset` 或按时间搜索（client/src/main/java/org/apache/rocketmq/client/impl/consumer/RebalancePushImpl.java:153-227）。
- `removeDirtyOffset()` 在重新分配前清理旧值，防止 offset 残留造成重复或跳读（client/src/main/java/org/apache/rocketmq/client/impl/consumer/RebalancePushImpl.java:137）。
- 再平衡完成后仍由 `OffsetStore.persistAll()` 定期刷盘，配合 Broker 端的消费进度保证一致性（client/src/main/java/org/apache/rocketmq/client/impl/consumer/DefaultMQPushConsumerImpl.java:1080-1088）。

---

## 自定义分配策略的小贴士

- 所有策略共享排序好的 `mqAll`、`cidAll`。务必保持确定性，否则不同实例之间结果不一致会导致重复消费。
- 返回集合需是当前客户端负责的完整列表，RocketMQ 不会自动补齐。
- 避免出现策略抛异常或返回 null，默认实现对异常有防护，但最终会导致继续沿用旧分配，延长收敛时间。
- 顺序消费时还需考虑 Broker 端锁粒度，确保策略不会让同一队列同时映射到多个消费者。

---

## 常见问答

- **为什么还需要周期性再平衡？**  
  即使没有拓扑变化，也可能因为网络短暂闪断或拉取超时导致个别队列长时间闲置。周期检查可主动修复异常状态。

- **拉取线程为什么是单线程？**  
  每个 `PullMessageService` 线程串行消费 `PullRequest`，但真正处理消息在下游消费线程池完成；队列级别的顺序在 `ProcessQueue` 内部实现。

- **广播模式需要锁吗？**  
  不需要，广播模式每个客户端都处理所有队列，不存在竞争，自然不会涉及 `lock()`。

---

## 总结

RocketMQ 客户端的再平衡流程利用固定周期 + 事件触发双保险，围绕 `RebalanceImpl` 构建了完整的数据结构与流程：

1. **触发**：`RebalanceService` 驱动，工厂遍历所有消费者；
2. **计算**：获取最新路由 & 消费者列表，策略计算目标队列集；
3. **迁移**：差集更新 `ProcessQueue`，顺序消费需先锁定；
4. **启动消费**：为新增队列生成 `PullRequest`，交由拉取线程及时拉取；
5. **守护**：动态阈值、offset 计算、防抖机制，共同保障负载均衡与数据一致性。

理解这一链路，不仅有助于排查消费不均衡、队列积压等问题，也为自定义分配策略或二次开发打下基础。

---

## Broker 端的配合机制

客户端再平衡能够奏效，离不开 Broker 侧的多项支持逻辑：

- **心跳注册**：`ClientManageProcessor` 在收到 `HEART_BEAT` 时解析消费者上报的订阅数据并调用 `ConsumerManager.registerConsumer`，把最新的连接、订阅信息维护到组级缓存中（broker/src/main/java/org/apache/rocketmq/broker/processor/ClientManageProcessor.java:74, broker/src/main/java/org/apache/rocketmq/broker/client/ConsumerManager.java:103）。
- **组内元数据维护**：`ConsumerManager` 以 `ConsumerGroupInfo` 记录每个通道与 `clientId` 的映射、订阅表等，为客户端查询成员信息提供数据源（broker/src/main/java/org/apache/rocketmq/broker/client/ConsumerManager.java:44, broker/src/main/java/org/apache/rocketmq/broker/client/ConsumerGroupInfo.java:46）。
- **成员变更通知**：注册或注销导致成员变化时，`DefaultConsumerIdsChangeListener` 触发 `Broker2Client.notifyConsumerIdsChanged` 下发 `NOTIFY_CONSUMER_IDS_CHANGED`，促使客户端立刻发起再平衡（broker/src/main/java/org/apache/rocketmq/broker/client/DefaultConsumerIdsChangeListener.java:48, broker/src/main/java/org/apache/rocketmq/broker/client/net/Broker2Client.java:84）。
- **成员查询接口**：客户端在集群模式下会调用 `GET_CONSUMER_LIST_BY_GROUP`，`ConsumerManageProcessor.getConsumerListByGroup` 从 `ConsumerManager` 返回在线 `clientId` 列表作为分配策略的输入（broker/src/main/java/org/apache/rocketmq/broker/processor/ConsumerManageProcessor.java:70）。
- **顺序锁管理**：Broker 暴露 `lockBatchMQ` / `unlockBatchMQ` 接口；`AdminBrokerProcessor` 调用 `RebalanceLockManager` 维护 `{MessageQueue → clientId}` 的锁表并处理过期回收，保证顺序消费时一个队列只被单客户端持有（broker/src/main/java/org/apache/rocketmq/broker/processor/AdminBrokerProcessor.java:661, broker/src/main/java/org/apache/rocketmq/broker/client/rebalance/RebalanceLockManager.java:30）。
- **离线清理**：`ConsumerManager.scanNotActiveChannel` 周期清理超时未心跳的连接并注销空组，避免客户端使用过期成员数据导致再平衡失准（broker/src/main/java/org/apache/rocketmq/broker/client/ConsumerManager.java:153）。

客户端依赖这些 Broker 提供的实时元数据、通知与锁服务，才能确保队列分配快速收敛、顺序语义得以保证。
