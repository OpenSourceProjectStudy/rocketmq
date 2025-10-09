# Topic 的创建

在 RocketMQ 中，Topic 的创建有 2 种方式:
> 1. 自动创建, Broker 配置 `autoCreateTopicEnable` 默认为 true, 当 Producer 或 Consumer 发送或订阅一个不存在的 Topic 时,
     Broker 会自动创建该 Topic (不建议使用)
> 2. 手动创建, 通过 `mqadmin` 命令行工具或 RocketMQ 提供的管理 API 来创建 Topic

## 使用 mqadmin 命令行工具创建 Topic

使用 `mqadmin` 命令行工具创建 Topic 的命令格式如下:

```sh
./mqadmin updateTopic -n <nameserver_address> -c <cluster_name> -t <topic_name> [-r <read_queue_nums>] [-w <write_queue_nums>] [-p <perm>]
./mqadmin updateTopic -n 192.168.77.129:9876 -c DefaultCluster -t TestTopic
```

备注:
> 项目启动: org.apache.rocketmq.tools.command.MQAdminStartup
> 项目启动时, 会将支持的命令缓存下来, 后面收到对应的命令时, 执行对于的命令类的逻辑即可

创建 Topic 的命令类是: org.apache.rocketmq.tools.command.topic.UpdateTopicSubCommand

核心逻辑

### MQAdmin

```java
public void execute(final CommandLine commandLine, final Options options, RPCHook rpcHook) throws SubCommandException {
    // 创建 Topic
    TopicConfig topicConfig = new TopicConfig();

    // 1. 设置默认值
    setDefaultValue(topicConfig);

    // 2. 解析命令行参数, 设置 TopicConfig 属性
    setCustomValue(commandLine, topicConfig);

    // 3. 输入的命令中 -b -c 2个必填其一, -b: 给单独的某个 Broker 创建 Topic, -c: 给集群创建 Topic
    if (commandLine.hasOption('b')) {
        // 省略, 正常不会出现单独给某个 Broker 创建 Topic 的场景
        return;
    }

    if (commandLine.hasOption('c')) {
        // 4. 获取制定集群下的所有 Broker 主节点
        Set<String> masterBrokerSet = fetchMasterAddrByClusterName(commandLine.getOptionValue('c'));
        // 5. 给集群下的所有 Broker 主节点创建 Topic
        for (String masterBrokerAddr : masterBrokerSet) {
            // 创建操作 给对应的 Broker 节点发送了一个 UPDATE_AND_CREATE_TOPIC = 17
            createAndUpdateTopicConfig(masterBrokerAddr, topicConfig);
        }

        // 进行顺序队列配置
        if (isOrder) {
            // 如果制定了 Topic 里面是顺序队列, 更新队列的为顺序队列 
            createOrUpdateOrderConf();
        }
    }
}
```

### Broker

```java
private synchronized RemotingCommand updateAndCreateTopic(ChannelHandlerContext ctx, RemotingCommand request) throws RemotingCommandException {
    // 1. topic 的各种校验, 名称是否合法，自定义 Topic 等
    validateTopic();

    // 2. 将最新的配置更新到 org.apache.rocketmq.broker.topic.TopicConfigManager.topicConfigTable 中
    // topicConfigTable key: Topic 名称, value: TopicConfig 对象 (Topic 的配置)
    updateTopicConfig();

    // 3. 将 topicConfigTable 持久化到 ${usr.home}/store/config/topics.json
    persistTopicConfigTable();

    // 4. 将新增的 TopicConfig 和当前 Broker 的部分信息 (所属的集群名称, Broker 的名称, Id 等) 发送给所有的 NameServer
    // 向 NameServer 发送了一个 REGISTER_BROKER = 103
    syncTopicConfigAndBrokerInfoToAllNameServer();

    // Broker 主从 Topic 不一致
    // Broker 从节点启动时, 会注册一个定时器, 定时从主节点拉取最新的 Topic 配置 (BrokerController.this.slaveSynchronize.syncAll())
}
```

### Broker 启动时, 注册 Topic 到 NameServer

```java
public class BrokerController {

     // Topic 配置管理器
     private TopicConfigManager topicConfigManager;

     public BrokerController() {
        // enableLmq: 是否支持轻量级消息队列, 默认为 false
        this.topicConfigManager = messageStoreConfig.isEnableLmq() ? new LmqTopicConfigManager(this) : new TopicConfigManager(this);    
     }

     public boolean initialize() throws CloneNotSupportedException {
          // 加载 Topic 配置, 存放到 topicConfigTable 中
          boolean result = this.topicConfigManager.load();
     }

     public void start() throws Exception {
         // 省略

         // 是否启用 DLedger，即是否启用 RocketMQ 主从切换，默认值为 false 
         if (!messageStoreConfig.isEnableDLegerCommitLog()) {
              // 注册当前 Broker 的路由和本身的信息到 所有的 NameServer, 逻辑和 syncTopicConfigAndBrokerInfoToAllNameServer 一样
              this.registerBrokerAll(true, false, true);
         }
     }
}
```

注: Broker 启动时, 还会注册一个定时器, 定时将最新的 Topic 配置和 Broker 信息同步到所有的 NameServer
同步时间间隔取值 10000 和 60000 毫秒之间，默认 30 秒


TopicConfigManager.load() 方法, 会将 ${usr.home}/store/config/topics.json 文件中的 Topic 配置加载到内存中

```java
public class TopicConfigManager extends ConfigManager {

     /**
      * Topic 配置表
      * key : Topic 名称, value: Topic 配置
      */
     private final ConcurrentMap<String, TopicConfig> topicConfigTable = new ConcurrentHashMap<String, TopicConfig>(1024);
    
     public TopicConfigManager(BrokerController brokerController) {
          // 如果允许自动创建 Topic, 会自动创建这个名为 TBW102 的 Topic, 后续自动创建的 Topic 都会根据这个 Topic 的配置进行创建
          if (this.brokerController.getBrokerConfig().isAutoCreateTopicEnable()) {
               String topic = TopicValidator.AUTO_CREATE_TOPIC_KEY_TOPIC;
               TopicConfig topicConfig = new TopicConfig(topic);
               // 省略配置
               this.topicConfigTable.put(topicConfig.getTopicName(), topicConfig);
          }
          // 创建一个 BenchmarkTest 的 Topic, 用于性能测试，存放到 topicConfigTable 中
          // 创建一个以当前集群名称命名的 Topic, 存放到 topicConfigTable 中
          // 创建一个以当前 Broker 名称命名的 Topic, 存放到 topicConfigTable 中
          // 创建一个 OFFSET_MOVED_EVENT 的 Topic, 存放到 topicConfigTable 中
          // 创建一个 SCHEDULE_TOPIC_XXXX 的 Topic, 存放到 topicConfigTable 中
          // 如果开启了消息轨迹功能开关, 创建一个 RMQ_SYS_TRACE_TOPIC 的 Topic, 存放到 topicConfigTable 中
          // 创建一个以当前集群名称_REPLY_TOPIC 的 Topic, 存放到 topicConfigTable 中
     }

     public boolean load() {
         // 将 ${usr.home}/store/config/topics.json 文件中的 Topic 配置加载到 topicConfigTable 中
          topicConfigTable.putAll(loadFromPath());          
     }
}
```


### NameServer

```java
public RemotingCommand registerBrokerWithFilterServer(ChannelHandlerContext ctx, RemotingCommand request) throws RemotingCommandException {
    // 1. 将当前 Broker 的名称添加到对应的集群中
    // 维护在 org.apache.rocketmq.namesrv.routeinfo.RouteInfoManager.clusterAddrTable 中 
    // key: 集群名称, value: Set<BrokerName> 
    addBrokerNameToMapCacheIfNeed(brokerName, clusterName);

    // 2. 将当前 Broker 的信息添加到对应的 Broker 集合中
    // 维护在 org.apache.rocketmq.namesrv.routeinfo.RouteInfoManager.brokerAddrTable 中
    // key: Broker 名称, value: BrokerDate, Broker 的配置     
    addBrokerDateToMapCacheIfNeed(brokerName, new BrokerData(clusterName, brokerName, new HashMap<>()));

    // 3. 当前 Broker 是主节点, 同时同步过来的 Topic 有变更或者 Broker 是第一次注册到 NameServer
    // 将 Broker 下的这个 Topic 对应的 Queue 配置维护在 org.apache.rocketmq.namesrv.routeinfo.RouteInfoManager.topicQueueTable
    // key1: BrokerName, key2: Topic 名称, value: QueueData 对象 (Queue 的配置)
    addQueueDateToMapCacheIfNeed(brokerName, new QueueData());

    // 4. 更新当前 Broker 的心跳上报时间
    // 维护在 org.apache.rocketmq.namesrv.routeinfo.RouteInfoManager.brokerLiveTable 中
    // key: Broker 地址, value: BrokerLiveInfo 对象 (里面包含上次心跳上报时间 和 其他信息)
    updateBrokerLiveInfo(brokerAddr, new BrokerLiveInfo(System.currentTimeMillis(), otherInfo));
}
```

## 自动创建

Producer 发送消息
> 1. 通过 Topic 查询对应的路由信息 TopicPublishInfo
> 2. 到 NameSever 获取默认的 Topic 的配置 (Topic 名称: TBW102, 如果 Broker 配置了支持自动创建, Broker 启动就会同步到
     Broker)
> 3. 根据获取到的默认 Topic 的配置, 创建 TopicPublishInfo 对象, Topic 为制定的 Topic
> 4. 发送消息


Broker 收到消息
> 1. 通过 Topic 查询对应的路由信息 TopicConfig, 获取不到, 尝试创建对应的 Topic (
     org.apache.rocketmq.broker.topic.TopicConfigManager.createTopicInSendMessageMethod)
> 2. 创建后, 就是同步到所有的 NameServer, 等操作

后面的逻辑差不多