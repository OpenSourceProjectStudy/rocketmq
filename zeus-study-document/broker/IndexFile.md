## RocketMQ IndexFile 设计

## IndexFile 的文件名

IndexFile 文件名称: 文件创建时的时间, 精确到毫秒, 例如 20250208105220772。
默认存放在 **$HOME/store/index/** 目录下。

## IndexFile 的文件格式

一个 Index File 文件的组成可以分成三部分
> Header, 1 个, 占 40 个字节
> Slot Table，500w 个 Slot, 每个 Slot (槽) 占 4 个字节
> Index Linked List, 2000w 个 Index Item，每个 Index Item (索引项) 占 20 个字节

### Header

Index File 文件头部, 40 个字节, 存放了 Index 文件的一些统计信息

> beginTimestamp: 8 个字节, 文件中第一条消息的存储时间 (最小存储时间)
> endTimestamp: 8 个字节, 文件中最后一条消息的存储时间 (最大存储时间)
> beginPhyOffset: 8 个字节, 文件中第一条消息在 CommitLog 的物理偏移量
> endPhyOffset: 8 个字节, 文件中最后一条消息在 CommitLog 的物理偏移量
> hashSlotCount: 4 个字节, 已用的 hash 槽个数
> indexCount: 4 个字节, 已用的 Index 个数 + 1

### Index Linked List

Slot Table 等一下配合着 Index Linked List, 所以先讲解一下 Index Linked List。

Index Linked List 索引项链表 (实际更像是数组), 一个 Index Linked List 由多个 Index Item 组成。
一个 Index Item, 占 20 个字节, 它的内容如下:
> Key Hash: 4 个字节, key 的 hash 值
> CommitLog Offset: 8 个字节, 消息在 CommitLog 的物理偏移量
> Timestamp: 4 个字节, 消息在 CommitLog 的存储时间与 IndexHeader 的开始时间的时间差
> NextIndex offset: 4 个字节, 这个索引项的 key 计算后, 得到他在 Slot Table 的对应位置上的值 (不为 0: 当前索引项的下一个在
     Index Linked List 的位置, 可以看完后面 Slot Table, 再回来)

将索引项放到 Index Linked List 中, 就是直接追加到最后一个索引项的后面。

### Slot Table

Index File 是为了能够通过 key 快速定位到消息在 CommitLog 中的物理偏移量。
但是, 通过 key 到 Index Linked List 中定位到对应的索引项, 需要从头往后一个个比较, 会比较慢, 所以引入了 Slot Table, 通过
key 快速定位到 Index Linked List 中的位置。

Slot Table 是一个 500w 个槽的数组, 每个槽占 4 个字节, 每个槽存储的是对应 Index Item 在 Index Linked List 中的位置。
向通过 Index Item 的 key 计算后的值 % 500w, 得到这个 Index Item 在 Slot Table 的位置, 然后读取这个位置的值, 就可以得到这个
Index Item 在 Index Linked List 中的位置。

整合 3 个部分, 可以得到一个消息是如何向 Index File 的添加索引的过程 (设定, 添加的消息不是文件的第一条消息)

> 1. 计算消息 key 的 hash 值, 然后 % 500w, 得到 Slot Table 的位置
> 2. 更新 Slot Table 的位置, 为 Header 的 indexCount (表示当前消息在 Index Linked List 的位置)
> 3. 计算消息存储的时间戳 - Index File 的 Header 的 beginTimestamp, 得到时间差
> 4. 将消息 key 的 hash, 消息在 commitLog 的偏移量, 计算后的时间戳组成 Index Item, 追加到 Index Linked List 的最后, 也就是
     Header 的 indexCount 的位置
> 5. 将消息的存储时间更新到 Index File 的 Header 的 endTimestamp
> 6. 将消息在 CommitLog 的物理偏移量更新到 Index File 的 Header 的 endPhyOffset
> 7. 更新 Index File 的 Header 的 hashSlotCount + 1
> 8. 更新 Index File 的 Header 的 indexCount + 1

这个过程没什么问题，但是消息 key 计算 hash 值后，可能会有 hash 冲突，也就是多个 key 计算后的值 % 500w 相同，
这个时候就会有多个 Index Item 在 Slot Table 的同一个槽上，只能保存最新一个的话, 以前的数据就会丢失,
所以需要将原本存放在这个槽的数据 (上一个索引项的位置) 存到当前索引项的 NextIndex offset 中, 串起来。

所以真正的添加索引的过程是:
> 1. 计算消息 key 的 hash 值, 然后 % 500w, 得到 Slot Table 的位置
> 2. 读取 Slot Table 的位置, 然后更新 Slot Table 的位置, 为 Header 的 indexCount (表示当前消息在 Index Linked List
     的位置)
> 3. 计算消息存储的时间戳 - Index File 的 Header 的 beginTimestamp, 得到时间差
> 4. 将消息 key 的 hash, 消息在 commitLog 的偏移量, 计算后的时间戳, 原 Slot Table 的位置的值组成 Index Item, 追加到
     Index Linked List 的最后, 也就是 Header 的 indexCount 的位置
> 5. 将消息的存储时间更新到 Index File 的 Header 的 endTimestamp
> 6. 将消息在 CommitLog 的物理偏移量更新到 Index File 的 Header 的 endPhyOffset
> 7. 如果原 Slot Table 的位置的值为 0  (不为 0, 表示对应的槽已经被使用了), 更新 Index File 的 Header 的 hashSlotCount + 1
> 8. 更新 Index File 的 Header 的 indexCount + 1

后面通过消息的 key 查询消息的时候,
> 1. 通过 key 的 hash 值, 到 Slot Table 对应的位置读取消息在 Index Linked List 的位置,
> 2. 在 Index Linked List 的对应的位置, 得到 Index Item
> 3. 如果 Index Item 的 NextIndex offset 不为 0, 继续找下一个直到最后一个的 NextIndex offset 为 0,
> 4. 通过找到的这些 Index Item 的 CommitLog Offset 到 CommitLog 读取对应的消息, 做最终的 key 精确匹配


Index Header + Slot Table + Index Linked List

1. 通过 IndexHeader 获取最后一个消息的偏移位置, 将新消息存放到这个位置后, 同时计算出这个位置的简单位置 （1，2，3, 这种）
2. key 计算出的 hash 值 % 500w, 得到 Slot Table 的位置, 读取里面保存的上一个 Index Item 在 Index Linked List 的位置
3. 将当前消息的 preIndex offset 设置为上一步读取到的值
4. 将 Slot Table 的位置更新为当前消息在 Index Linked List 的位置

## IndexFile 文件名为时间的设计

主要为了满足时间维度的查询，例如查询某个时间段的消息，可以通过 IndexFile 的文件名来快速定位到对应的 IndexFile 文件，然后再通过
IndexFile 文件中的索引信息，快速定位到对应的消息。