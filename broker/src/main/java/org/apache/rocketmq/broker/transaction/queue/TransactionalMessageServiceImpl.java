/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.rocketmq.broker.transaction.queue;

import org.apache.rocketmq.broker.transaction.AbstractTransactionalMessageCheckListener;
import org.apache.rocketmq.broker.transaction.OperationResult;
import org.apache.rocketmq.broker.transaction.TransactionalMessageService;
import org.apache.rocketmq.client.consumer.PullResult;
import org.apache.rocketmq.client.consumer.PullStatus;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.protocol.ResponseCode;
import org.apache.rocketmq.common.protocol.header.EndTransactionRequestHeader;
import org.apache.rocketmq.common.topic.TopicValidator;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.store.MessageExtBrokerInner;
import org.apache.rocketmq.store.PutMessageResult;
import org.apache.rocketmq.store.PutMessageStatus;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

public class TransactionalMessageServiceImpl implements TransactionalMessageService {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.TRANSACTION_LOGGER_NAME);

    private TransactionalMessageBridge transactionalMessageBridge;

    private static final int PULL_MSG_RETRY_NUMBER = 1;

    private static final int MAX_PROCESS_TIME_LIMIT = 60000;

    private static final int MAX_RETRY_COUNT_WHEN_HALF_NULL = 1;

    public TransactionalMessageServiceImpl(TransactionalMessageBridge transactionBridge) {
        this.transactionalMessageBridge = transactionBridge;
    }

    private ConcurrentHashMap<MessageQueue, MessageQueue> opQueueMap = new ConcurrentHashMap<>();

    @Override
    public CompletableFuture<PutMessageResult> asyncPrepareMessage(MessageExtBrokerInner messageInner) {
        // 桥接模式, 将具体的逻辑委托给内部的 transactionalMessageBridge
        return transactionalMessageBridge.asyncPutHalfMessage(messageInner);
    }

    @Override
    public PutMessageResult prepareMessage(MessageExtBrokerInner messageInner) {
        return transactionalMessageBridge.putHalfMessage(messageInner);
    }

    private boolean needDiscard(MessageExt msgExt, int transactionCheckMax) {

        // 从 PROPERTY_TRANSACTION_CHECK_TIMES 属性获取回查次数
        String checkTimes = msgExt.getProperty(MessageConst.PROPERTY_TRANSACTION_CHECK_TIMES);
        int checkTime = 1;
        if (null != checkTimes) {
            checkTime = getInt(checkTimes);
            // 如果回查次数大于等于最大值，那么需要丢弃
            if (checkTime >= transactionCheckMax) {
                return true;
            } else {
                // 否则, 回查次数自增 1
                checkTime++;
            }
        }
        // 回查次数设置到属性中
        msgExt.putUserProperty(MessageConst.PROPERTY_TRANSACTION_CHECK_TIMES, String.valueOf(checkTime));
        return false;
    }

    private boolean needSkip(MessageExt msgExt) {
        // 当前时间戳减去消息发送时间戳
        long valueOfCurrentMinusBorn = System.currentTimeMillis() - msgExt.getBornTimestamp();
        // 如果中间间隔的时间大于 fileReservedTime，则跳过该消息，fileReservedTime 为消息日志文件保留的时间默认 72h，即 3 天
        if (valueOfCurrentMinusBorn
                > transactionalMessageBridge.getBrokerController().getMessageStoreConfig().getFileReservedTime()
                * 3600L * 1000) {
            log.info("Half message exceed file reserved time ,so skip it.messageId {},bornTime {}",
                    msgExt.getMsgId(), msgExt.getBornTimestamp());
            return true;
        }
        return false;
    }

    private boolean putBackHalfMsgQueue(MessageExt msgExt, long offset) {
        PutMessageResult putMessageResult = putBackToHalfQueueReturnResult(msgExt);
        if (putMessageResult != null
                && putMessageResult.getPutMessageStatus() == PutMessageStatus.PUT_OK) {
            msgExt.setQueueOffset(
                    putMessageResult.getAppendMessageResult().getLogicsOffset());
            msgExt.setCommitLogOffset(
                    putMessageResult.getAppendMessageResult().getWroteOffset());
            msgExt.setMsgId(putMessageResult.getAppendMessageResult().getMsgId());
            log.debug(
                    "Send check message, the offset={} restored in queueOffset={} "
                            + "commitLogOffset={} "
                            + "newMsgId={} realMsgId={} topic={}",
                    offset, msgExt.getQueueOffset(), msgExt.getCommitLogOffset(), msgExt.getMsgId(),
                    msgExt.getUserProperty(MessageConst.PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX),
                    msgExt.getTopic());
            return true;
        } else {
            log.error(
                    "PutBackToHalfQueueReturnResult write failed, topic: {}, queueId: {}, "
                            + "msgId: {}",
                    msgExt.getTopic(), msgExt.getQueueId(), msgExt.getMsgId());
            return false;
        }
    }

    @Override
    public void check(long transactionTimeout, int transactionCheckMax,
                      AbstractTransactionalMessageCheckListener listener) {
        try {
            String topic = TopicValidator.RMQ_SYS_TRANS_HALF_TOPIC;
            // 获取事务 half 消息的 topic RMQ_SYS_TRANS_HALF_TOPIC 下的所有 mq, 默认就一个
            Set<MessageQueue> msgQueues = transactionalMessageBridge.fetchMessageQueues(topic);
            if (msgQueues == null || msgQueues.size() == 0) {
                log.warn("The queue of topic is empty :" + topic);
                return;
            }
            log.debug("Check topic={}, queues={}", topic, msgQueues);
            // 遍历所有的队列
            for (MessageQueue messageQueue : msgQueues) {
                long startTime = System.currentTimeMillis();
                // 获取当前队列, 在 RMQ_SYS_TRANS_OP_HALF_TOPIC topic 中对的 MessageQueue
                MessageQueue opQueue = getOpQueue(messageQueue);
                // 获取内部消费者组 CID_SYS_RMQ_TRANS 对于该 half mq 的消费偏移量
                long halfOffset = transactionalMessageBridge.fetchConsumeOffset(messageQueue);
                // 获取内部消费者组 CID_SYS_RMQ_TRANS 对于该 Op mq 的消费偏移量
                long opOffset = transactionalMessageBridge.fetchConsumeOffset(opQueue);
                log.info("Before check, the queue={} msgOffset={} opOffset={}", messageQueue, halfOffset, opOffset);
                if (halfOffset < 0 || opOffset < 0) {
                    log.error("MessageQueue: {} illegal offset read: {}, op offset: {},skip this queue", messageQueue,
                            halfOffset, opOffset);
                    continue;
                }
                // halfOffset < 最新消费的 halfOffset 的消息，已处理完成的消息，value：opOffset
                List<Long> doneOpOffset = new ArrayList<>();
                // halfOffset >= 最新消费的 halfOffset，需要移除的消息，key：halfOffset，value：opOffset
                HashMap<Long, Long> removeMap = new HashMap<>();

                // 根据最新已处理的 op 消息队列消费偏移量和 half 消息队列消费偏移量，拉取 op 消息，填充 removeMap 和 doneOpOffset，找出已处理的 half 消息，避免重复发送事物状态回查请求
                PullResult pullResult = fillOpRemoveMap(removeMap, opQueue, opOffset, halfOffset, doneOpOffset);
                if (null == pullResult) {
                    // 没拉取到
                    log.error("The queue={} check msgOffset={} with opOffset={} failed, pullResult is null",
                            messageQueue, halfOffset, opOffset);
                    continue;
                }
                // single thread
                int getMessageNullCount = 1;
                long newOffset = halfOffset;
                long i = halfOffset;
                while (true) {
                    // 每一轮消息回查最多进行 60s，超时就退出，检测下一个队列
                    if (System.currentTimeMillis() - startTime > MAX_PROCESS_TIME_LIMIT) {
                        log.info("Queue={} process time reach max={}", messageQueue, MAX_PROCESS_TIME_LIMIT);
                        break;
                    }
                    // 如果 removeMap 中已包含该 offset，从 removeMap 移除并且加入到 doneOpOffset，那么表示已经确定了的事物消息，无需回查
                    if (removeMap.containsKey(i)) {
                        // 如果 removeMap 中已包含该 offset, 那么表示已经确定了的事物消息，无需回查
                        log.debug("Half offset {} has been committed/rolled back", i);
                        Long removedOpOffset = removeMap.remove(i);
                        doneOpOffset.add(removedOpOffset);
                    } else {
                        // 根据 offset 查询该 half 事物消息
                        GetResult getResult = getHalfMsg(messageQueue, i);
                        MessageExt msgExt = getResult.getMsg();
                        if (msgExt == null) {
                            // 如果没找到消息

                            // 判断是否可以重试，最多重试一次，如果超过次数则结束该消息队列的回查
                            if (getMessageNullCount++ > MAX_RETRY_COUNT_WHEN_HALF_NULL) {
                                break;
                            }
                            if (getResult.getPullResult().getPullStatus() == PullStatus.NO_NEW_MSG) {
                                log.debug("No new msg, the miss offset={} in={}, continue check={}, pull result={}", i,
                                        messageQueue, getMessageNullCount, getResult.getPullResult());
                                break;
                            } else {
                                log.info("Illegal offset, the miss offset={} in={}, continue check={}, pull result={}",
                                        i, messageQueue, getMessageNullCount, getResult.getPullResult());
                                i = getResult.getPullResult().getNextBeginOffset();
                                newOffset = i;
                                continue;
                            }
                        }

                        // 判断是否需要丢弃、跳过该消息
                        if (needDiscard(msgExt, transactionCheckMax) || needSkip(msgExt)) {
                            // 通过 listener 丢弃该 half 消息，即将消息存入 TRANS_CHECK_MAX_TIME_TOPIC 这个内部 topic 中
                            listener.resolveDiscardMsg(msgExt);
                            // 增加 offset
                            newOffset = i + 1;
                            i++;
                            continue;
                        }
                        // 判断事务是否到达超时时间，超时后才会检测
                        if (msgExt.getStoreTimestamp() >= startTime) {
                            log.debug("Fresh stored. the miss offset={}, check it later, store={}", i,
                                    new Date(msgExt.getStoreTimestamp()));
                            break;
                        }
                        // 当前时间戳 减去 消息发送时间戳，得到消息已经存储的时间戳
                        long valueOfCurrentMinusBorn = System.currentTimeMillis() - msgExt.getBornTimestamp();
                        // 立即检测事务消息的时间，初始化为事务消息的超时时间，默认为 6s，这个时间是 broker 中设置的
                        long checkImmunityTime = transactionTimeout;
                        // 从 half 消息的 PROPERTY_CHECK_IMMUNITY_TIME_IN_SECONDS 属性中获取 consumer 客户端设置的事务消息检测时间
                        String checkImmunityTimeStr = msgExt.getUserProperty(MessageConst.PROPERTY_CHECK_IMMUNITY_TIME_IN_SECONDS);
                        // 如果设置了该属性，一般是没人设置的
                        if (null != checkImmunityTimeStr) {
                            // 如果 consumer 设置了事务超时时间，那么就使用自己设置的时间，否则使用 broker 端的默认超时时间 6s
                            checkImmunityTime = getImmunityTime(checkImmunityTimeStr, transactionTimeout);
                            if (valueOfCurrentMinusBorn < checkImmunityTime) {
                                // 检查 half 队列偏移量，返回 true 则跳过该消息
                                if (checkPrepareQueueOffset(removeMap, doneOpOffset, msgExt)) {
                                    newOffset = i + 1;
                                    i++;
                                    continue;
                                }
                            }
                        } else {
                            // 如果没有设置 PROPERTY_CHECK_IMMUNITY_TIME_IN_SECONDS 属性，并且消息存储的时间小于事务超时时间
                            // 那么说明还没到事务回查的时候，当前mq的回查结束
                            if ((0 <= valueOfCurrentMinusBorn) && (valueOfCurrentMinusBorn < checkImmunityTime)) {
                                log.debug("New arrived, the miss offset={}, check it later checkImmunity={}, born={}", i,
                                        checkImmunityTime, new Date(msgExt.getBornTimestamp()));
                                break;
                            }
                        }

                        /*
                         * 判断是否需要检测
                         * 1. 如果拉取的 op 消息为 null 并且当前消息存储的时间大于事务超时时间
                         * 2. 拉取的 op 消息不为 null 并且最后一个 op 消息的发送存储时减去起始时间的结果大于事务超时时间
                         * 3. 当前时间小于当前消息发送时间戳
                         * 这 3 种情况都会检测
                         */
                        List<MessageExt> opMsg = pullResult.getMsgFoundList();
                        boolean isNeedCheck = (opMsg == null && valueOfCurrentMinusBorn > checkImmunityTime)
                                || (opMsg != null && (opMsg.get(opMsg.size() - 1).getBornTimestamp() - startTime > transactionTimeout))
                                || (valueOfCurrentMinusBorn <= -1);

                        if (isNeedCheck) {
                            // 需要回查
                            // 首先将该消息再次存入 half 队列
                            if (!putBackHalfMsgQueue(msgExt, i)) {
                                continue;
                            }
                            // 然后通过 listener 向 consumer 客户端发起一个单向消息回查请求
                            listener.resolveHalfMsg(msgExt);
                        } else {
                            // 如果不需要执行回查，那么从已拉取的 op 消息的下一个 offset 开始，再次执行 fillOpRemoveMap，拉取下一轮的 op 消息，继续下一个循环检测
                            pullResult = fillOpRemoveMap(removeMap, opQueue, pullResult.getNextBeginOffset(), halfOffset, doneOpOffset);
                            log.debug("The miss offset:{} in messageQueue:{} need to get more opMsg, result is:{}", i,
                                    messageQueue, pullResult);
                            continue;
                        }
                    }
                    newOffset = i + 1;
                    i++;
                }
                // 更新 half 消息队列偏移量
                if (newOffset != halfOffset) {
                    transactionalMessageBridge.updateConsumeOffset(messageQueue, newOffset);
                }
                // 更新 op 消息队列偏移量
                long newOpOffset = calculateOpOffset(doneOpOffset, opOffset);
                if (newOpOffset != opOffset) {
                    transactionalMessageBridge.updateConsumeOffset(opQueue, newOpOffset);
                }
            }
        } catch (Throwable e) {
            log.error("Check error", e);
        }

    }

    private long getImmunityTime(String checkImmunityTimeStr, long transactionTimeout) {
        long checkImmunityTime;

        checkImmunityTime = getLong(checkImmunityTimeStr);
        if (-1 == checkImmunityTime) {
            checkImmunityTime = transactionTimeout;
        } else {
            checkImmunityTime *= 1000;
        }
        return checkImmunityTime;
    }

    /**
     * Read op message, parse op message, and fill removeMap
     *
     * @param removeMap      Half message to be remove, key:halfOffset, value: opOffset.
     * @param opQueue        Op message queue.
     * @param pullOffsetOfOp The begin offset of op message queue.
     * @param miniOffset     The current minimum offset of half message queue.
     * @param doneOpOffset   Stored op messages that have been processed.
     * @return Op message result.
     */
    private PullResult fillOpRemoveMap(HashMap<Long, Long> removeMap,
                                       MessageQueue opQueue, long pullOffsetOfOp, long miniOffset, List<Long> doneOpOffset) {
        // 通过 CID_SYS_RMQ_TRANS 消费者组拉取 32 条最新 Op 消息
        PullResult pullResult = pullOpMsg(opQueue, pullOffsetOfOp, 32);
        if (null == pullResult) {
            return null;
        }
        // 请求 offset 不合法，过大或者过小
        if (pullResult.getPullStatus() == PullStatus.OFFSET_ILLEGAL
                || pullResult.getPullStatus() == PullStatus.NO_MATCHED_MSG) {
            log.warn("The miss op offset={} in queue={} is illegal, pullResult={}", pullOffsetOfOp, opQueue,
                    pullResult);
            transactionalMessageBridge.updateConsumeOffset(opQueue, pullResult.getNextBeginOffset());
            return pullResult;
        } else if (pullResult.getPullStatus() == PullStatus.NO_NEW_MSG) {
            log.warn("The miss op offset={} in queue={} is NO_NEW_MSG, pullResult={}", pullOffsetOfOp, opQueue,
                    pullResult);
            return pullResult;
        }

        // 获取拉取到的 Op 消息
        List<MessageExt> opMsg = pullResult.getMsgFoundList();
        if (opMsg == null) {
            log.warn("The miss op offset={} in queue={} is empty, pullResult={}", pullOffsetOfOp, opQueue, pullResult);
            return pullResult;
        }
        for (MessageExt opMessageExt : opMsg) {
            // 解析 Op 消息的消息体，结果就是对应的 half 消息在 half 消息队列的相对偏移量
            Long queueOffset = getLong(new String(opMessageExt.getBody(), TransactionalMessageUtil.charset));
            log.debug("Topic: {} tags: {}, OpOffset: {}, HalfOffset: {}", opMessageExt.getTopic(),
                    opMessageExt.getTags(), opMessageExt.getQueueOffset(), queueOffset);

            // 是否有 d 的 tag 标记
            if (TransactionalMessageUtil.REMOVETAG.equals(opMessageExt.getTags())) {
                // 如果有标记，并且小于最新的 half 消息消费偏移量
                if (queueOffset < miniOffset) {
                    // 加入到 doneOpOffset 集合，表示已处理的 half 消息
                    doneOpOffset.add(opMessageExt.getQueueOffset());
                } else {
                    // 加入到 removeMap 集合，表示当前 half 消息需要移除, key：halfOffset，value：opOffset。
                    removeMap.put(queueOffset, opMessageExt.getQueueOffset());
                }
            } else {
                log.error("Found a illegal tag in opMessageExt= {} ", opMessageExt);
            }
        }
        log.debug("Remove map: {}", removeMap);
        log.debug("Done op list: {}", doneOpOffset);
        return pullResult;
    }

    /**
     * If return true, skip this msg
     *
     * @param removeMap    Op message map to determine whether a half message was responded by producer.
     * @param doneOpOffset Op Message which has been checked.
     * @param msgExt       Half message
     * @return Return true if put success, otherwise return false.
     */
    private boolean checkPrepareQueueOffset(HashMap<Long, Long> removeMap, List<Long> doneOpOffset,
                                            MessageExt msgExt) {
        // 从该消息获取 PROPERTY_TRANSACTION_PREPARED_QUEUE_OFFSET 属性，即此前该消息在 half 队列的 offset，也就是第一次存放该消息的 offset
        String prepareQueueOffsetStr = msgExt.getUserProperty(MessageConst.PROPERTY_TRANSACTION_PREPARED_QUEUE_OFFSET);

        if (null == prepareQueueOffsetStr) {
            // 如果没有该属性，说明该消息第一次遇见
            return putImmunityMsgBackToHalfQueue(msgExt);
        } else {
            // 获取该属性值，也就是第一次存放该消息的 offset
            long prepareQueueOffset = getLong(prepareQueueOffsetStr);
            if (-1 == prepareQueueOffset) {
                return false;
            } else {
                // 如果 removeMap 包含该 offset，那么移除并加入 doneOpOffset
                if (removeMap.containsKey(prepareQueueOffset)) {
                    long tmpOpOffset = removeMap.remove(prepareQueueOffset);
                    doneOpOffset.add(tmpOpOffset);
                    return true;
                } else {
                    // 将该消息重新存入 half 队列，等待下一次的回查，存放成功则返回 true
                    return putImmunityMsgBackToHalfQueue(msgExt);
                }
            }
        }
    }

    /**
     * Write messageExt to Half topic again
     *
     * @param messageExt Message will be write back to queue
     * @return Put result can used to determine the specific results of storage.
     */
    private PutMessageResult putBackToHalfQueueReturnResult(MessageExt messageExt) {
        PutMessageResult putMessageResult = null;
        try {
            MessageExtBrokerInner msgInner = transactionalMessageBridge.renewHalfMessageInner(messageExt);
            putMessageResult = transactionalMessageBridge.putMessageReturnResult(msgInner);
        } catch (Exception e) {
            log.warn("PutBackToHalfQueueReturnResult error", e);
        }
        return putMessageResult;
    }

    private boolean putImmunityMsgBackToHalfQueue(MessageExt messageExt) {
        // 重建一个 MessageExtBrokerInner，将最开始的消息的 offset 存入 PROPERTY_TRANSACTION_PREPARED_QUEUE_OFFSET 属性中
        MessageExtBrokerInner msgInner = transactionalMessageBridge.renewImmunityHalfMessageInner(messageExt);
        // 消息存入 half 队列
        return transactionalMessageBridge.putMessage(msgInner);
    }

    /**
     * Read half message from Half Topic
     *
     * @param mq     Target message queue, in this method, it means the half message queue.
     * @param offset Offset in the message queue.
     * @param nums   Pull message number.
     * @return Messages pulled from half message queue.
     */
    private PullResult pullHalfMsg(MessageQueue mq, long offset, int nums) {
        return transactionalMessageBridge.getHalfMessage(mq.getQueueId(), offset, nums);
    }

    /**
     * Read op message from Op Topic
     *
     * @param mq     Target Message Queue
     * @param offset Offset in the message queue
     * @param nums   Pull message number
     * @return Messages pulled from operate message queue.
     */
    private PullResult pullOpMsg(MessageQueue mq, long offset, int nums) {
        return transactionalMessageBridge.getOpMessage(mq.getQueueId(), offset, nums);
    }

    private Long getLong(String s) {
        long v = -1;
        try {
            v = Long.parseLong(s);
        } catch (Exception e) {
            log.error("GetLong error", e);
        }
        return v;

    }

    private Integer getInt(String s) {
        int v = -1;
        try {
            v = Integer.parseInt(s);
        } catch (Exception e) {
            log.error("GetInt error", e);
        }
        return v;

    }

    private long calculateOpOffset(List<Long> doneOffset, long oldOffset) {
        Collections.sort(doneOffset);
        long newOffset = oldOffset;
        for (int i = 0; i < doneOffset.size(); i++) {
            if (doneOffset.get(i) == newOffset) {
                newOffset++;
            } else {
                break;
            }
        }
        return newOffset;

    }

    private MessageQueue getOpQueue(MessageQueue messageQueue) {
        MessageQueue opQueue = opQueueMap.get(messageQueue);
        if (opQueue == null) {
            opQueue = new MessageQueue(TransactionalMessageUtil.buildOpTopic(), messageQueue.getBrokerName(),
                    messageQueue.getQueueId());
            opQueueMap.put(messageQueue, opQueue);
        }
        return opQueue;

    }

    private GetResult getHalfMsg(MessageQueue messageQueue, long offset) {
        GetResult getResult = new GetResult();

        PullResult result = pullHalfMsg(messageQueue, offset, PULL_MSG_RETRY_NUMBER);
        if (result != null) {
            getResult.setPullResult(result);
            List<MessageExt> messageExts = result.getMsgFoundList();
            if (messageExts == null || messageExts.size() == 0) {
                return getResult;
            }
            getResult.setMsg(messageExts.get(0));
        }
        return getResult;
    }

    private OperationResult getHalfMessageByOffset(long commitLogOffset) {
        OperationResult response = new OperationResult();
        MessageExt messageExt = this.transactionalMessageBridge.lookMessageByOffset(commitLogOffset);
        if (messageExt != null) {
            response.setPrepareMessage(messageExt);
            response.setResponseCode(ResponseCode.SUCCESS);
        } else {
            response.setResponseCode(ResponseCode.SYSTEM_ERROR);
            response.setResponseRemark("Find prepared transaction message failed");
        }
        return response;
    }

    @Override
    public boolean deletePrepareMessage(MessageExt msgExt) {
        // 写入事务 Op 消息
        if (this.transactionalMessageBridge.putOpMessage(msgExt, TransactionalMessageUtil.REMOVETAG)) {
            log.debug("Transaction op message write successfully. messageId={}, queueId={} msgExt:{}", msgExt.getMsgId(), msgExt.getQueueId(), msgExt);
            return true;
        } else {
            log.error("Transaction op message write failed. messageId is {}, queueId is {}", msgExt.getMsgId(), msgExt.getQueueId());
            return false;
        }
    }

    @Override
    public OperationResult commitMessage(EndTransactionRequestHeader requestHeader) {
        return getHalfMessageByOffset(requestHeader.getCommitLogOffset());
    }

    @Override
    public OperationResult rollbackMessage(EndTransactionRequestHeader requestHeader) {
        return getHalfMessageByOffset(requestHeader.getCommitLogOffset());
    }

    @Override
    public boolean open() {
        return true;
    }

    @Override
    public void close() {

    }

}
