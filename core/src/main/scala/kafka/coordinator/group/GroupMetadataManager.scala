/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package kafka.coordinator.group

import java.io.PrintStream
import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import java.util.Optional
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.locks.ReentrantLock

import com.yammer.metrics.core.Gauge
import kafka.api.{ApiVersion, KAFKA_0_10_1_IV0, KAFKA_2_1_IV0, KAFKA_2_1_IV1, KAFKA_2_3_IV0}
import kafka.common.OffsetAndMetadata
import kafka.log.AppendOrigin
import kafka.metrics.KafkaMetricsGroup
import kafka.server.{FetchLogEnd, ReplicaManager}
import kafka.utils.CoreUtils.inLock
import kafka.utils._
import kafka.utils.Implicits._
import kafka.zk.KafkaZkClient
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.internals.ConsumerProtocol
import org.apache.kafka.common.internals.Topic
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.metrics.stats.{Avg, Max, Meter}
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.protocol.types.Type._
import org.apache.kafka.common.protocol.types._
import org.apache.kafka.common.record._
import org.apache.kafka.common.requests.OffsetFetchResponse.PartitionData
import org.apache.kafka.common.requests.ProduceResponse.PartitionResponse
import org.apache.kafka.common.requests.{OffsetCommitRequest, OffsetFetchResponse}
import org.apache.kafka.common.utils.{Time, Utils}
import org.apache.kafka.common.{KafkaException, MessageFormatter, TopicPartition}

import scala.collection._
import scala.collection.mutable.ArrayBuffer
import scala.jdk.CollectionConverters._

/**
 * 消费者组管理器。是在消费者组 Coordinator 组件被创建时被实例化的。每个 Broker 在启动过程中，都会创建并维持一个 GroupMetadataManager 实例，以实现对该 Broker 负责的消费者组进行管理。
 * @param brokerId                       所在Broker的Id
 * @param interBrokerProtocolVersion     Broker 间通讯使用的请求版本。Broker端参数inter.broker.protocol.version值，这个参数的主要用途是确定位移主题消息格式的版本。
 * @param config                         内部位移主题配置类，该类型定义了与位移管理相关的重要参数，比如位移主题日志段大小设置、位移主题备份因子、位移主题分区数配置等。
 * @param replicaManager                 副本管理器类。GroupMetadataManager 类使用该字段实现获取分区对象、日志对象以及写入分区消息的目的。
 * @param zkClient                       ZooKeeper客户端，该类中的此字段只有一个目的：从 ZooKeeper 中获取位移主题的分区数
 * @param time
 * @param metrics
 */
class GroupMetadataManager(brokerId: Int,
                           interBrokerProtocolVersion: ApiVersion,
                           config: OffsetConfig,
                           val replicaManager: ReplicaManager,
                           zkClient: KafkaZkClient,
                           time: Time,
                           metrics: Metrics) extends Logging with KafkaMetricsGroup {
  // 压缩器类型。向位移主题写入消息时执行压缩操作。是否压缩，取决于 Broker 端参数 offsets.topic.compression.codec 值，默认是不进行压缩。
  private val compressionType: CompressionType = CompressionType.forId(config.offsetsTopicCompressionCodec.codec)
  // 保存这个 Broker 上 GroupCoordinator 组件管理的所有消费者组元数据。
  // 它的 Key 是消费者组名称，Value是消费者组元数据，也就是 GroupMetadata。通过该字段实现对消费者组的添加、删除和遍历操作。
  // **重要**这个字段是Broker对外提供消费者元数据的Broker缓存
  private val groupMetadataCache = new Pool[String, GroupMetadata]

  /* lock protecting access to loading and owned partition sets */
  private val partitionLock = new ReentrantLock()

  /* partitions of consumer groups that are being loaded, its lock should be always called BEFORE the group lock if needed */
  // 位移主题下正在执行加载操作的分区号集合，这些分区都是位移主题（ __consumer_offsets）分区，所谓的加载，是指读取位移主题消息数据，填充 GroupMetadataCache 字段的操作
  private val loadingPartitions: mutable.Set[Int] = mutable.Set()

  /* partitions of consumer groups that are assigned, using the same loading partition lock */
  // 位移主题下完成加载操作的分区号集合
  private val ownedPartitions: mutable.Set[Int] = mutable.Set()

  /* shutting down flag */
  private val shuttingDown = new AtomicBoolean(false)

  /* number of partitions for the consumer metadata topic */
  // 位移主题总分区数。它是 Broker 端参数 offsets.topic.num.partitions 的值，默认是 50 个分区。若要修改分区数，除了变更该参数值之外，你也可以手动创建位移主题，并指定不同的分区数
  private val groupMetadataTopicPartitionCount = getGroupMetadataTopicPartitionCount

  /* single-thread scheduler to handle offset/group metadata cache loading and unloading */
  private val scheduler = new KafkaScheduler(threads = 1, threadNamePrefix = "group-metadata-manager-")

  /* The groups with open transactional offsets commits per producer. We need this because when the commit or abort
   * marker comes in for a transaction, it is for a particular partition on the offsets topic and a particular producerId.
   * We use this structure to quickly find the groups which need to be updated by the commit/abort marker. */
  private val openGroupsForProducer = mutable.HashMap[Long, mutable.Set[String]]()

  /* setup metrics*/
  private val partitionLoadSensor = metrics.sensor(GroupMetadataManager.LoadTimeSensor)

  partitionLoadSensor.add(metrics.metricName("partition-load-time-max",
    GroupMetadataManager.MetricsGroup,
    "The max time it took to load the partitions in the last 30sec"), new Max())
  partitionLoadSensor.add(metrics.metricName("partition-load-time-avg",
    GroupMetadataManager.MetricsGroup,
    "The avg time it took to load the partitions in the last 30sec"), new Avg())

  val offsetCommitsSensor = metrics.sensor("OffsetCommits")

  offsetCommitsSensor.add(new Meter(
    metrics.metricName("offset-commit-rate",
      "group-coordinator-metrics",
      "The rate of committed offsets"),
    metrics.metricName("offset-commit-count",
      "group-coordinator-metrics",
      "The total number of committed offsets")))

  val offsetExpiredSensor = metrics.sensor("OffsetExpired")

  offsetExpiredSensor.add(new Meter(
    metrics.metricName("offset-expiration-rate",
      "group-coordinator-metrics",
      "The rate of expired offsets"),
    metrics.metricName("offset-expiration-count",
      "group-coordinator-metrics",
      "The total number of expired offsets")))

  this.logIdent = s"[GroupMetadataManager brokerId=$brokerId] "

  private def recreateGauge[T](name: String, gauge: Gauge[T]): Gauge[T] = {
    removeMetric(name)
    newGauge(name, gauge)
  }

  recreateGauge("NumOffsets",
    () => groupMetadataCache.values.map { group =>
      group.inLock { group.numOffsets }
    }.sum
  )

  recreateGauge("NumGroups",
    () => groupMetadataCache.size
  )

  recreateGauge("NumGroupsPreparingRebalance",
    () => groupMetadataCache.values.count { group =>
      group synchronized {
        group.is(PreparingRebalance)
      }
    })

  recreateGauge("NumGroupsCompletingRebalance",
    () => groupMetadataCache.values.count { group =>
      group synchronized {
        group.is(CompletingRebalance)
      }
    })

  recreateGauge("NumGroupsStable",
    () => groupMetadataCache.values.count { group =>
      group synchronized {
        group.is(Stable)
      }
    })

  recreateGauge("NumGroupsDead",
    () => groupMetadataCache.values.count { group =>
      group synchronized {
        group.is(Dead)
      }
    })

  recreateGauge("NumGroupsEmpty",
    () => groupMetadataCache.values.count { group =>
      group synchronized {
        group.is(Empty)
      }
    })

  def startup(enableMetadataExpiration: Boolean): Unit = {
    scheduler.startup()
    if (enableMetadataExpiration) {
      scheduler.schedule(name = "delete-expired-group-metadata",
        fun = () => cleanupGroupMetadata(),
        period = config.offsetsRetentionCheckIntervalMs,
        unit = TimeUnit.MILLISECONDS)
    }
  }

  def currentGroups: Iterable[GroupMetadata] = groupMetadataCache.values

  def isPartitionOwned(partition: Int) = inLock(partitionLock) { ownedPartitions.contains(partition) }

  def isPartitionLoading(partition: Int) = inLock(partitionLock) { loadingPartitions.contains(partition) }

  /**
   * 获取消费者组在内部位移主题（__consumer_offsets)所在分区
   * 而且，位移主题分区 Leader 副本所在的 Broker 被选定为指定消费者组的 Coordinator
   * 位移主题比较特殊，我们是不能提前知道提交位移消息所在分区的（不像普通消息从生产者动端发送消息时，就决定了消息所在分区），是需要根据消费者组名确定要提交位移消息所在分区
   * @param groupId 消费者组ID
   * @return （消费者组名哈希值 % 位移主题分区数） 的绝对值
   */
  def partitionFor(groupId: String): Int = Utils.abs(groupId.hashCode) % groupMetadataTopicPartitionCount

  def isGroupLocal(groupId: String): Boolean = isPartitionOwned(partitionFor(groupId))

  def isGroupLoading(groupId: String): Boolean = isPartitionLoading(partitionFor(groupId))

  def isLoading: Boolean = inLock(partitionLock) { loadingPartitions.nonEmpty }

  // return true iff group is owned and the group doesn't exist
  def groupNotExists(groupId: String) = inLock(partitionLock) {
    isGroupLocal(groupId) && getGroup(groupId).forall { group =>
      group.inLock(group.is(Dead))
    }
  }

  // visible for testing
  private[group] def isGroupOpenForProducer(producerId: Long, groupId: String) = openGroupsForProducer.get(producerId) match {
    case Some(groups) =>
      groups.contains(groupId)
    case None =>
      false
  }

  /**
   * Get the group associated with the given groupId or null if not found
   * 返回给定消费者组的元数据信息，如果该组信息不存在，就返回 None
   */
  def getGroup(groupId: String): Option[GroupMetadata] = {
    Option(groupMetadataCache.get(groupId))
  }

  /**
   * Get the group associated with the given groupId - the group is created if createIfNotExist
   * is true - or null if not found
   * 返回给定消费者组的元数据信息，若不存在，则视createIfNotExist参数值决定是否需要添加该消费者组。
   * 在消费者组第一个成员加入组时被调用的，用于把组创建出来。
   */
  def getOrMaybeCreateGroup(groupId: String, createIfNotExist: Boolean): Option[GroupMetadata] = {
    if (createIfNotExist)
    // 若不存在且允许添加，则添加一个状态是Empty的消费者组元数据对象
      Option(groupMetadataCache.getAndMaybePut(groupId, new GroupMetadata(groupId, Empty, time)))
    else
      Option(groupMetadataCache.get(groupId))
  }

  /**
   * Add a group or get the group associated with the given groupId if it already exists
   */
  def addGroup(group: GroupMetadata): GroupMetadata = {
    val currentGroup = groupMetadataCache.putIfNotExists(group.groupId, group)
    if (currentGroup != null) {
      currentGroup
    } else {
      group
    }
  }

  /**
   * 注册类的消息写入，注册类的消息写入时机：
   * 1、所有成员都加入组后：Coordinator 向位移主题写入注册消息，只是该消息不含分区消费分配方案；
   * 2、Leader 成员发送方案给 Coordinator 后：当 Leader 成员将分区消费分配方案发给 Coordinator 后，Coordinator 写入携带分配方案的注册消息。
   * @param group
   * @param groupAssignment
   * @param responseCallback
   */
  def storeGroup(group: GroupMetadata,
                 groupAssignment: Map[String, Array[Byte]],
                 responseCallback: Errors => Unit): Unit = {
    // 判断当前 Broker 是否是该消费者组的 Coordinator
    getMagic(partitionFor(group.groupId)) match {
      // 如果当前 Broker 是 Coordinator
      case Some(magicValue) =>
        // We always use CREATE_TIME, like the producer. The conversion to LOG_APPEND_TIME (if necessary) happens automatically.
        val timestampType = TimestampType.CREATE_TIME
        val timestamp = time.milliseconds()
        // 构建注册消息的 Key
        val key = GroupMetadataManager.groupMetadataKey(group.groupId)
        // 构建注册消息的 Value
        val value = GroupMetadataManager.groupMetadataValue(group, groupAssignment, interBrokerProtocolVersion)
        // 使用 Key 和 Value 构建待写入消息集合
        val records = {
          val buffer = ByteBuffer.allocate(AbstractRecords.estimateSizeInBytes(magicValue, compressionType,
            Seq(new SimpleRecord(timestamp, key, value)).asJava))
          val builder = MemoryRecords.builder(buffer, magicValue, compressionType, timestampType, 0L)
          builder.append(timestamp, key, value)
          builder.build()
        }
        // 计算要写入的目标分区
        val groupMetadataPartition = new TopicPartition(Topic.GROUP_METADATA_TOPIC_NAME, partitionFor(group.groupId))
        val groupMetadataRecords = Map(groupMetadataPartition -> records)
        val generationId = group.generationId

        // set the callback function to insert the created group into cache after log append completed
        // putCacheCallback方法，消息写入后，响应处理函数 TODO 调用responseCallback完成cache的填充？
        def putCacheCallback(responseStatus: Map[TopicPartition, PartitionResponse]): Unit = {
          // the append response should only contain the topics partition
          if (responseStatus.size != 1 || !responseStatus.contains(groupMetadataPartition))
            throw new IllegalStateException("Append status %s should only have one partition %s"
              .format(responseStatus, groupMetadataPartition))

          // construct the error status in the propagated assignment response in the cache
          val status = responseStatus(groupMetadataPartition)

          val responseError = if (status.error == Errors.NONE) {
            Errors.NONE
          } else {
            debug(s"Metadata from group ${group.groupId} with generation $generationId failed when appending to log " +
              s"due to ${status.error.exceptionName}")

            // transform the log append error code to the corresponding the commit status error code
            status.error match {
              case Errors.UNKNOWN_TOPIC_OR_PARTITION
                   | Errors.NOT_ENOUGH_REPLICAS
                   | Errors.NOT_ENOUGH_REPLICAS_AFTER_APPEND =>
                Errors.COORDINATOR_NOT_AVAILABLE

              case Errors.NOT_LEADER_OR_FOLLOWER
                   | Errors.KAFKA_STORAGE_ERROR =>
                Errors.NOT_COORDINATOR

              case Errors.REQUEST_TIMED_OUT =>
                Errors.REBALANCE_IN_PROGRESS

              case Errors.MESSAGE_TOO_LARGE
                   | Errors.RECORD_LIST_TOO_LARGE
                   | Errors.INVALID_FETCH_SIZE =>

                error(s"Appending metadata message for group ${group.groupId} generation $generationId failed due to " +
                  s"${status.error.exceptionName}, returning UNKNOWN error code to the client")

                Errors.UNKNOWN_SERVER_ERROR

              case other =>
                error(s"Appending metadata message for group ${group.groupId} generation $generationId failed " +
                  s"due to unexpected error: ${status.error.exceptionName}")

                other
            }
          }

          responseCallback(responseError)
        }
        // 将待写入消息插入到位移主题的目标分区下
        appendForGroup(group, groupMetadataRecords, putCacheCallback)

      case None =>
        responseCallback(Errors.NOT_COORDINATOR)
        None
    }
  }

  private def appendForGroup(group: GroupMetadata,
                             records: Map[TopicPartition, MemoryRecords],
                             callback: Map[TopicPartition, PartitionResponse] => Unit): Unit = {
    // call replica manager to append the group message
    replicaManager.appendRecords(
      timeout = config.offsetCommitTimeoutMs.toLong,
      requiredAcks = config.offsetCommitRequiredAcks,
      internalTopicsAllowed = true,
      origin = AppendOrigin.Coordinator,
      entriesPerPartition = records,
      delayedProduceLock = Some(group.lock),
      responseCallback = callback)
  }

  /**
   * * Store offsets by appending it to the replicated log and then inserting to cache
   * 提交位移消息的写入
   * @param group           消费者组元数据
   * @param consumerId      消费者组成员ID，仅用于 DEBUG 调试
   * @param offsetMetadata  待保存的位移值，按照分区分组
   * @param responseCallback 处理完成后的回调函数
   * @param producerId        事务型Producer ID
   * @param producerEpoch     事务型Producer Epoch值
   */
  def storeOffsets(group: GroupMetadata,
                   consumerId: String,
                   offsetMetadata: immutable.Map[TopicPartition, OffsetAndMetadata],
                   responseCallback: immutable.Map[TopicPartition, Errors] => Unit,
                   producerId: Long = RecordBatch.NO_PRODUCER_ID,
                   producerEpoch: Short = RecordBatch.NO_PRODUCER_EPOCH): Unit = {
    // first filter out partitions with offset metadata size exceeding limit
    // 过滤出满足特定条件的待保存位移数据
    val filteredOffsetMetadata = offsetMetadata.filter { case (_, offsetAndMetadata) =>
      // 位移提交记录中的自定义数据大小，要小于 Broker 端参数 offset.metadata.max.bytes 的值，默认值是 4KB。
      validateOffsetMetadataLength(offsetAndMetadata.metadata)
    }

    group.inLock {
      if (!group.hasReceivedConsistentOffsetCommits)
        warn(s"group: ${group.groupId} with leader: ${group.leaderOrNull} has received offset commits from consumers as well " +
          s"as transactional producers. Mixing both types of offset commits will generally result in surprises and " +
          s"should be avoided.")
    }

    val isTxnOffsetCommit = producerId != RecordBatch.NO_PRODUCER_ID
    // construct the message set to append
    // 如果没有一个分区满足条件
    if (filteredOffsetMetadata.isEmpty) {
      // compute the final error codes for the commit response
      // 构造 OFFSET_METADATA_TOO_LARGE 异常
      val commitStatus = offsetMetadata.map { case (k, _) => k -> Errors.OFFSET_METADATA_TOO_LARGE }
      // 调用回调函数，执行发送位移提交 Response 的动作
      responseCallback(commitStatus)
      None
      // 有分区满足了条件
    } else {
      // 查看当前Broker是否为给定消费者组的Coordinator，一个消费者组只有一个Coordinator，同时也是消费者组的Leader节点
      // partitionFor获取消费者所在分区，getMagic获取消息格式版本
      getMagic(partitionFor(group.groupId)) match {
        // 如果是Coordinator
        // TODO 能获取到消费者组消息格式版本的broker是Coordinator节点？
        //  只有Coordinator节点在kafka.server.ReplicaManager.allPartitions中维护了位移主题的分区（位移主题分区对应到消费者组，参考kafka.coordinator.group.GroupMetadataManager.partitionFor）？）
        case Some(magicValue) =>
          // We always use CREATE_TIME, like the producer. The conversion to LOG_APPEND_TIME (if necessary) happens automatically.
          val timestampType = TimestampType.CREATE_TIME
          val timestamp = time.milliseconds()
          // 构造位移主题的位移提交消息
          val records = filteredOffsetMetadata.map { case (topicPartition, offsetAndMetadata) =>
            // 构造位移提交消息的 key，获得的是ByteBuffer的底层数组
            val key = GroupMetadataManager.offsetCommitKey(group.groupId, topicPartition)
            // 构造位移提交消息的 value，获得的是ByteBuffer的底层数组
            val value = GroupMetadataManager.offsetCommitValue(offsetAndMetadata, interBrokerProtocolVersion)
            new SimpleRecord(timestamp, key, value)
          }
          // 构建分区对象，partitionFor 获取消费者组在内部位移主题（__consumer_offsets)所在分区
          val offsetTopicPartition = new TopicPartition(Topic.GROUP_METADATA_TOPIC_NAME, partitionFor(group.groupId))
          // 为写入消息创建内存Buffer，magicValue是消息格式版本
          val buffer = ByteBuffer.allocate(AbstractRecords.estimateSizeInBytes(magicValue, compressionType, records.asJava))

          if (isTxnOffsetCommit && magicValue < RecordBatch.MAGIC_VALUE_V2)
            throw Errors.UNSUPPORTED_FOR_MESSAGE_FORMAT.exception("Attempting to make a transaction offset commit with an invalid magic: " + magicValue)

          val builder = MemoryRecords.builder(buffer, magicValue, compressionType, timestampType, 0L, time.milliseconds(),
            producerId, producerEpoch, 0, isTxnOffsetCommit, RecordBatch.NO_PARTITION_LEADER_EPOCH)
          // 添加消息到 MemoryRecords的 构建者，实际是把records中的key和val ByteBuffer内容添加到MemoryRecords.builder的ByteBuffer，当然包括时间戳、相对位移等
          records.foreach(builder.append)
          // key->TopicPartition val->MemoryRecords
          val entries = Map(offsetTopicPartition -> builder.build())

          // set the callback function to insert offsets into cache after log append completed
          def putCacheCallback(responseStatus: Map[TopicPartition, PartitionResponse]): Unit = {
            // the append response should only contain the topics partition
            // 确保消息写入到指定位移主题分区（offsetTopicPartition），否则抛出异常
            if (responseStatus.size != 1 || !responseStatus.contains(offsetTopicPartition))
              throw new IllegalStateException("Append status %s should only have one partition %s"
                .format(responseStatus, offsetTopicPartition))

            // record the number of offsets committed to the log
            // 更新已提交位移数指标
            offsetCommitsSensor.record(records.size)

            // construct the commit response status and insert
            // the offset and metadata to cache if the append status has no error
            val status = responseStatus(offsetTopicPartition)

            val responseError = group.inLock {
              // 写入结果没有错误
              if (status.error == Errors.NONE) {
                // 如果不是Dead状态
                if (!group.is(Dead)) {
                  filteredOffsetMetadata.forKeyValue { (topicPartition, offsetAndMetadata) =>
                    if (isTxnOffsetCommit)
                      group.onTxnOffsetCommitAppend(producerId, topicPartition, CommitRecordMetadataAndOffset(Some(status.baseOffset), offsetAndMetadata))
                    else {
                      // ** 重要：这里更新内存缓存中提交位移元数据 ** 更新内存中的分区位移提交元数据 kafka.coordinator.group.GroupMetadata.offsets
                      group.onOffsetCommitAppend(topicPartition, CommitRecordMetadataAndOffset(Some(status.baseOffset), offsetAndMetadata))
                    }
                  }
                }
                Errors.NONE
                // 写入结果有错误
              } else {
                if (!group.is(Dead)) {
                  if (!group.hasPendingOffsetCommitsFromProducer(producerId))
                    removeProducerGroup(producerId, group.groupId)
                  filteredOffsetMetadata.forKeyValue { (topicPartition, offsetAndMetadata) =>
                    if (isTxnOffsetCommit)
                      group.failPendingTxnOffsetCommit(producerId, topicPartition)
                    else
                    // 取消未完成的位移消息写入，即把分区从kafka.coordinator.group.GroupMetadata.pendingOffsetCommits集合中移除
                      group.failPendingOffsetWrite(topicPartition, offsetAndMetadata)
                  }
                }

                debug(s"Offset commit $filteredOffsetMetadata from group ${group.groupId}, consumer $consumerId " +
                  s"with generation ${group.generationId} failed when appending to log due to ${status.error.exceptionName}")

                // transform the log append error code to the corresponding the commit status error code
                // 确认异常类型
                status.error match {
                  case Errors.UNKNOWN_TOPIC_OR_PARTITION
                       | Errors.NOT_ENOUGH_REPLICAS
                       | Errors.NOT_ENOUGH_REPLICAS_AFTER_APPEND =>
                    Errors.COORDINATOR_NOT_AVAILABLE

                  case Errors.NOT_LEADER_OR_FOLLOWER
                       | Errors.KAFKA_STORAGE_ERROR =>
                    Errors.NOT_COORDINATOR

                  case Errors.MESSAGE_TOO_LARGE
                       | Errors.RECORD_LIST_TOO_LARGE
                       | Errors.INVALID_FETCH_SIZE =>
                    Errors.INVALID_COMMIT_OFFSET_SIZE

                  case other => other
                }
              }
            }

            // compute the final error codes for the commit response
            // 利用异常类型构建提交返回状态
            val commitStatus = offsetMetadata.map { case (topicPartition, offsetAndMetadata) =>
              if (validateOffsetMetadataLength(offsetAndMetadata.metadata))
                (topicPartition, responseError)
              else
                (topicPartition, Errors.OFFSET_METADATA_TOO_LARGE)
            }

            // finally trigger the callback logic passed from the API layer
            // 调用回调函数
            responseCallback(commitStatus)
          }

          if (isTxnOffsetCommit) {
            group.inLock {
              addProducerGroup(producerId, group.groupId)
              group.prepareTxnOffsetCommit(producerId, offsetMetadata)
            }
          } else {
            group.inLock {
              // 写入位移消息前，先添加到 kafka.coordinator.group.GroupMetadata.pendingOffsetCommits，HashMap[TopicPartition, OffsetAndMetadata]
              group.prepareOffsetCommit(offsetMetadata)
            }
          }

          // 写入消息到位移主题，同时调用putCacheCallback方法更新消费者元数据
          appendForGroup(group, entries, putCacheCallback)

        case None =>
          // 如果不是Coordinator，构造NOT_COORDINATOR异常并提交给responseCallback方法
          val commitStatus = offsetMetadata.map { case (topicPartition, _) =>
            (topicPartition, Errors.NOT_COORDINATOR)
          }
          responseCallback(commitStatus)
          None
      }
    }
  }

  /**
   * The most important guarantee that this API provides is that it should never return a stale offset. i.e., it either
   * returns the current offset or it begins to sync the cache from the log (and returns an error code).
   */
  def getOffsets(groupId: String, requireStable: Boolean, topicPartitionsOpt: Option[Seq[TopicPartition]]): Map[TopicPartition, PartitionData] = {
    trace("Getting offsets of %s for group %s.".format(topicPartitionsOpt.getOrElse("all partitions"), groupId))
    // 从groupMetadataCache字段中获取指定消费者组的元数据
    val group = groupMetadataCache.get(groupId)
    // 如果没有组数据，返回空数据
    if (group == null) {
      topicPartitionsOpt.getOrElse(Seq.empty[TopicPartition]).map { topicPartition =>
        val partitionData = new PartitionData(OffsetFetchResponse.INVALID_OFFSET,
          Optional.empty(), "", Errors.NONE)
        topicPartition -> partitionData
      }.toMap
      // 如果存在组数据
    } else {
      group.inLock {
        // 如果组处于Dead状态（消费者组已经被销毁了，位移数据也被视为不可用），则返回空数据
        if (group.is(Dead)) {
          topicPartitionsOpt.getOrElse(Seq.empty[TopicPartition]).map { topicPartition =>
            val partitionData = new PartitionData(OffsetFetchResponse.INVALID_OFFSET,
              Optional.empty(), "", Errors.NONE)
            topicPartition -> partitionData
          }.toMap
        } else {
          val topicPartitions = topicPartitionsOpt.getOrElse(group.allOffsets.keySet)
          // 获取多个分区的提交位移元数据
          topicPartitions.map { topicPartition =>
            if (requireStable && group.hasPendingOffsetCommitsForTopicPartition(topicPartition)) {
              topicPartition -> new PartitionData(OffsetFetchResponse.INVALID_OFFSET,
                Optional.empty(), "", Errors.UNSTABLE_OFFSET_COMMIT)
            } else {
              val partitionData = group.offset(topicPartition) match {
                // 如果没有该分区位移数据，返回空数据
                case None =>
                  new PartitionData(OffsetFetchResponse.INVALID_OFFSET,
                    Optional.empty(), "", Errors.NONE)
                // 如果有该分区位移数据
                case Some(offsetAndMetadata) =>
                  // 从消费者组元数据中返回指定分区的位移数据
                  new PartitionData(offsetAndMetadata.offset,
                    offsetAndMetadata.leaderEpoch, offsetAndMetadata.metadata, Errors.NONE)
              }
              topicPartition -> partitionData
            }
          }.toMap
        }
      }
    }
  }

  /**
   * Asynchronously read the partition from the offsets topic and populate the cache
   * 创建一个异步任务，来读取位移主题消息，并填充缓存
   */
  def scheduleLoadGroupAndOffsets(offsetsPartition: Int, onGroupLoaded: GroupMetadata => Unit): Unit = {
    val topicPartition = new TopicPartition(Topic.GROUP_METADATA_TOPIC_NAME, offsetsPartition)
    // 在执行加载位移主题分区操作之前，先对 kafka.coordinator.group.GroupMetadataManager.partitionLock 进行加锁，并添加到 kafka.coordinator.group.GroupMetadataManager.loadingPartitions 集合中
    // 避免对应分区加载的并发操作。执行位移主题分区加载操作后，在loadGroupsAndOffsets函数最后会进行相反的操作，即释放partitionLock锁，并从 loadingPartitions 集合删除分区，同时添加到ownedPartitions集合（完成加载的分区）中
    if (addLoadingPartition(offsetsPartition)) {
      info(s"Scheduling loading of offsets and group metadata from $topicPartition")
      val startTimeMs = time.milliseconds()
      scheduler.schedule(topicPartition.toString, () => loadGroupsAndOffsets(topicPartition, onGroupLoaded, startTimeMs))
    } else {
      info(s"Already loading offsets and group metadata from $topicPartition")
    }
  }

  /**
   * 读取位移主题中的消息（注册消息和提交位移消息），填充到缓存
   * 读取位移主题的时机：当前 Broker 当选消费者组的 Coordinator节点，也就是Broker 成为了位移主题某分区的 Leader 副本时。
   * 一旦当前 Broker 当选为位移主题某分区的 Leader 副本，它就需要将它内存中的元数据缓存填充起来，因此需要读取位移主题。
   * @param topicPartition
   * @param onGroupLoaded
   * @param startTimeMs
   */
  private[group] def loadGroupsAndOffsets(topicPartition: TopicPartition, onGroupLoaded: GroupMetadata => Unit, startTimeMs: java.lang.Long): Unit = {
    try {
      val schedulerTimeMs = time.milliseconds() - startTimeMs
      doLoadGroupsAndOffsets(topicPartition, onGroupLoaded)
      val endTimeMs = time.milliseconds()
      val totalLoadingTimeMs = endTimeMs - startTimeMs
      partitionLoadSensor.record(totalLoadingTimeMs.toDouble, endTimeMs, false)
      info(s"Finished loading offsets and group metadata from $topicPartition "
        + s"in $totalLoadingTimeMs milliseconds, of which $schedulerTimeMs milliseconds"
        + s" was spent in the scheduler.")
    } catch {
      case t: Throwable => error(s"Error loading offsets from $topicPartition", t)
    } finally {
      inLock(partitionLock) {
        ownedPartitions.add(topicPartition.partition)
        loadingPartitions.remove(topicPartition.partition)
      }
    }
  }

  /**
   * 读取位移主题中的消息（注册消息和提交位移消息），只有位移主题的Leader副本（消费者组的Coordinator节点所在Broker）才会执行该函数
   * @param topicPartition 位移主题目标分区
   * @param onGroupLoaded 加载完成后要执行的逻辑
   */
  private def doLoadGroupsAndOffsets(topicPartition: TopicPartition, onGroupLoaded: GroupMetadata => Unit): Unit = {
    // 获取位移主题指定分区的LEO值
    // 重要：如果当前Broker不是该分区的Leader副本，则返回-1（如果当前 Broker 不是该分区的 Leader 副本，会返回-1，以此判断分区的 Leader 副本是否发生变更）
    // TODO 为什么通过能否获取LEO值，来判断是不是位移主题该分区的Leader副本？
    def logEndOffset: Long = replicaManager.getLogEndOffset(topicPartition).getOrElse(-1L)
    // 获取位移主题分区的本地日志
    replicaManager.getLog(topicPartition) match {
      // 如果无法获取到日志对象
      case None =>
        warn(s"Attempted to load offsets and group metadata from $topicPartition, but found no log")

      case Some(log) =>
        // 已完成位移值加载的分区列表
        val loadedOffsets = mutable.Map[GroupTopicPartition, CommitRecordMetadataAndOffset]()
        // 处于位移加载中的分区列表，只用于Kafka事务
        val pendingOffsets = mutable.Map[Long, mutable.Map[GroupTopicPartition, CommitRecordMetadataAndOffset]]()
        // 已完成组信息加载的消费者组列表，key 消费者组ID val 组元数据GroupMetadata
        val loadedGroups = mutable.Map[String, GroupMetadata]()
        // 待移除的消费者组列表
        val removedGroups = mutable.Set[String]()

        // buffer may not be needed if records are read from memory
        // 保存消息集合的ByteBuffer缓冲区
        var buffer = ByteBuffer.allocate(0)

        // loop breaks if leader changes at any time during the load, since logEndOffset is -1
        // 位移主题目标分区日志起始位移值
        var currOffset = log.logStartOffset

        // loop breaks if no records have been read, since the end of the log has been reached
        // 至少要求读取一条消息
        var readAtLeastOneRecord = true
        // 当前读取位移 < LEO（如果不是位移主题该分区的Leader副本，则LEO为-1，也就是不会进入循环），且至少要求读取一条消息，且GroupMetadataManager未关闭
        while (currOffset < logEndOffset && readAtLeastOneRecord && !shuttingDown.get()) {
          // 读取位移主题指定分区
          val fetchDataInfo = log.read(currOffset,
            maxLength = config.loadBufferSize,
            isolation = FetchLogEnd,
            minOneMessage = true)
          // 如果无消息可读，则不再要求至少读取一条消息
          readAtLeastOneRecord = fetchDataInfo.records.sizeInBytes > 0
          // 创建消息集合，因为要将读取的消息填充到缓存中，所以这里必须做出 MemoryRecords 类型（内存中的消息集合）的消息集合。
          val memRecords = fetchDataInfo.records match {
            case records: MemoryRecords => records
            // FileRecords是磁盘中的消息集合，需要解析成 MemoryRecords
            case fileRecords: FileRecords =>
              val sizeInBytes = fileRecords.sizeInBytes
              val bytesNeeded = Math.max(config.loadBufferSize, sizeInBytes)

              // minOneMessage = true in the above log.read means that the buffer may need to be grown to ensure progress can be made
              if (buffer.capacity < bytesNeeded) {
                if (config.loadBufferSize < bytesNeeded)
                  warn(s"Loaded offsets and group metadata from $topicPartition with buffer larger ($bytesNeeded bytes) than " +
                    s"configured offsets.load.buffer.size (${config.loadBufferSize} bytes)")

                buffer = ByteBuffer.allocate(bytesNeeded)
              } else {
                buffer.clear()
              }

              fileRecords.readInto(buffer, 0)
              MemoryRecords.readableRecords(buffer)
          }
          // 遍历消息集合的每个消息批次(RecordBatch)
          memRecords.batches.forEach { batch =>
            val isTxnOffsetCommit = batch.isTransactional
            // 如果是控制类消息批次
            // 控制类消息批次属于Kafka事务范畴，这里不展开讲
            if (batch.isControlBatch) {
              val recordIterator = batch.iterator
              if (recordIterator.hasNext) {
                val record = recordIterator.next()
                val controlRecord = ControlRecordType.parse(record.key)
                if (controlRecord == ControlRecordType.COMMIT) {
                  pendingOffsets.getOrElse(batch.producerId, mutable.Map[GroupTopicPartition, CommitRecordMetadataAndOffset]())
                    .foreach {
                      case (groupTopicPartition, commitRecordMetadataAndOffset) =>
                        if (!loadedOffsets.contains(groupTopicPartition) || loadedOffsets(groupTopicPartition).olderThan(commitRecordMetadataAndOffset))
                          loadedOffsets.put(groupTopicPartition, commitRecordMetadataAndOffset)
                    }
                }
                pendingOffsets.remove(batch.producerId)
              }
            } else {
              // 保存消息批次第一条消息的位移值
              var batchBaseOffset: Option[Long] = None
              // 遍历消息批次下的所有消息
              for (record <- batch.asScala) {
                // 确保消息必须有Key，否则抛出异常
                require(record.hasKey, "Group metadata/offset entry key should not be null")
                if (batchBaseOffset.isEmpty)
                // 记录消息批次第一条消息的位移值 TODO 感觉这里应该是记录每条消息的位移值，难道 record（RecordBatch类） 中包含多条消息，这里的batchBaseOffset就是RecordBatch的起始位移值
                  batchBaseOffset = Some(record.offset)
                // 读取消息Key（可能是注册消息，也可能是提交位移消息，是哪种，由key中的version决定，CURRENT_OFFSET_KEY_SCHEMA_VERSION或者CURRENT_GROUP_KEY_SCHEMA_VERSION）
                GroupMetadataManager.readMessageKey(record.key) match {
                  // 如果是 OffsetKey，说明是提交位移消息
                  case offsetKey: OffsetKey =>
                    if (isTxnOffsetCommit && !pendingOffsets.contains(batch.producerId))
                      pendingOffsets.put(batch.producerId, mutable.Map[GroupTopicPartition, CommitRecordMetadataAndOffset]())

                    // load offset
                    val groupTopicPartition = offsetKey.key
                    // 如果该消息没有Value，说明提交位移消息中出现了 Tombstone消息，就表示 Kafka 能够将该消费者组在当前主题分区上的位移提交数据删除。
                    if (!record.hasValue) {
                      if (isTxnOffsetCommit)
                        pendingOffsets(batch.producerId).remove(groupTopicPartition)
                      else
                      // 将目标分区从已完成位移值加载的分区列表中移除
                        loadedOffsets.remove(groupTopicPartition)
                    } else {
                      // 如果该消息有Value，说明是正常的提交位移消息，进行消息的解析
                      val offsetAndMetadata = GroupMetadataManager.readOffsetMessageValue(record.value)
                      if (isTxnOffsetCommit)
                        pendingOffsets(batch.producerId).put(groupTopicPartition, CommitRecordMetadataAndOffset(batchBaseOffset, offsetAndMetadata))
                      else
                      // 将目标分区加入到已完成位移值加载的分区列表：batchBaseOffset：第一条消息的位移值 ，offsetAndMetadata：消息批次（RecordBatch）
                      // TODO 但是我觉得，这个函数是只加载一个分区（topicPartition）的提交位移消息，那只一个有一条消息，为什么这里还要遍历消息批次集合MemoryRecords中消息批次，以及消息批次RecordBatch 呢？
                        loadedOffsets.put(groupTopicPartition, CommitRecordMetadataAndOffset(batchBaseOffset, offsetAndMetadata))
                    }
                  // 如果是GroupMetadataKey，说明是注册消息
                  case groupMetadataKey: GroupMetadataKey =>
                    // load group metadata
                    val groupId = groupMetadataKey.key
                    // 解析注册消息批次的 value
                    val groupMetadata = GroupMetadataManager.readGroupMessageValue(groupId, record.value, time)
                    // 如果消息Value不为空
                    if (groupMetadata != null) {
                      // 把该消费者组从待移除消费者组列表中移除
                      removedGroups.remove(groupId)
                      // 将消费者组加入到已完成加载的消费组列表
                      loadedGroups.put(groupId, groupMetadata)
                      // 如果消息Value为空，说明是Tombstone消息
                    } else {
                      // 把该消费者组从已完成加载的组列表中移除
                      loadedGroups.remove(groupId)
                      // 将消费者组加入到待移除消费组列表，注册消息中出现了 Tombstone 消息，就表示 Kafka 可以将该消费者组元数据从位移主题中删除；
                      removedGroups.add(groupId)
                    }

                  case unknownKey =>
                    throw new IllegalStateException(s"Unexpected message key $unknownKey while loading offsets and group metadata")
                }
              }
            }
            // 更新读取位置到消息批次最后一条消息的位移值+1，等待下次while循环
            currOffset = batch.nextOffset
          }
        }
        // 处理loadedOffsets
        val (groupOffsets, emptyGroupOffsets) = loadedOffsets
          // 按照消费者组名（kafka.coordinator.group.GroupTopicPartition.group，是string类）分类
          .groupBy(_._1.group)
          .map { case (k, v) =>
            // value 提取出Map <主题名，分区号>（TopicPartition） -> 提交位移值（ CommitRecordMetadataAndOffset）
            k -> v.map { case (groupTopicPartition, offset) => (groupTopicPartition.topicPartition, offset) }
            // 按照 已经加载的消费者组 loadedGroups中是否包含消费者组名group，如果不在其中，说明没有当前提交位移消息没有组名，就放到emptyGroupOffsets中。有组名的放到groupOffsets中。
          }.partition { case (group, _) => loadedGroups.contains(group) }

        val pendingOffsetsByGroup = mutable.Map[String, mutable.Map[Long, mutable.Map[TopicPartition, CommitRecordMetadataAndOffset]]]()
        pendingOffsets.forKeyValue { (producerId, producerOffsets) =>
          producerOffsets.keySet.map(_.group).foreach(addProducerGroup(producerId, _))
          producerOffsets
            .groupBy(_._1.group)
            .forKeyValue { (group, offsets) =>
              val groupPendingOffsets = pendingOffsetsByGroup.getOrElseUpdate(group, mutable.Map.empty[Long, mutable.Map[TopicPartition, CommitRecordMetadataAndOffset]])
              val groupProducerOffsets = groupPendingOffsets.getOrElseUpdate(producerId, mutable.Map.empty[TopicPartition, CommitRecordMetadataAndOffset])
              groupProducerOffsets ++= offsets.map { case (groupTopicPartition, offset) =>
                (groupTopicPartition.topicPartition, offset)
              }
            }
        }

        val (pendingGroupOffsets, pendingEmptyGroupOffsets) = pendingOffsetsByGroup
          .partition { case (group, _) => loadedGroups.contains(group)}
        // 处理loadedGroups
        loadedGroups.values.foreach { group =>
          // 提取消费者组的已提交位移
          val offsets = groupOffsets.getOrElse(group.groupId, Map.empty[TopicPartition, CommitRecordMetadataAndOffset])
          val pendingOffsets = pendingGroupOffsets.getOrElse(group.groupId, Map.empty[Long, mutable.Map[TopicPartition, CommitRecordMetadataAndOffset]])
          debug(s"Loaded group metadata $group with offsets $offsets and pending offsets $pendingOffsets")
          // **重要**：为已完成加载的组loadedGroups中元素，执行加载组操作，在这里完成消费者组分区位移的提交，并添加消费者组元数据
          loadGroup(group, offsets, pendingOffsets)
          // 为已完成加载的组执行加载组操作之后的逻辑 onGroupLoaded（当前函数传入的回调函数），是上层调用组件 Coordinator 传入的。它主要的作用是处理消费者组下所有成员的心跳超时设置，并指定下一次心跳的超时时间。
          onGroupLoaded(group)
        }

        // load groups which store offsets in kafka, but which have no active members and thus no group
        // metadata stored in the log
        (emptyGroupOffsets.keySet ++ pendingEmptyGroupOffsets.keySet).foreach { groupId =>
          val group = new GroupMetadata(groupId, Empty, time)
          val offsets = emptyGroupOffsets.getOrElse(groupId, Map.empty[TopicPartition, CommitRecordMetadataAndOffset])
          val pendingOffsets = pendingEmptyGroupOffsets.getOrElse(groupId, Map.empty[Long, mutable.Map[TopicPartition, CommitRecordMetadataAndOffset]])
          debug(s"Loaded group metadata $group with offsets $offsets and pending offsets $pendingOffsets")
          // 为空的消费者组执行加载组操作
          loadGroup(group, offsets, pendingOffsets)
          // 为空的消费者执行加载组操作之后的逻辑
          onGroupLoaded(group)
        }
        // 处理removedGroups，确保它们不能出现在消费者组元数据缓存中，否则将抛出异常。
        removedGroups.foreach { groupId =>
          // if the cache already contains a group which should be removed, raise an error. Note that it
          // is possible (however unlikely) for a consumer group to be removed, and then to be used only for
          // offset storage (i.e. by "simple" consumers)
          if (groupMetadataCache.contains(groupId) && !emptyGroupOffsets.contains(groupId))
            throw new IllegalStateException(s"Unexpected unload of active group $groupId while " +
              s"loading partition $topicPartition")
        }
    }
  }

  /**
   * 加载消费者组
   * @param group
   * @param offsets
   * @param pendingTransactionalOffsets
   */
  private def loadGroup(group: GroupMetadata, offsets: Map[TopicPartition, CommitRecordMetadataAndOffset],
                        pendingTransactionalOffsets: Map[Long, mutable.Map[TopicPartition, CommitRecordMetadataAndOffset]]): Unit = {
    // offsets are initialized prior to loading the group into the cache to ensure that clients see a consistent
    // view of the group's offsets
    trace(s"Initialized offsets $offsets for group ${group.groupId}")
    // 初始化消费者组的位移信息，即位移信息添加到offsets集合中，实现加载消费者组订阅主题分区提交位移的目的
    group.initializeOffsets(offsets, pendingTransactionalOffsets.toMap)
    // 调用addGroup方法添加消费者组，实现加载消费者组元数据的目的
    val currentGroup = addGroup(group)
    if (group != currentGroup)
      debug(s"Attempt to load group ${group.groupId} from log with generation ${group.generationId} failed " +
        s"because there is already a cached group with generation ${currentGroup.generationId}")
  }

  /**
   *
   * When this broker becomes a follower for an offsets topic partition clear out the cache for groups that belong to
   * that partition.
   *
   * @param offsetsPartition Groups belonging to this partition of the offsets topic will be deleted from the cache.
   *                         表示 Leader 发生变更的位移主题分区，因此，这些分区保存的消费者组都要从该 Broker 上移除掉。
   * @param onGroupUnloaded
   */
  def removeGroupsForPartition(offsetsPartition: Int,
                               onGroupUnloaded: GroupMetadata => Unit): Unit = {
    // 位移主题分区
    val topicPartition = new TopicPartition(Topic.GROUP_METADATA_TOPIC_NAME, offsetsPartition)
    info(s"Scheduling unloading of offsets and group metadata from $topicPartition")
    // 创建异步任务，移除组信息和位移信息
    scheduler.schedule(topicPartition.toString, () => removeGroupsAndOffsets())
    // 内部方法，用于移除组信息和位移信息
    def removeGroupsAndOffsets(): Unit = {
      var numOffsetsRemoved = 0
      var numGroupsRemoved = 0

      inLock(partitionLock) {
        // we need to guard the group removal in cache in the loading partition lock
        // to prevent coordinator's check-and-get-group race condition
        // 移除ownedPartitions中特定位移主题分区记录
        ownedPartitions.remove(offsetsPartition)
        // 遍历所有消费者组信息
        for (group <- groupMetadataCache.values) {
          // 如果该组信息保存在特定位移主题分区中，每个消费者组及其位移的数据，都只会保存在位移主题的一个分区下。一旦给定了位移主题分区，那么，元数据保存在这个位移主题分区下的消费者组就要被移除掉。
          if (partitionFor(group.groupId) == offsetsPartition) {
            // 执行组卸载逻辑，是上层组件GroupCoordinator 传过来的。它主要做两件事情：
            // 将消费者组状态变更到 Dead 状态；
            // 封装异常表示 Coordinator 已发生变更，然后调用回调函数返回。
            onGroupUnloaded(group)
            // 关键步骤！将组信息从groupMetadataCache中移除
            groupMetadataCache.remove(group.groupId, group)
            // 把消费者组从producer对应的组集合中移除。这里的 producer，是给 Kafka 事务用的。
            removeGroupFromAllProducers(group.groupId)
            // 更新已移除组计数器
            numGroupsRemoved += 1
            // 更新已移除位移值计数器
            numOffsetsRemoved += group.numOffsets
          }
        }
      }

      info(s"Finished unloading $topicPartition. Removed $numOffsetsRemoved cached offsets " +
        s"and $numGroupsRemoved cached groups.")
    }
  }

  // visible for testing
  private[group] def cleanupGroupMetadata(): Unit = {
    val currentTimestamp = time.milliseconds()
    val numOffsetsRemoved = cleanupGroupMetadata(groupMetadataCache.values, group => {
      group.removeExpiredOffsets(currentTimestamp, config.offsetsRetentionMs)
    })
    offsetExpiredSensor.record(numOffsetsRemoved)
    if (numOffsetsRemoved > 0)
      info(s"Removed $numOffsetsRemoved expired offsets in ${time.milliseconds() - currentTimestamp} milliseconds.")
  }

  /**
    * This function is used to clean up group offsets given the groups and also a function that performs the offset deletion.
    * @param groups Groups whose metadata are to be cleaned up
    * @param selector A function that implements deletion of (all or part of) group offsets. This function is called while
    *                 a group lock is held, therefore there is no need for the caller to also obtain a group lock.
    * @return The cumulative number of offsets removed
    */
  def cleanupGroupMetadata(groups: Iterable[GroupMetadata], selector: GroupMetadata => Map[TopicPartition, OffsetAndMetadata]): Int = {
    var offsetsRemoved = 0

    groups.foreach { group =>
      val groupId = group.groupId
      val (removedOffsets, groupIsDead, generation) = group.inLock {
        val removedOffsets = selector(group)
        if (group.is(Empty) && !group.hasOffsets) {
          info(s"Group $groupId transitioned to Dead in generation ${group.generationId}")
          group.transitionTo(Dead)
        }
        (removedOffsets, group.is(Dead), group.generationId)
      }

    val offsetsPartition = partitionFor(groupId)
    val appendPartition = new TopicPartition(Topic.GROUP_METADATA_TOPIC_NAME, offsetsPartition)
    getMagic(offsetsPartition) match {
      case Some(magicValue) =>
        // We always use CREATE_TIME, like the producer. The conversion to LOG_APPEND_TIME (if necessary) happens automatically.
        val timestampType = TimestampType.CREATE_TIME
        val timestamp = time.milliseconds()

          replicaManager.nonOfflinePartition(appendPartition).foreach { partition =>
            val tombstones = ArrayBuffer.empty[SimpleRecord]
            removedOffsets.forKeyValue { (topicPartition, offsetAndMetadata) =>
              trace(s"Removing expired/deleted offset and metadata for $groupId, $topicPartition: $offsetAndMetadata")
              val commitKey = GroupMetadataManager.offsetCommitKey(groupId, topicPartition)
              // 提交位移消息对应的Tombstone消息
              tombstones += new SimpleRecord(timestamp, commitKey, null)
            }
            trace(s"Marked ${removedOffsets.size} offsets in $appendPartition for deletion.")

            // We avoid writing the tombstone when the generationId is 0, since this group is only using
            // Kafka for offset storage.
            if (groupIsDead && groupMetadataCache.remove(groupId, group) && generation > 0) {
              // Append the tombstone messages to the partition. It is okay if the replicas don't receive these (say,
              // if we crash or leaders move) since the new leaders will still expire the consumers with heartbeat and
              // retry removing this group.
              val groupMetadataKey = GroupMetadataManager.groupMetadataKey(group.groupId)
              // 注册消息对应的Tombstone消息
              tombstones += new SimpleRecord(timestamp, groupMetadataKey, null)
              trace(s"Group $groupId removed from the metadata cache and marked for deletion in $appendPartition.")
            }

            if (tombstones.nonEmpty) {
              try {
                // do not need to require acks since even if the tombstone is lost,
                // it will be appended again in the next purge cycle
                val records = MemoryRecords.withRecords(magicValue, 0L, compressionType, timestampType, tombstones.toArray: _*)
                partition.appendRecordsToLeader(records, origin = AppendOrigin.Coordinator, requiredAcks = 0)

                offsetsRemoved += removedOffsets.size
                trace(s"Successfully appended ${tombstones.size} tombstones to $appendPartition for expired/deleted " +
                  s"offsets and/or metadata for group $groupId")
              } catch {
                case t: Throwable =>
                  error(s"Failed to append ${tombstones.size} tombstones to $appendPartition for expired/deleted " +
                    s"offsets and/or metadata for group $groupId.", t)
                // ignore and continue
              }
            }
          }

        case None =>
          info(s"BrokerId $brokerId is no longer a coordinator for the group $groupId. Proceeding cleanup for other alive groups")
      }
    }

    offsetsRemoved
  }

  /**
   * Complete pending transactional offset commits of the groups of `producerId` from the provided
   * `completedPartitions`. This method is invoked when a commit or abort marker is fully written
   * to the log. It may be invoked when a group lock is held by the caller, for instance when delayed
   * operations are completed while appending offsets for a group. Since we need to acquire one or
   * more group metadata locks to handle transaction completion, this operation is scheduled on
   * the scheduler thread to avoid deadlocks.
   */
  def scheduleHandleTxnCompletion(producerId: Long, completedPartitions: Set[Int], isCommit: Boolean): Unit = {
    scheduler.schedule(s"handleTxnCompletion-$producerId", () =>
      handleTxnCompletion(producerId, completedPartitions, isCommit))
  }

  private[group] def handleTxnCompletion(producerId: Long, completedPartitions: Set[Int], isCommit: Boolean): Unit = {
    val pendingGroups = groupsBelongingToPartitions(producerId, completedPartitions)
    pendingGroups.foreach { groupId =>
      getGroup(groupId) match {
        case Some(group) => group.inLock {
          if (!group.is(Dead)) {
            group.completePendingTxnOffsetCommit(producerId, isCommit)
            removeProducerGroup(producerId, groupId)
          }
        }
        case _ =>
          info(s"Group $groupId has moved away from $brokerId after transaction marker was written but before the " +
            s"cache was updated. The cache on the new group owner will be updated instead.")
      }
    }
  }

  private def addProducerGroup(producerId: Long, groupId: String) = openGroupsForProducer synchronized {
    openGroupsForProducer.getOrElseUpdate(producerId, mutable.Set.empty[String]).add(groupId)
  }

  private def removeProducerGroup(producerId: Long, groupId: String) = openGroupsForProducer synchronized {
    openGroupsForProducer.getOrElseUpdate(producerId, mutable.Set.empty[String]).remove(groupId)
    if (openGroupsForProducer(producerId).isEmpty)
      openGroupsForProducer.remove(producerId)
  }

  private def groupsBelongingToPartitions(producerId: Long, partitions: Set[Int]) = openGroupsForProducer synchronized {
    val (ownedGroups, _) = openGroupsForProducer.getOrElse(producerId, mutable.Set.empty[String])
      .partition { case (group) => partitions.contains(partitionFor(group)) }
    ownedGroups
  }

  private def removeGroupFromAllProducers(groupId: String) = openGroupsForProducer synchronized {
    openGroupsForProducer.forKeyValue { (_, groups) =>
      groups.remove(groupId)
    }
  }

  /*
   * Check if the offset metadata length is valid
   */
  private def validateOffsetMetadataLength(metadata: String) : Boolean = {
    metadata == null || metadata.length() <= config.maxMetadataSize
  }


  def shutdown(): Unit = {
    shuttingDown.set(true)
    if (scheduler.isStarted)
      scheduler.shutdown()

    // TODO: clear the caches
  }

  /**
   * Gets the partition count of the group metadata topic from ZooKeeper.
   * If the topic does not exist, the configured partition count is returned.
   */
  private def getGroupMetadataTopicPartitionCount: Int = {
    zkClient.getTopicPartitionCount(Topic.GROUP_METADATA_TOPIC_NAME).getOrElse(config.offsetsTopicNumPartitions)
  }

  /**
   * Check if the replica is local and return the message format version and timestamp
   * 检查分区是否在Broker上有副本（获取分区在当前Broker上的底层日志，如果能够获取到，就说明在Broker上有该分区的副本），并且返回消息格式版本和时间戳
   * @param   partition  Partition of GroupMetadataTopic
   * @return  Some(MessageFormatVersion) if replica is local, None otherwise
   */
  private def getMagic(partition: Int): Option[Byte] =
    replicaManager.getMagic(new TopicPartition(Topic.GROUP_METADATA_TOPIC_NAME, partition))

  /**
   * Add the partition into the owned list
   *
   * NOTE: this is for test only
   */
  private[group] def addPartitionOwnership(partition: Int): Unit = {
    inLock(partitionLock) {
      ownedPartitions.add(partition)
    }
  }

  /**
   * Add a partition to the loading partitions set. Return true if the partition was not
   * already loading.
   *
   * Visible for testing
   */
  private[group] def addLoadingPartition(partition: Int): Boolean = {
    inLock(partitionLock) {
      loadingPartitions.add(partition)
    }
  }

}

/**
 * Messages stored for the group topic has versions for both the key and value fields. Key
 * version is used to indicate the type of the message (also to differentiate different types
 * of messages from being compacted together if they have the same field values); and value
 * version is used to evolve the messages within their data types:
 *
 * key version 0:       group consumption offset
 *    -> value version 0:       [offset, metadata, timestamp]
 *
 * key version 1:       group consumption offset
 *    -> value version 1:       [offset, metadata, commit_timestamp, expire_timestamp]
 *
 * key version 2:       group metadata
 *    -> value version 0:       [protocol_type, generation, protocol, leader, members]
 */
object GroupMetadataManager {
  // Metrics names
  val MetricsGroup: String = "group-coordinator-metrics"
  val LoadTimeSensor: String = "GroupPartitionLoadTime"

  private val CURRENT_OFFSET_KEY_SCHEMA_VERSION = 1.toShort
  private val CURRENT_GROUP_KEY_SCHEMA_VERSION = 2.toShort

  private val OFFSET_COMMIT_KEY_SCHEMA = new Schema(new Field("group", STRING),
    new Field("topic", STRING),
    new Field("partition", INT32))
  private val OFFSET_KEY_GROUP_FIELD = OFFSET_COMMIT_KEY_SCHEMA.get("group")
  private val OFFSET_KEY_TOPIC_FIELD = OFFSET_COMMIT_KEY_SCHEMA.get("topic")
  private val OFFSET_KEY_PARTITION_FIELD = OFFSET_COMMIT_KEY_SCHEMA.get("partition")

  private val OFFSET_COMMIT_VALUE_SCHEMA_V0 = new Schema(new Field("offset", INT64),
    new Field("metadata", STRING, "Associated metadata.", ""),
    new Field("timestamp", INT64))
  private val OFFSET_VALUE_OFFSET_FIELD_V0 = OFFSET_COMMIT_VALUE_SCHEMA_V0.get("offset")
  private val OFFSET_VALUE_METADATA_FIELD_V0 = OFFSET_COMMIT_VALUE_SCHEMA_V0.get("metadata")
  private val OFFSET_VALUE_TIMESTAMP_FIELD_V0 = OFFSET_COMMIT_VALUE_SCHEMA_V0.get("timestamp")

  private val OFFSET_COMMIT_VALUE_SCHEMA_V1 = new Schema(new Field("offset", INT64),
    new Field("metadata", STRING, "Associated metadata.", ""),
    new Field("commit_timestamp", INT64),
    new Field("expire_timestamp", INT64))
  private val OFFSET_VALUE_OFFSET_FIELD_V1 = OFFSET_COMMIT_VALUE_SCHEMA_V1.get("offset")
  private val OFFSET_VALUE_METADATA_FIELD_V1 = OFFSET_COMMIT_VALUE_SCHEMA_V1.get("metadata")
  private val OFFSET_VALUE_COMMIT_TIMESTAMP_FIELD_V1 = OFFSET_COMMIT_VALUE_SCHEMA_V1.get("commit_timestamp")
  private val OFFSET_VALUE_EXPIRE_TIMESTAMP_FIELD_V1 = OFFSET_COMMIT_VALUE_SCHEMA_V1.get("expire_timestamp")

  private val OFFSET_COMMIT_VALUE_SCHEMA_V2 = new Schema(new Field("offset", INT64),
    new Field("metadata", STRING, "Associated metadata.", ""),
    new Field("commit_timestamp", INT64))
  private val OFFSET_VALUE_OFFSET_FIELD_V2 = OFFSET_COMMIT_VALUE_SCHEMA_V2.get("offset")
  private val OFFSET_VALUE_METADATA_FIELD_V2 = OFFSET_COMMIT_VALUE_SCHEMA_V2.get("metadata")
  private val OFFSET_VALUE_COMMIT_TIMESTAMP_FIELD_V2 = OFFSET_COMMIT_VALUE_SCHEMA_V2.get("commit_timestamp")

  private val OFFSET_COMMIT_VALUE_SCHEMA_V3 = new Schema(
    new Field("offset", INT64),
    new Field("leader_epoch", INT32),
    new Field("metadata", STRING, "Associated metadata.", ""),
    new Field("commit_timestamp", INT64))
  private val OFFSET_VALUE_OFFSET_FIELD_V3 = OFFSET_COMMIT_VALUE_SCHEMA_V3.get("offset")
  private val OFFSET_VALUE_LEADER_EPOCH_FIELD_V3 = OFFSET_COMMIT_VALUE_SCHEMA_V3.get("leader_epoch")
  private val OFFSET_VALUE_METADATA_FIELD_V3 = OFFSET_COMMIT_VALUE_SCHEMA_V3.get("metadata")
  private val OFFSET_VALUE_COMMIT_TIMESTAMP_FIELD_V3 = OFFSET_COMMIT_VALUE_SCHEMA_V3.get("commit_timestamp")

  private val GROUP_METADATA_KEY_SCHEMA = new Schema(new Field("group", STRING))
  private val GROUP_KEY_GROUP_FIELD = GROUP_METADATA_KEY_SCHEMA.get("group")

  private val MEMBER_ID_KEY = "member_id"
  private val GROUP_INSTANCE_ID_KEY = "group_instance_id"
  private val CLIENT_ID_KEY = "client_id"
  private val CLIENT_HOST_KEY = "client_host"
  private val REBALANCE_TIMEOUT_KEY = "rebalance_timeout"
  private val SESSION_TIMEOUT_KEY = "session_timeout"
  private val SUBSCRIPTION_KEY = "subscription"
  private val ASSIGNMENT_KEY = "assignment"

  private val MEMBER_METADATA_V0 = new Schema(
    new Field(MEMBER_ID_KEY, STRING),
    new Field(CLIENT_ID_KEY, STRING),
    new Field(CLIENT_HOST_KEY, STRING),
    new Field(SESSION_TIMEOUT_KEY, INT32),
    new Field(SUBSCRIPTION_KEY, BYTES),
    new Field(ASSIGNMENT_KEY, BYTES))

  private val MEMBER_METADATA_V1 = new Schema(
    new Field(MEMBER_ID_KEY, STRING),
    new Field(CLIENT_ID_KEY, STRING),
    new Field(CLIENT_HOST_KEY, STRING),
    new Field(REBALANCE_TIMEOUT_KEY, INT32),
    new Field(SESSION_TIMEOUT_KEY, INT32),
    new Field(SUBSCRIPTION_KEY, BYTES),
    new Field(ASSIGNMENT_KEY, BYTES))

  private val MEMBER_METADATA_V2 = MEMBER_METADATA_V1

  private val MEMBER_METADATA_V3 = new Schema(
    new Field(MEMBER_ID_KEY, STRING),
    new Field(GROUP_INSTANCE_ID_KEY, NULLABLE_STRING),
    new Field(CLIENT_ID_KEY, STRING),
    new Field(CLIENT_HOST_KEY, STRING),
    new Field(REBALANCE_TIMEOUT_KEY, INT32),
    new Field(SESSION_TIMEOUT_KEY, INT32),
    new Field(SUBSCRIPTION_KEY, BYTES),
    new Field(ASSIGNMENT_KEY, BYTES))

  private val PROTOCOL_TYPE_KEY = "protocol_type"
  private val GENERATION_KEY = "generation"
  private val PROTOCOL_KEY = "protocol"
  private val LEADER_KEY = "leader"
  private val CURRENT_STATE_TIMESTAMP_KEY = "current_state_timestamp"
  private val MEMBERS_KEY = "members"

  private val GROUP_METADATA_VALUE_SCHEMA_V0 = new Schema(
    new Field(PROTOCOL_TYPE_KEY, STRING),
    new Field(GENERATION_KEY, INT32),
    new Field(PROTOCOL_KEY, NULLABLE_STRING),
    new Field(LEADER_KEY, NULLABLE_STRING),
    new Field(MEMBERS_KEY, new ArrayOf(MEMBER_METADATA_V0)))

  private val GROUP_METADATA_VALUE_SCHEMA_V1 = new Schema(
    new Field(PROTOCOL_TYPE_KEY, STRING),
    new Field(GENERATION_KEY, INT32),
    new Field(PROTOCOL_KEY, NULLABLE_STRING),
    new Field(LEADER_KEY, NULLABLE_STRING),
    new Field(MEMBERS_KEY, new ArrayOf(MEMBER_METADATA_V1)))

  private val GROUP_METADATA_VALUE_SCHEMA_V2 = new Schema(
    new Field(PROTOCOL_TYPE_KEY, STRING),
    new Field(GENERATION_KEY, INT32),
    new Field(PROTOCOL_KEY, NULLABLE_STRING),
    new Field(LEADER_KEY, NULLABLE_STRING),
    new Field(CURRENT_STATE_TIMESTAMP_KEY, INT64),
    new Field(MEMBERS_KEY, new ArrayOf(MEMBER_METADATA_V2)))

  private val GROUP_METADATA_VALUE_SCHEMA_V3 = new Schema(
    new Field(PROTOCOL_TYPE_KEY, STRING),
    new Field(GENERATION_KEY, INT32),
    new Field(PROTOCOL_KEY, NULLABLE_STRING),
    new Field(LEADER_KEY, NULLABLE_STRING),
    new Field(CURRENT_STATE_TIMESTAMP_KEY, INT64),
    new Field(MEMBERS_KEY, new ArrayOf(MEMBER_METADATA_V3)))

  // map of versions to key schemas as data types
  private val MESSAGE_TYPE_SCHEMAS = Map(
    0 -> OFFSET_COMMIT_KEY_SCHEMA,
    1 -> OFFSET_COMMIT_KEY_SCHEMA,
    2 -> GROUP_METADATA_KEY_SCHEMA)

  // map of version of offset value schemas
  private val OFFSET_VALUE_SCHEMAS = Map(
    0 -> OFFSET_COMMIT_VALUE_SCHEMA_V0,
    1 -> OFFSET_COMMIT_VALUE_SCHEMA_V1,
    2 -> OFFSET_COMMIT_VALUE_SCHEMA_V2,
    3 -> OFFSET_COMMIT_VALUE_SCHEMA_V3)

  // map of version of group metadata value schemas
  private val GROUP_VALUE_SCHEMAS = Map(
    0 -> GROUP_METADATA_VALUE_SCHEMA_V0,
    1 -> GROUP_METADATA_VALUE_SCHEMA_V1,
    2 -> GROUP_METADATA_VALUE_SCHEMA_V2,
    3 -> GROUP_METADATA_VALUE_SCHEMA_V3)

  private val CURRENT_OFFSET_KEY_SCHEMA = schemaForKey(CURRENT_OFFSET_KEY_SCHEMA_VERSION)
  private val CURRENT_GROUP_KEY_SCHEMA = schemaForKey(CURRENT_GROUP_KEY_SCHEMA_VERSION)
  private val CURRENT_GROUP_METADATA_VALUE_SCHEMA_VERSION = GROUP_VALUE_SCHEMAS.keySet.max

  private def schemaForKey(version: Int) = {
    val schemaOpt = MESSAGE_TYPE_SCHEMAS.get(version)
    schemaOpt match {
      case Some(schema) => schema
      case _ => throw new KafkaException("Unknown message key schema version " + version)
    }
  }

  private def schemaForOffsetValue(version: Int) = {
    val schemaOpt = OFFSET_VALUE_SCHEMAS.get(version)
    schemaOpt match {
      case Some(schema) => schema
      case _ => throw new KafkaException("Unknown offset schema version " + version)
    }
  }

  private def schemaForGroupValue(version: Int) = {
    val schemaOpt = GROUP_VALUE_SCHEMAS.get(version)
    schemaOpt match {
      case Some(schema) => schema
      case _ => throw new KafkaException("Unknown group metadata version " + version)
    }
  }

  /**
   * Generates the key for offset commit message for given (group, topic, partition)
   * 提交位移消息的 key 序列化
   * @param groupId the ID of the group to generate the key         消费者组名
   * @param topicPartition the TopicPartition to generate the key  主题 + 分区号
   * @return key for offset commit message
   */
  def offsetCommitKey(groupId: String, topicPartition: TopicPartition): Array[Byte] = {
    // 创建结构体，依次写入消费者组名、主题和分区号
    val key = new Struct(CURRENT_OFFSET_KEY_SCHEMA)
    key.set(OFFSET_KEY_GROUP_FIELD, groupId)
    key.set(OFFSET_KEY_TOPIC_FIELD, topicPartition.topic)
    key.set(OFFSET_KEY_PARTITION_FIELD, topicPartition.partition)
    // 构造ByteBuffer，写入格式版本和结构体
    val byteBuffer = ByteBuffer.allocate(2 /* version */ + key.sizeOf)
    byteBuffer.putShort(CURRENT_OFFSET_KEY_SCHEMA_VERSION)
    key.writeTo(byteBuffer)
    // 返回字节数组
    byteBuffer.array()
  }

  /**
   * Generates the key for group metadata message for given group
   *  注册消息 Key 的序列化
   * @param groupId the ID of the group to generate the key
   * @return key bytes for group metadata message
   */
  def groupMetadataKey(groupId: String): Array[Byte] = {
    val key = new Struct(CURRENT_GROUP_KEY_SCHEMA)
    key.set(GROUP_KEY_GROUP_FIELD, groupId)
    // 构造一个ByteBuffer对象，容纳version和key数据
    val byteBuffer = ByteBuffer.allocate(2 /* version */ + key.sizeOf)
    // 写入 Short 型的消息格式版本
    byteBuffer.putShort(CURRENT_GROUP_KEY_SCHEMA_VERSION)
    // 写入消费者组名
    key.writeTo(byteBuffer)
    byteBuffer.array()
  }

  /**
   * Generates the payload for offset commit message from given offset and metadata
   * 提交位移消息的 value 序列化
   * @param offsetAndMetadata consumer's current offset and metadata
   * @param apiVersion the api version
   * @return payload for offset commit message
   */
  def offsetCommitValue(offsetAndMetadata: OffsetAndMetadata,
                        apiVersion: ApiVersion): Array[Byte] = {
    // generate commit value according to schema version
    // 确定消息格式版本以及创建对应的结构体对象
    val (version, value) = {
      if (apiVersion < KAFKA_2_1_IV0 || offsetAndMetadata.expireTimestamp.nonEmpty) {
        val value = new Struct(OFFSET_COMMIT_VALUE_SCHEMA_V1)
        value.set(OFFSET_VALUE_OFFSET_FIELD_V1, offsetAndMetadata.offset)
        value.set(OFFSET_VALUE_METADATA_FIELD_V1, offsetAndMetadata.metadata)
        value.set(OFFSET_VALUE_COMMIT_TIMESTAMP_FIELD_V1, offsetAndMetadata.commitTimestamp)
        // version 1 has a non empty expireTimestamp field
        value.set(OFFSET_VALUE_EXPIRE_TIMESTAMP_FIELD_V1,
          offsetAndMetadata.expireTimestamp.getOrElse(OffsetCommitRequest.DEFAULT_TIMESTAMP))
        (1, value)
      } else if (apiVersion < KAFKA_2_1_IV1) {
        val value = new Struct(OFFSET_COMMIT_VALUE_SCHEMA_V2)
        value.set(OFFSET_VALUE_OFFSET_FIELD_V2, offsetAndMetadata.offset)
        value.set(OFFSET_VALUE_METADATA_FIELD_V2, offsetAndMetadata.metadata)
        value.set(OFFSET_VALUE_COMMIT_TIMESTAMP_FIELD_V2, offsetAndMetadata.commitTimestamp)
        (2, value)
      } else {
        val value = new Struct(OFFSET_COMMIT_VALUE_SCHEMA_V3)
        // 依次写入位移值、Leader Epoch值、自定义元数据以及时间戳
        value.set(OFFSET_VALUE_OFFSET_FIELD_V3, offsetAndMetadata.offset)
        value.set(OFFSET_VALUE_LEADER_EPOCH_FIELD_V3,
          offsetAndMetadata.leaderEpoch.orElse(RecordBatch.NO_PARTITION_LEADER_EPOCH))
        value.set(OFFSET_VALUE_METADATA_FIELD_V3, offsetAndMetadata.metadata)
        value.set(OFFSET_VALUE_COMMIT_TIMESTAMP_FIELD_V3, offsetAndMetadata.commitTimestamp)
        (3, value)
      }
    }
    // 构建ByteBuffer，写入消息格式版本和结构体
    val byteBuffer = ByteBuffer.allocate(2 /* version */ + value.sizeOf)
    byteBuffer.putShort(version.toShort)
    value.writeTo(byteBuffer)
    // 返回ByteBuffer底层字节数组
    byteBuffer.array()
  }

  /**
   * Generates the payload for group metadata message from given offset and metadata
   * assuming the generation id, selected protocol, leader and member assignment are all available
   *  注册消息 value 的序列化（将消费者组重要的元数据写入到字节数组）
   * @param groupMetadata current group metadata   消费者组元数据对象
   * @param assignment the assignment for the rebalancing generation  分区消费分配方案
   * @param apiVersion the api version              Kafka API版本号，apiVersion 的取值是 Broker 端参数 inter.broker.protocol.version 的值，这个参数的值永远指向当前最新的 Kafka 版本
   * @return payload for offset commit message
   */
  def groupMetadataValue(groupMetadata: GroupMetadata,
                         assignment: Map[String, Array[Byte]],
                         apiVersion: ApiVersion): Array[Byte] = {
    // 确定消息格式版本以及格式结构（创建对应版本的结构体（Struct）来保存这些元数据）
    val (version, value) = {
      if (apiVersion < KAFKA_0_10_1_IV0)
        (0.toShort, new Struct(GROUP_METADATA_VALUE_SCHEMA_V0))
      else if (apiVersion < KAFKA_2_1_IV0)
        (1.toShort, new Struct(GROUP_METADATA_VALUE_SCHEMA_V1))
      else if (apiVersion < KAFKA_2_3_IV0)
        (2.toShort, new Struct(GROUP_METADATA_VALUE_SCHEMA_V2))
      else
        (3.toShort, new Struct(GROUP_METADATA_VALUE_SCHEMA_V3))
    }
    // 依次写入消费者组主要的元数据信息
    // 包括协议类型、Generation ID、分区分配策略和Leader成员ID
    value.set(PROTOCOL_TYPE_KEY, groupMetadata.protocolType.getOrElse(""))
    value.set(GENERATION_KEY, groupMetadata.generationId)
    value.set(PROTOCOL_KEY, groupMetadata.protocolName.orNull)
    value.set(LEADER_KEY, groupMetadata.leaderOrNull)
    // 写入最近一次状态变更时间戳
    if (version >= 2)
      value.set(CURRENT_STATE_TIMESTAMP_KEY, groupMetadata.currentStateTimestampOrDefault)
    // 写入各个成员的元数据信息
    // 包括成员ID、client.id、主机名以及会话超时时间
    val memberArray = groupMetadata.allMemberMetadata.map { memberMetadata =>
      val memberStruct = value.instance(MEMBERS_KEY)
      memberStruct.set(MEMBER_ID_KEY, memberMetadata.memberId)
      memberStruct.set(CLIENT_ID_KEY, memberMetadata.clientId)
      memberStruct.set(CLIENT_HOST_KEY, memberMetadata.clientHost)
      memberStruct.set(SESSION_TIMEOUT_KEY, memberMetadata.sessionTimeoutMs)
      // 格式版本≥0 的结构体，写入Rebalance超时时间
      if (version > 0)
        memberStruct.set(REBALANCE_TIMEOUT_KEY, memberMetadata.rebalanceTimeoutMs)
      // 格式版本≥3 的结构体，写入用于静态消费者组管理的 Group Instance ID
      if (version >= 3)
        memberStruct.set(GROUP_INSTANCE_ID_KEY, memberMetadata.groupInstanceId.orNull)

      // The group is non-empty, so the current protocol must be defined
      // 必须要确保消费者组选出了分区分配策略，否则抛出异常
      val protocol = groupMetadata.protocolName.orNull
      if (protocol == null)
        throw new IllegalStateException("Attempted to write non-empty group metadata with no defined protocol")
      // 写入成员消费订阅信息
      val metadata = memberMetadata.metadata(protocol)
      memberStruct.set(SUBSCRIPTION_KEY, ByteBuffer.wrap(metadata))

      val memberAssignment = assignment(memberMetadata.memberId)
      assert(memberAssignment != null)
      // 写入成员消费分配信息
      memberStruct.set(ASSIGNMENT_KEY, ByteBuffer.wrap(memberAssignment))

      memberStruct
    }

    value.set(MEMBERS_KEY, memberArray.toArray)
    // 向Buffer依次写入版本信息和以上写入的元数据信息
    val byteBuffer = ByteBuffer.allocate(2 /* version */ + value.sizeOf)
    byteBuffer.putShort(version)
    value.writeTo(byteBuffer)
    // 返回Buffer底层的字节数组
    byteBuffer.array()
  }

  /**
   * Decodes the offset messages' key
   *
   * @param buffer input byte-buffer
   * @return an OffsetKey or GroupMetadataKey object from the message
   */
  def readMessageKey(buffer: ByteBuffer): BaseKey = {
    val version = buffer.getShort
    val keySchema = schemaForKey(version)
    val key = keySchema.read(buffer)

    if (version <= CURRENT_OFFSET_KEY_SCHEMA_VERSION) {
      // version 0 and 1 refer to offset
      val group = key.get(OFFSET_KEY_GROUP_FIELD).asInstanceOf[String]
      val topic = key.get(OFFSET_KEY_TOPIC_FIELD).asInstanceOf[String]
      val partition = key.get(OFFSET_KEY_PARTITION_FIELD).asInstanceOf[Int]

      OffsetKey(version, GroupTopicPartition(group, new TopicPartition(topic, partition)))

    } else if (version == CURRENT_GROUP_KEY_SCHEMA_VERSION) {
      // version 2 refers to offset
      val group = key.get(GROUP_KEY_GROUP_FIELD).asInstanceOf[String]

      GroupMetadataKey(version, group)
    } else {
      throw new IllegalStateException(s"Unknown group metadata message version: $version")
    }
  }

  /**
   * Decodes the offset messages' payload and retrieves offset and metadata from it
   *
   * @param buffer input byte-buffer
   * @return an offset-metadata object from the message
   */
  def readOffsetMessageValue(buffer: ByteBuffer): OffsetAndMetadata = {
    if (buffer == null) { // tombstone
      null
    } else {
      val version = buffer.getShort
      val valueSchema = schemaForOffsetValue(version)
      val value = valueSchema.read(buffer)

      if (version == 0) {
        val offset = value.get(OFFSET_VALUE_OFFSET_FIELD_V0).asInstanceOf[Long]
        val metadata = value.get(OFFSET_VALUE_METADATA_FIELD_V0).asInstanceOf[String]
        val timestamp = value.get(OFFSET_VALUE_TIMESTAMP_FIELD_V0).asInstanceOf[Long]

        OffsetAndMetadata(offset, metadata, timestamp)
      } else if (version == 1) {
        val offset = value.get(OFFSET_VALUE_OFFSET_FIELD_V1).asInstanceOf[Long]
        val metadata = value.get(OFFSET_VALUE_METADATA_FIELD_V1).asInstanceOf[String]
        val commitTimestamp = value.get(OFFSET_VALUE_COMMIT_TIMESTAMP_FIELD_V1).asInstanceOf[Long]
        val expireTimestamp = value.get(OFFSET_VALUE_EXPIRE_TIMESTAMP_FIELD_V1).asInstanceOf[Long]

        if (expireTimestamp == OffsetCommitRequest.DEFAULT_TIMESTAMP)
          OffsetAndMetadata(offset, metadata, commitTimestamp)
        else
          OffsetAndMetadata(offset, metadata, commitTimestamp, expireTimestamp)
      } else if (version == 2) {
        val offset = value.get(OFFSET_VALUE_OFFSET_FIELD_V2).asInstanceOf[Long]
        val metadata = value.get(OFFSET_VALUE_METADATA_FIELD_V2).asInstanceOf[String]
        val commitTimestamp = value.get(OFFSET_VALUE_COMMIT_TIMESTAMP_FIELD_V2).asInstanceOf[Long]

        OffsetAndMetadata(offset, metadata, commitTimestamp)
      } else if (version == 3) {
        val offset = value.get(OFFSET_VALUE_OFFSET_FIELD_V3).asInstanceOf[Long]
        val leaderEpoch = value.get(OFFSET_VALUE_LEADER_EPOCH_FIELD_V3).asInstanceOf[Int]
        val metadata = value.get(OFFSET_VALUE_METADATA_FIELD_V3).asInstanceOf[String]
        val commitTimestamp = value.get(OFFSET_VALUE_COMMIT_TIMESTAMP_FIELD_V3).asInstanceOf[Long]

        val leaderEpochOpt: Optional[Integer] = if (leaderEpoch < 0) Optional.empty() else Optional.of(leaderEpoch)
        OffsetAndMetadata(offset, leaderEpochOpt, metadata, commitTimestamp)
      } else {
        throw new IllegalStateException(s"Unknown offset message version: $version")
      }
    }
  }

  /**
   * Decodes the group metadata messages' payload and retrieves its member metadata from it
   *
   * @param groupId The ID of the group to be read
   * @param buffer input byte-buffer
   * @param time the time instance to use
   * @return a group metadata object from the message
   */
  def readGroupMessageValue(groupId: String, buffer: ByteBuffer, time: Time): GroupMetadata = {
    if (buffer == null) { // tombstone
      null
    } else {
      val version = buffer.getShort
      val valueSchema = schemaForGroupValue(version)
      val value = valueSchema.read(buffer)

      if (version >= 0 && version <= CURRENT_GROUP_METADATA_VALUE_SCHEMA_VERSION) {
        val generationId = value.get(GENERATION_KEY).asInstanceOf[Int]
        val protocolType = value.get(PROTOCOL_TYPE_KEY).asInstanceOf[String]
        val protocol = value.get(PROTOCOL_KEY).asInstanceOf[String]
        val leaderId = value.get(LEADER_KEY).asInstanceOf[String]
        val memberMetadataArray = value.getArray(MEMBERS_KEY)
        val initialState = if (memberMetadataArray.isEmpty) Empty else Stable
        val currentStateTimestamp: Option[Long] =
          if (version >= 2 && value.hasField(CURRENT_STATE_TIMESTAMP_KEY)) {
            val timestamp = value.getLong(CURRENT_STATE_TIMESTAMP_KEY)
            if (timestamp == -1) None else Some(timestamp)
          } else None

        val members = memberMetadataArray.map { memberMetadataObj =>
          val memberMetadata = memberMetadataObj.asInstanceOf[Struct]
          val memberId = memberMetadata.get(MEMBER_ID_KEY).asInstanceOf[String]
          val groupInstanceId =
            if (version >= 3)
              Some(memberMetadata.get(GROUP_INSTANCE_ID_KEY).asInstanceOf[String])
            else
              None
          val clientId = memberMetadata.get(CLIENT_ID_KEY).asInstanceOf[String]
          val clientHost = memberMetadata.get(CLIENT_HOST_KEY).asInstanceOf[String]
          val sessionTimeout = memberMetadata.get(SESSION_TIMEOUT_KEY).asInstanceOf[Int]
          val rebalanceTimeout = if (version == 0) sessionTimeout else memberMetadata.get(REBALANCE_TIMEOUT_KEY).asInstanceOf[Int]
          val subscription = Utils.toArray(memberMetadata.get(SUBSCRIPTION_KEY).asInstanceOf[ByteBuffer])

          val member = new MemberMetadata(memberId, groupId, groupInstanceId, clientId, clientHost, rebalanceTimeout, sessionTimeout,
            protocolType, List((protocol, subscription)))
          member.assignment = Utils.toArray(memberMetadata.get(ASSIGNMENT_KEY).asInstanceOf[ByteBuffer])
          member
        }
        GroupMetadata.loadGroup(groupId, initialState, generationId, protocolType, protocol, leaderId, currentStateTimestamp, members, time)
      } else {
        throw new IllegalStateException(s"Unknown group metadata message version: $version")
      }
    }
  }

  // Formatter for use with tools such as console consumer: Consumer should also set exclude.internal.topics to false.
  // (specify --formatter "kafka.coordinator.group.GroupMetadataManager\$OffsetsMessageFormatter" when consuming __consumer_offsets)
  class OffsetsMessageFormatter extends MessageFormatter {
    def writeTo(consumerRecord: ConsumerRecord[Array[Byte], Array[Byte]], output: PrintStream): Unit = {
      Option(consumerRecord.key).map(key => GroupMetadataManager.readMessageKey(ByteBuffer.wrap(key))).foreach {
        // Only print if the message is an offset record.
        // We ignore the timestamp of the message because GroupMetadataMessage has its own timestamp.
        case offsetKey: OffsetKey =>
          val groupTopicPartition = offsetKey.key
          val value = consumerRecord.value
          val formattedValue =
            if (value == null) "NULL"
            else GroupMetadataManager.readOffsetMessageValue(ByteBuffer.wrap(value)).toString
          output.write(groupTopicPartition.toString.getBytes(StandardCharsets.UTF_8))
          output.write("::".getBytes(StandardCharsets.UTF_8))
          output.write(formattedValue.getBytes(StandardCharsets.UTF_8))
          output.write("\n".getBytes(StandardCharsets.UTF_8))
        case _ => // no-op
      }
    }
  }

  // Formatter for use with tools to read group metadata history
  class GroupMetadataMessageFormatter extends MessageFormatter {
    def writeTo(consumerRecord: ConsumerRecord[Array[Byte], Array[Byte]], output: PrintStream): Unit = {
      Option(consumerRecord.key).map(key => GroupMetadataManager.readMessageKey(ByteBuffer.wrap(key))).foreach {
        // Only print if the message is a group metadata record.
        // We ignore the timestamp of the message because GroupMetadataMessage has its own timestamp.
        case groupMetadataKey: GroupMetadataKey =>
          val groupId = groupMetadataKey.key
          val value = consumerRecord.value
          val formattedValue =
            if (value == null) "NULL"
            else GroupMetadataManager.readGroupMessageValue(groupId, ByteBuffer.wrap(value), Time.SYSTEM).toString
          output.write(groupId.getBytes(StandardCharsets.UTF_8))
          output.write("::".getBytes(StandardCharsets.UTF_8))
          output.write(formattedValue.getBytes(StandardCharsets.UTF_8))
          output.write("\n".getBytes(StandardCharsets.UTF_8))
        case _ => // no-op
      }
    }
  }

  /**
   * Exposed for printing records using [[kafka.tools.DumpLogSegments]]
   */
  def formatRecordKeyAndValue(record: Record): (Option[String], Option[String]) = {
    if (!record.hasKey) {
      throw new KafkaException("Failed to decode message using offset topic decoder (message had a missing key)")
    } else {
      GroupMetadataManager.readMessageKey(record.key) match {
        case offsetKey: OffsetKey => parseOffsets(offsetKey, record.value)
        case groupMetadataKey: GroupMetadataKey => parseGroupMetadata(groupMetadataKey, record.value)
        case _ => throw new KafkaException("Failed to decode message using offset topic decoder (message had an invalid key)")
      }
    }
  }

  private def parseOffsets(offsetKey: OffsetKey, payload: ByteBuffer): (Option[String], Option[String]) = {
    val groupId = offsetKey.key.group
    val topicPartition = offsetKey.key.topicPartition
    val keyString = s"offset_commit::group=$groupId,partition=$topicPartition"

    val offset = GroupMetadataManager.readOffsetMessageValue(payload)
    val valueString = if (offset == null) {
      "<DELETE>"
    } else {
      if (offset.metadata.isEmpty)
        s"offset=${offset.offset}"
      else
        s"offset=${offset.offset},metadata=${offset.metadata}"
    }

    (Some(keyString), Some(valueString))
  }

  private def parseGroupMetadata(groupMetadataKey: GroupMetadataKey, payload: ByteBuffer): (Option[String], Option[String]) = {
    val groupId = groupMetadataKey.key
    val keyString = s"group_metadata::group=$groupId"

    val group = GroupMetadataManager.readGroupMessageValue(groupId, payload, Time.SYSTEM)
    val valueString = if (group == null)
      "<DELETE>"
    else {
      val protocolType = group.protocolType.getOrElse("")

      val assignment = group.allMemberMetadata.map { member =>
        if (protocolType == ConsumerProtocol.PROTOCOL_TYPE) {
          val partitionAssignment = ConsumerProtocol.deserializeAssignment(ByteBuffer.wrap(member.assignment))
          val userData = Option(partitionAssignment.userData)
            .map(Utils.toArray)
            .map(hex)
            .getOrElse("")

          if (userData.isEmpty)
            s"${member.memberId}=${partitionAssignment.partitions()}"
          else
            s"${member.memberId}=${partitionAssignment.partitions()}:$userData"
        } else {
          s"${member.memberId}=${hex(member.assignment)}"
        }
      }.mkString("{", ",", "}")

      Json.encodeAsString(Map(
        "protocolType" -> protocolType,
        "protocol" -> group.protocolName.orNull,
        "generationId" -> group.generationId,
        "assignment" -> assignment
      ).asJava)
    }
    (Some(keyString), Some(valueString))
  }

  private def hex(bytes: Array[Byte]): String = {
    if (bytes.isEmpty)
      ""
    else
      "%X".format(BigInt(1, bytes))
  }

}

/**
 * 封装 < 消费者组名，主题，分区号 > 的三元组，数据容器类。已提交位移消息时，会用到
 * @param group
 * @param topicPartition
 */
case class GroupTopicPartition(group: String, topicPartition: TopicPartition) {

  def this(group: String, topic: String, partition: Int) =
    this(group, new TopicPartition(topic, partition))

  override def toString: String =
    "[%s,%s,%d]".format(group, topicPartition.topic, topicPartition.partition)
}

/**
   * 位移主题的两类消息的 Key 类型。Kafka 创建位移主题的目的，是保存消费者组的注册消息和提交位移消息，除此之外，关于位移主题Kafka还存在一类Tombstone消息。
 * 1、注册消息：标识消费者组的身份信息（消费者组的元数据，主要是指消费者组名称以及成员分区消费分配方案）
 * 2、提交位移消息：保存消费者组消费的进度信息
 * 3、Tombstone 消息：Value 为 null 的消息。因此，注册消息和提交位移消息都有对应的 Tombstone 消息。
 * 这个消息的主要作用，是让 Kafka 识别哪些 Key 对应的消息是可以被删除的，有了它，Kafka 就能保证，内部位移主题不会持续增加磁盘占用空间。
 * 注册消息中出现了 Tombstone 消息，就表示 Kafka 可以将该消费者组元数据从位移主题中删除；
 * 提交位移消息中出现了 Tombstone 消息，就表示 Kafka 能够将该消费者组在某主题分区上的位移提交数据删除。
 *
 * BaseKey就是用来区分这两类消息，而且BaseKey
 * version：消息格式版本
 *  key：消息类型
 *
 */
trait BaseKey{
  def version: Short
  def key: Any
}

/**
 * 提交位移消息的 Key
 * @param version
 * @param key       < 消费者组名，主题，分区号 > 三元组
 */
case class OffsetKey(version: Short, key: GroupTopicPartition) extends BaseKey {

  override def toString: String = key.toString
}

/**
 * 注册消息的 Key 定义
 * @param version
 * @param key     消费者组的名称
 */
case class GroupMetadataKey(version: Short, key: String) extends BaseKey {

  override def toString: String = key
}

