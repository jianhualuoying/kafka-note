/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
*/
package kafka.controller

import kafka.api.LeaderAndIsr
import kafka.common.StateChangeFailedException
import kafka.controller.Election._
import kafka.server.KafkaConfig
import kafka.utils.Implicits._
import kafka.utils.Logging
import kafka.zk.KafkaZkClient
import kafka.zk.KafkaZkClient.UpdateLeaderAndIsrResult
import kafka.zk.TopicPartitionStateZNode
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors.ControllerMovedException
import org.apache.zookeeper.KeeperException
import org.apache.zookeeper.KeeperException.Code
import scala.collection.{Map, Seq, mutable}

/**
 * 负责定义 Kafka 分区状态、合法的状态转换，以及管理状态之间的转换
 * 每个分区都必须选举出 Leader 才能正常提供服务
 * @param controllerContext
 */
abstract class PartitionStateMachine(controllerContext: ControllerContext) extends Logging {
  /**
   * Invoked on successful controller election.
   */
  def startup(): Unit = {
    info("Initializing partition state")
    initializePartitionState()
    info("Triggering online partition state changes")
    triggerOnlinePartitionStateChange()
    debug(s"Started partition state machine with initial state -> ${controllerContext.partitionStates}")
  }

  /**
   * Invoked on controller shutdown.
   */
  def shutdown(): Unit = {
    info("Stopped partition state machine")
  }

  /**
   * This API invokes the OnlinePartition state change on all partitions in either the NewPartition or OfflinePartition
   * state. This is called on a successful controller election and on broker changes
   * 把 OfflinePartition 和 NewPartition 状态的 Broker 转换成  OnlinePartition状态
   */
  def triggerOnlinePartitionStateChange(): Unit = {
    val partitions = controllerContext.partitionsInStates(Set(OfflinePartition, NewPartition))
    triggerOnlineStateChangeForPartitions(partitions)
  }

  def triggerOnlinePartitionStateChange(topic: String): Unit = {
    val partitions = controllerContext.partitionsInStates(topic, Set(OfflinePartition, NewPartition))
    triggerOnlineStateChangeForPartitions(partitions)
  }

  /**
   * 将一组分区(待删除主题的分区除外)变更到OnlinePartition状态
   * 调用时机：
   * 1、Controller启动，选举Controller成功之后；
   * kafka.controller.KafkaController#onBrokerStartup(scala.collection.Seq) -> kafka.controller.PartitionStateMachine#triggerOnlinePartitionStateChange(java.lang.String)
   * 2. Broker下线或者启动时（kafka.controller.KafkaController#processBrokerChange()的第8步和第9步调用kafka.controller.KafkaController#onReplicasBecomeOffline(scala.collection.Set)）
   * kafka.controller.KafkaController#onReplicasBecomeOffline(scala.collection.Set) -> kafka.controller.PartitionStateMachine#triggerOnlinePartitionStateChange(java.lang.String)
   * 3、Unclean Leader选举时
   * kafka.controller.KafkaController#processUncleanLeaderElectionEnable() -> kafka.controller.PartitionStateMachine#triggerOnlinePartitionStateChange(java.lang.String)
   *
   * @param partitions
   */
  private def triggerOnlineStateChangeForPartitions(partitions: collection.Set[TopicPartition]): Unit = {
    // try to move all partitions in NewPartition or OfflinePartition state to OnlinePartition state except partitions
    // that belong to topics to be deleted
    // 判断这些分区所属的主题当前没有被执行删除操作
    val partitionsToTrigger = partitions.filter { partition =>
      !controllerContext.isTopicQueuedUpForDeletion(partition.topic)
    }.toSeq

    handleStateChanges(partitionsToTrigger, OnlinePartition, Some(OfflinePartitionLeaderElectionStrategy(false)))
    // TODO: If handleStateChanges catches an exception, it is not enough to bail out and log an error.
    // It is important to trigger leader election for those partitions.
  }

  /**
   * Invoked on startup of the partition's state machine to set the initial state for all existing partitions in
   * zookeeper
   */
  private def initializePartitionState(): Unit = {
    for (topicPartition <- controllerContext.allPartitions) {
      // check if leader and isr path exists for partition. If not, then it is in NEW state
      controllerContext.partitionLeadershipInfo(topicPartition) match {
        case Some(currentLeaderIsrAndEpoch) =>
          // else, check if the leader for partition is alive. If yes, it is in Online state, else it is in Offline state
          if (controllerContext.isReplicaOnline(currentLeaderIsrAndEpoch.leaderAndIsr.leader, topicPartition))
          // leader is alive
            controllerContext.putPartitionState(topicPartition, OnlinePartition)
          else
            controllerContext.putPartitionState(topicPartition, OfflinePartition)
        case None =>
          controllerContext.putPartitionState(topicPartition, NewPartition)
      }
    }
  }

  def handleStateChanges(
    partitions: Seq[TopicPartition],
    targetState: PartitionState
  ): Map[TopicPartition, Either[Throwable, LeaderAndIsr]] = {
    handleStateChanges(partitions, targetState, None)
  }

  def handleStateChanges(
    partitions: Seq[TopicPartition],
    targetState: PartitionState,
    leaderElectionStrategy: Option[PartitionLeaderElectionStrategy]
  ): Map[TopicPartition, Either[Throwable, LeaderAndIsr]]

}

/**
 * This class represents the state machine for partitions. It defines the states that a partition can be in, and
 * transitions to move the partition to another legal state. The different states that a partition can be in are -
 * 1. NonExistentPartition: This state indicates that the partition was either never created or was created and then
 *                          deleted. Valid previous state, if one exists, is OfflinePartition
 * 2. NewPartition        : After creation, the partition is in the NewPartition state. In this state, the partition should have
 *                          replicas assigned to it, but no leader/isr yet. Valid previous states are NonExistentPartition
 * 3. OnlinePartition     : Once a leader is elected for a partition, it is in the OnlinePartition state.
 *                          Valid previous states are NewPartition/OfflinePartition
 * 4. OfflinePartition    : If, after successful leader election, the leader for partition dies, then the partition
 *                          moves to the OfflinePartition state. Valid previous states are NewPartition/OnlinePartition
 *     每个 Broker 进程启动时，会在创建 KafkaController 对象的过程中，生成 ZkPartitionStateMachine 实例，而只有 Controller 组件所在的 Broker，才会启动分区状态机。
 */
class ZkPartitionStateMachine(config: KafkaConfig,
                              stateChangeLogger: StateChangeLogger,
                              controllerContext: ControllerContext,
                              zkClient: KafkaZkClient,
                              controllerBrokerRequestBatch: ControllerBrokerRequestBatch)
  extends PartitionStateMachine(controllerContext) {
  // Controller所在Broker的Id
  private val controllerId = config.brokerId
  this.logIdent = s"[PartitionStateMachine controllerId=$controllerId] "

  /**
   * Try to change the state of the given partitions to the given targetState, using the given
   * partitionLeaderElectionStrategyOpt if a leader election is required.
   *  把 partitions 的状态设置为 targetState，同时，还可能需要用 leaderElectionStrategy 策略为 partitions 选举新的 Leader，
   *  最终将 partitions 的 Leader 信息返回。
   * @param partitions The partitions 待执行状态变更的目标分区列表
   * @param targetState The state 目标状态
   * @param partitionLeaderElectionStrategyOpt The leader election strategy if a leader election is required. 可选项，如果传入了，就表示要执行 Leader 选举
   * @return A map of failed and successful elections when targetState is OnlinePartitions. The keys are the
   *         topic partitions and the corresponding values are either the exception that was thrown or new
   *         leader & ISR.
   */
  override def handleStateChanges(
    partitions: Seq[TopicPartition],
    targetState: PartitionState,
    partitionLeaderElectionStrategyOpt: Option[PartitionLeaderElectionStrategy]
  ): Map[TopicPartition, Either[Throwable, LeaderAndIsr]] = {
    if (partitions.nonEmpty) {
      try {
        // 清空Controller待发送请求集合，准备本次请求发送
        controllerBrokerRequestBatch.newBatch()
        //第1步： 调用doHandleStateChanges方法执行真正的状态变更逻辑
        val result = doHandleStateChanges(
          partitions,
          targetState,
          partitionLeaderElectionStrategyOpt
        )
        // 第2部：Controller给相关Broker发送请求通知状态变化
        // 调用链如下：
        // kafka.controller.ZkPartitionStateMachine.doHandleStateChanges -> kafka.controller.ZkPartitionStateMachine.electLeaderForPartitions -> kafka.controller.ZkPartitionStateMachine.doElectLeaderForPartitions
        //                                                               |->kafka.controller.ZkPartitionStateMachine.initializeLeaderAndIsrForPartitions
        // 方法 initializeLeaderAndIsrForPartitions 和 doElectLeaderForPartitions 都有往 kafka.controller.AbstractControllerBrokerRequestBatch.leaderAndIsrRequestMap 中放入数据
        controllerBrokerRequestBatch.sendRequestsToBrokers(controllerContext.epoch)
        // 返回状态变更处理结果
        result
      } catch {
        // 如果Controller易主，则记录错误日志，然后重新抛出异常
        // 上层代码会捕获该异常并执行maybeResign方法执行卸任逻辑
        case e: ControllerMovedException =>
          error(s"Controller moved to another broker when moving some partitions to $targetState state", e)
          throw e
        // 如果是其他异常，记录错误日志，封装错误返回
        case e: Throwable =>
          error(s"Error while moving some partitions to $targetState state", e)
          partitions.iterator.map(_ -> Left(e)).toMap
      }
    } else {
      // 如果partitions为空，什么都不用做
      Map.empty
    }
  }

  private def partitionState(partition: TopicPartition): PartitionState = {
    controllerContext.partitionState(partition)
  }

  /**
   * This API exercises the partition's state machine. It ensures that every state transition happens from a legal
   * previous state to the target state. Valid state transitions are:
   * NonExistentPartition -> NewPartition:
   * --load assigned replicas from ZK to controller cache
   *
   * NewPartition -> OnlinePartition
   * --assign first live replica as the leader and all live replicas as the isr; write leader and isr to ZK for this partition
   * --send LeaderAndIsr request to every live replica and UpdateMetadata request to every live broker
   *
   * OnlinePartition,OfflinePartition -> OnlinePartition
   * --select new leader and isr for this partition and a set of replicas to receive the LeaderAndIsr request, and write leader and isr to ZK
   * --for this partition, send LeaderAndIsr request to every receiving replica and UpdateMetadata request to every live broker
   *
   * NewPartition,OnlinePartition,OfflinePartition -> OfflinePartition
   * --nothing other than marking partition state as Offline
   *
   * OfflinePartition -> NonExistentPartition
   * --nothing other than marking the partition state as NonExistentPartition
   * @param partitions  The partitions for which the state transition is invoked
   * @param targetState The end state that the partition should be moved to
   * @return A map of failed and successful elections when targetState is OnlinePartitions. The keys are the
   *         topic partitions and the corresponding values are either the exception that was thrown or new
   *         leader & ISR.
   */
  private def doHandleStateChanges(
    partitions: Seq[TopicPartition],
    targetState: PartitionState,
    partitionLeaderElectionStrategyOpt: Option[PartitionLeaderElectionStrategy]
  ): Map[TopicPartition, Either[Throwable, LeaderAndIsr]] = {
    val stateChangeLog = stateChangeLogger.withControllerEpoch(controllerContext.epoch)
    val traceEnabled = stateChangeLog.isTraceEnabled
    // 初始化新分区（不在元数据缓存中的所有分区的状态）的状态为NonExistentPartition
    partitions.foreach(partition => controllerContext.putPartitionStateIfNotExists(partition, NonExistentPartition))
    // 找出要执行非法状态转换的分区，记录错误日志
    val (validPartitions, invalidPartitions) = controllerContext.checkValidPartitionStateChange(partitions, targetState)
    invalidPartitions.foreach(partition => logInvalidTransition(partition, targetState))
    // 根据targetState进入到不同的case分支
    targetState match {
      case NewPartition =>
        validPartitions.foreach { partition =>
          stateChangeLog.info(s"Changed partition $partition state from ${partitionState(partition)} to $targetState with " +
            s"assigned replicas ${controllerContext.partitionReplicaAssignment(partition).mkString(",")}")
          controllerContext.putPartitionState(partition, NewPartition)
        }
        Map.empty
      case OnlinePartition =>
        // TODO 不太明白这里为什么要分成两个集合uninitializedPartitions、partitionsToElectLeader分别进行同样的处理
        // （第1步需要）获取未初始化分区列表（kafka.controller.ControllerContext.partitionStates中获取分区状态），也就是NewPartition状态下的所有分区
        val uninitializedPartitions = validPartitions.filter(partition => partitionState(partition) == NewPartition)
        // （第2步需要）获取具备Leader选举资格的分区列表，只能为OnlinePartition和OfflinePartition状态的分区选举Leader
        val partitionsToElectLeader = validPartitions.filter(partition => partitionState(partition) == OfflinePartition || partitionState(partition) == OnlinePartition)
        // 第1步：初始化NewPartition状态分区，在ZooKeeper中写入Leader和ISR数据
        if (uninitializedPartitions.nonEmpty) {
          val successfulInitializations = initializeLeaderAndIsrForPartitions(uninitializedPartitions)
          successfulInitializations.foreach { partition =>
            stateChangeLog.info(s"Changed partition $partition from ${partitionState(partition)} to $targetState with state " +
              s"${controllerContext.partitionLeadershipInfo(partition).get.leaderAndIsr}")
            controllerContext.putPartitionState(partition, OnlinePartition)
          }
        }
        // 第2步：为具备Leader选举资格的分区推选Leader
        if (partitionsToElectLeader.nonEmpty) {
          val electionResults = electLeaderForPartitions(
            partitionsToElectLeader,
            partitionLeaderElectionStrategyOpt.getOrElse(
              throw new IllegalArgumentException("Election strategy is a required field when the target state is OnlinePartition")
            )
          )

          electionResults.foreach {
            case (partition, Right(leaderAndIsr)) =>
              stateChangeLog.info(
                s"Changed partition $partition from ${partitionState(partition)} to $targetState with state $leaderAndIsr"
              )
              // 将成功选举Leader后的分区设置成OnlinePartition状态
              controllerContext.putPartitionState(partition, OnlinePartition)
            case (_, Left(_)) => // Ignore; no need to update partition state on election error  如果选举失败，忽略之
          }
          // 返回Leader选举结果
          electionResults
        } else {
          Map.empty
        }
      case OfflinePartition =>
        validPartitions.foreach { partition =>
          if (traceEnabled)
            stateChangeLog.trace(s"Changed partition $partition state from ${partitionState(partition)} to $targetState")
          controllerContext.putPartitionState(partition, OfflinePartition)
        }
        Map.empty
      case NonExistentPartition =>
        validPartitions.foreach { partition =>
          if (traceEnabled)
            stateChangeLog.trace(s"Changed partition $partition state from ${partitionState(partition)} to $targetState")
          controllerContext.putPartitionState(partition, NonExistentPartition)
        }
        Map.empty
    }
  }

  /**
   * Initialize leader and isr partition state in zookeeper.
   * 初始化分区的leader和isr数据，更新zk数据，controller缓存和isr Broker的缓存，每个分区的分配的副本这里是不用初始化的，缓存中就有，这里需要过滤那些副本是存活的
   * @param partitions The partitions  that we're trying to initialize.
   * @return The partitions that have been successfully initialized.
   */
  private def initializeLeaderAndIsrForPartitions(partitions: Seq[TopicPartition]): Seq[TopicPartition] = {
    val successfulInitializations = mutable.Buffer.empty[TopicPartition]
    // 获取每个分区的副本列表，partitionReplicaAssignment方法从 kafka.controller.ControllerContext.partitionAssignments 中获取分区partition所在主题org.apache.kafka.common.TopicPartition.topic的分区分配情况
    // 即 <TopicPartition.partition int, ReplicaAssignment(副本)>>，然后再获取当前分区partition的副本分配情况 ReplicaAssignment（包含分区下所有副本列表其实就是分配给当前分区的 Broker ID列表）
    val replicasPerPartition = partitions.map(partition => partition -> controllerContext.partitionReplicaAssignment(partition))
    // replicasPerPartition是一个map
    // 获取每个分区的所有存活副本
    val liveReplicasPerPartition = replicasPerPartition.map { case (partition, replicas) =>
        val liveReplicasForPartition = replicas.filter(replica => controllerContext.isReplicaOnline(replica, partition))
        partition -> liveReplicasForPartition
    }
    // 按照有无存活副本对分区进行分组分为两组：有存活副本的分区；无任何存活副本的分区
    val (partitionsWithoutLiveReplicas, partitionsWithLiveReplicas) = liveReplicasPerPartition.partition { case (_, liveReplicas) => liveReplicas.isEmpty }

    partitionsWithoutLiveReplicas.foreach { case (partition, replicas) =>
      val failMsg = s"Controller $controllerId epoch ${controllerContext.epoch} encountered error during state change of " +
        s"partition $partition from New to Online, assigned replicas are " +
        s"[${replicas.mkString(",")}], live brokers are [${controllerContext.liveBrokerIds}]. No assigned " +
        "replica is alive."
      logFailedStateChange(partition, NewPartition, OnlinePartition, new StateChangeFailedException(failMsg))
    }
    // 为"有存活副本的分区"确定Leader和ISR；
    // Leader确认依据： 存活副本列表的首个副本（liveReplicas.head）被认定为Leader；
    // ISR确认依据：存活副本列表（liveReplicas.toList）被认定为ISR
    // leaderIsrAndControllerEpochs 为 Map[TopicPartition, LeaderIsrAndControllerEpoch]
    val leaderIsrAndControllerEpochs = partitionsWithLiveReplicas.map { case (partition, liveReplicas) =>
      val leaderAndIsr = LeaderAndIsr(liveReplicas.head, liveReplicas.toList)
      val leaderIsrAndControllerEpoch = LeaderIsrAndControllerEpoch(leaderAndIsr, controllerContext.epoch)
      partition -> leaderIsrAndControllerEpoch
    }.toMap
    val createResponses = try {
      zkClient.createTopicPartitionStatesRaw(leaderIsrAndControllerEpochs, controllerContext.epochZkVersion)
    } catch {
      case e: ControllerMovedException =>
        error("Controller moved to another broker when trying to create the topic partition state znode", e)
        throw e
      case e: Exception =>
        partitionsWithLiveReplicas.foreach { case (partition, _) => logFailedStateChange(partition, partitionState(partition), NewPartition, e) }
        Seq.empty
    }
    createResponses.foreach { createResponse =>
      val code = createResponse.resultCode
      val partition = createResponse.ctx.get.asInstanceOf[TopicPartition]
      val leaderIsrAndControllerEpoch = leaderIsrAndControllerEpochs(partition)
      if (code == Code.OK) {
        // 当前分区的zk请求成功后，更新当前分区的缓存数据，leaderIsrAndControllerEpoch：每个分区对应的分区主副本，isr集合，还有controller的epoch数
        controllerContext.putPartitionLeadershipInfo(partition, leaderIsrAndControllerEpoch)
        // 向当前分区的isr列表中的Broker发送leaderAndIsrRequest请求，让其更新缓存数据
        controllerBrokerRequestBatch.addLeaderAndIsrRequestForBrokers(leaderIsrAndControllerEpoch.leaderAndIsr.isr,
          partition, leaderIsrAndControllerEpoch, controllerContext.partitionFullReplicaAssignment(partition), isNew = true)
        successfulInitializations += partition
      } else {
        logFailedStateChange(partition, NewPartition, OnlinePartition, code)
      }
    }
    successfulInitializations
  }

  /**
   * Repeatedly attempt to elect leaders for multiple partitions until there are no more remaining partitions to retry.
   * @param partitions The partitions that we're trying to elect leaders for.
   * @param partitionLeaderElectionStrategy The election strategy to use.
   * @return A map of failed and successful elections. The keys are the topic partitions and the corresponding values are
   *         either the exception that was thrown or new leader & ISR.
   */
  private def electLeaderForPartitions(
    partitions: Seq[TopicPartition],
    partitionLeaderElectionStrategy: PartitionLeaderElectionStrategy
  ): Map[TopicPartition, Either[Throwable, LeaderAndIsr]] = {
    var remaining = partitions
    val finishedElections = mutable.Map.empty[TopicPartition, Either[Throwable, LeaderAndIsr]]
    // 不断尝试为多个分区选举 Leader，直到所有分区都成功选出 Leader
    while (remaining.nonEmpty) {
      val (finished, updatesToRetry) = doElectLeaderForPartitions(remaining, partitionLeaderElectionStrategy)
      remaining = updatesToRetry
      // Scala中的Either是一个有趣的类，它代表两个可能的类型的其中一个类型的值。这两个值之间没有交集。Either是抽象类，有两个具体的实现类: Left和Right。Either可以作为scala.Option的替换，可以用scala.util.Left替换scala.None,用scala.Right替换scala.Some。一般在时实践中使用scala.util.Left作为Failure,而scala.util.Right作为成功的值。
      // 这里 finished 之所以能对应Left和Right， 是因为在 kafka.zk.KafkaZkClient.updateLeaderAndIsr方法中本地变量 finished 的定义是如果成功 就返回 Right(updatedLeaderAndIsr)，如果失败就返回 Left(...Exception)，
      // 所以这里尽管这里的finished中有选举失败的，有选举成功的，也能够区分开，Left代表失败，Right代表成功。
      finished.foreach {
        case (partition, Left(e)) =>
          logFailedStateChange(partition, partitionState(partition), OnlinePartition, e)
        case (_, Right(_)) => // Ignore; success so no need to log failed state change
      }

      finishedElections ++= finished

      if (remaining.nonEmpty)
        logger.info(s"Retrying leader election with strategy $partitionLeaderElectionStrategy for partitions $remaining")
    }

    finishedElections.toMap
  }

  /**
   * Try to elect leaders for multiple partitions.
   * Electing a leader for a partition updates partition state in zookeeper.
   *
   * @param partitions The partitions that we're trying to elect leaders for.
   * @param partitionLeaderElectionStrategy The election strategy to use.
   * @return A tuple of two values:
   *         1. The partitions and the expected leader and isr that successfully had a leader elected. And exceptions
   *         corresponding to failed elections that should not be retried.
   *         2. The partitions that we should retry due to a zookeeper BADVERSION conflict. Version conflicts can occur if
   *         the partition leader updated partition state while the controller attempted to update partition state.
   */
  private def doElectLeaderForPartitions(
    partitions: Seq[TopicPartition],
    partitionLeaderElectionStrategy: PartitionLeaderElectionStrategy
  ): (Map[TopicPartition, Either[Exception, LeaderAndIsr]], Seq[TopicPartition]) = {
    val getDataResponses = try {
      // 批量获取ZooKeeper中给定分区的znode节点数据，这里从zk获取的主要是分区的controllerEpoch数据，拿这个数据和controller缓存中的controllerContext.epoch比较，避免controller重新选举后产生数据的不一致
      zkClient.getTopicPartitionStatesRaw(partitions)
    } catch {
      case e: Exception =>
        return (partitions.iterator.map(_ -> Left(e)).toMap, Seq.empty)
    }
    // 构建两个容器，分别保存可选举Leader分区列表和选举失败分区列表
    val failedElections = mutable.Map.empty[TopicPartition, Either[Exception, LeaderAndIsr]]
    val validLeaderAndIsrs = mutable.Buffer.empty[(TopicPartition, LeaderAndIsr)]
    // 遍历每个分区的znode节点数据
    getDataResponses.foreach { getDataResponse =>
      val partition = getDataResponse.ctx.get.asInstanceOf[TopicPartition]
      val currState = partitionState(partition)
      // 如果成功拿到znode节点数据
      if (getDataResponse.resultCode == Code.OK) {
        TopicPartitionStateZNode.decode(getDataResponse.data, getDataResponse.stat) match {
          // 节点数据中含Leader和ISR信息
          case Some(leaderIsrAndControllerEpoch) =>
            // 如果节点数据的Controller Epoch值大于当前Controller Epoch值
            if (leaderIsrAndControllerEpoch.controllerEpoch > controllerContext.epoch) {
              val failMsg = s"Aborted leader election for partition $partition since the LeaderAndIsr path was " +
                s"already written by another controller. This probably means that the current controller $controllerId went through " +
                s"a soft failure and another controller was elected with epoch ${leaderIsrAndControllerEpoch.controllerEpoch}."
              // 将该分区加入到选举失败分区列表
              failedElections.put(partition, Left(new StateChangeFailedException(failMsg)))
            } else {
              // 将该分区加入到可选举Leader分区列表
              validLeaderAndIsrs += partition -> leaderIsrAndControllerEpoch.leaderAndIsr
            }
          // 如果节点数据不含Leader和ISR信息
          case None =>
            val exception = new StateChangeFailedException(s"LeaderAndIsr information doesn't exist for partition $partition in $currState state")
            // 将该分区加入到选举失败分区列表
            failedElections.put(partition, Left(exception))
        }
        // 如果没有拿到znode节点数据，则将该分区加入到选举失败分区列表
      } else if (getDataResponse.resultCode == Code.NONODE) {
        val exception = new StateChangeFailedException(s"LeaderAndIsr information doesn't exist for partition $partition in $currState state")
        failedElections.put(partition, Left(exception))
      } else {
        failedElections.put(partition, Left(getDataResponse.resultException.get))
      }
    }
    // 是否包含可选举 Leader 的分区。如果一个满足选举 Leader 的分区都没有，方法直接返回
    if (validLeaderAndIsrs.isEmpty) {
      return (failedElections.toMap, Seq.empty)
    }
    // 开始选举Leader，并根据有无Leader将分区进行分区，【这里根据选举策略选举不出来的就直接选举失败，不再重试】
    val (partitionsWithoutLeaders, partitionsWithLeaders) = partitionLeaderElectionStrategy match {
      case OfflinePartitionLeaderElectionStrategy(allowUnclean) =>
        val partitionsWithUncleanLeaderElectionState = collectUncleanLeaderElectionState(
          validLeaderAndIsrs,
          allowUnclean
        )
        // 这四个方法大同小异，基本上，选择 Leader 的规则，就是选择副本集合中首个存活且处于 ISR 中的副本作为 Leader。
        // 为OffinePartition分区选举Leader
        leaderForOffline(controllerContext, partitionsWithUncleanLeaderElectionState).partition(_.leaderAndIsr.isEmpty)
      case ReassignPartitionLeaderElectionStrategy =>
        // 为副本重分配的分区选举Leader
        leaderForReassign(controllerContext, validLeaderAndIsrs).partition(_.leaderAndIsr.isEmpty)
      case PreferredReplicaPartitionLeaderElectionStrategy =>
        // 为分区执行Preferred副本Leader选举
        leaderForPreferredReplica(controllerContext, validLeaderAndIsrs).partition(_.leaderAndIsr.isEmpty)
      case ControlledShutdownPartitionLeaderElectionStrategy =>
        // 为因Broker正常关闭而受影响的分区选举Leader
        leaderForControlledShutdown(controllerContext, validLeaderAndIsrs).partition(_.leaderAndIsr.isEmpty)
    }
    // 将所有选举失败的分区全部加入到Leader选举失败分区列表
    partitionsWithoutLeaders.foreach { electionResult =>
      val partition = electionResult.topicPartition
      val failMsg = s"Failed to elect leader for partition $partition under strategy $partitionLeaderElectionStrategy"
      failedElections.put(partition, Left(new StateChangeFailedException(failMsg)))
    }
    // 选举成功分区的存活的 Broker
    val recipientsPerPartition = partitionsWithLeaders.map(result => result.topicPartition -> result.liveReplicas).toMap
    // 选举成功分区的存活的 leaderAndIsr
    val adjustedLeaderAndIsrs = partitionsWithLeaders.map(result => result.topicPartition -> result.leaderAndIsr.get).toMap
    // 使用新选举的Leader和ISR信息更新ZooKeeper上分区的znode节点数据【更新失败的放到updatesToRetry中，继续重试进行更新】
    val UpdateLeaderAndIsrResult(finishedUpdates, updatesToRetry) = zkClient.updateLeaderAndIsr(
      adjustedLeaderAndIsrs, controllerContext.epoch, controllerContext.epochZkVersion)
    finishedUpdates.forKeyValue { (partition, result) =>
      // 对于ZooKeeper znode节点数据更新成功的分区，封装对应的Leader和ISR信息
      // 构建LeaderAndIsr请求，并将该请求加入到Controller待发送请求集合，等待后续统一发送
      // 这里为分区选举主副本，更新leaderIsrAndControllerEpoch缓存，发送leaderAndIsrRequest请求和kafka.controller.ZkPartitionStateMachine.initializeLeaderAndIsrForPartitions方法中做的事类似，
      // initializeLeaderAndIsrForPartitions是针对还未初始化的分区（NewPartition状态），一个是满足选举条件的分区（OnlinePartition和OfflinePartition状态）
      result.foreach { leaderAndIsr =>
        val replicaAssignment = controllerContext.partitionFullReplicaAssignment(partition)
        val leaderIsrAndControllerEpoch = LeaderIsrAndControllerEpoch(leaderAndIsr, controllerContext.epoch)
        // 更新缓存的 leaderIsrAndControllerEpoch：每个分区对应的分区主副本，isr集合，还有controller的epoch数
        controllerContext.putPartitionLeadershipInfo(partition, leaderIsrAndControllerEpoch)
        // 向当前分区的isr列表中的Broker（recipientsPerPartition有存储）发送leaderAndIsrRequest请求，让其更新缓存数据
        // kafka.controller.ZkPartitionStateMachine.handleStateChanges方法的第 2 步是 Controller 给相关的 Broker 发送请求，请求的数据是这里放到kafka.controller.AbstractControllerBrokerRequestBatch.leaderAndIsrRequestMap中的
        controllerBrokerRequestBatch.addLeaderAndIsrRequestForBrokers(recipientsPerPartition(partition), partition,
          leaderIsrAndControllerEpoch, replicaAssignment, isNew = false)
      }
    }
    // 返回选举结果，包括成功选举并更新ZooKeeper节点的分区（finishedUpdates）、选举失败分区（failedElections）以及 ZooKeeper节点更新失败的分区（updatesToRetry）
    (finishedUpdates ++ failedElections, updatesToRetry)
  }

  /* For the provided set of topic partition and partition sync state it attempts to determine if unclean
   * leader election should be performed. Unclean election should be performed if there are no live
   * replica which are in sync and unclean leader election is allowed (allowUnclean parameter is true or
   * the topic has been configured to allow unclean election).
   *
   * @param leaderIsrAndControllerEpochs set of partition to determine if unclean leader election should be
   *                                     allowed
   * @param allowUnclean whether to allow unclean election without having to read the topic configuration
   * @return a sequence of three element tuple:
   *         1. topic partition
   *         2. leader, isr and controller epoc. Some means election should be performed
   *         3. allow unclean
   */
  private def collectUncleanLeaderElectionState(
    leaderAndIsrs: Seq[(TopicPartition, LeaderAndIsr)],
    allowUnclean: Boolean
  ): Seq[(TopicPartition, Option[LeaderAndIsr], Boolean)] = {
    val (partitionsWithNoLiveInSyncReplicas, partitionsWithLiveInSyncReplicas) = leaderAndIsrs.partition {
      case (partition, leaderAndIsr) =>
        val liveInSyncReplicas = leaderAndIsr.isr.filter(controllerContext.isReplicaOnline(_, partition))
        liveInSyncReplicas.isEmpty
    }

    val electionForPartitionWithoutLiveReplicas = if (allowUnclean) {
      partitionsWithNoLiveInSyncReplicas.map { case (partition, leaderAndIsr) =>
        (partition, Option(leaderAndIsr), true)
      }
    } else {
      val (logConfigs, failed) = zkClient.getLogConfigs(
        partitionsWithNoLiveInSyncReplicas.iterator.map { case (partition, _) => partition.topic }.toSet,
        config.originals()
      )

      partitionsWithNoLiveInSyncReplicas.map { case (partition, leaderAndIsr) =>
        if (failed.contains(partition.topic)) {
          logFailedStateChange(partition, partitionState(partition), OnlinePartition, failed(partition.topic))
          (partition, None, false)
        } else {
          (
            partition,
            Option(leaderAndIsr),
            logConfigs(partition.topic).uncleanLeaderElectionEnable.booleanValue()
          )
        }
      }
    }

    electionForPartitionWithoutLiveReplicas ++
    partitionsWithLiveInSyncReplicas.map { case (partition, leaderAndIsr) =>
      (partition, Option(leaderAndIsr), false)
    }
  }

  private def logInvalidTransition(partition: TopicPartition, targetState: PartitionState): Unit = {
    val currState = partitionState(partition)
    val e = new IllegalStateException(s"Partition $partition should be in one of " +
      s"${targetState.validPreviousStates.mkString(",")} states before moving to $targetState state. Instead it is in " +
      s"$currState state")
    logFailedStateChange(partition, currState, targetState, e)
  }

  private def logFailedStateChange(partition: TopicPartition, currState: PartitionState, targetState: PartitionState, code: Code): Unit = {
    logFailedStateChange(partition, currState, targetState, KeeperException.create(code))
  }

  private def logFailedStateChange(partition: TopicPartition, currState: PartitionState, targetState: PartitionState, t: Throwable): Unit = {
    stateChangeLogger.withControllerEpoch(controllerContext.epoch)
      .error(s"Controller $controllerId epoch ${controllerContext.epoch} failed to change state for partition $partition " +
        s"from $currState to $targetState", t)
  }
}

object PartitionLeaderElectionAlgorithms {
  /**
   *
   * @param assignment 分区的副本列表。专属的名称: Assigned Replicas，简称 AR。kafka-topics 脚本查看主题时，
   *                   应该可以看到名为 Replicas 的一列数据。这列数据显示的，就是主题下每个分区的 AR
   *                   因为类型为Seq[Int]，所以，AR 是有顺序的，而且不一定和 ISR 的顺序相同
   * @param isr        分区所有与 Leader 副本保持同步的副本列表，Leader 副本自己也在 ISR 中，作为 Seq[Int]类型的变量，isr 自身也是有顺序的
   * @param liveReplicas  该分区下所有处于存活状态的副本（根据 Controller 元数据缓存中的数据来判定副本是否存活）
   * @param uncleanLeaderElectionEnabled 在默认配置下，只要不是由 AdminClient 发起的 Leader 选举，这个参数的值一般是false，即 Kafka 不允许执行 Unclean Leader 选举
   *                                     Unclean Leader 选举，是指在 ISR 列表为空的情况下，Kafka 选择一个非 ISR 副本作为新的 Leader，存在丢失数据的风险
   * @param controllerContext
   * @return
   */
  def offlinePartitionLeaderElection(assignment: Seq[Int], isr: Seq[Int], liveReplicas: Set[Int], uncleanLeaderElectionEnabled: Boolean, controllerContext: ControllerContext): Option[Int] = {
    // 从当前分区副本列表中寻找首个处于存活状态[liveReplicas.contains(id)]的ISR副本[isr.contains(id)]
    assignment.find(id => liveReplicas.contains(id) && isr.contains(id)).orElse {
      // 如果找不到满足条件的副本，查看是否允许Unclean Leader选举
      // 即Broker端参数unclean.leader.election.enable是否等于true
      if (uncleanLeaderElectionEnabled) {
        // 选择当前副本列表中的第一个存活副本作为Leader
        val leaderOpt = assignment.find(liveReplicas.contains)
        if (leaderOpt.isDefined)
          controllerContext.stats.uncleanLeaderElectionRate.mark()
        leaderOpt
      } else {
        // 如果不允许Unclean Leader选举，则返回None表示无法选举Leader
        None
      }
    }
  }

  def reassignPartitionLeaderElection(reassignment: Seq[Int], isr: Seq[Int], liveReplicas: Set[Int]): Option[Int] = {
    reassignment.find(id => liveReplicas.contains(id) && isr.contains(id))
  }

  def preferredReplicaPartitionLeaderElection(assignment: Seq[Int], isr: Seq[Int], liveReplicas: Set[Int]): Option[Int] = {
    assignment.headOption.filter(id => liveReplicas.contains(id) && isr.contains(id))
  }

  def controlledShutdownPartitionLeaderElection(assignment: Seq[Int], isr: Seq[Int], liveReplicas: Set[Int], shuttingDownBrokers: Set[Int]): Option[Int] = {
    assignment.find(id => liveReplicas.contains(id) && isr.contains(id) && !shuttingDownBrokers.contains(id))
  }
}
// 分区Leader选举策略接口
sealed trait PartitionLeaderElectionStrategy
// 离线分区Leader选举策略: 因为 Leader 副本下线而引发的分区 Leader选举
final case class OfflinePartitionLeaderElectionStrategy(allowUnclean: Boolean) extends PartitionLeaderElectionStrategy
// 分区副本重分配Leader选举策略: 因为执行分区副本重分配操作而引发的分区 Leader 选举
final case object ReassignPartitionLeaderElectionStrategy extends PartitionLeaderElectionStrategy
// 分区Preferred副本Leader选举策略: 因为执行 Preferred 副本 Leader 选举而引发的分区 Leader 选举
final case object PreferredReplicaPartitionLeaderElectionStrategy extends PartitionLeaderElectionStrategy
// Broker Controlled关闭时Leader选举策略: 因为正常关闭 Broker 而引发的分区 Leader 选举
final case object ControlledShutdownPartitionLeaderElectionStrategy extends PartitionLeaderElectionStrategy

sealed trait PartitionState {
  // 状态序号，无实际用途
  def state: Byte
  // 合法前置状态集合
  def validPreviousStates: Set[PartitionState]
}

/**
 * 分区被创建后被设置成这个状态，表明它是一个全新的分区对象。处于这个状态的分区，被 Kafka 认为是“未初始化”，因此，不能选举 Leader
 */
case object NewPartition extends PartitionState {
  val state: Byte = 0
  val validPreviousStates: Set[PartitionState] = Set(NonExistentPartition)
}

/**
 * 分区正式提供服务时所处的状态
 */
case object OnlinePartition extends PartitionState {
  val state: Byte = 1
  val validPreviousStates: Set[PartitionState] = Set(NewPartition, OnlinePartition, OfflinePartition)
}

/**
 * 分区下线后所处的状态
 */
case object OfflinePartition extends PartitionState {
  val state: Byte = 2
  val validPreviousStates: Set[PartitionState] = Set(NewPartition, OnlinePartition, OfflinePartition)
}

/**
 * 分区被删除，并且从分区状态机移除后所处的状态
 */
case object NonExistentPartition extends PartitionState {
  val state: Byte = 3
  val validPreviousStates: Set[PartitionState] = Set(OfflinePartition)
}
