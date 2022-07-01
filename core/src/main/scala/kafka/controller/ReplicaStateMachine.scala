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
import kafka.server.KafkaConfig
import kafka.utils.Implicits._
import kafka.utils.Logging
import kafka.zk.KafkaZkClient
import kafka.zk.KafkaZkClient.UpdateLeaderAndIsrResult
import kafka.zk.TopicPartitionStateZNode
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors.ControllerMovedException
import org.apache.zookeeper.KeeperException.Code
import scala.collection.{Seq, mutable}

/**
 * 负责定义 Kafka 副本状态、合法的状态转换，以及管理状态之间的转换。
 * @param controllerContext
 */
abstract class ReplicaStateMachine(controllerContext: ControllerContext) extends Logging {
  /**
   * Invoked on successful controller election.
   */
  def startup(): Unit = {
    info("Initializing replica state")
    initializeReplicaState()
    info("Triggering online replica state changes")
    val (onlineReplicas, offlineReplicas) = controllerContext.onlineAndOfflineReplicas
    handleStateChanges(onlineReplicas.toSeq, OnlineReplica)
    info("Triggering offline replica state changes")
    handleStateChanges(offlineReplicas.toSeq, OfflineReplica)
    debug(s"Started replica state machine with initial state -> ${controllerContext.replicaStates}")
  }

  /**
   * Invoked on controller shutdown.
   */
  def shutdown(): Unit = {
    info("Stopped replica state machine")
  }

  /**
   * Invoked on startup of the replica's state machine to set the initial state for replicas of all existing partitions
   * in zookeeper
   */
  private def initializeReplicaState(): Unit = {
    controllerContext.allPartitions.foreach { partition =>
      val replicas = controllerContext.partitionReplicaAssignment(partition)
      replicas.foreach { replicaId =>
        val partitionAndReplica = PartitionAndReplica(partition, replicaId)
        if (controllerContext.isReplicaOnline(replicaId, partition)) {
          controllerContext.putReplicaState(partitionAndReplica, OnlineReplica)
        } else {
          // mark replicas on dead brokers as failed for topic deletion, if they belong to a topic to be deleted.
          // This is required during controller failover since during controller failover a broker can go down,
          // so the replicas on that broker should be moved to ReplicaDeletionIneligible to be on the safer side.
          controllerContext.putReplicaState(partitionAndReplica, ReplicaDeletionIneligible)
        }
      }
    }
  }

  def handleStateChanges(replicas: Seq[PartitionAndReplica], targetState: ReplicaState): Unit
}

/**
 * This class represents the state machine for replicas. It defines the states that a replica can be in, and
 * transitions to move the replica to another legal state. The different states that a replica can be in are -
 * 1. NewReplica        : The controller can create new replicas during partition reassignment. In this state, a
 *                        replica can only get become follower state change request.  Valid previous
 *                        state is NonExistentReplica 副本被创建之后所处的状态
 * 2. OnlineReplica     : Once a replica is started and part of the assigned replicas for its partition, it is in this
 *                        state. In this state, it can get either become leader or become follower state change requests.
 *                        Valid previous state are NewReplica, OnlineReplica or OfflineReplica 副本被创建之后所处的状态。
 * 3. OfflineReplica    : If a replica dies, it moves to this state. This happens when the broker hosting the replica
 *                        is down. Valid previous state are NewReplica, OnlineReplica 副本服务下线时所处的状态
 * 4. ReplicaDeletionStarted: If replica deletion starts, it is moved to this state. Valid previous state is OfflineReplica 副本被删除时所处的状态
 * 5. ReplicaDeletionSuccessful: If replica responds with no error code in response to a delete replica request, it is
 *                        moved to this state. Valid previous state is ReplicaDeletionStarted 副本被成功删除后所处的状态
 * 6. ReplicaDeletionIneligible: If replica deletion fails, it is moved to this state. Valid previous states are
 *                        ReplicaDeletionStarted and OfflineReplica 开启副本删除，但副本暂时无法被删除时所处的状态
 * 7. NonExistentReplica: If a replica is deleted successfully, it is moved to this state. Valid previous state is
 *                        ReplicaDeletionSuccessful 副本从副本状态机被移除前所处的状态
 *                       controllerBrokerRequestBatch 用于给集群 Broker 发送控制类请求
 *                       所有 Broker 在启动时，都会创建 KafkaController 实例，因而也会创建 ZKReplicaStateMachine 实例
 *                       每个 Broker 都会创建这些实例，并不代表每个 Broker 都会启动副本状态机。事实上，只有在 Controller 所在的 Broker 上，副本状态机才会被启动
 */
class ZkReplicaStateMachine(config: KafkaConfig,
                            stateChangeLogger: StateChangeLogger,
                            controllerContext: ControllerContext,
                            zkClient: KafkaZkClient,
                            controllerBrokerRequestBatch: ControllerBrokerRequestBatch)
  extends ReplicaStateMachine(controllerContext) with Logging {

  private val controllerId = config.brokerId
  this.logIdent = s"[ReplicaStateMachine controllerId=$controllerId] "

  override def handleStateChanges(replicas: Seq[PartitionAndReplica], targetState: ReplicaState): Unit = {
    if (replicas.nonEmpty) {
      try {
        // 清空Controller待发送请求集合
        controllerBrokerRequestBatch.newBatch()
        // 将所有副本对象按照Broker进行分组，依次执行状态转换操作
        // PartitionAndReplica.TopicPartition.topic 主题名 PartitionAndReplica.TopicPartition.partition 分区  PartitionAndReplica.replica 副本 Broker ID
        // groupBy 方法前： < 主题名，分区号，副本 Broker ID>  <test, 0, 0>, <test, 0, 1>, <test, 1, 0>, <test, 1, 1>）
        // groupBy 方法后： Map(0 -> Set(<test, 0, 0>, <test, 1, 0>)，1 -> Set(<test, 0, 1>, <test, 1, 1>))
        // forKeyValue 是 Kafka 自定义的方法，获取的 key 为 replicaId（副本 Broker ID），获取的值为 replicas（PartitionAndReplica）
        replicas.groupBy(_.replica).forKeyValue { (replicaId, replicas) =>
          doHandleStateChanges(replicaId, replicas, targetState)
        }
        // 发送对应的Controller请求给Broker（在kafka.controller.ZkReplicaStateMachine.doHandleStateChanges方法中，有往kafka.controller.AbstractControllerBrokerRequestBatch.leaderAndIsrRequestMap中添加leaderAndIsrRequest请求）
        controllerBrokerRequestBatch.sendRequestsToBrokers(controllerContext.epoch)
      } catch {
        // 如果Controller易主，则记录错误日志然后抛出异常
        case e: ControllerMovedException =>
          error(s"Controller moved to another broker when moving some replicas to $targetState state", e)
          throw e
        case e: Throwable => error(s"Error while moving some replicas to $targetState state", e)
      }
    }
  }

  /**
   * This API exercises the replica's state machine. It ensures that every state transition happens from a legal
   * previous state to the target state. Valid state transitions are:
   * NonExistentReplica --> NewReplica
   * --send LeaderAndIsr request with current leader and isr to the new replica and UpdateMetadata request for the
   *   partition to every live broker
   *
   * NewReplica -> OnlineReplica
   * --add the new replica to the assigned replica list if needed
   *
   * OnlineReplica,OfflineReplica -> OnlineReplica
   * --send LeaderAndIsr request with current leader and isr to the new replica and UpdateMetadata request for the
   *   partition to every live broker
   *
   * NewReplica,OnlineReplica,OfflineReplica,ReplicaDeletionIneligible -> OfflineReplica
   * --send StopReplicaRequest to the replica (w/o deletion)
   * --remove this replica from the isr and send LeaderAndIsr request (with new isr) to the leader replica and
   *   UpdateMetadata request for the partition to every live broker.
   *
   * OfflineReplica -> ReplicaDeletionStarted
   * --send StopReplicaRequest to the replica (with deletion)
   *
   * ReplicaDeletionStarted -> ReplicaDeletionSuccessful
   * -- mark the state of the replica in the state machine
   *
   * ReplicaDeletionStarted -> ReplicaDeletionIneligible
   * -- mark the state of the replica in the state machine
   *
   * ReplicaDeletionSuccessful -> NonExistentReplica
   * -- remove the replica from the in memory partition replica assignment cache
   *
   * @param replicaId The replica for which the state transition is invoked 这里应该是指副本所在的 Broker ID
   * @param replicas The partitions on this replica for which the state transition is invoked
   * @param targetState The end state that the replica should be moved to
   *                    执行状态变更和转换操作的主力方法
   */
  private def doHandleStateChanges(replicaId: Int, replicas: Seq[PartitionAndReplica], targetState: ReplicaState): Unit = {
    val stateLogger = stateChangeLogger.withControllerEpoch(controllerContext.epoch)
    val traceEnabled = stateLogger.isTraceEnabled
    // 元数据是否保存了副本状态，如果保存该副本状态，这里什么也不做，否则，则把当前副本放到  kafka.controller.ControllerContext.replicaStates字段中，并初始化为NonExistentReplica状态
    replicas.foreach(replica => controllerContext.putReplicaStateIfNotExists(replica, NonExistentReplica))
    // 副本 replicas 按照它从当前状态转换成 目标状态 targetState是否合法分成两组validReplicas（合法）和invalidReplicas（不合法）
    val (validReplicas, invalidReplicas) = controllerContext.checkValidReplicaStateChange(replicas, targetState)
    // 所有状态转换不合法的副本，记录错误日志
    invalidReplicas.foreach(replica => logInvalidTransition(replica, targetState))

    targetState match {
      // 转换到 NewReplica 状态
      case NewReplica =>
        // 遍历所有能够执行转换的副本对象
        validReplicas.foreach { replica =>
          // 获取该副本对象的分区对象，即<主题名，分区号>数据
          val partition = replica.topicPartition
          // 获取副本对象的当前状态
          val currentState = controllerContext.replicaState(replica)
          // 尝试从元数据缓存中获取该分区当前信息
          // 包括Leader是谁、ISR都有哪些副本等数据
          controllerContext.partitionLeadershipInfo(partition) match {
            // 如果成功拿到分区数据信息
            case Some(leaderIsrAndControllerEpoch) =>
              // 如果该副本是Leader副本
              if (leaderIsrAndControllerEpoch.leaderAndIsr.leader == replicaId) {
                val exception = new StateChangeFailedException(s"Replica $replicaId for partition $partition cannot be moved to NewReplica state as it is being requested to become leader")
                // 记录错误日志。Leader副本不能被设置成NewReplica状态
                logFailedStateChange(replica, currentState, OfflineReplica, exception)
              } else {
                // 否则，给该副本所在的Broker发送LeaderAndIsrRequest
                // 向它同步该分区的数据, 之后给集群当前所有Broker发送
                // UpdateMetadataRequest通知它们该分区数据发生变更（我理解的是，当前副本不是分区的leader节点，要转换成NewReplica状态，说明当前副本刚刚创建，所以要获取当前分区的信息，
                // 同时由于当前副本加入分区中，需要向其他Broker同步对当前分区的信息，让它们感知到新副本的加入）
                controllerBrokerRequestBatch.addLeaderAndIsrRequestForBrokers(Seq(replicaId),
                  replica.topicPartition,
                  leaderIsrAndControllerEpoch,
                  controllerContext.partitionFullReplicaAssignment(replica.topicPartition),
                  isNew = true)
                if (traceEnabled) {
                  logSuccessfulTransition(stateLogger, replicaId, partition, currentState, NewReplica)
                }
                // 更新缓存中的元数据，把当前副本转换成 NewReplica 状态
                controllerContext.putReplicaState(replica, NewReplica)
              }
            case None =>
              // 如果没有相应数据
              if (traceEnabled)
                logSuccessfulTransition(stateLogger, replicaId, partition, currentState, NewReplica)
              // 仅仅更新元数据缓存中该副本对象的当前状态为NewReplica即可
              controllerContext.putReplicaState(replica, NewReplica)
          }
        }
        // 转换到 OnlineReplica 状态
      case OnlineReplica =>
        validReplicas.foreach { replica =>
          // 获取副本所在分区
          val partition = replica.topicPartition
          // 获取副本当前状态
          val currentState = controllerContext.replicaState(replica)

          currentState match {
            // 如果当前状态是NewReplica
            case NewReplica =>
              // 从元数据缓存中拿到分区副本列表，ReplicaAssignment.replicas 表示 分配给当前分区副本的 Broker ID
              // 目前看副本从 NewReplica -> OnlineReplica 状态，需要有完全的主题分区的副本分配数据，但是如果集群刚启动，主题分区的副本分配数据是如何生成的呢
              // 我理解是下面的，如果副本列表不包含当前副本，调用kafka.controller.ControllerContext.updatePartitionFullReplicaAssignment方法完成主题分区的副本分配数据的更新
              val assignment = controllerContext.partitionFullReplicaAssignment(partition)
              // 如果副本列表不包含当前副本，视为异常情况
              if (!assignment.replicas.contains(replicaId)) {
                error(s"Adding replica ($replicaId) that is not part of the assignment $assignment")
                // 如果副本列表不包含当前副本，将该副本加入到副本列表中，并更新元数据缓存中该分区的副本列表
                val newAssignment = assignment.copy(replicas = assignment.replicas :+ replicaId)
                controllerContext.updatePartitionFullReplicaAssignment(partition, newAssignment)
              }
            // 如果当前状态是其他状态
            // TODO 为什么如果是除了NewReplica状态的副本更新成OnlineReplica状态，向该副本对象所在Broker发送更新数据请求，
            //  而NewReplica状态的副本更新成OnlineReplica状态则需要更新主题分区的副本分配数据
            case _ =>
              // 尝试获取该分区当前信息数据
              controllerContext.partitionLeadershipInfo(partition) match {
                // 如果存在分区信息
                // 向该副本对象所在Broker发送请求，令其同步该分区数据 TODO 为什么是LeaderAndIsrRequest请求，同步数据不应该是UpdateMetadataPartitionState吗？
                case Some(leaderIsrAndControllerEpoch) =>
                  controllerBrokerRequestBatch.addLeaderAndIsrRequestForBrokers(Seq(replicaId),
                    replica.topicPartition,
                    leaderIsrAndControllerEpoch,
                    controllerContext.partitionFullReplicaAssignment(partition), isNew = false)
                case None =>
              }
          }
          if (traceEnabled)
            logSuccessfulTransition(stateLogger, replicaId, partition, currentState, OnlineReplica)
          // 将该副本对象设置成OnlineReplica状态
          controllerContext.putReplicaState(replica, OnlineReplica)
        }
      case OfflineReplica =>
        validReplicas.foreach { replica =>
          // 向副本所在Broker发送StopReplicaRequest请求，停止对应副本
          controllerBrokerRequestBatch.addStopReplicaRequestForBrokers(Seq(replicaId), replica.topicPartition, deletePartition = false)
        }
        // 将副本对象集合划分成有Leader信息的副本集合和无Leader信息的副本集合
        // 有无 Leader 信息并不仅仅包含 Leader，还有 ISR 和 controllerEpoch 等数据
        val (replicasWithLeadershipInfo, replicasWithoutLeadershipInfo) = validReplicas.partition { replica =>
          // isDefined 和 nonEmpty 相等，从 kafka.controller.ControllerContext.partitionLeadershipInfo这个Map里面 获取replica.topicPartition这个分区的 LeaderIsrAndControllerEpoch 数据，如果有就是isDefined
          controllerContext.partitionLeadershipInfo(replica.topicPartition).isDefined
        }
        // 对于有Leader信息的副本集合而言，从它们对应的所有分区中移除该副本对象并更新ZooKeeper节点 TODO 感觉这里的replicaId应该是Broker ID，因为一个副本只能属于一个分区，但是多个分区中的副本可能在同样的Broke上
        // 会遍历有 Leader 的子集合，向这些副本所在的 Broker 发送LeaderAndIsrRequest 请求，去更新停止副本操作之后的分区信息
        val updatedLeaderIsrAndControllerEpochs = removeReplicasFromIsr(replicaId, replicasWithLeadershipInfo.map(_.topicPartition))
        // 遍历每个更新过的分区信息
        updatedLeaderIsrAndControllerEpochs.forKeyValue { (partition, leaderIsrAndControllerEpoch) =>
          stateLogger.info(s"Partition $partition state changed to $leaderIsrAndControllerEpoch after removing replica $replicaId from the ISR as part of transition to $OfflineReplica")
          // 如果分区对应主题并未被删除
          if (!controllerContext.isTopicQueuedUpForDeletion(partition.topic)) {
            // 获取该分区除给定副本以外的其他副本所在的Broker
            val recipients = controllerContext.partitionReplicaAssignment(partition).filterNot(_ == replicaId)
            // 向这些Broker发送请求更新该分区更新过的分区LeaderAndIsr数据
            controllerBrokerRequestBatch.addLeaderAndIsrRequestForBrokers(recipients,
              partition,
              leaderIsrAndControllerEpoch,
              controllerContext.partitionFullReplicaAssignment(partition), isNew = false)
          }
          val replica = PartitionAndReplica(partition, replicaId)
          val currentState = controllerContext.replicaState(replica)
          if (traceEnabled)
            logSuccessfulTransition(stateLogger, replicaId, partition, currentState, OfflineReplica)
          // 设置该分区给定副本的状态为OfflineReplica
          controllerContext.putReplicaState(replica, OfflineReplica)
        }
        // 遍历无Leader信息的所有副本对象 TODO 为什么无Leader信息的所有副本对象，只是更新元数据即可
        // 因为我们没有执行任何 Leader 选举操作，所以给这些副本所在的 Broker发送的就不是 LeaderAndIsrRequest 请求了，
        // 而是 UpdateMetadataRequest 请求，显式去告知它们更新对应分区的元数据即可
        replicasWithoutLeadershipInfo.foreach { replica =>
          val currentState = controllerContext.replicaState(replica)
          if (traceEnabled)
            logSuccessfulTransition(stateLogger, replicaId, replica.topicPartition, currentState, OfflineReplica)
          // 向集群所有Broker发送请求，更新对应分区的元数据
          controllerBrokerRequestBatch.addUpdateMetadataRequestForBrokers(controllerContext.liveOrShuttingDownBrokerIds.toSeq, Set(replica.topicPartition))
          // 设置该分区给定副本的状态为OfflineReplica
          controllerContext.putReplicaState(replica, OfflineReplica)
        }
      case ReplicaDeletionStarted =>
        validReplicas.foreach { replica =>
          val currentState = controllerContext.replicaState(replica)
          if (traceEnabled)
            logSuccessfulTransition(stateLogger, replicaId, replica.topicPartition, currentState, ReplicaDeletionStarted)
          controllerContext.putReplicaState(replica, ReplicaDeletionStarted)
          controllerBrokerRequestBatch.addStopReplicaRequestForBrokers(Seq(replicaId), replica.topicPartition, deletePartition = true)
        }
      case ReplicaDeletionIneligible =>
        validReplicas.foreach { replica =>
          val currentState = controllerContext.replicaState(replica)
          if (traceEnabled)
            logSuccessfulTransition(stateLogger, replicaId, replica.topicPartition, currentState, ReplicaDeletionIneligible)
          controllerContext.putReplicaState(replica, ReplicaDeletionIneligible)
        }
      case ReplicaDeletionSuccessful =>
        validReplicas.foreach { replica =>
          val currentState = controllerContext.replicaState(replica)
          if (traceEnabled)
            logSuccessfulTransition(stateLogger, replicaId, replica.topicPartition, currentState, ReplicaDeletionSuccessful)
          controllerContext.putReplicaState(replica, ReplicaDeletionSuccessful)
        }
      case NonExistentReplica =>
        // 遍历所有能够执行状态转换操作的副本
        validReplicas.foreach { replica =>
          // 去获取副本当前的状态
          val currentState = controllerContext.replicaState(replica)
          // 去获取副本所在分区的完整副本列表，将该副本从副本列表中移除
          val newAssignedReplicas = controllerContext
            .partitionFullReplicaAssignment(replica.topicPartition)
            .removeReplica(replica.replica)
          // 将得到的新副本列表更新到Controller元数据缓存中
          controllerContext.updatePartitionFullReplicaAssignment(replica.topicPartition, newAssignedReplicas)
          if (traceEnabled) {
            logSuccessfulTransition(stateLogger, replicaId, replica.topicPartition, currentState, NonExistentReplica)
          }
          // 将该副本从副本状态列表中移除
          controllerContext.removeReplicaState(replica)
        }
    }
  }

  /**
   * Repeatedly attempt to remove a replica from the isr of multiple partitions until there are no more remaining partitions
   * to retry.
   * @param replicaId The replica being removed from isr of multiple partitions
   * @param partitions The partitions from which we're trying to remove the replica from isr
   * @return The updated LeaderIsrAndControllerEpochs of all partitions for which we successfully removed the replica from isr.
   * 调用 doRemoveReplicasFromIsr 方法，实现将给定的副本对象从给定分区 ISR 中移除的功能。
   */
  private def removeReplicasFromIsr(
    replicaId: Int,
    partitions: Seq[TopicPartition]
  ): Map[TopicPartition, LeaderIsrAndControllerEpoch] = {
    var results = Map.empty[TopicPartition, LeaderIsrAndControllerEpoch]
    var remaining = partitions
    while (remaining.nonEmpty) {
      val (finishedRemoval, removalsToRetry) = doRemoveReplicasFromIsr(replicaId, remaining)
      remaining = removalsToRetry

      finishedRemoval.foreach {
        case (partition, Left(e)) =>
            val replica = PartitionAndReplica(partition, replicaId)
            val currentState = controllerContext.replicaState(replica)
            logFailedStateChange(replica, currentState, OfflineReplica, e)
        case (partition, Right(leaderIsrAndEpoch)) =>
          results += partition -> leaderIsrAndEpoch
      }
    }
    results
  }

  /**
   * Try to remove a replica from the isr of multiple partitions.
   * Removing a replica from isr updates partition state in zookeeper.
   *
   * @param replicaId The replica being removed from isr of multiple partitions
   * @param partitions The partitions from which we're trying to remove the replica from isr
   * @return A tuple of two elements:
   *         1. The updated Right[LeaderIsrAndControllerEpochs] of all partitions for which we successfully
   *         removed the replica from isr. Or Left[Exception] corresponding to failed removals that should
   *         not be retried
   *         2. The partitions that we should retry due to a zookeeper BADVERSION conflict. Version conflicts can occur if
   *         the partition leader updated partition state while the controller attempted to update partition state.
   *         把给定的副本对象从给定分区 ISR 中移除
   */
  private def doRemoveReplicasFromIsr(
    replicaId: Int,
    partitions: Seq[TopicPartition]
  ): (Map[TopicPartition, Either[Exception, LeaderIsrAndControllerEpoch]], Seq[TopicPartition]) = {
    // 从zk获取leaderAndIsrs数据
    val (leaderAndIsrs, partitionsWithNoLeaderAndIsrInZk) = getTopicPartitionStatesFromZk(partitions)
    // 把leaderAndIsrs分成两组，一个组中分区的isr包含这个副本，另一个isr不包含这个副本
    // （这里的replicaId应该就是Broker Id，分区中有副本在这个Broker的和没有副本在这个Broker的）
    val (leaderAndIsrsWithReplica, leaderAndIsrsWithoutReplica) = leaderAndIsrs.partition { case (_, result) =>
      result.map { leaderAndIsr =>
        leaderAndIsr.isr.contains(replicaId)
      }.getOrElse(false)
    }

    val adjustedLeaderAndIsrs: Map[TopicPartition, LeaderAndIsr] = leaderAndIsrsWithReplica.flatMap {
      case (partition, result) =>
        result.toOption.map { leaderAndIsr =>
          val newLeader = if (replicaId == leaderAndIsr.leader) LeaderAndIsr.NoLeader else leaderAndIsr.leader
          val adjustedIsr = if (leaderAndIsr.isr.size == 1) leaderAndIsr.isr else leaderAndIsr.isr.filter(_ != replicaId)
          partition -> leaderAndIsr.newLeaderAndIsr(newLeader, adjustedIsr)
        }
    }
    // 修改zk分区上的LeaderAndIsr为adjustedLeaderAndIsrs，调整成功的放到finishedPartitions，失败的放到updatesToRetry
    val UpdateLeaderAndIsrResult(finishedPartitions, updatesToRetry) = zkClient.updateLeaderAndIsr(
      adjustedLeaderAndIsrs, controllerContext.epoch, controllerContext.epochZkVersion)
    // 没有LeaderAndIsr的分区所在主题如果没有待删除队列中，则创建状态转换异常StateChangeFailedException
    val exceptionsForPartitionsWithNoLeaderAndIsrInZk: Map[TopicPartition, Either[Exception, LeaderIsrAndControllerEpoch]] =
      partitionsWithNoLeaderAndIsrInZk.iterator.flatMap { partition =>
        if (!controllerContext.isTopicQueuedUpForDeletion(partition.topic)) {
          val exception = new StateChangeFailedException(
            s"Failed to change state of replica $replicaId for partition $partition since the leader and isr " +
            "path in zookeeper is empty"
          )
          Option(partition -> Left(exception))
        } else None
      }.toMap
// ??
    val leaderIsrAndControllerEpochs: Map[TopicPartition, Either[Exception, LeaderIsrAndControllerEpoch]] =
      (leaderAndIsrsWithoutReplica ++ finishedPartitions).map { case (partition, result) =>
        (partition, result.map { leaderAndIsr =>
          val leaderIsrAndControllerEpoch = LeaderIsrAndControllerEpoch(leaderAndIsr, controllerContext.epoch)
          controllerContext.putPartitionLeadershipInfo(partition, leaderIsrAndControllerEpoch)
          leaderIsrAndControllerEpoch
        })
      }

    (leaderIsrAndControllerEpochs ++ exceptionsForPartitionsWithNoLeaderAndIsrInZk, updatesToRetry)
  }

  /**
   * Gets the partition state from zookeeper
   * @param partitions the partitions whose state we want from zookeeper
   * @return A tuple of two values:
   *         1. The Right(LeaderAndIsrs) of partitions whose state we successfully read from zookeeper.
   *         The Left(Exception) to failed zookeeper lookups or states whose controller epoch exceeds our current epoch
   *         2. The partitions that had no leader and isr state in zookeeper. This happens if the controller
   *         didn't finish partition initialization.
   *         从 ZooKeeper 中获取指定分区的状态信息，包括每个分区的 Leader 副本、ISR 集合等数据
   */
  private def getTopicPartitionStatesFromZk(
    partitions: Seq[TopicPartition]
  ): (Map[TopicPartition, Either[Exception, LeaderAndIsr]], Seq[TopicPartition]) = {
    val getDataResponses = try {
      zkClient.getTopicPartitionStatesRaw(partitions)
    } catch {
      case e: Exception =>
        return (partitions.iterator.map(_ -> Left(e)).toMap, Seq.empty)
    }

    val partitionsWithNoLeaderAndIsrInZk = mutable.Buffer.empty[TopicPartition]
    val result = mutable.Map.empty[TopicPartition, Either[Exception, LeaderAndIsr]]

    getDataResponses.foreach[Unit] { getDataResponse =>
      val partition = getDataResponse.ctx.get.asInstanceOf[TopicPartition]
      if (getDataResponse.resultCode == Code.OK) {
        TopicPartitionStateZNode.decode(getDataResponse.data, getDataResponse.stat) match {
          case None =>
            partitionsWithNoLeaderAndIsrInZk += partition
          case Some(leaderIsrAndControllerEpoch) =>
            if (leaderIsrAndControllerEpoch.controllerEpoch > controllerContext.epoch) {
              val exception = new StateChangeFailedException(
                "Leader and isr path written by another controller. This probably " +
                s"means the current controller with epoch ${controllerContext.epoch} went through a soft failure and " +
                s"another controller was elected with epoch ${leaderIsrAndControllerEpoch.controllerEpoch}. Aborting " +
                "state change by this controller"
              )
              result += (partition -> Left(exception))
            } else {
              result += (partition -> Right(leaderIsrAndControllerEpoch.leaderAndIsr))
            }
        }
      } else if (getDataResponse.resultCode == Code.NONODE) {
        partitionsWithNoLeaderAndIsrInZk += partition
      } else {
        result += (partition -> Left(getDataResponse.resultException.get))
      }
    }

    (result.toMap, partitionsWithNoLeaderAndIsrInZk)
  }

  /**
   * 记录一次成功的状态转换操作
   * @param logger
   * @param replicaId
   * @param partition
   * @param currState
   * @param targetState
   */
  private def logSuccessfulTransition(logger: StateChangeLogger, replicaId: Int, partition: TopicPartition,
                                      currState: ReplicaState, targetState: ReplicaState): Unit = {
    logger.trace(s"Changed state of replica $replicaId for partition $partition from $currState to $targetState")
  }

  /**
   * 同样也是记录错误之用，记录一次非法的状态转换
   * @param replica
   * @param targetState
   */
  private def logInvalidTransition(replica: PartitionAndReplica, targetState: ReplicaState): Unit = {
    val currState = controllerContext.replicaState(replica)
    val e = new IllegalStateException(s"Replica $replica should be in the ${targetState.validPreviousStates.mkString(",")} " +
      s"states before moving to $targetState state. Instead it is in $currState state")
    logFailedStateChange(replica, currState, targetState, e)
  }

  /**
   * 仅仅是记录一条错误日志，表明执行了一次无效的状态变更
   * @param replica
   * @param currState
   * @param targetState
   * @param t
   */
  private def logFailedStateChange(replica: PartitionAndReplica, currState: ReplicaState, targetState: ReplicaState, t: Throwable): Unit = {
    stateChangeLogger.withControllerEpoch(controllerContext.epoch)
      .error(s"Controller $controllerId epoch ${controllerContext.epoch} initiated state change of replica ${replica.replica} " +
        s"for partition ${replica.topicPartition} from $currState to $targetState failed", t)
  }
}

/**
 * 副本状态
 */
sealed trait ReplicaState {
  def state: Byte
  // 定义合法的前置状态
  def validPreviousStates: Set[ReplicaState]
}

case object NewReplica extends ReplicaState {
  val state: Byte = 1
  val validPreviousStates: Set[ReplicaState] = Set(NonExistentReplica)
}
// OnlineReplica状态
case object OnlineReplica extends ReplicaState {
  val state: Byte = 2
  // 包含 NewReplica、OnlineReplica、OfflineReplica 和 ReplicaDeletionIneligible，
  // Kafka 只允许副本从这 4 种状态变更到 OnlineReplica 状态
  val validPreviousStates: Set[ReplicaState] = Set(NewReplica, OnlineReplica, OfflineReplica, ReplicaDeletionIneligible)
}

case object OfflineReplica extends ReplicaState {
  val state: Byte = 3
  val validPreviousStates: Set[ReplicaState] = Set(NewReplica, OnlineReplica, OfflineReplica, ReplicaDeletionIneligible)
}

case object ReplicaDeletionStarted extends ReplicaState {
  val state: Byte = 4
  val validPreviousStates: Set[ReplicaState] = Set(OfflineReplica)
}

case object ReplicaDeletionSuccessful extends ReplicaState {
  val state: Byte = 5
  val validPreviousStates: Set[ReplicaState] = Set(ReplicaDeletionStarted)
}

case object ReplicaDeletionIneligible extends ReplicaState {
  val state: Byte = 6
  val validPreviousStates: Set[ReplicaState] = Set(OfflineReplica, ReplicaDeletionStarted)
}

case object NonExistentReplica extends ReplicaState {
  val state: Byte = 7
  val validPreviousStates: Set[ReplicaState] = Set(ReplicaDeletionSuccessful)
}
