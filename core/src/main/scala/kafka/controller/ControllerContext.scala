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

import kafka.cluster.Broker
import kafka.utils.Implicits._
import org.apache.kafka.common.TopicPartition

import scala.collection.{Map, Seq, Set, mutable}

object ReplicaAssignment {
  def apply(replicas: Seq[Int]): ReplicaAssignment = {
    apply(replicas, Seq.empty, Seq.empty)
  }

  val empty: ReplicaAssignment = apply(Seq.empty)
}


/**
 * @param replicas the sequence of brokers assigned to the partition. It includes the set of brokers
 *                 that were added (`addingReplicas`) and removed (`removingReplicas`). 分配给当前分区的 Broker ID
 * @param addingReplicas the replicas that are being added if there is a pending reassignment
 * @param removingReplicas the replicas that are being removed if there is a pending reassignment
 */
case class ReplicaAssignment private (replicas: Seq[Int],
                                      addingReplicas: Seq[Int],
                                      removingReplicas: Seq[Int]) {

  lazy val originReplicas: Seq[Int] = replicas.diff(addingReplicas)
  lazy val targetReplicas: Seq[Int] = replicas.diff(removingReplicas)

  def isBeingReassigned: Boolean = {
    addingReplicas.nonEmpty || removingReplicas.nonEmpty
  }

  def reassignTo(target: Seq[Int]): ReplicaAssignment = {
    val fullReplicaSet = (target ++ originReplicas).distinct
    ReplicaAssignment(
      fullReplicaSet,
      fullReplicaSet.diff(originReplicas),
      fullReplicaSet.diff(target)
    )
  }

  def removeReplica(replica: Int): ReplicaAssignment = {
    ReplicaAssignment(
      replicas.filterNot(_ == replica),
      addingReplicas.filterNot(_ == replica),
      removingReplicas.filterNot(_ == replica)
    )
  }

  override def toString: String = s"ReplicaAssignment(" +
    s"replicas=${replicas.mkString(",")}, " +
    s"addingReplicas=${addingReplicas.mkString(",")}, " +
    s"removingReplicas=${removingReplicas.mkString(",")})"
}

/**
 * 元数据信息全部封装在这个类中
 */
class ControllerContext {
  // Controller统计信息类
  val stats = new ControllerStats
  // 离线分区计数器
  var offlinePartitionCount = 0
  var preferredReplicaImbalanceCount = 0
  // 所有正在关闭中的 Broker ID 列表
  // 当 Controller 在管理集群Broker 时，它要依靠这个字段来甄别 Broker 当前是否已关闭，
  // 因为处于关闭状态的Broker 是不适合执行某些操作的，如分区重分配（Reassignment）以及主题删除等
  val shuttingDownBrokerIds = mutable.Set.empty[Int]
  // 当前所有运行中的 Broker 对象, 每个 Broker 对象就是一个 <Id，EndPoint，机架信息 > 的三元组
  private val liveBrokers = mutable.Set.empty[Broker]
  // 保存所有运行中 Broker 的 Epoch 信息，每个 Broker 都有自己的 Epoch 值。Kafka 使用 Epoch 数据防止 Zombie Broker，即一个非常老的 Broker 被选举成为 Controller。
  private val liveBrokerEpochs = mutable.Map.empty[Int, Long]
  // epoch 实际上就是 ZooKeeper 中 /controller_epoch 节点的值（属于 Controller 侧的数据），你可以认为它就是 Controller 在整个 Kafka 集群的版本号
  var epoch: Int = KafkaController.InitialControllerEpoch
  // epochZkVersion 实际上是 controller_epoch 节点的 dataVersion 值属于 Controller 侧的数据）。Kafka 使用 epochZkVersion 来判断和防止 Zombie Controller。
  var epochZkVersion: Int = KafkaController.InitialControllerEpochZkVersion
  // 保存集群上所有的主题名称
  val allTopics = mutable.Set.empty[String]
  // 保存所有主题分区的副本分配情况 <TopicPartition.topic String  <TopicPartition.partition int, ReplicaAssignment(副本)>>
  val partitionAssignments = mutable.Map.empty[String, mutable.Map[Int, ReplicaAssignment]]
  // 保存每个分区对应的分区主副本，isr集合，还有controller的epoch数
  private val partitionLeadershipInfo = mutable.Map.empty[TopicPartition, LeaderIsrAndControllerEpoch]
  // 正处于副本重分配过程的主题分区列表
  val partitionsBeingReassigned = mutable.Set.empty[TopicPartition]
  // 主题分区状态列表
  val partitionStates = mutable.Map.empty[TopicPartition, PartitionState]
  // 主题分区的副本状态列表
  val replicaStates = mutable.Map.empty[PartitionAndReplica, ReplicaState]
  // 不可用磁盘路径上的副本列表
  val replicasOnOfflineDirs = mutable.Map.empty[Int, Set[TopicPartition]]
  // 待删除主题列表
  val topicsToBeDeleted = mutable.Set.empty[String]

  /** The following topicsWithDeletionStarted variable is used to properly update the offlinePartitionCount metric.
   * When a topic is going through deletion, we don't want to keep track of its partition state
   * changes in the offlinePartitionCount metric. This goal means if some partitions of a topic are already
   * in OfflinePartition state when deletion starts, we need to change the corresponding partition
   * states to NonExistentPartition first before starting the deletion.
   *
   * However we can NOT change partition states to NonExistentPartition at the time of enqueuing topics
   * for deletion. The reason is that when a topic is enqueued for deletion, it may be ineligible for
   * deletion due to ongoing partition reassignments. Hence there might be a delay between enqueuing
   * a topic for deletion and the actual start of deletion. In this delayed interval, partitions may still
   * transition to or out of the OfflinePartition state.
   *
   * Hence we decide to change partition states to NonExistentPartition only when the actual deletion have started.
   * For topics whose deletion have actually started, we keep track of them in the following topicsWithDeletionStarted
   * variable. And once a topic is in the topicsWithDeletionStarted set, we are sure there will no longer
   * be partition reassignments to any of its partitions, and only then it's safe to move its partitions to
   * NonExistentPartition state. Once a topic is in the topicsWithDeletionStarted set, we will stop monitoring
   * its partition state changes in the offlinePartitionCount metric
   */
  // 已开启删除的主题列表
  val topicsWithDeletionStarted = mutable.Set.empty[String]
  // 暂时无法执行删除的主题列表
  val topicsIneligibleForDeletion = mutable.Set.empty[String]

  private def clearTopicsState(): Unit = {
    allTopics.clear()
    partitionAssignments.clear()
    partitionLeadershipInfo.clear()
    partitionsBeingReassigned.clear()
    replicasOnOfflineDirs.clear()
    partitionStates.clear()
    offlinePartitionCount = 0
    preferredReplicaImbalanceCount = 0
    replicaStates.clear()
  }

  /**
   * 获取某主题分区副本列表
   * @param topicPartition
   * @return
   */
  def partitionReplicaAssignment(topicPartition: TopicPartition): Seq[Int] = {
    partitionAssignments.getOrElse(topicPartition.topic, mutable.Map.empty).get(topicPartition.partition) match {
      case Some(partitionAssignment) => partitionAssignment.replicas
      case None => Seq.empty
    }
  }

  def partitionFullReplicaAssignment(topicPartition: TopicPartition): ReplicaAssignment = {
    // <TopicPartition.topic String  <TopicPartition.partition int, ReplicaAssignment(副本)>>
    partitionAssignments.getOrElse(topicPartition.topic, mutable.Map.empty)
      .getOrElse(topicPartition.partition, ReplicaAssignment.empty)
  }

  def updatePartitionFullReplicaAssignment(topicPartition: TopicPartition, newAssignment: ReplicaAssignment): Unit = {
    val assignments = partitionAssignments.getOrElseUpdate(topicPartition.topic, mutable.Map.empty)
    val previous = assignments.put(topicPartition.partition, newAssignment)
    val leadershipInfo = partitionLeadershipInfo.get(topicPartition)
    updatePreferredReplicaImbalanceMetric(topicPartition, previous, leadershipInfo,
      Some(newAssignment), leadershipInfo)
  }

  def partitionReplicaAssignmentForTopic(topic : String): Map[TopicPartition, Seq[Int]] = {
    partitionAssignments.getOrElse(topic, Map.empty).map {
      case (partition, assignment) => (new TopicPartition(topic, partition), assignment.replicas)
    }.toMap
  }

  def partitionFullReplicaAssignmentForTopic(topic : String): Map[TopicPartition, ReplicaAssignment] = {
    partitionAssignments.getOrElse(topic, Map.empty).map {
      case (partition, assignment) => (new TopicPartition(topic, partition), assignment)
    }.toMap
  }

  /**
   * 获取集群上所有主题分区对象
   * @return
   */
  def allPartitions: Set[TopicPartition] = {
    partitionAssignments.flatMap {
      case (topic, topicReplicaAssignment) => topicReplicaAssignment.map {
        case (partition, _) => new TopicPartition(topic, partition)
      }
    }.toSet
  }

  def setLiveBrokers(brokerAndEpochs: Map[Broker, Long]): Unit = {
    clearLiveBrokers()
    addLiveBrokers(brokerAndEpochs)
  }

  private def clearLiveBrokers(): Unit = {
    liveBrokers.clear()
    liveBrokerEpochs.clear()
  }

  def addLiveBrokers(brokerAndEpochs: Map[Broker, Long]): Unit = {
    liveBrokers ++= brokerAndEpochs.keySet
    liveBrokerEpochs ++= brokerAndEpochs.map { case (broker, brokerEpoch) => (broker.id, brokerEpoch) }
  }

  def removeLiveBrokers(brokerIds: Set[Int]): Unit = {
    liveBrokers --= liveBrokers.filter(broker => brokerIds.contains(broker.id))
    liveBrokerEpochs --= brokerIds
  }

  def updateBrokerMetadata(oldMetadata: Broker, newMetadata: Broker): Unit = {
    liveBrokers -= oldMetadata
    liveBrokers += newMetadata
  }

  // getter
  def liveBrokerIds: Set[Int] = liveBrokerEpochs.keySet.diff(shuttingDownBrokerIds)
  def liveOrShuttingDownBrokerIds: Set[Int] = liveBrokerEpochs.keySet
  def liveOrShuttingDownBrokers: Set[Broker] = liveBrokers
  def liveBrokerIdAndEpochs: Map[Int, Long] = liveBrokerEpochs
  def liveOrShuttingDownBroker(brokerId: Int): Option[Broker] = liveOrShuttingDownBrokers.find(_.id == brokerId)

  /**
   * 获取某个 Broker 上的所有分区
   * @param brokerId
   * @return
   */
  def partitionsOnBroker(brokerId: Int): Set[TopicPartition] = {
    partitionAssignments.flatMap {
      case (topic, topicReplicaAssignment) => topicReplicaAssignment.filter {
        case (_, partitionAssignment) => partitionAssignment.replicas.contains(brokerId)
      }.map {
        case (partition, _) => new TopicPartition(topic, partition)
      }
    }.toSet
  }

  def isReplicaOnline(brokerId: Int, topicPartition: TopicPartition, includeShuttingDownBrokers: Boolean = false): Boolean = {
    val brokerOnline = {
      if (includeShuttingDownBrokers) liveOrShuttingDownBrokerIds.contains(brokerId)
      else liveBrokerIds.contains(brokerId)
    }
    brokerOnline && !replicasOnOfflineDirs.getOrElse(brokerId, Set.empty).contains(topicPartition)
  }

  /**
   * 获取 brokerIds 指定的Broker上的分区
   * @param brokerIds
   * @return
   */
  def replicasOnBrokers(brokerIds: Set[Int]): Set[PartitionAndReplica] = {
    brokerIds.flatMap { brokerId =>
      partitionAssignments.flatMap {
        case (topic, topicReplicaAssignment) => topicReplicaAssignment.collect {
          case (partition, partitionAssignment) if partitionAssignment.replicas.contains(brokerId) =>
            PartitionAndReplica(new TopicPartition(topic, partition), brokerId)
        }
      }
    }
  }

  def replicasForTopic(topic: String): Set[PartitionAndReplica] = {
    partitionAssignments.getOrElse(topic, mutable.Map.empty).flatMap {
      case (partition, assignment) => assignment.replicas.map { r =>
        PartitionAndReplica(new TopicPartition(topic, partition), r)
      }
    }.toSet
  }

  /**
   * Kafka 要获取某个主题的所有分区对象
   * @param topic
   * @return
   */
  def partitionsForTopic(topic: String): collection.Set[TopicPartition] = {
    partitionAssignments.getOrElse(topic, mutable.Map.empty).map {
      case (partition, _) => new TopicPartition(topic, partition)
    }.toSet
  }

  /**
    * Get all online and offline replicas.
    *
    * @return a tuple consisting of first the online replicas and followed by the offline replicas
    */
  def onlineAndOfflineReplicas: (Set[PartitionAndReplica], Set[PartitionAndReplica]) = {
    val onlineReplicas = mutable.Set.empty[PartitionAndReplica]
    val offlineReplicas = mutable.Set.empty[PartitionAndReplica]
    for ((topic, partitionAssignments) <- partitionAssignments;
         (partitionId, assignment) <- partitionAssignments) {
      val partition = new TopicPartition(topic, partitionId)
      for (replica <- assignment.replicas) {
        val partitionAndReplica = PartitionAndReplica(partition, replica)
        if (isReplicaOnline(replica, partition))
          onlineReplicas.add(partitionAndReplica)
        else
          offlineReplicas.add(partitionAndReplica)
      }
    }
    (onlineReplicas, offlineReplicas)
  }

  def replicasForPartition(partitions: collection.Set[TopicPartition]): collection.Set[PartitionAndReplica] = {
    partitions.flatMap { p =>
      val replicas = partitionReplicaAssignment(p)
      replicas.map(PartitionAndReplica(p, _))
    }
  }

  def resetContext(): Unit = {
    topicsToBeDeleted.clear()
    topicsWithDeletionStarted.clear()
    topicsIneligibleForDeletion.clear()
    shuttingDownBrokerIds.clear()
    epoch = 0
    epochZkVersion = 0
    clearTopicsState()
    clearLiveBrokers()
  }

  def setAllTopics(topics: Set[String]): Unit = {
    allTopics.clear()
    allTopics ++= topics
  }

  def removeTopic(topic: String): Unit = {
    // Metric is cleaned when the topic is queued up for deletion so
    // we don't clean it twice. We clean it only if it is deleted
    // directly.
    if (!topicsToBeDeleted.contains(topic))
      cleanPreferredReplicaImbalanceMetric(topic)
    // 更新元数据缓存中的待删除主题列表
    topicsToBeDeleted -= topic
    // 更新元数据缓存中的已开始删除的主题列表
    topicsWithDeletionStarted -= topic
    // 更新元数据缓存中的所有主题列表
    allTopics -= topic
    // 更新元数据缓存中的所有主题分区信息
    partitionAssignments.remove(topic).foreach { assignments =>
      assignments.keys.foreach { partition =>
        partitionLeadershipInfo.remove(new TopicPartition(topic, partition))
      }
    }
  }

  def queueTopicDeletion(topics: Set[String]): Unit = {
    topicsToBeDeleted ++= topics
    topics.foreach(cleanPreferredReplicaImbalanceMetric)
  }

  def beginTopicDeletion(topics: Set[String]): Unit = {
    topicsWithDeletionStarted ++= topics
  }

  def isTopicDeletionInProgress(topic: String): Boolean = {
    topicsWithDeletionStarted.contains(topic)
  }

  def isTopicQueuedUpForDeletion(topic: String): Boolean = {
    topicsToBeDeleted.contains(topic)
  }

  def isTopicEligibleForDeletion(topic: String): Boolean = {
    topicsToBeDeleted.contains(topic) && !topicsIneligibleForDeletion.contains(topic)
  }

  def topicsQueuedForDeletion: Set[String] = {
    topicsToBeDeleted
  }

  def replicasInState(topic: String, state: ReplicaState): Set[PartitionAndReplica] = {
    replicasForTopic(topic).filter(replica => replicaStates(replica) == state).toSet
  }

  def areAllReplicasInState(topic: String, state: ReplicaState): Boolean = {
    replicasForTopic(topic).forall(replica => replicaStates(replica) == state)
  }

  /**
   * 判断某个主题下是否有副本处于某种状态
   * @param topic
   * @param state
   * @return
   */
  def isAnyReplicaInState(topic: String, state: ReplicaState): Boolean = {
    replicasForTopic(topic).exists(replica => replicaStates(replica) == state)
  }

  def checkValidReplicaStateChange(replicas: Seq[PartitionAndReplica], targetState: ReplicaState): (Seq[PartitionAndReplica], Seq[PartitionAndReplica]) = {
    // 副本 replicas 按照它从当前状态转换成 目标状态 targetState是否合法分成两组，分别对应着出参元组中对应的两个Seq
    replicas.partition(replica => isValidReplicaStateTransition(replica, targetState))
  }

  def checkValidPartitionStateChange(partitions: Seq[TopicPartition], targetState: PartitionState): (Seq[TopicPartition], Seq[TopicPartition]) = {
    partitions.partition(p => isValidPartitionStateTransition(p, targetState))
  }

  def putReplicaState(replica: PartitionAndReplica, state: ReplicaState): Unit = {
    replicaStates.put(replica, state)
  }

  def removeReplicaState(replica: PartitionAndReplica): Unit = {
    replicaStates.remove(replica)
  }

  def putReplicaStateIfNotExists(replica: PartitionAndReplica, state: ReplicaState): Unit = {
    // 如果 Map replicaStates 中，存在 replica（key），则获取 replica对应的状态（value）；否则，初始化为指定的状态 state，并返回该初始化状态state
    replicaStates.getOrElseUpdate(replica, state)
  }

  def putPartitionState(partition: TopicPartition, targetState: PartitionState): Unit = {
    val currentState = partitionStates.put(partition, targetState).getOrElse(NonExistentPartition)
    updatePartitionStateMetrics(partition, currentState, targetState)
  }
  // 更新offlinePartitionCount元数据
  private def updatePartitionStateMetrics(partition: TopicPartition,
                                          currentState: PartitionState,
                                          targetState: PartitionState): Unit = {
    // 如果该主题当前并未处于删除中状态
    if (!isTopicDeletionInProgress(partition.topic)) {
      // targetState表示该分区要变更到的状态
      // 如果当前状态不是OfflinePartition，即离线状态并且目标状态是离线状态
      // 这个if语句判断是否要将该主题分区状态转换到离线状态
      if (currentState != OfflinePartition && targetState == OfflinePartition) {
        offlinePartitionCount = offlinePartitionCount + 1
        // 如果当前状态已经是离线状态，但targetState不是
        // 这个else if语句判断是否要将该主题分区状态转换到非离线状态
      } else if (currentState == OfflinePartition && targetState != OfflinePartition) {
        offlinePartitionCount = offlinePartitionCount - 1
      }
    }
  }

  def putPartitionStateIfNotExists(partition: TopicPartition, state: PartitionState): Unit = {
    // 如果 Map replicaStates 中，存在 replica（key），则获取 replica对应的状态（value）；否则，初始化为指定的状态 state，并返回该初始化状态state
    // 初始化为指定的状态 state的partition，调用updatePartitionStateMetrics方法
    if (partitionStates.getOrElseUpdate(partition, state) == state)
      updatePartitionStateMetrics(partition, NonExistentPartition, state)
  }

  def replicaState(replica: PartitionAndReplica): ReplicaState = {
    replicaStates(replica)
  }

  def partitionState(partition: TopicPartition): PartitionState = {
    partitionStates(partition)
  }

  def partitionsInState(state: PartitionState): Set[TopicPartition] = {
    partitionStates.filter { case (_, s) => s == state }.keySet.toSet
  }

  def partitionsInStates(states: Set[PartitionState]): Set[TopicPartition] = {
    partitionStates.filter { case (_, s) => states.contains(s) }.keySet.toSet
  }

  def partitionsInState(topic: String, state: PartitionState): Set[TopicPartition] = {
    partitionsForTopic(topic).filter { partition => state == partitionState(partition) }.toSet
  }

  def partitionsInStates(topic: String, states: Set[PartitionState]): Set[TopicPartition] = {
    partitionsForTopic(topic).filter { partition => states.contains(partitionState(partition)) }.toSet
  }

  def putPartitionLeadershipInfo(partition: TopicPartition,
                                 leaderIsrAndControllerEpoch: LeaderIsrAndControllerEpoch): Unit = {
    val previous = partitionLeadershipInfo.put(partition, leaderIsrAndControllerEpoch)
    val replicaAssignment = partitionFullReplicaAssignment(partition)
    updatePreferredReplicaImbalanceMetric(partition, Some(replicaAssignment), previous,
      Some(replicaAssignment), Some(leaderIsrAndControllerEpoch))
  }

  def partitionLeadershipInfo(partition: TopicPartition): Option[LeaderIsrAndControllerEpoch] = {
    partitionLeadershipInfo.get(partition)
  }

  def partitionsLeadershipInfo: Map[TopicPartition, LeaderIsrAndControllerEpoch] =
    partitionLeadershipInfo

  def partitionsWithLeaders: Set[TopicPartition] =
    partitionLeadershipInfo.keySet.filter(tp => !isTopicQueuedUpForDeletion(tp.topic))

  def partitionsWithOfflineLeader: Set[TopicPartition] = {
    partitionLeadershipInfo.filter { case (topicPartition, leaderIsrAndControllerEpoch) =>
      !isReplicaOnline(leaderIsrAndControllerEpoch.leaderAndIsr.leader, topicPartition) &&
        !isTopicQueuedUpForDeletion(topicPartition.topic)
    }.keySet
  }

  def partitionLeadersOnBroker(brokerId: Int): Set[TopicPartition] = {
    partitionLeadershipInfo.filter { case (topicPartition, leaderIsrAndControllerEpoch) =>
      !isTopicQueuedUpForDeletion(topicPartition.topic) &&
        leaderIsrAndControllerEpoch.leaderAndIsr.leader == brokerId &&
        partitionReplicaAssignment(topicPartition).size > 1
    }.keySet
  }

  def clearPartitionLeadershipInfo(): Unit = partitionLeadershipInfo.clear()

  def partitionWithLeadersCount: Int = partitionLeadershipInfo.size

  private def updatePreferredReplicaImbalanceMetric(partition: TopicPartition,
                                                    oldReplicaAssignment: Option[ReplicaAssignment],
                                                    oldLeadershipInfo: Option[LeaderIsrAndControllerEpoch],
                                                    newReplicaAssignment: Option[ReplicaAssignment],
                                                    newLeadershipInfo: Option[LeaderIsrAndControllerEpoch]): Unit = {
    if (!isTopicQueuedUpForDeletion(partition.topic)) {
      oldReplicaAssignment.foreach { replicaAssignment =>
        oldLeadershipInfo.foreach { leadershipInfo =>
          if (!hasPreferredLeader(replicaAssignment, leadershipInfo))
            preferredReplicaImbalanceCount -= 1
        }
      }

      newReplicaAssignment.foreach { replicaAssignment =>
        newLeadershipInfo.foreach { leadershipInfo =>
          if (!hasPreferredLeader(replicaAssignment, leadershipInfo))
            preferredReplicaImbalanceCount += 1
        }
      }
    }
  }

  private def cleanPreferredReplicaImbalanceMetric(topic: String): Unit = {
    partitionAssignments.getOrElse(topic, mutable.Map.empty).forKeyValue { (partition, replicaAssignment) =>
      partitionLeadershipInfo.get(new TopicPartition(topic, partition)).foreach { leadershipInfo =>
        if (!hasPreferredLeader(replicaAssignment, leadershipInfo))
          preferredReplicaImbalanceCount -= 1
      }
    }
  }

  private def hasPreferredLeader(replicaAssignment: ReplicaAssignment,
                                 leadershipInfo: LeaderIsrAndControllerEpoch): Boolean = {
    val preferredReplica = replicaAssignment.replicas.head
    if (replicaAssignment.isBeingReassigned && replicaAssignment.addingReplicas.contains(preferredReplica))
      // reassigning partitions are not counted as imbalanced until the new replica joins the ISR (completes reassignment)
      !leadershipInfo.leaderAndIsr.isr.contains(preferredReplica)
    else
      leadershipInfo.leaderAndIsr.leader == preferredReplica
  }

  /**
   * 可以从某个状态转换成目标状态的合法状态集合中是否包含副本replica对应的状态
   * @param replica
   * @param targetState
   * @return
   */
  private def isValidReplicaStateTransition(replica: PartitionAndReplica, targetState: ReplicaState): Boolean = {
    // replicaStates(replica)从集合中获取副本replica的状态
    targetState.validPreviousStates.contains(replicaStates(replica))
  }

  private def isValidPartitionStateTransition(partition: TopicPartition, targetState: PartitionState): Boolean =
    targetState.validPreviousStates.contains(partitionStates(partition))

}
