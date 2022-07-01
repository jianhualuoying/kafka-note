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

import kafka.server.KafkaConfig
import kafka.utils.Logging
import kafka.zk.KafkaZkClient
import org.apache.kafka.common.TopicPartition

import scala.collection.Set
import scala.collection.mutable

trait DeletionClient {
  def deleteTopic(topic: String, epochZkVersion: Int): Unit
  def deleteTopicDeletions(topics: Seq[String], epochZkVersion: Int): Unit
  def mutePartitionModifications(topic: String): Unit
  def sendMetadataUpdate(partitions: Set[TopicPartition]): Unit
}

class ControllerDeletionClient(controller: KafkaController, zkClient: KafkaZkClient) extends DeletionClient {
  override def deleteTopic(topic: String, epochZkVersion: Int): Unit = {
    zkClient.deleteTopicZNode(topic, epochZkVersion)
    zkClient.deleteTopicConfigs(Seq(topic), epochZkVersion)
    zkClient.deleteTopicDeletions(Seq(topic), epochZkVersion)
  }

  override def deleteTopicDeletions(topics: Seq[String], epochZkVersion: Int): Unit = {
    zkClient.deleteTopicDeletions(topics, epochZkVersion)
  }

  override def mutePartitionModifications(topic: String): Unit = {
    controller.unregisterPartitionModificationsHandlers(Seq(topic))
  }

  override def sendMetadataUpdate(partitions: Set[TopicPartition]): Unit = {
    controller.sendUpdateMetadataRequest(controller.controllerContext.liveOrShuttingDownBrokerIds.toSeq, partitions)
  }
}

/**
 * This manages the state machine for topic deletion.
 * 1. TopicCommand issues topic deletion by creating a new admin path /admin/delete_topics/<topic>
 * 2. The controller listens for child changes on /admin/delete_topic and starts topic deletion for the respective topics
 * 3. The controller's ControllerEventThread handles topic deletion. A topic will be ineligible
 *    for deletion in the following scenarios -
  *   3.1 broker hosting one of the replicas for that topic goes down
  *   3.2 partition reassignment for partitions of that topic is in progress
 * 4. Topic deletion is resumed when -
 *    4.1 broker hosting one of the replicas for that topic is started
 *    4.2 partition reassignment for partitions of that topic completes
 * 5. Every replica for a topic being deleted is in either of the 3 states -
 *    5.1 TopicDeletionStarted Replica enters TopicDeletionStarted phase when onPartitionDeletion is invoked.
 *        This happens when the child change watch for /admin/delete_topics fires on the controller. As part of this state
 *        change, the controller sends StopReplicaRequests to all replicas. It registers a callback for the
 *        StopReplicaResponse when deletePartition=true thereby invoking a callback when a response for delete replica
 *        is received from every replica)
 *    5.2 TopicDeletionSuccessful moves replicas from
 *        TopicDeletionStarted->TopicDeletionSuccessful depending on the error codes in StopReplicaResponse
 *    5.3 TopicDeletionFailed moves replicas from
 *        TopicDeletionStarted->TopicDeletionFailed depending on the error codes in StopReplicaResponse.
 *        In general, if a broker dies and if it hosted replicas for topics being deleted, the controller marks the
 *        respective replicas in TopicDeletionFailed state in the onBrokerFailure callback. The reason is that if a
 *        broker fails before the request is sent and after the replica is in TopicDeletionStarted state,
 *        it is possible that the replica will mistakenly remain in TopicDeletionStarted state and topic deletion
 *        will not be retried when the broker comes back up.
 * 6. A topic is marked successfully deleted only if all replicas are in TopicDeletionSuccessful
 *    state. Topic deletion teardown mode deletes all topic state from the controllerContext
 *    as well as from zookeeper. This is the only time the /brokers/topics/<topic> path gets deleted. On the other hand,
 *    if no replica is in TopicDeletionStarted state and at least one replica is in TopicDeletionFailed state, then
 *    it marks the topic for deletion retry.
 *    负责对指定 Kafka 主题执行删除操作，清除待删除主题在集群上的各类“痕迹”。
 * @param controller
 */
class TopicDeletionManager(config: KafkaConfig,
                           controllerContext: ControllerContext,
                           replicaStateMachine: ReplicaStateMachine,
                           partitionStateMachine: PartitionStateMachine,
                           client: DeletionClient) extends Logging {
  this.logIdent = s"[Topic Deletion Manager ${config.brokerId}] "
  val isDeleteTopicEnabled: Boolean = config.deleteTopicEnable

  def init(initialTopicsToBeDeleted: Set[String], initialTopicsIneligibleForDeletion: Set[String]): Unit = {
    info(s"Initializing manager with initial deletions: $initialTopicsToBeDeleted, " +
      s"initial ineligible deletions: $initialTopicsIneligibleForDeletion")

    if (isDeleteTopicEnabled) {
      controllerContext.queueTopicDeletion(initialTopicsToBeDeleted)
      controllerContext.topicsIneligibleForDeletion ++= initialTopicsIneligibleForDeletion & controllerContext.topicsToBeDeleted
    } else {
      // if delete topic is disabled clean the topic entries under /admin/delete_topics
      info(s"Removing $initialTopicsToBeDeleted since delete topic is disabled")
      client.deleteTopicDeletions(initialTopicsToBeDeleted.toSeq, controllerContext.epochZkVersion)
    }
  }

  def tryTopicDeletion(): Unit = {
    if (isDeleteTopicEnabled) {
      resumeDeletions()
    }
  }

  /**
   * Invoked by the child change listener on /admin/delete_topics to queue up the topics for deletion. The topic gets added
   * to the topicsToBeDeleted list and only gets removed from the list when the topic deletion has completed successfully
   * i.e. all replicas of all partitions of that topic are deleted successfully.
   * @param topics Topics that should be deleted
   */
  def enqueueTopicsForDeletion(topics: Set[String]): Unit = {
    if (isDeleteTopicEnabled) {
      controllerContext.queueTopicDeletion(topics)
      resumeDeletions()
    }
  }

  /**
   * Invoked when any event that can possibly resume topic deletion occurs. These events include -
   * 1. New broker starts up. Any replicas belonging to topics queued up for deletion can be deleted since the broker is up
   * 2. Partition reassignment completes. Any partitions belonging to topics queued up for deletion finished reassignment
   * @param topics Topics for which deletion can be resumed
   */
  def resumeDeletionForTopics(topics: Set[String] = Set.empty): Unit = {
    if (isDeleteTopicEnabled) {
      val topicsToResumeDeletion = topics & controllerContext.topicsToBeDeleted
      if (topicsToResumeDeletion.nonEmpty) {
        controllerContext.topicsIneligibleForDeletion --= topicsToResumeDeletion
        resumeDeletions()
      }
    }
  }

  /**
   * Invoked when a broker that hosts replicas for topics to be deleted goes down. Also invoked when the callback for
   * StopReplicaResponse receives an error code for the replicas of a topic to be deleted. As part of this, the replicas
   * are moved from ReplicaDeletionStarted to ReplicaDeletionIneligible state. Also, the topic is added to the list of topics
   * ineligible for deletion until further notice.
   * @param replicas Replicas for which deletion has failed
   */
  def failReplicaDeletion(replicas: Set[PartitionAndReplica]): Unit = {
    if (isDeleteTopicEnabled) {
      val replicasThatFailedToDelete = replicas.filter(r => isTopicQueuedUpForDeletion(r.topic))
      if (replicasThatFailedToDelete.nonEmpty) {
        val topics = replicasThatFailedToDelete.map(_.topic)
        debug(s"Deletion failed for replicas ${replicasThatFailedToDelete.mkString(",")}. Halting deletion for topics $topics")
        replicaStateMachine.handleStateChanges(replicasThatFailedToDelete.toSeq, ReplicaDeletionIneligible)
        markTopicIneligibleForDeletion(topics, reason = "replica deletion failure")
        resumeDeletions()
      }
    }
  }

  /**
   * 标记给定主题为”暂时不可删除"，后面TopicDeletionManager在恢复主题删除过程中会再次尝试对topicsIneligibleForDeletion集合中的主题进行删除
   *  Halt delete topic if -
   * 1. replicas being down
   * 2. partition reassignment in progress for some partitions of the topic
   * @param topics Topics that should be marked ineligible for deletion. No op if the topic is was not previously queued up for deletion
   */
  def markTopicIneligibleForDeletion(topics: Set[String], reason: => String): Unit = {
    if (isDeleteTopicEnabled) {
      // & 用来取两个集合的交集
      val newTopicsToHaltDeletion = controllerContext.topicsToBeDeleted & topics
      // 把 不可删除的集合放到 topicsIneligibleForDeletion
      controllerContext.topicsIneligibleForDeletion ++= newTopicsToHaltDeletion
      if (newTopicsToHaltDeletion.nonEmpty)
        info(s"Halted deletion of topics ${newTopicsToHaltDeletion.mkString(",")} due to $reason")
    }
  }

  private def isTopicIneligibleForDeletion(topic: String): Boolean = {
    if (isDeleteTopicEnabled) {
      controllerContext.topicsIneligibleForDeletion.contains(topic)
    } else
      true
  }

  private def isTopicDeletionInProgress(topic: String): Boolean = {
    if (isDeleteTopicEnabled) {
      controllerContext.isAnyReplicaInState(topic, ReplicaDeletionStarted)
    } else
      false
  }

  def isTopicQueuedUpForDeletion(topic: String): Boolean = {
    if (isDeleteTopicEnabled) {
      controllerContext.isTopicQueuedUpForDeletion(topic)
    } else
      false
  }

  /**
   * Invoked by the StopReplicaResponse callback when it receives no error code for a replica of a topic to be deleted.
   * As part of this, the replicas are moved from ReplicaDeletionStarted to ReplicaDeletionSuccessful state. Tears down
   * the topic if all replicas of a topic have been successfully deleted
   * @param replicas Replicas that were successfully deleted by the broker
   */
  def completeReplicaDeletion(replicas: Set[PartitionAndReplica]): Unit = {
    val successfullyDeletedReplicas = replicas.filter(r => isTopicQueuedUpForDeletion(r.topic))
    debug(s"Deletion successfully completed for replicas ${successfullyDeletedReplicas.mkString(",")}")
    replicaStateMachine.handleStateChanges(successfullyDeletedReplicas.toSeq, ReplicaDeletionSuccessful)
    resumeDeletions()
  }

  /**
   * Topic deletion can be retried if -
   * 1. Topic deletion is not already complete
   * 2. Topic deletion is currently not in progress for that topic
   * 3. Topic is currently marked ineligible for deletion
   * @param topic Topic
   * @return Whether or not deletion can be retried for the topic
   */
  private def isTopicEligibleForDeletion(topic: String): Boolean = {
    // 是否在controllerContext的topicsToBeDeleted集合中
    controllerContext.isTopicQueuedUpForDeletion(topic) &&
      //  如果属性delete.topic.enable为true并且topic分区状态是ReplicaDeletionStarted，就返回false？？？？
      !isTopicDeletionInProgress(topic) &&
    // 如果属性delete.topic.enable为true并且ControllerContext中的topicsIneligibleForDeletion包含topic
      !isTopicIneligibleForDeletion(topic)
  }

  /**
   * If the topic is queued for deletion but deletion is not currently under progress, then deletion is retried for that topic
   * To ensure a successful retry, reset states for respective replicas from ReplicaDeletionIneligible to OfflineReplica state
   * @param topics Topics for which deletion should be retried
   */
  private def retryDeletionForIneligibleReplicas(topics: Set[String]): Unit = {
    // reset replica states from ReplicaDeletionIneligible to OfflineReplica
    val failedReplicas = topics.flatMap(controllerContext.replicasInState(_, ReplicaDeletionIneligible))
    debug(s"Retrying deletion of topics ${topics.mkString(",")} since replicas ${failedReplicas.mkString(",")} were not successfully deleted")
    replicaStateMachine.handleStateChanges(failedReplicas.toSeq, OfflineReplica)
  }

  private def completeDeleteTopic(topic: String): Unit = {
    // deregister partition change listener on the deleted topic. This is to prevent the partition change listener
    // firing before the new topic listener when a deleted topic gets auto created
    // 注销删除主题分区上的变更监听器，防止删除过程中因分区数据变更监听器被触发（导致监听器被触发，引起状态不一致）
    client.mutePartitionModifications(topic)
    // 获取该主题下处于ReplicaDeletionSuccessful状态的所有副本对象，即所有已经被成功删除的副本对象
    val replicasForDeletedTopic = controllerContext.replicasInState(topic, ReplicaDeletionSuccessful)
    // controller will remove this replica from the state machine as well as its partition assignment cache
    // 利用副本状态机将这些副本对象转换成 NonExistentReplica 状态。等同于在状态机中删除这些副本
    replicaStateMachine.handleStateChanges(replicasForDeletedTopic.toSeq, NonExistentReplica)
    // 移除ZooKeeper上关于该主题的一切“痕迹”
    client.deleteTopic(topic, controllerContext.epochZkVersion)
    // 移除元数据缓存中关于该主题的一切“痕迹”
    controllerContext.removeTopic(topic)
  }

  /**
   * Invoked with the list of topics to be deleted
   * It invokes onPartitionDeletion for all partitions of a topic.
   * The updateMetadataRequest is also going to set the leader for the topics being deleted to
   * {@link LeaderAndIsr#LeaderDuringDelete}. This lets each broker know that this topic is being deleted and can be
   * removed from their caches.
   */
  private def onTopicDeletion(topics: Set[String]): Unit = {
    // 找出给定待删除主题列表中那些尚未开启删除操作的所有主题
    val unseenTopicsForDeletion = topics.diff(controllerContext.topicsWithDeletionStarted)
    if (unseenTopicsForDeletion.nonEmpty) {
      // 获取到这些主题的所有分区对象
      val unseenPartitionsForDeletion = unseenTopicsForDeletion.flatMap(controllerContext.partitionsForTopic)
      // 将这些分区的状态依次调整成OfflinePartition和NonExistentPartition
      // 等同于将这些分区从分区状态机中删除
      partitionStateMachine.handleStateChanges(unseenPartitionsForDeletion.toSeq, OfflinePartition)
      partitionStateMachine.handleStateChanges(unseenPartitionsForDeletion.toSeq, NonExistentPartition)
      // adding of unseenTopicsForDeletion to topics with deletion started must be done after the partition
      // state changes to make sure the offlinePartitionCount metric is properly updated
      // 把这些主题加到“已开启删除操作”主题列表中
      controllerContext.beginTopicDeletion(unseenTopicsForDeletion)
    }

    // send update metadata so that brokers stop serving data for topics to be deleted
    // 给集群所有Broker发送元数据更新请求，告诉它们不要再为这些主题处理数据了
    client.sendMetadataUpdate(topics.flatMap(controllerContext.partitionsForTopic))
    // 分区删除操作会执行底层的物理磁盘文件删除动作
    onPartitionDeletion(topics)
  }

  /**
   * Invoked by onTopicDeletion with the list of partitions for topics to be deleted
   * It does the following -
   * 1. Move all dead replicas directly to ReplicaDeletionIneligible state. Also mark the respective topics ineligible
   *    for deletion if some replicas are dead since it won't complete successfully anyway
   * 2. Move all replicas for the partitions to OfflineReplica state. This will send StopReplicaRequest to the replicas
   *    and LeaderAndIsrRequest to the leader with the shrunk ISR. When the leader replica itself is moved to OfflineReplica state,
   *    it will skip sending the LeaderAndIsrRequest since the leader will be updated to -1
   * 3. Move all replicas to ReplicaDeletionStarted state. This will send StopReplicaRequest with deletePartition=true. And
   *    will delete all persistent data from all replicas of the respective partitions
   */
  private def onPartitionDeletion(topicsToBeDeleted: Set[String]): Unit = {
    val allDeadReplicas = mutable.ListBuffer.empty[PartitionAndReplica]
    val allReplicasForDeletionRetry = mutable.ListBuffer.empty[PartitionAndReplica]
    val allTopicsIneligibleForDeletion = mutable.Set.empty[String]

    topicsToBeDeleted.foreach { topic =>
      val (aliveReplicas, deadReplicas) = controllerContext.replicasForTopic(topic).partition { r =>
        controllerContext.isReplicaOnline(r.replica, r.topicPartition)
      }

      val successfullyDeletedReplicas = controllerContext.replicasInState(topic, ReplicaDeletionSuccessful)
      val replicasForDeletionRetry = aliveReplicas.diff(successfullyDeletedReplicas)

      allDeadReplicas ++= deadReplicas
      allReplicasForDeletionRetry ++= replicasForDeletionRetry

      if (deadReplicas.nonEmpty) {
        debug(s"Dead Replicas (${deadReplicas.mkString(",")}) found for topic $topic")
        allTopicsIneligibleForDeletion += topic
      }
    }

    // move dead replicas directly to failed state
    replicaStateMachine.handleStateChanges(allDeadReplicas, ReplicaDeletionIneligible)
    // send stop replica to all followers that are not in the OfflineReplica state so they stop sending fetch requests to the leader
    replicaStateMachine.handleStateChanges(allReplicasForDeletionRetry, OfflineReplica)
    replicaStateMachine.handleStateChanges(allReplicasForDeletionRetry, ReplicaDeletionStarted)

    if (allTopicsIneligibleForDeletion.nonEmpty) {
      markTopicIneligibleForDeletion(allTopicsIneligibleForDeletion, reason = "offline replicas")
    }
  }

  /**
   * Kafka 会调整集群中三个地方的数据：ZooKeeper、元数据缓存和磁盘日志文件。删除主题时，ZooKeeper 上与该主题相关的所有 ZNode 节点必须被清除；
   * Controller 端元数据缓存中的相关项，也必须要被处理，并且要被同步到集群的其他 Broker 上；而磁盘日志文件，更是要清理的首要目标。
   * 这三个地方必须要统一处理，就好似我们常说的原子性操作一样。
   */
  private def resumeDeletions(): Unit = {
    // 从元数据缓存中获取要删除的主题列表
    val topicsQueuedForDeletion = Set.empty[String] ++ controllerContext.topicsToBeDeleted
    // 待重试主题列表
    val topicsEligibleForRetry = mutable.Set.empty[String]
    // 待删除主题列表
    val topicsEligibleForDeletion = mutable.Set.empty[String]

    if (topicsQueuedForDeletion.nonEmpty)
      info(s"Handling deletion for topics ${topicsQueuedForDeletion.mkString(",")}")
    // 遍历每个待删除主题
    topicsQueuedForDeletion.foreach { topic =>
      // if all replicas are marked as deleted successfully, then topic deletion is done
      if (controllerContext.areAllReplicasInState(topic, ReplicaDeletionSuccessful)) {
        // 如果该主题所有副本已经是ReplicaDeletionSuccessful状态，即该主题已经被删除
        // clear up all state for this topic from controller cache and zookeeper
        // 调用completeDeleteTopic方法完成元数据的清除和zk状态的删除
        completeDeleteTopic(topic)
        info(s"Deletion of topic $topic successfully completed")
      } else if (!controllerContext.isAnyReplicaInState(topic, ReplicaDeletionStarted)) {
        // 如果主题删除尚未开始(ReplicaDeletionStarted)
        // if you come here, then no replica is in TopicDeletionStarted and all replicas are not in
        // TopicDeletionSuccessful. That means, that either given topic haven't initiated deletion
        // or there is at least one failed replica (which means topic deletion should be retried).
        // 主题当前无法执行删除的话(topic中有任意一个分区是ReplicaDeletionIneligible状态)
        if (controllerContext.isAnyReplicaInState(topic, ReplicaDeletionIneligible)) {
          // 把该主题加到待重试主题列表中用于后续重试
          topicsEligibleForRetry += topic
        }
      }

      // Add topic to the eligible set if it is eligible for deletion.
      // 如果该主题能够被删除 TODO 感觉这里是表示主题不能被删除，先放到 topicsEligibleForDeletion 集合中，后面等主题能删除了，再去执行删除
      if (isTopicEligibleForDeletion(topic)) {
        info(s"Deletion of topic $topic (re)started")
        topicsEligibleForDeletion += topic
      }
    }

    // topic deletion retry will be kicked off
    if (topicsEligibleForRetry.nonEmpty) {
      // 把ReplicaDeletionIneligible 变更到 OfflineReplica，这样主题就能够删除了，后续删除交给onTopicDeletion方法，或者在此触发主题删除时，调用resumeDeletions方法删除
      retryDeletionForIneligibleReplicas(topicsEligibleForRetry)
    }

    // topic deletion will be kicked off
    // 执行主题删除操作用
    if (topicsEligibleForDeletion.nonEmpty) {
      onTopicDeletion(topicsEligibleForDeletion)
    }
  }
}
