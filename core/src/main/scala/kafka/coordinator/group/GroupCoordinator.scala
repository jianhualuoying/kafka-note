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
package kafka.coordinator.group

import java.util.Properties
import java.util.concurrent.atomic.AtomicBoolean

import kafka.common.OffsetAndMetadata
import kafka.log.LogConfig
import kafka.message.ProducerCompressionCodec
import kafka.server._
import kafka.utils.Logging
import kafka.zk.KafkaZkClient
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.internals.Topic
import org.apache.kafka.common.message.JoinGroupResponseData.JoinGroupResponseMember
import org.apache.kafka.common.message.LeaveGroupRequestData.MemberIdentity
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.metrics.stats.Meter
import org.apache.kafka.common.protocol.{ApiKeys, Errors}
import org.apache.kafka.common.requests._
import org.apache.kafka.common.utils.Time

import scala.collection.{Map, Seq, Set, immutable, mutable}
import scala.math.max

/**
 *
 * GroupCoordinator handles general group membership and offset management.
 *
 * Each Kafka server instantiates a coordinator which is responsible for a set of
 * groups. Groups are assigned to coordinators based on their group names.
 * <p>
 * <b>Delayed operation locking notes:</b>
 * Delayed operations in GroupCoordinator use `group` as the delayed operation
 * lock. ReplicaManager.appendRecords may be invoked while holding the group lock
 * used by its callback.  The delayed callback may acquire the group lock
 * since the delayed operation is completed only if the group lock can be acquired.
 *
 * @param brokerId
 * @param groupConfig   用于记录 Consumer Group 相关配置项的样例类（case class）
 * @param offsetConfig  用于记录 OffsetMetadata 相关配置项的样例类（case class）
 * @param groupManager
 * @param heartbeatPurgatory    用于处理 DelayedHeartbeat 延迟任务的时间轮
 * @param joinPurgatory
 * @param time
 * @param metrics
 */
class GroupCoordinator(val brokerId: Int,
                       val groupConfig: GroupConfig,
                       val offsetConfig: OffsetConfig,
                       val groupManager: GroupMetadataManager,
                       val heartbeatPurgatory: DelayedOperationPurgatory[DelayedHeartbeat],
                       val joinPurgatory: DelayedOperationPurgatory[DelayedJoin],
                       time: Time,
                       metrics: Metrics) extends Logging {
  import GroupCoordinator._
  // 处理JoinGroupRequest的回调函数类型
  type JoinCallback = JoinGroupResult => Unit
  // 处理SyncGroupRequest的回调函数类型
  type SyncCallback = SyncGroupResult => Unit

  /* setup metrics */
  val offsetDeletionSensor = metrics.sensor("OffsetDeletions")

  offsetDeletionSensor.add(new Meter(
    metrics.metricName("offset-deletion-rate",
      "group-coordinator-metrics",
      "The rate of administrative deleted offsets"),
    metrics.metricName("offset-deletion-count",
      "group-coordinator-metrics",
      "The total number of administrative deleted offsets")))

  val groupCompletedRebalanceSensor = metrics.sensor("CompletedRebalances")

  groupCompletedRebalanceSensor.add(new Meter(
    metrics.metricName("group-completed-rebalance-rate",
      "group-coordinator-metrics",
      "The rate of completed rebalance"),
    metrics.metricName("group-completed-rebalance-count",
      "group-coordinator-metrics",
      "The total number of completed rebalance")))

  this.logIdent = "[GroupCoordinator " + brokerId + "]: "
  // 标识当前GroupCoordinator是否正在运行
  private val isActive = new AtomicBoolean(false)

  def offsetsTopicConfigs: Properties = {
    val props = new Properties
    props.put(LogConfig.CleanupPolicyProp, LogConfig.Compact)
    props.put(LogConfig.SegmentBytesProp, offsetConfig.offsetsTopicSegmentBytes.toString)
    props.put(LogConfig.CompressionTypeProp, ProducerCompressionCodec.name)
    props
  }

  /**
   * NOTE: If a group lock and metadataLock are simultaneously needed,
   * be sure to acquire the group lock before metadataLock to prevent deadlock
   */

  /**
   * Startup logic executed at the same time when the server starts up.
   */
  def startup(enableMetadataExpiration: Boolean = true): Unit = {
    info("Starting up.")
    groupManager.startup(enableMetadataExpiration)
    isActive.set(true)
    info("Startup complete.")
  }

  /**
   * Shutdown logic executed at the same time when server shuts down.
   * Ordering of actions should be reversed from the startup process.
   */
  def shutdown(): Unit = {
    info("Shutting down.")
    isActive.set(false)
    groupManager.shutdown()
    heartbeatPurgatory.shutdown()
    joinPurgatory.shutdown()
    info("Shutdown complete.")
  }

  /**
   * Verify if the group has space to accept the joining member. The various
   * criteria are explained below.
   * 根据消费者组状态确定是否满员
   */
  private def acceptJoiningMember(group: GroupMetadata, member: String): Boolean = {
    group.currentState match {
      // Always accept the request when the group is empty or dead
      // 如果是 Empty 或 Dead 状态，肯定不会是满员，直接返回 True，表示可以接纳申请入组的成员；
      case Empty | Dead =>
        true

      // An existing member is accepted if it is already awaiting. New members are accepted
      // up to the max group size. Note that the number of awaiting members is used here
      // for two reasons:
      //   如果是 PreparingRebalance 状态，那么，批准成员入组的条件是必须满足以下两个条件之一：
      // 1) the group size is not reliable as it could already be above the max group size
      //    if the max group size was reduced.
      // 1) 当前等待加入组的成员数小于 Broker 端参数 group.max.size 值；（这个条件保证即使当前加入组的成员不是之前消费者组已有的成员，也能够加入组）
      // 2) using the number of awaiting members allows to kick out the last rejoining
      //    members of the group.
      // 2) 该成员是之前已有的成员，且当前正在等待加入组；
      case PreparingRebalance =>
        (group.has(member) && group.get(member).isAwaitingJoin) ||
          group.numAwaiting < groupConfig.groupMaxSize

      // An existing member is accepted. New members are accepted up to the max group size.
      // Note that the group size is used here. When the group transitions to CompletingRebalance,
      // members which haven't rejoined are removed.
      // 如果是其他状态，那么，入组的条件是该成员是已有成员，或者是当前组总成员数小于 Broker 端参数 group.max.size 值。
      // 需要注意的是，这里比较的是组当前的总成员数，而不是等待入组的成员数，这是因为，一旦 Rebalance 过渡到 CompletingRebalance 之后，没有完成加入组的成员，就会被移除，
      // 这个时候应该所有成员已经完成加入，接下来就等待分配分区方案，所以比较 group.size。
      case CompletingRebalance | Stable =>
        group.has(member) || group.size < groupConfig.groupMaxSize
    }
  }

  /**
   * 处理消费者组成员发送过来的加入组请求，是指消费者组下的各个成员向 Coordinator 发送 JoinGroupRequest 请求加入进组的过程。
   *
   * Consumer 端参数 session.timeout.ms 决定了完成一次 Rebalance 流程的最大时间。这种认知是不对的，
   * 实际上，这个参数是用于检测消费者组成员存活性的，即如果在这段超时时间内，没有收到该成员发给 Coordinator 的心跳请求，则把该成员标记为 Dead，而且要显式地将其从消费者组中移除，并触发新一轮的 Rebalance。
   * 而真正决定单次 Rebalance 所用最大时长的参数，是 Consumer 端的 max.poll.interval.ms。
   * @param groupId               消费者组名
   * @param memberId              消费者组成员ID
   * @param groupInstanceId       组实例ID，用于标识静态成员，如果 consumer 启动的时候明确指定了 group.instance.id 配置值，consumer 会在 JoinGroup Request 中携带该值，表示该 consumer 为 static member。
   *                              为了保证 group.instance.id 的唯一性，我们可以考虑使用 hostname、ip 等。
   *                              在 GroupCoordinator 端会记录 group.instance.id → member.id 的映射关系，以及已有的 partition 分配关系。当 GroupCoordinator 收到已知 group.instance.id 的 consumer 的 JoinGroup Request 时，不会进行 rebalance，而是将其原来对应的 partition 分配给它。
   *                              Static Membership 可以让 consumer group 只在下面的 4 种情况下进行 rebalance：
   *                               1、     有新 consumer 加入 consumer group
   *                               2、     Group Leader 重新加入 Group 时
   *                               3、     consumer 下线时间超过阈值（session.timeout.ms）
   *                               4、     GroupCoordinator 收到 static member 的 LeaveGroup Request
   *                              这样的话，在使用 Static Membership 场景下，只要在 consumer 重新启动的时候，不发送 LeaveGroup Request 且在 session.timeout.ms 时长内重启成功，就不会触发 rebalance。
   *                              所以，这里推荐设置一个足够 consumer 重启的时长 session.timeout.ms，这样能有效降低因 consumer 短暂不可用导致的 reblance 次数。
   * @param requireKnownMemberId  是否需要成员ID不为空
   * @param clientId              client.id值
   * @param clientHost            消费者程序主机名
   * @param rebalanceTimeoutMs    Rebalance超时时间,默认是max.poll.interval.ms值
   * @param sessionTimeoutMs      会话超时时间
   * @param protocolType          协议类型
   * @param protocols             按照分配策略分组的订阅分区 TODO 没有搞明白这个字段是什么意思，元组的第二个 Array[Byte] 是什么意思
   * @param responseCallback      回调函数
   */
  def handleJoinGroup(groupId: String,
                      memberId: String,
                      groupInstanceId: Option[String],
                      requireKnownMemberId: Boolean,
                      clientId: String,
                      clientHost: String,
                      rebalanceTimeoutMs: Int,
                      sessionTimeoutMs: Int,
                      protocolType: String,
                      protocols: List[(String, Array[Byte])],
                      responseCallback: JoinCallback): Unit = {
    // 验证消费者组状态的合法性，，也就是消费者组名 groupId 不能为空，以及 JoinGroupRequest 请求发送给了正确的 Coordinator（当前Broker就是 Coordinator 才行）
    validateGroupStatus(groupId, ApiKeys.JOIN_GROUP).foreach { error =>
      responseCallback(JoinGroupResult(memberId, error))
      return
    }
    // 确保消费者端配置的sessionTimeoutMs介于GroupConfig中配置的 [group.min.session.timeout.ms值，group.max.session.timeout.ms值]（服务端配置）之间，否则抛出异常，表示超时时间设置无效
    if (sessionTimeoutMs < groupConfig.groupMinSessionTimeoutMs ||
      sessionTimeoutMs > groupConfig.groupMaxSessionTimeoutMs) {
      responseCallback(JoinGroupResult(memberId, Errors.INVALID_SESSION_TIMEOUT))
    } else {
      // 消费者组成员ID是否为空
      val isUnknownMember = memberId == JoinGroupRequest.UNKNOWN_MEMBER_ID
      // group is created if it does not exist and the member id is UNKNOWN. if member
      // is specified but group does not exist, request is rejected with UNKNOWN_MEMBER_ID
      // 从缓存中获取消费者组信息，如果组不存在，就创建一个新的消费者组
      groupManager.getOrMaybeCreateGroup(groupId, isUnknownMember) match {
        case None =>
          // 如果消费者组元数据不存在，并且消费者组当前成员的 ID 不为空（getOrMaybeCreateGroup方法中isUnknownMember为false时，group为空时不会创建），说明消费者属于一个不存在的消费者组，说明消费者发送了错误的JoinGroupRequest，这种情况会封装“未知成员 ID”的异常返回
          responseCallback(JoinGroupResult(memberId, Errors.UNKNOWN_MEMBER_ID))
        case Some(group) =>
          group.inLock {
            // 如果该消费者组已满员
            if (!acceptJoiningMember(group, memberId)) {
              // 移除该消费者组成员
              group.remove(memberId)
              group.removeStaticMember(groupInstanceId)
              // 封装异常表明组已满员
              responseCallback(JoinGroupResult(JoinGroupRequest.UNKNOWN_MEMBER_ID, Errors.GROUP_MAX_SIZE_REACHED))
              // 如果消费者组元数据不存在，但是消费者组成员ID为空，说明这个JoinGroupRequest可能是这个新的消费者组中的第一个消费者发送的，会在GroupMetadataManager.getOrMaybeCreateGroup方法中会创建新的消费者组元数据
            } else if (isUnknownMember) {
              // 为空ID成员执行加入组操作
              doUnknownJoinGroup(group, groupInstanceId, requireKnownMemberId, clientId, clientHost, rebalanceTimeoutMs, sessionTimeoutMs, protocolType, protocols, responseCallback)
            } else {
              // 为非空ID成员执行加入组操作
              doJoinGroup(group, memberId, groupInstanceId, clientId, clientHost, rebalanceTimeoutMs, sessionTimeoutMs, protocolType, protocols, responseCallback)
            }

            // attempt to complete JoinGroup
            // 如果消费者组正处于 PreparingRebalance 状态，消费者组的状态应该要变更到 PreparingRebalance 后，Rebalance 才能完成加入组操作 TODO 没有看懂，不是前面的doUnknownJoinGroup和doJoinGroup已经把成员加入组了吗？
            if (group.is(PreparingRebalance)) {
              // 放入Purgatory，等待后面统一延时处理
              joinPurgatory.checkAndComplete(GroupKey(group.groupId))
            }
          }
      }
    }
  }

  private def doUnknownJoinGroup(group: GroupMetadata,
                                 groupInstanceId: Option[String],
                                 requireKnownMemberId: Boolean,
                                 clientId: String,
                                 clientHost: String,
                                 rebalanceTimeoutMs: Int,
                                 sessionTimeoutMs: Int,
                                 protocolType: String,
                                 protocols: List[(String, Array[Byte])],
                                 responseCallback: JoinCallback): Unit = {
    group.inLock {
      // 如果状态是Dead状态，既然是向该组添加成员，为什么组状态还能是 Dead 呢？因为，在成员加入组的同时，可能存在另一个线程，已经把组的元数据信息从 Coordinator 中移除 了。
      // 比如，组对应的 Coordinator 发生了变更，移动到了其他的 Broker 上，此时，代码封装一个异常返回给消费者程序，后者会去寻找最新的 Coordinator，然后重新发起加入组操作。
      if (group.is(Dead)) {
        // if the group is marked as dead, it means some other thread has just removed the group
        // from the coordinator metadata; it is likely that the group has migrated to some other
        // coordinator OR the group is in a transient unstable phase. Let the member retry
        // finding the correct coordinator and rejoin.
        // 封装异常调用回调函数返回
        responseCallback(JoinGroupResult(JoinGroupRequest.UNKNOWN_MEMBER_ID, Errors.COORDINATOR_NOT_AVAILABLE))
        // 成员配置的协议类型/分区消费分配策略与消费者组的不匹配，匹配是指成员的协议类型与消费者组的是否一致，以及成员设定的分区消费分配策略是否被消费者组下的其它成员支持。
        // 新加入成员设置的分区分配策略，必须至少有一个策略是组内所有成员都支持的，因为消费者组选举分区分配策略时，第一步就是要获取所有成员都支持的分区分配策略
      } else if (!group.supportsProtocols(protocolType, MemberMetadata.plainProtocolSet(protocols))) {
        // 如果不匹配，依然是封装异常，然后调用回调函数返回
        responseCallback(JoinGroupResult(JoinGroupRequest.UNKNOWN_MEMBER_ID, Errors.INCONSISTENT_GROUP_PROTOCOL))
        // 如果状态不是 Dead，并且成员配置的协议类型/分区消费分配策略与消费者组的匹配
      } else {
        // 根据规则为该成员创建成员ID，生成规则是 clientIdUUID
        val newMemberId = group.generateMemberId(clientId, groupInstanceId)
        // 如果配置了静态成员
        if (group.hasStaticMember(groupInstanceId)) {
          updateStaticMemberAndRebalance(group, newMemberId, groupInstanceId, protocols, responseCallback)
        } else if (requireKnownMemberId) {
          // 如果要求成员ID不为空，通常如果没有启用静态成员机制，并且requireKnownMemberId值为true，就会走到该分支，而 requireKnownMemberId = joinGroupRequest.version >= 4 && groupInstanceId.isEmpty（kafka.server.KafkaApis#handleJoinGroupRequest方法中定义）
          // 所以，如果你使用的是比较新的 Kafka 客户端版本，而且没有配置过 Consumer 端参数 group.instance.id 的话，那么，这个字段的值就是 True，这说明，Kafka 要求消费者成员加入组时，必须要分配好成员 ID
            // If member id required (dynamic membership), register the member in the pending member list
            // and send back a response to call for another join group request with allocated member id.
          debug(s"Dynamic member with unknown member id joins group ${group.groupId} in " +
              s"${group.currentState} state. Created a new member id $newMemberId and request the member to rejoin with this id.")
          // 将该成员加入到待决成员列表
          group.addPendingMember(newMemberId)
          // 延迟发送当前成员心跳过期的请求
          addPendingMemberExpiration(group, newMemberId, sessionTimeoutMs)
          // 封装一个异常以及生成好的成员 ID，将该成员的入组申请“打回去”，令其分配好了成员 ID 之后再重新申请
          responseCallback(JoinGroupResult(newMemberId, Errors.MEMBER_ID_REQUIRED))
        } else {
          info(s"${if (groupInstanceId.isDefined) "Static" else "Dynamic"} Member with unknown member id joins group ${group.groupId} in " +
            s"${group.currentState} state. Created a new member id $newMemberId for this member and add to the group.")
          // 添加成员
          addMemberAndRebalance(rebalanceTimeoutMs, sessionTimeoutMs, newMemberId, groupInstanceId,
            clientId, clientHost, protocolType, protocols, group, responseCallback)
        }
      }
    }
  }

  /**
   * 为那些设置了成员 ID 的成员，执行加入组逻辑的方法
   * @param group
   * @param memberId
   * @param groupInstanceId
   * @param clientId
   * @param clientHost
   * @param rebalanceTimeoutMs
   * @param sessionTimeoutMs
   * @param protocolType
   * @param protocols
   * @param responseCallback
   */
  private def doJoinGroup(group: GroupMetadata,
                          memberId: String,
                          groupInstanceId: Option[String],
                          clientId: String,
                          clientHost: String,
                          rebalanceTimeoutMs: Int,
                          sessionTimeoutMs: Int,
                          protocolType: String,
                          protocols: List[(String, Array[Byte])],
                          responseCallback: JoinCallback): Unit = {
    group.inLock {
      // 如果是Dead状态，封装COORDINATOR_NOT_AVAILABLE异常调用回调函数返回
      if (group.is(Dead)) {
        // if the group is marked as dead, it means some other thread has just removed the group
        // from the coordinator metadata; this is likely that the group has migrated to some other
        // coordinator OR the group is in a transient unstable phase. Let the member retry
        // finding the correct coordinator and rejoin.
        responseCallback(JoinGroupResult(memberId, Errors.COORDINATOR_NOT_AVAILABLE))
        // 如果协议类型或分区消费分配策略与消费者组的不匹配
        // 封装INCONSISTENT_GROUP_PROTOCOL异常调用回调函数返回
      } else if (!group.supportsProtocols(protocolType, MemberMetadata.plainProtocolSet(protocols))) {
        responseCallback(JoinGroupResult(memberId, Errors.INCONSISTENT_GROUP_PROTOCOL))
        // 如果是待决成员，说明该成员已经分配了成员ID，故允许加入组
      } else if (group.isPendingMember(memberId)) {
        // A rejoining pending member will be accepted. Note that pending member will never be a static member.
        if (groupInstanceId.isDefined) {
          throw new IllegalStateException(s"the static member $groupInstanceId was not expected to be assigned " +
            s"into pending member bucket with member id $memberId")
        } else {
          // 令其加入组 TODO 此时，消费者组切换到了 PreparingRebalance 状态，但是什么时候切换到 CompletingRebalance 状态
          debug(s"Dynamic Member with specific member id $memberId joins group ${group.groupId} in " +
            s"${group.currentState} state. Adding to the group now.")
          addMemberAndRebalance(rebalanceTimeoutMs, sessionTimeoutMs, memberId, groupInstanceId,
            clientId, clientHost, protocolType, protocols, group, responseCallback)
        }
      } else {
        val groupInstanceIdNotFound = groupInstanceId.isDefined && !group.hasStaticMember(groupInstanceId)
        if (group.isStaticMemberFenced(memberId, groupInstanceId, "join-group")) {
          // given member id doesn't match with the groupInstanceId. Inform duplicate instance to shut down immediately.
          responseCallback(JoinGroupResult(memberId, Errors.FENCED_INSTANCE_ID))
        } else if (!group.has(memberId) || groupInstanceIdNotFound) {
          //  JoinGroupRequest可能是来自Consumer Group中已知的Member，此时请求会携带之前被分配过的memberId，这里就要检测memberId是否能被GroupMetadata识别(!group.has(memberId))
            // If the dynamic member trying to register with an unrecognized id, or
            // the static member joins with unknown group instance id, send the response to let
            // it reset its member id and retry.
          responseCallback(JoinGroupResult(memberId, Errors.UNKNOWN_MEMBER_ID))
        } else {
          // 获取该成员的元数据信息
          val member = group.get(memberId)

          group.currentState match {
            // 如果是PreparingRebalance状态，就说明消费者组正要开启 Rebalance 流程
            case PreparingRebalance =>
              // 已知Member重新申请加入，则更新GroupMetadata中记录的Member信息
              // 因为当前消费者组的状态是 PreparingRebalance，所以不会调用 updateMemberAndRebalance 方法中的 maybePrepareRebalance 方法，所以，不会重新开启 Rebalance 流程，
              // 这里就没有把member添加到group中，为什么会默认member已经加入了group？我理解的是，成员申请加入组，分为两种情况：
              // 1. 新的消费者成员加入消费者组，第一次 JoinGroupRequest 请求会调用到 GroupCoordinator.doUnknownJoinGroup 方法，会生成一个成员ID，并返回给消费者，
              //  然后消费者成员会拿着这个成员ID，再次发送 JoinGroupRequest 请求，走到 GroupCoordinator.doJoinGroup 方法，会在前面判断待决成员时，调用addMemberAndRebalance方法，在消费者组元数据缓存中，添加消费者元数据
              // 2. 已知消费者成员重新申请加入，发送 JoinGroupRequest 请求时会携带之前被分配过的成员ID，因为是已经加入过组的成员，所以协调者节点中消费者组元数据缓存中会会存有该消费者元数据
              updateMemberAndRebalance(group, member, protocols, s"Member ${member.memberId} joining group during ${group.currentState}", responseCallback)
            // 如果是CompletingRebalance状态
            case CompletingRebalance =>
              // 判断该成员的分区消费分配策略与订阅分区列表是否和已保存记录中的一致，如果相同，就说明如果成员以前申请过加入组，并且 Coordinator 已经批准了，只是该成员没有收到
              // 当CompletingRebalance下，收到了元数据一致的JOIN_GROUP，会直接返回JOIN_GROUP响应。这是因为消费者可能没收到JOIN_GROUP响应，导致请求重发。这时候不需要再次重平衡，只需返回JOIN_GROUP响应即可。
              if (member.matches(protocols)) {
                // member is joining with the same metadata (which could be because it failed to
                // receive the initial JoinGroup response), so just return current group information
                // for the current generation.
                // 直接返回当前组信息
                responseCallback(JoinGroupResult(
                  members = if (group.isLeader(memberId)) {
                    group.currentMemberMetadata
                  } else {
                    List.empty
                  },
                  memberId = memberId,
                  generationId = group.generationId,
                  protocolType = group.protocolType,
                  protocolName = group.protocolName,
                  leaderId = group.leaderOrNull,
                  error = Errors.NONE))
              } else {
                // member has changed metadata, so force a rebalance
                // 但是，如果 protocols 不相同，那么，就说明成员变更了订阅信息或分配策略，就要调用updateMemberAndRebalance 方法，更新成员信息，并开始准备新一轮Rebalance
                // 更新成员信息并开始准备Rebalance
                updateMemberAndRebalance(group, member, protocols, s"Updating metadata for member ${member.memberId} during ${group.currentState}", responseCallback)
              }
            // 如果是Stable状态
            case Stable =>
              val member = group.get(memberId)
              // 如果成员是Leader成员 TODO 在Consumer Group处理完所有Consumer的加入，并转换为Stable状态之后，如果有新的Consumer要求加入时，这个时候消费者组涉不涉及状态转换呢？目前看不涉及，如果不是领导者消费者或者协议不匹配，则直接返回响应数据
              if (group.isLeader(memberId)) {
                // force a rebalance if the leader sends JoinGroup;
                // This allows the leader to trigger rebalances for changes affecting assignment
                // which do not affect the member metadata (such as topic metadata changes for the consumer)
                // 更新成员信息并开始准备Rebalance
                updateMemberAndRebalance(group, member, protocols, s"leader ${member.memberId} re-joining group during ${group.currentState}", responseCallback)
                // 成员变更了分区分配策略，即比较缓存中（member）的成员支持的分区分配策略（MemberMetadata.supportedProtocols）
                // 是否和 JoinGroupRequest请求中会发过来当前消费者组成员支持的分区分配策略一致，如果不一致，则调用updateMemberAndRebalance方法
              } else if (!member.matches(protocols)) {
                updateMemberAndRebalance(group, member, protocols, s"Updating metadata for member ${member.memberId} during ${group.currentState}", responseCallback)
              } else {
                // for followers with no actual change to their metadata, just return group information
                // for the current generation which will allow them to issue SyncGroup
                // 当前情况下，消费者组处于stable状态，有退出的消费者重新加入组，而且消费者不是领导消费者，直接回复JoinGroupResponse，以便让消费者继续发送SyncGroupRequest请求以获取分配给自己的分区。
                // 返回当前组信息给该成员即可，通知它们可以发起 Rebalance 的下一步操作。
                responseCallback(JoinGroupResult(
                  members = List.empty,
                  memberId = memberId,
                  generationId = group.generationId,
                  protocolType = group.protocolType,
                  protocolName = group.protocolName,
                  leaderId = group.leaderOrNull,
                  error = Errors.NONE))
              }
            // 如果是其它状态，封装异常调用回调函数返回
            case Empty | Dead =>
              // Group reaches unexpected state. Let the joining member reset their generation and rejoin.
              warn(s"Attempt to add rejoining member $memberId of group ${group.groupId} in " +
                s"unexpected group state ${group.currentState}")
              responseCallback(JoinGroupResult(memberId, Errors.UNKNOWN_MEMBER_ID))
          }
        }
      }
    }
  }

  /**
   * 该方法被 KafkaApis 类的 handleSyncGroupRequest 方法调用，用于处理消费者组成员发送的SyncGroupRequest 请求。
   * 逻辑为给消费者成员下发分配方案，同时在元数据缓存中注册组消息，以及把组状态变更为 Stable。
   * 其他消费者成员向 Coordinator 发送 SyncGroupRequest 请求，等待 Coordinator发送分配方案。
   * @param groupId          消费者组名，标识这个成员属于哪个消费者组。
   * @param generation       消费者组Generation号。Generation 类似于任期的概念，标识了Coordinator 负责为该消费者组处理的 Rebalance 次数。
   *                         每当有新的 Rebalance 开启时，Generation 都会自动加 1。
   * @param memberId         消费者组成员ID，成员 ID 的值不是由你直接指定的，但是你可以通过 client.id 参数，间接影响该字段的取值。
   * @param protocolType      协议类型，这个字段可能的取值有两个：consumer 和 connect。对于普通的消费者组而言，这个字段的取值就是 consumer，该字段是Option 类型，
   *                          因此，实际的取值是 Some(“consumer”)；Kafka Connect 组件中也会用到消费者组机制，那里的消费者组的取值就是 connect
   * @param protocolName      分区消费分配策略名称，这里的选择方法，就是 GroupMetadata.selectProtocol 方法。
   * @param groupInstanceId   静态成员Instance ID
   * @param groupAssignment   按照成员分组的分配方案，按照成员 ID 分组的分配方案。需要注意的是，只有 Leader 成员发送的 SyncGroupRequest 请求，才包含这个方案，
   *                          因此，Coordinator 在处理Leader 成员的请求时，该字段才有值。
   * @param responseCallback   回调函数
   *
   * protocolType 和 protocolName 都是 Option 类型，这说明，它们的取值可能是 None，其实都是 Coordinator 帮助消费者组确定的，也就是在 Rebalance 流程中的发送JoinGroupRequest 请求加入组这个步骤中确定的。
   * 如果成员成功加入组，那么，Coordinator 会给这两个字段赋上正确的值，并封装进JoinGroupRequest 的 Response 里，发送给消费者程序。
   * 一旦消费者拿到了 Response中的数据，就提取出这两个字段的值，封装进 SyncGroupRequest 请求中，再次发送给Coordinator。
   */
  def handleSyncGroup(groupId: String,
                      generation: Int,
                      memberId: String,
                      protocolType: Option[String],
                      protocolName: Option[String],
                      groupInstanceId: Option[String],
                      groupAssignment: Map[String, Array[Byte]],
                      responseCallback: SyncCallback): Unit = {
    // 验证消费者状态及合法性
    // 如果是因为 Coordinator 正在执行加载，就意味着本次 Rebalance 的所有状态都丢失了。这里的状态，指的是消费者组下的成员信息。
    // 那么，此时最安全的做法，是让消费者组重新从加入组开始，因此，代码会封装 REBALANCE_IN_PROGRESS 异常，然后调用回调函数返回。
    //一旦消费者组成员接收到此异常，就会知道，它至少找到了正确的 Coordinator，只需要重新开启 Rebalance，而不需要在开启 Rebalance 之前，再大费周章地去定位Coordinator 组件了。
    // 但如果是其它错误，就封装该错误，然后调用回调函数返回。
    validateGroupStatus(groupId, ApiKeys.SYNC_GROUP) match {
      // 如果未通过合法性检查，且错误原因是Coordinator正在加载
      // 那么，封装REBALANCE_IN_PROGRESS异常，并调用回调函数返回
      case Some(error) if error == Errors.COORDINATOR_LOAD_IN_PROGRESS =>
        // The coordinator is loading, which means we've lost the state of the active rebalance and the
        // group will need to start over at JoinGroup. By returning rebalance in progress, the consumer
        // will attempt to rejoin without needing to rediscover the coordinator. Note that we cannot
        // return COORDINATOR_LOAD_IN_PROGRESS since older clients do not expect the error.
        responseCallback(SyncGroupResult(Errors.REBALANCE_IN_PROGRESS))
      // 如果是其它错误，则封装对应错误，并调用回调函数返回
      case Some(error) => responseCallback(SyncGroupResult(error))

      case None =>
        // 获取消费者组元数据，注意kafka.coordinator.group.GroupMetadataManager.getGroup获取的GroupMetadata是元数据缓存
        groupManager.getGroup(groupId) match {
          // 如果未找到，则封装 UNKNOWN_MEMBER_ID 异常，并调用回调函数返回
          case None => responseCallback(SyncGroupResult(Errors.UNKNOWN_MEMBER_ID))
          // 如果找到的话，则调用doSyncGroup方法执行组同步任务
          case Some(group) => doSyncGroup(group, generation, memberId, protocolType, protocolName,
            groupInstanceId, groupAssignment, responseCallback)
        }
    }
  }

  /**
  CompletingRebalance 状态下的组同步操作，
  组同步操作完成了以下 3 件事情：
  1.将包含组成员分配方案的消费者组元数据，添加到消费者组元数据缓存以及内部位移主题中；
  2. 将分配方案通过 SyncGroupRequest 响应的方式，下发给组下所有成员。
  3. 将消费者组状态变更到 Stable。
   * @param group             注意，GroupMetadata是调用方通过kafka.coordinator.group.GroupMetadataManager#getGroup(java.lang.String)获取的元数据缓存
   * @param generationId
   * @param memberId
   * @param protocolType
   * @param protocolName
   * @param groupInstanceId
   * @param groupAssignment
   * @param responseCallback
   */
  private def doSyncGroup(group: GroupMetadata,
                          generationId: Int,
                          memberId: String,
                          protocolType: Option[String],
                          protocolName: Option[String],
                          groupInstanceId: Option[String],
                          groupAssignment: Map[String, Array[Byte]],
                          responseCallback: SyncCallback): Unit = {
    group.inLock {
      if (group.is(Dead)) {
        // if the group is marked as dead, it means some other thread has just removed the group
        // from the coordinator metadata; this is likely that the group has migrated to some other
        // coordinator OR the group is in a transient unstable phase. Let the member retry
        // finding the correct coordinator and rejoin.
        // 判断消费者组的状态是否是 Dead。如果是的话，就说明该组的元数据信息已经被其他线程从 Coordinator 中移除了，这很可能是因为 Coordinator 发生了变更。
        // 此时，最佳的做法是拒绝该成员的组同步操作，封装COORDINATOR_NOT_AVAILABLE 异常，显式告知它去寻找最新 Coordinator 所在的 Broker 节点，然后再尝试重新加入组。
        responseCallback(SyncGroupResult(Errors.COORDINATOR_NOT_AVAILABLE))
      } else if (group.isStaticMemberFenced(memberId, groupInstanceId, "sync-group")) {
        // 有关静态成员的判断
        responseCallback(SyncGroupResult(Errors.FENCED_INSTANCE_ID))
      } else if (!group.has(memberId)) {
        // 判断成员是否属于这个组
        responseCallback(SyncGroupResult(Errors.UNKNOWN_MEMBER_ID))
      } else if (generationId != group.generationId) {
        // 判断成员的 Generation 是否和消费者组的相同。如果不同的话，则封装ILLEGAL_GENERATION 异常给回调函数
        responseCallback(SyncGroupResult(Errors.ILLEGAL_GENERATION))
      } else if (protocolType.isDefined && !group.protocolType.contains(protocolType.get)) {
        // 判断成员和消费者组的协议类型是否一致
        responseCallback(SyncGroupResult(Errors.INCONSISTENT_GROUP_PROTOCOL))
      } else if (protocolName.isDefined && !group.protocolName.contains(protocolName.get)) {
        // 判断成员和消费者组的分区消费分配策略是否一致
        responseCallback(SyncGroupResult(Errors.INCONSISTENT_GROUP_PROTOCOL))
      } else {
        group.currentState match {
          case Empty =>
            // 封装UNKNOWN_MEMBER_ID异常，调用回调函数返回
            responseCallback(SyncGroupResult(Errors.UNKNOWN_MEMBER_ID))

          case PreparingRebalance =>
            // 封装REBALANCE_IN_PROGRESS异常，调用回调函数返回
            responseCallback(SyncGroupResult(Errors.REBALANCE_IN_PROGRESS))

          case CompletingRebalance =>
            //设置awaitingSyncCallback回调函数(组同步回调函数)，在延迟任务完成时触发（回调函数逻辑是将传递给回调函数的数据，通过 Response 的方式发送给消费者组成员）
            // responseCallback在kafka.server.KafkaApis.handleSyncGroupRequest中定义，用来发送SyncGroupRequest请求的响应
            group.get(memberId).awaitingSyncCallback = responseCallback

            // if this is the leader, then we can attempt to persist state and transition to stable
            // 如果当前成员是消费者组的Leader成员(组Leader成员发送的SyncGroupRequest请求需要特殊处理，因为只有 Leader 成员的 groupAssignment 字段才携带了分配方案，其他成员是没有分配方案的)
            if (group.isLeader(memberId)) {
              info(s"Assignment received from leader for group ${group.groupId} for generation ${group.generationId}")

              // fill any missing members with an empty assignment
              // 将未分配到分区的member对应的分配结果填充为空的byte数组，这步主要是为了构造一个统一格式的分配方案字段 assignment TODO 消费者组leader成员为什么发送SyncGroupRequest请求，不是只有消费者组的follower成员才发送吗
              val missing = group.allMembers.diff(groupAssignment.keySet)
              val assignment = groupAssignment ++ missing.map(_ -> Array.empty[Byte]).toMap

              if (missing.nonEmpty) {
                warn(s"Setting empty assignments for members $missing of ${group.groupId} for generation ${group.generationId}")
              }
              // 把消费者组分区分配方案写入到内部位移主题，并通过第三个参数回调函数更新到消费者组元数据（group参数即为缓存）中，在storeGroup函数中定义的putCacheCallback最后调用
              groupManager.storeGroup(group, assignment, (error: Errors) => {
                group.inLock {
                  // another member may have joined the group while we were awaiting this callback,
                  // so we must ensure we are still in the CompletingRebalance state and the same generation
                  // when it gets invoked. if we have transitioned to another state, then do nothing
                  // 如果组状态是CompletingRebalance以及成员和组的generationId相同
                  if (group.is(CompletingRebalance) && generationId == group.generationId) {
                    // 如果有错误
                    if (error != Errors.NONE) {
                      // 清空分区的分配结果，发送异常响应
                      resetAndPropagateAssignmentError(group, error)
                      // 切换成PreparingRebalance状态，准备开启新一轮的Rebalance
                      maybePrepareRebalance(group, s"error when storing group assignment during SyncGroup (member: $memberId)")
                      // 如果没错误
                    } else {
                      // ** 重要 ** 在消费者组元数据（缓存）中保存分区分配方案，并发送给所有成员（SyncGroupResponse）
                      setAndPropagateAssignment(group, assignment)
                      // 处理完成后将组状态转换为Stable
                      group.transitionTo(Stable)
                    }
                  }
                }
              })
              groupCompletedRebalanceSensor.record()
            }

          case Stable =>
            // 如果是 Stable 状态，则说明，此时消费者组已处于正常工作状态，无需进行组同步的操作。因此，在这种情况下，简单返回消费者组当前的分配方案给回调函数，供它后面发送给消费者组成员即可
            // if the group is stable, we just return the current assignment
            // 将分配给member的负责处理的分区信息返回
            val memberMetadata = group.get(memberId)
            responseCallback(SyncGroupResult(group.protocolType, group.protocolName, memberMetadata.assignment, Errors.NONE))
            // 更新最后一次收到心跳的时间戳，并创建新的delayedHeartbeat对象放入heartbeatPurgatory中等待下次心跳。
            completeAndScheduleNextHeartbeatExpiration(group, group.get(memberId))

          case Dead =>
            // 如果是 Dead 状态，那就说明，这是一个异常的情况了，因为理论上，不应该为处于 Dead 状态的组执行组同步，因此，代码只能选择抛出 IllegalStateException 异常
            throw new IllegalStateException(s"Reached unexpected condition for Dead group ${group.groupId}")
        }
      }
    }
  }

  def handleLeaveGroup(groupId: String,
                       leavingMembers: List[MemberIdentity],
                       responseCallback: LeaveGroupResult => Unit): Unit = {
    validateGroupStatus(groupId, ApiKeys.LEAVE_GROUP) match {
      case Some(error) =>
        responseCallback(leaveError(error, List.empty))
      case None =>
        groupManager.getGroup(groupId) match {
          case None =>
            responseCallback(leaveError(Errors.NONE, leavingMembers.map {leavingMember =>
              memberLeaveError(leavingMember, Errors.UNKNOWN_MEMBER_ID)
            }))
          case Some(group) =>
            group.inLock {
              if (group.is(Dead)) {
                responseCallback(leaveError(Errors.COORDINATOR_NOT_AVAILABLE, List.empty))
              } else {
                val memberErrors = leavingMembers.map { leavingMember =>
                  val memberId = leavingMember.memberId
                  val groupInstanceId = Option(leavingMember.groupInstanceId)
                  if (memberId != JoinGroupRequest.UNKNOWN_MEMBER_ID
                    && group.isStaticMemberFenced(memberId, groupInstanceId, "leave-group")) {
                    memberLeaveError(leavingMember, Errors.FENCED_INSTANCE_ID)
                  } else if (group.isPendingMember(memberId)) {
                    if (groupInstanceId.isDefined) {
                      throw new IllegalStateException(s"the static member $groupInstanceId was not expected to be leaving " +
                        s"from pending member bucket with member id $memberId")
                    } else {
                      // if a pending member is leaving, it needs to be removed from the pending list, heartbeat cancelled
                      // and if necessary, prompt a JoinGroup completion.
                      info(s"Pending member $memberId is leaving group ${group.groupId}.")
                      removePendingMemberAndUpdateGroup(group, memberId)
                      heartbeatPurgatory.checkAndComplete(MemberKey(group.groupId, memberId))
                      memberLeaveError(leavingMember, Errors.NONE)
                    }
                  } else if (!group.has(memberId) && !group.hasStaticMember(groupInstanceId)) {
                    memberLeaveError(leavingMember, Errors.UNKNOWN_MEMBER_ID)
                  } else {
                    val member = if (group.hasStaticMember(groupInstanceId))
                      group.get(group.getStaticMemberId(groupInstanceId))
                    else
                      group.get(memberId)
                    removeHeartbeatForLeavingMember(group, member)
                    info(s"Member[group.instance.id ${member.groupInstanceId}, member.id ${member.memberId}] " +
                      s"in group ${group.groupId} has left, removing it from the group")
                    removeMemberAndUpdateGroup(group, member, s"removing member $memberId on LeaveGroup")
                    memberLeaveError(leavingMember, Errors.NONE)
                  }
                }
                responseCallback(leaveError(Errors.NONE, memberErrors))
              }
            }
        }
    }
  }

  def handleDeleteGroups(groupIds: Set[String]): Map[String, Errors] = {
    val groupErrors = mutable.Map.empty[String, Errors]
    val groupsEligibleForDeletion = mutable.ArrayBuffer[GroupMetadata]()

    groupIds.foreach { groupId =>
      validateGroupStatus(groupId, ApiKeys.DELETE_GROUPS) match {
        case Some(error) =>
          groupErrors += groupId -> error

        case None =>
          groupManager.getGroup(groupId) match {
            case None =>
              groupErrors += groupId ->
                (if (groupManager.groupNotExists(groupId)) Errors.GROUP_ID_NOT_FOUND else Errors.NOT_COORDINATOR)
            case Some(group) =>
              group.inLock {
                group.currentState match {
                  case Dead =>
                    groupErrors += groupId ->
                      (if (groupManager.groupNotExists(groupId)) Errors.GROUP_ID_NOT_FOUND else Errors.NOT_COORDINATOR)
                  case Empty =>
                    group.transitionTo(Dead)
                    groupsEligibleForDeletion += group
                  case Stable | PreparingRebalance | CompletingRebalance =>
                    groupErrors(groupId) = Errors.NON_EMPTY_GROUP
                }
              }
          }
      }
    }

    if (groupsEligibleForDeletion.nonEmpty) {
      val offsetsRemoved = groupManager.cleanupGroupMetadata(groupsEligibleForDeletion, _.removeAllOffsets())
      groupErrors ++= groupsEligibleForDeletion.map(_.groupId -> Errors.NONE).toMap
      info(s"The following groups were deleted: ${groupsEligibleForDeletion.map(_.groupId).mkString(", ")}. " +
        s"A total of $offsetsRemoved offsets were removed.")
    }

    groupErrors
  }

  def handleDeleteOffsets(groupId: String, partitions: Seq[TopicPartition]): (Errors, Map[TopicPartition, Errors]) = {
    var groupError: Errors = Errors.NONE
    var partitionErrors: Map[TopicPartition, Errors] = Map()
    var partitionsEligibleForDeletion: Seq[TopicPartition] = Seq()

    validateGroupStatus(groupId, ApiKeys.OFFSET_DELETE) match {
      case Some(error) =>
        groupError = error

      case None =>
        groupManager.getGroup(groupId) match {
          case None =>
            groupError = if (groupManager.groupNotExists(groupId))
              Errors.GROUP_ID_NOT_FOUND else Errors.NOT_COORDINATOR

          case Some(group) =>
            group.inLock {
              group.currentState match {
                case Dead =>
                  groupError = if (groupManager.groupNotExists(groupId))
                    Errors.GROUP_ID_NOT_FOUND else Errors.NOT_COORDINATOR

                case Empty =>
                  partitionsEligibleForDeletion = partitions

                case PreparingRebalance | CompletingRebalance | Stable if group.isConsumerGroup =>
                  val (consumed, notConsumed) =
                    partitions.partition(tp => group.isSubscribedToTopic(tp.topic()))

                  partitionsEligibleForDeletion = notConsumed
                  partitionErrors = consumed.map(_ -> Errors.GROUP_SUBSCRIBED_TO_TOPIC).toMap

                case _ =>
                  groupError = Errors.NON_EMPTY_GROUP
              }
            }

            if (partitionsEligibleForDeletion.nonEmpty) {
              val offsetsRemoved = groupManager.cleanupGroupMetadata(Seq(group), group => {
                group.removeOffsets(partitionsEligibleForDeletion)
              })

              partitionErrors ++= partitionsEligibleForDeletion.map(_ -> Errors.NONE).toMap

              offsetDeletionSensor.record(offsetsRemoved)

              info(s"The following offsets of the group $groupId were deleted: ${partitionsEligibleForDeletion.mkString(", ")}. " +
                s"A total of $offsetsRemoved offsets were removed.")
            }
        }
    }

    // If there is a group error, the partition errors is empty
    groupError -> partitionErrors
  }

  def handleHeartbeat(groupId: String,
                      memberId: String,
                      groupInstanceId: Option[String],
                      generationId: Int,
                      responseCallback: Errors => Unit): Unit = {
    validateGroupStatus(groupId, ApiKeys.HEARTBEAT).foreach { error =>
      if (error == Errors.COORDINATOR_LOAD_IN_PROGRESS)
        // the group is still loading, so respond just blindly
        responseCallback(Errors.NONE)
      else
        responseCallback(error)
      return
    }

    groupManager.getGroup(groupId) match {
      case None =>
        responseCallback(Errors.UNKNOWN_MEMBER_ID)

      case Some(group) => group.inLock {
        if (group.is(Dead)) {
          // if the group is marked as dead, it means some other thread has just removed the group
          // from the coordinator metadata; this is likely that the group has migrated to some other
          // coordinator OR the group is in a transient unstable phase. Let the member retry
          // finding the correct coordinator and rejoin.
          responseCallback(Errors.COORDINATOR_NOT_AVAILABLE)
        } else if (group.isStaticMemberFenced(memberId, groupInstanceId, "heartbeat")) {
          responseCallback(Errors.FENCED_INSTANCE_ID)
        } else if (!group.has(memberId)) {
          responseCallback(Errors.UNKNOWN_MEMBER_ID)
        } else if (generationId != group.generationId) {
          responseCallback(Errors.ILLEGAL_GENERATION)
        } else {
          group.currentState match {
            case Empty =>
              responseCallback(Errors.UNKNOWN_MEMBER_ID)

            case CompletingRebalance =>
              // consumers may start sending heartbeat after join-group response, in which case
              // we should treat them as normal hb request and reset the timer
              val member = group.get(memberId)
              completeAndScheduleNextHeartbeatExpiration(group, member)
              responseCallback(Errors.NONE)

            case PreparingRebalance =>
                val member = group.get(memberId)
              // 会完成这次心跳请求，并添加下一次的心跳到时间轮
                completeAndScheduleNextHeartbeatExpiration(group, member)
              // 如果消费者组处于 Stable 状态，由于某个消费者心跳操作超时，或协调者收到某个消费者发送的JoinGroupRequest请求后，消费者组会切换为PreparingRebalance状态，这时如果协调者收到其他消费者发送来的心跳请求
              // 发现消费者组的状态为 PreparingRebalance 状态，协调者会直接返回REBALANCE_IN_PROGRESS的错误，这样就能通知其他消费者进行Rebalance操作
                responseCallback(Errors.REBALANCE_IN_PROGRESS)

            case Stable =>
                val member = group.get(memberId)
                completeAndScheduleNextHeartbeatExpiration(group, member)
                responseCallback(Errors.NONE)

            case Dead =>
              throw new IllegalStateException(s"Reached unexpected condition for Dead group $groupId")
          }
        }
      }
    }
  }

  def handleTxnCommitOffsets(groupId: String,
                             producerId: Long,
                             producerEpoch: Short,
                             memberId: String,
                             groupInstanceId: Option[String],
                             generationId: Int,
                             offsetMetadata: immutable.Map[TopicPartition, OffsetAndMetadata],
                             responseCallback: immutable.Map[TopicPartition, Errors] => Unit): Unit = {
    validateGroupStatus(groupId, ApiKeys.TXN_OFFSET_COMMIT) match {
      case Some(error) => responseCallback(offsetMetadata.map { case (k, _) => k -> error })
      case None =>
        val group = groupManager.getGroup(groupId).getOrElse {
          groupManager.addGroup(new GroupMetadata(groupId, Empty, time))
        }
        doTxnCommitOffsets(group, memberId, groupInstanceId, generationId, producerId, producerEpoch, offsetMetadata, responseCallback)
    }
  }

  def handleCommitOffsets(groupId: String,
                          memberId: String,
                          groupInstanceId: Option[String],
                          generationId: Int,
                          offsetMetadata: immutable.Map[TopicPartition, OffsetAndMetadata],
                          responseCallback: immutable.Map[TopicPartition, Errors] => Unit): Unit = {
    validateGroupStatus(groupId, ApiKeys.OFFSET_COMMIT) match {
      case Some(error) => responseCallback(offsetMetadata.map { case (k, _) => k -> error })
      case None =>
        groupManager.getGroup(groupId) match {
          case None =>
            if (generationId < 0) {
              // the group is not relying on Kafka for group management, so allow the commit
              val group = groupManager.addGroup(new GroupMetadata(groupId, Empty, time))
              doCommitOffsets(group, memberId, groupInstanceId, generationId, offsetMetadata, responseCallback)
            } else {
              // or this is a request coming from an older generation. either way, reject the commit
              responseCallback(offsetMetadata.map { case (k, _) => k -> Errors.ILLEGAL_GENERATION })
            }

          case Some(group) =>
            doCommitOffsets(group, memberId, groupInstanceId, generationId, offsetMetadata, responseCallback)
        }
    }
  }

  def scheduleHandleTxnCompletion(producerId: Long,
                                  offsetsPartitions: Iterable[TopicPartition],
                                  transactionResult: TransactionResult): Unit = {
    require(offsetsPartitions.forall(_.topic == Topic.GROUP_METADATA_TOPIC_NAME))
    val isCommit = transactionResult == TransactionResult.COMMIT
    groupManager.scheduleHandleTxnCompletion(producerId, offsetsPartitions.map(_.partition).toSet, isCommit)
  }

  private def doTxnCommitOffsets(group: GroupMetadata,
                                 memberId: String,
                                 groupInstanceId: Option[String],
                                 generationId: Int,
                                 producerId: Long,
                                 producerEpoch: Short,
                                 offsetMetadata: immutable.Map[TopicPartition, OffsetAndMetadata],
                                 responseCallback: immutable.Map[TopicPartition, Errors] => Unit): Unit = {
    group.inLock {
      if (group.is(Dead)) {
        // if the group is marked as dead, it means some other thread has just removed the group
        // from the coordinator metadata; it is likely that the group has migrated to some other
        // coordinator OR the group is in a transient unstable phase. Let the member retry
        // finding the correct coordinator and rejoin.
        responseCallback(offsetMetadata.map { case (k, _) => k -> Errors.COORDINATOR_NOT_AVAILABLE })
      } else if (group.isStaticMemberFenced(memberId, groupInstanceId, "txn-commit-offsets")) {
        responseCallback(offsetMetadata.map { case (k, _) => k -> Errors.FENCED_INSTANCE_ID })
      } else if (memberId != JoinGroupRequest.UNKNOWN_MEMBER_ID && !group.has(memberId)) {
        // Enforce member id when it is set.
        responseCallback(offsetMetadata.map { case (k, _) => k -> Errors.UNKNOWN_MEMBER_ID })
      } else if (generationId >= 0 && generationId != group.generationId) {
        // Enforce generation check when it is set.
        responseCallback(offsetMetadata.map { case (k, _) => k -> Errors.ILLEGAL_GENERATION })
      } else {
        groupManager.storeOffsets(group, memberId, offsetMetadata, responseCallback, producerId, producerEpoch)
      }
    }
  }

  private def doCommitOffsets(group: GroupMetadata,
                              memberId: String,
                              groupInstanceId: Option[String],
                              generationId: Int,
                              offsetMetadata: immutable.Map[TopicPartition, OffsetAndMetadata],
                              responseCallback: immutable.Map[TopicPartition, Errors] => Unit): Unit = {
    group.inLock {
      if (group.is(Dead)) {
        // if the group is marked as dead, it means some other thread has just removed the group
        // from the coordinator metadata; it is likely that the group has migrated to some other
        // coordinator OR the group is in a transient unstable phase. Let the member retry
        // finding the correct coordinator and rejoin.
        responseCallback(offsetMetadata.map { case (k, _) => k -> Errors.COORDINATOR_NOT_AVAILABLE })
      } else if (group.isStaticMemberFenced(memberId, groupInstanceId, "commit-offsets")) {
        responseCallback(offsetMetadata.map { case (k, _) => k -> Errors.FENCED_INSTANCE_ID })
      } else if (generationId < 0 && group.is(Empty)) {
        // The group is only using Kafka to store offsets.
        groupManager.storeOffsets(group, memberId, offsetMetadata, responseCallback)
      } else if (!group.has(memberId)) {
        responseCallback(offsetMetadata.map { case (k, _) => k -> Errors.UNKNOWN_MEMBER_ID })
      } else if (generationId != group.generationId) {
        responseCallback(offsetMetadata.map { case (k, _) => k -> Errors.ILLEGAL_GENERATION })
      } else {
        group.currentState match {
          case Stable | PreparingRebalance =>
            // During PreparingRebalance phase, we still allow a commit request since we rely
            // on heartbeat response to eventually notify the rebalance in progress signal to the consumer
            val member = group.get(memberId)
            completeAndScheduleNextHeartbeatExpiration(group, member)
            groupManager.storeOffsets(group, memberId, offsetMetadata, responseCallback)

          case CompletingRebalance =>
            // We should not receive a commit request if the group has not completed rebalance;
            // but since the consumer's member.id and generation is valid, it means it has received
            // the latest group generation information from the JoinResponse.
            // So let's return a REBALANCE_IN_PROGRESS to let consumer handle it gracefully.
            responseCallback(offsetMetadata.map { case (k, _) => k -> Errors.REBALANCE_IN_PROGRESS })

          case _ =>
            throw new RuntimeException(s"Logic error: unexpected group state ${group.currentState}")
        }
      }
    }
  }

  def handleFetchOffsets(groupId: String, requireStable: Boolean, partitions: Option[Seq[TopicPartition]] = None):
  (Errors, Map[TopicPartition, OffsetFetchResponse.PartitionData]) = {

    validateGroupStatus(groupId, ApiKeys.OFFSET_FETCH) match {
      case Some(error) => error -> Map.empty
      case None =>
        // return offsets blindly regardless the current group state since the group may be using
        // Kafka commit storage without automatic group management
        (Errors.NONE, groupManager.getOffsets(groupId, requireStable, partitions))
    }
  }

  def handleListGroups(states: Set[String]): (Errors, List[GroupOverview]) = {
    if (!isActive.get) {
      (Errors.COORDINATOR_NOT_AVAILABLE, List[GroupOverview]())
    } else {
      val errorCode = if (groupManager.isLoading) Errors.COORDINATOR_LOAD_IN_PROGRESS else Errors.NONE
      // if states is empty, return all groups
      val groups = if (states.isEmpty)
        groupManager.currentGroups
      else
        groupManager.currentGroups.filter(g => states.contains(g.summary.state))
      (errorCode, groups.map(_.overview).toList)
    }
  }

  def handleDescribeGroup(groupId: String): (Errors, GroupSummary) = {
    validateGroupStatus(groupId, ApiKeys.DESCRIBE_GROUPS) match {
      case Some(error) => (error, GroupCoordinator.EmptyGroup)
      case None =>
        groupManager.getGroup(groupId) match {
          case None => (Errors.NONE, GroupCoordinator.DeadGroup)
          case Some(group) =>
            group.inLock {
              (Errors.NONE, group.summary)
            }
        }
    }
  }

  def handleDeletedPartitions(topicPartitions: Seq[TopicPartition]): Unit = {
    val offsetsRemoved = groupManager.cleanupGroupMetadata(groupManager.currentGroups, group => {
      group.removeOffsets(topicPartitions)
    })
    info(s"Removed $offsetsRemoved offsets associated with deleted partitions: ${topicPartitions.mkString(", ")}.")
  }

  private def isValidGroupId(groupId: String, api: ApiKeys): Boolean = {
    api match {
      case ApiKeys.OFFSET_COMMIT | ApiKeys.OFFSET_FETCH | ApiKeys.DESCRIBE_GROUPS | ApiKeys.DELETE_GROUPS =>
        // For backwards compatibility, we support the offset commit APIs for the empty groupId, and also
        // in DescribeGroups and DeleteGroups so that users can view and delete state of all groups.
        groupId != null
      case _ =>
        // The remaining APIs are groups using Kafka for group coordination and must have a non-empty groupId
        groupId != null && !groupId.isEmpty
    }
  }

  /**
   * Check that the groupId is valid, assigned to this coordinator and that the group has been loaded.
   * 校验消费者组状态及合法性
   */
  private def validateGroupStatus(groupId: String, api: ApiKeys): Option[Errors] = {
    if (!isValidGroupId(groupId, api)) {
      //  消费者组名不能为空
      Some(Errors.INVALID_GROUP_ID)
    } else if (!isActive.get)
    // Coordinator 组件当前没有执行加载过程
      Some(Errors.COORDINATOR_NOT_AVAILABLE)
    else if (isCoordinatorLoadInProgress(groupId)) {
      // 判断该Consumer Group对应的Offsets Topic分区是否还处于加载过程中
      // 如果发送给了正确的 Coordinator，但此时 Coordinator 正在执行加载（当 Coordinator 变更到其他 Broker 上时，需要从内部位移主题中读取消息数据，并填充到内存上的消费者组元数据缓存）过程，
      Some(Errors.COORDINATOR_LOAD_IN_PROGRESS)
    } else if (!isCoordinatorForGroup(groupId)) {
      // SyncGroupRequest 请求发送给了正确的 Coordinator 组件（当前GroupCoordinator是否负责管理该Consumer Group）
      // 如果 Coordinator 变更了（消费者组在kafka.coordinator.group.GroupMetadataManager.ownedPartitions集合中，则说明当前Broker加载了消费者组分区日志数据，则表明当前节点是Coordinator节点），
      // 那么，发送给老 Coordinator 所在 Broker 的请求就失效了，即没有发送给正确的 Coordinator；
      Some(Errors.NOT_COORDINATOR)
    } else
      None
  }

  private def onGroupUnloaded(group: GroupMetadata): Unit = {
    group.inLock {
      info(s"Unloading group metadata for ${group.groupId} with generation ${group.generationId}")
      val previousState = group.currentState
      // 要移除的组状态转换为Dead
      group.transitionTo(Dead)

      previousState match {
        case Empty | Dead =>
        case PreparingRebalance =>
          for (member <- group.allMemberMetadata) {
            group.maybeInvokeJoinCallback(member, JoinGroupResult(member.memberId, Errors.NOT_COORDINATOR))
          }

          joinPurgatory.checkAndComplete(GroupKey(group.groupId))
        // TODO 没有看懂
        case Stable | CompletingRebalance =>
          for (member <- group.allMemberMetadata) {
            group.maybeInvokeSyncCallback(member, SyncGroupResult(Errors.NOT_COORDINATOR))
            heartbeatPurgatory.checkAndComplete(MemberKey(member.groupId, member.memberId))
          }
      }
    }
  }

  /**
   * 加载完提交位移消息和注册消息到缓存后的回调函数
   * @param group
   */
  private def onGroupLoaded(group: GroupMetadata): Unit = {
    group.inLock {
      info(s"Loading group metadata for ${group.groupId} with generation ${group.generationId}")
      assert(group.is(Stable) || group.is(Empty))
      if (groupIsOverCapacity(group)) {
        prepareRebalance(group, s"Freshly-loaded group is over capacity ($groupConfig.groupMaxSize). Rebalacing in order to give a chance for consumers to commit offsets")
      }
      // 检查消费者组中每个成员的心跳
      group.allMemberMetadata.foreach(completeAndScheduleNextHeartbeatExpiration(group, _))
    }
  }

  /**
   * Load cached state from the given partition and begin handling requests for groups which map to it.
   *
   * @param offsetTopicPartitionId The partition we are now leading
   */
  def onElection(offsetTopicPartitionId: Int): Unit = {
    groupManager.scheduleLoadGroupAndOffsets(offsetTopicPartitionId, onGroupLoaded)
  }

  /**
   * Unload cached state for the given partition and stop handling requests for groups which map to it.
   *
   * @param offsetTopicPartitionId The partition we are no longer leading
   */
  def onResignation(offsetTopicPartitionId: Int): Unit = {
    groupManager.removeGroupsForPartition(offsetTopicPartitionId, onGroupUnloaded)
  }

  /**
   * 在消费者组元数据（缓存）中保存分区分配方案，并发送给所有成员（SyncGroupResponse）
   * @param group     消费者组元数据（缓存）
   * @param assignment 分区分配方案
   */
  private def setAndPropagateAssignment(group: GroupMetadata, assignment: Map[String, Array[Byte]]): Unit = {
    assert(group.is(CompletingRebalance))
    // 在消费者组元数据（缓存）中保存分区分配方案
    group.allMemberMetadata.foreach(member => member.assignment = assignment(member.memberId))
    // 分区分配方案发送给所有成员（SyncGroupResponse）
    propagateAssignment(group, Errors.NONE)
  }

  private def resetAndPropagateAssignmentError(group: GroupMetadata, error: Errors): Unit = {
    // 校验状态
    assert(group.is(CompletingRebalance))
    // 将每个member的分配方案都置空
    group.allMemberMetadata.foreach(_.assignment = Array.empty)
    // 传递分配结果 TODO 没看懂这里为什么处理的是JoinGroupRequest，却要调用处理SyncGroupRequest时定义的回调函数
    propagateAssignment(group, error)
  }

  /**
   * 传递分配结果，其实内部(GroupMetadata#maybeInvokeSyncCallback(MemberMetadata, SyncGroupResult))调用了KafkaApis中处理SyncGroupRequest时定义的回调函数
   * @param group
   * @param error
   */
  private def propagateAssignment(group: GroupMetadata, error: Errors): Unit = {
    val (protocolType, protocolName) = if (error == Errors.NONE)
      (group.protocolType, group.protocolName)
    else
      (None, None)
    for (member <- group.allMemberMetadata) {
      if (member.assignment.isEmpty && error == Errors.NONE) {
        warn(s"Sending empty assignment to member ${member.memberId} of ${group.groupId} for generation ${group.generationId} with no errors")
      }
      // 分区分配方案发送到消费者成员
      if (group.maybeInvokeSyncCallback(member, SyncGroupResult(protocolType, protocolName, member.assignment, error))) {
        // reset the session timeout for members after propagating the member's assignment.
        // This is because if any member's session expired while we were still awaiting either
        // the leader sync group or the storage callback, its expiration will be ignored and no
        // future heartbeat expectations will not be scheduled.
        // 开启等待下次心跳的延迟任务  TODO 没看懂这里发送心跳的用意
        completeAndScheduleNextHeartbeatExpiration(group, member)
      }
    }
  }

  /**
   *
   * Complete existing DelayedHeartbeats for the given member and schedule the next one
   * 检查消费者心跳
   *
   * @param group
   * @param member
   */
  private def completeAndScheduleNextHeartbeatExpiration(group: GroupMetadata, member: MemberMetadata): Unit = {
    completeAndScheduleNextExpiration(group, member, member.sessionTimeoutMs)
  }

  /**
   * 检查消费者心跳，会被completeAndScheduleNextHeartbeatExpiration调用，completeAndScheduleNextHeartbeatExpiration方法会被多个方法调用，
   * 如处理JoinGroupRequest、SyncGroupRequest、OffsetCommitRequest请求，处理GroupCoordinator迁移（onGroupLoaded）等操作中都会调用
   *  GroupCoordinator.propagateAssignment(...)
    ↖ GroupCoordinator.handleHeartbeat(...)
    ↖ GroupCoordinator.onGroupLoaded(...)
    ↖ GroupCoordinator.doSyncGroup(...)
    ↖ GroupCoordinator.onCompleteJoin(...)
    ↖ GroupCoordinator.handleCommitOffsets(...)
  第一次DelayedHeartbeat任务的添加时机，是在消费者向服务端发送SyncGroupRequest被正确处理时就添加了。？？？
   * @param group
   * @param member
   * @param timeoutMs 会话超时时间。当前消费者组成员依靠心跳机制“保活”。如果在会话超时时间之内未能成功发送心跳，组成员就被判定成“下线”，从而触发新一轮的 Rebalance。这个字段的值是 Consumer 端参数 session.timeout.ms 的值。
   */
  private def completeAndScheduleNextExpiration(group: GroupMetadata, member: MemberMetadata, timeoutMs: Long): Unit = {
    // 构造以Group ID和Member ID构成的键
    val memberKey = MemberKey(member.groupId, member.memberId)

    // complete current heartbeat expectation
    // 设置成员收到心跳
    member.heartbeatSatisfied = true
    // 检查该键对应的Watchers，完成该Watchers中本次的心跳任务，即已经收到心跳了，检查这次心跳是否超时的任务可以取消了。
    heartbeatPurgatory.checkAndComplete(memberKey)

    // reschedule the next heartbeat expiration deadline
    // 创建新一轮延迟心跳
    // 设置本轮心跳状态为 false（因为本轮还没发起心跳）
    member.heartbeatSatisfied = false
    // 创建新的延迟心跳操作
    val delayedHeartbeat = new DelayedHeartbeat(this, group, member.memberId, isPending = false, timeoutMs)
    // 尝试完成延迟心跳，这里尝试会失败，会直接放入到缓存（时间轮）进行监控
    heartbeatPurgatory.tryCompleteElseWatch(delayedHeartbeat, Seq(memberKey))
  }

  /**
    * Add pending member expiration to heartbeat purgatory
    */
  private def addPendingMemberExpiration(group: GroupMetadata, pendingMemberId: String, timeoutMs: Long): Unit = {
    val pendingMemberKey = MemberKey(group.groupId, pendingMemberId)
    val delayedHeartbeat = new DelayedHeartbeat(this, group, pendingMemberId, isPending = true, timeoutMs)
    heartbeatPurgatory.tryCompleteElseWatch(delayedHeartbeat, Seq(pendingMemberKey))
  }

  private def removeHeartbeatForLeavingMember(group: GroupMetadata, member: MemberMetadata): Unit = {
    member.isLeaving = true
    val memberKey = MemberKey(member.groupId, member.memberId)
    heartbeatPurgatory.checkAndComplete(memberKey)
  }

  /**
   * 1、向消费者组添加成员
   * 2、准备 Rebalance。
   * @param rebalanceTimeoutMs
   * @param sessionTimeoutMs
   * @param memberId
   * @param groupInstanceId
   * @param clientId
   * @param clientHost
   * @param protocolType
   * @param protocols
   * @param group
   * @param callback
   */
  private def addMemberAndRebalance(rebalanceTimeoutMs: Int,
                                    sessionTimeoutMs: Int,
                                    memberId: String,
                                    groupInstanceId: Option[String],
                                    clientId: String,
                                    clientHost: String,
                                    protocolType: String,
                                    protocols: List[(String, Array[Byte])],
                                    group: GroupMetadata,
                                    callback: JoinCallback): Unit = {
    // 创建MemberMetadata对象实例
    val member = new MemberMetadata(memberId, group.groupId, groupInstanceId,
      clientId, clientHost, rebalanceTimeoutMs,
      sessionTimeoutMs, protocolType, protocols)
    // 标识该成员是新成员，isNew 字段与心跳设置相关联，你可以阅读下 MemberMetadata 的 hasSatisfiedHeartbeat 方法的代码，
    // 搞明白该字段是如何帮助Coordinator 确认消费者组成员心跳的。
    member.isNew = true

    // update the newMemberAdded flag to indicate that the join group can be further delayed
    // 如果消费者组准备开启首次 Rebalance，设置 newMemberAdded 为 True
    // group.newMemberAdded 是 Kafka 为消费者组 Rebalance 流程做的一个性能优化。大致的思想，是在消费者组首次进行 Rebalance 时，
    // 让 Coordinator 多等待一段时间，从而让更多的消费者组成员加入到组中，以免后来者申请入组而反复进行 Rebalance。
    // 这段多等待的时间，就是 Broker 端参数 group.initial.rebalance.delay.ms 的值。这里的 newMemberAdded 字段，就是用于判断是否需要多等待这段时间的一个变量。
    if (group.is(PreparingRebalance) && group.generationId == 0)
      group.newMemberAdded = true
    // 将该成员添加到消费者组
    group.add(member, callback)

    // The session timeout does not affect new members since they do not have their memberId and
    // cannot send heartbeats. Furthermore, we cannot detect disconnects because sockets are muted
    // while the JoinGroup is in purgatory. If the client does disconnect (e.g. because of a request
    // timeout during a long rebalance), they may simply retry which will lead to a lot of defunct
    // members in the rebalance. To prevent this going on indefinitely, we timeout JoinGroup requests
    // for new members. If the new member is still there, we expect it to retry.
    // 设置该成员下次心跳超期时间
    completeAndScheduleNextExpiration(group, member, NewMemberJoinTimeoutMs)

    if (member.isStaticMember) {
      info(s"Adding new static member $groupInstanceId to group ${group.groupId} with member id $memberId.")
      group.addStaticMember(groupInstanceId, memberId)
    } else {
      // 从待决成员列表中移除，毕竟，它已经正式加入到组中了，就不需要待在待决列表中了
      group.removePendingMember(memberId)
    }
    // 准备开启Rebalance
    maybePrepareRebalance(group, s"Adding new member $memberId with group instance id $groupInstanceId")
  }

  private def updateStaticMemberAndRebalance(group: GroupMetadata,
                                             newMemberId: String,
                                             groupInstanceId: Option[String],
                                             protocols: List[(String, Array[Byte])],
                                             responseCallback: JoinCallback): Unit = {
    val oldMemberId = group.getStaticMemberId(groupInstanceId)
    info(s"Static member $groupInstanceId of group ${group.groupId} with unknown member id rejoins, assigning new member id $newMemberId, while " +
      s"old member id $oldMemberId will be removed.")

    val currentLeader = group.leaderOrNull
    val member = group.replaceGroupInstance(oldMemberId, newMemberId, groupInstanceId)
    // Heartbeat of old member id will expire without effect since the group no longer contains that member id.
    // New heartbeat shall be scheduled with new member id.
    completeAndScheduleNextHeartbeatExpiration(group, member)

    val knownStaticMember = group.get(newMemberId)
    group.updateMember(knownStaticMember, protocols, responseCallback)
    val oldProtocols = knownStaticMember.supportedProtocols

    group.currentState match {
      case Stable =>
        // check if group's selectedProtocol of next generation will change, if not, simply store group to persist the
        // updated static member, if yes, rebalance should be triggered to let the group's assignment and selectProtocol consistent
        val selectedProtocolOfNextGeneration = group.selectProtocol
        if (group.protocolName.contains(selectedProtocolOfNextGeneration)) {
          info(s"Static member which joins during Stable stage and doesn't affect selectProtocol will not trigger rebalance.")
          val groupAssignment: Map[String, Array[Byte]] = group.allMemberMetadata.map(member => member.memberId -> member.assignment).toMap
          groupManager.storeGroup(group, groupAssignment, error => {
            if (error != Errors.NONE) {
              warn(s"Failed to persist metadata for group ${group.groupId}: ${error.message}")

              // Failed to persist member.id of the given static member, revert the update of the static member in the group.
              group.updateMember(knownStaticMember, oldProtocols, null)
              val oldMember = group.replaceGroupInstance(newMemberId, oldMemberId, groupInstanceId)
              completeAndScheduleNextHeartbeatExpiration(group, oldMember)
              responseCallback(JoinGroupResult(
                List.empty,
                memberId = JoinGroupRequest.UNKNOWN_MEMBER_ID,
                generationId = group.generationId,
                protocolType = group.protocolType,
                protocolName = group.protocolName,
                leaderId = currentLeader,
                error = error
              ))
            } else {
              group.maybeInvokeJoinCallback(member, JoinGroupResult(
                members = List.empty,
                memberId = newMemberId,
                generationId = group.generationId,
                protocolType = group.protocolType,
                protocolName = group.protocolName,
                // We want to avoid current leader performing trivial assignment while the group
                // is in stable stage, because the new assignment in leader's next sync call
                // won't be broadcast by a stable group. This could be guaranteed by
                // always returning the old leader id so that the current leader won't assume itself
                // as a leader based on the returned message, since the new member.id won't match
                // returned leader id, therefore no assignment will be performed.
                leaderId = currentLeader,
                error = Errors.NONE))
            }
          })
        } else {
          maybePrepareRebalance(group, s"Group's selectedProtocol will change because static member ${member.memberId} with instance id $groupInstanceId joined with change of protocol")
        }
      case CompletingRebalance =>
        // if the group is in after-sync stage, upon getting a new join-group of a known static member
        // we should still trigger a new rebalance, since the old member may already be sent to the leader
        // for assignment, and hence when the assignment gets back there would be a mismatch of the old member id
        // with the new replaced member id. As a result the new member id would not get any assignment.
        prepareRebalance(group, s"Updating metadata for static member ${member.memberId} with instance id $groupInstanceId")
      case Empty | Dead =>
        throw new IllegalStateException(s"Group ${group.groupId} was not supposed to be " +
          s"in the state ${group.currentState} when the unknown static member $groupInstanceId rejoins.")
      case PreparingRebalance =>
    }
  }

  private def updateMemberAndRebalance(group: GroupMetadata,
                                       member: MemberMetadata,
                                       protocols: List[(String, Array[Byte])],
                                       reason: String,
                                       callback: JoinCallback): Unit = {
    // 更新消费者组的分区分配策略支持票数，更新当前消费者组成员（member）的分区分配策略为最新（protocols），并且更新消费者成员的回调函数
    group.updateMember(member, protocols, callback)
    maybePrepareRebalance(group, reason)
  }

  private def maybePrepareRebalance(group: GroupMetadata, reason: String): Unit = {
    group.inLock {
      // 只有在Stable, CompletingRebalance, Empty的状态下才会执行prepareRebalance()
      if (group.canRebalance)
        prepareRebalance(group, reason)
    }
  }

  // package private for testing

  /**
   * 将消费者组状态变更到PreparingRebalance，然后创建 DelayedJoin 对象（用来追踪当前消费者是否再次发送JoinGroupRequest超时），并交由 Purgatory，等待延时处理加入组操作。
   * @param group
   * @param reason
   */
  private[group] def prepareRebalance(group: GroupMetadata, reason: String): Unit = {
    // if any members are awaiting sync, cancel their request and have them rejoin
    // 如果有成员发送 SyncGroupRequest 请求，GroupCoordinator在等待Consumer Group的Consumer Leader通过SyncGroupRequest将分区的分配结果发送过来。
    // 如果此时进行状态切换，需要对这些已经发送SyncGroupRequest的Consumer Follower返回错误码（TODO 感觉这里的表述有问题，
    //  因为prepareRebalance方法只在GroupCoordinator.updateMemberAndRebalance和GroupCoordinator.updateStaticMemberAndRebalance中调用，前者是在GroupCoordinator.doJoinGroup中调用，后者是在GroupCoordinator.doUnknownJoinGroup中调用
    //  都是和JoinGroupRequest有关 ）
    // 这里为什么没有 stable 状态去向所有消费者组中的成员发送错误信息，是由于如果消费者组处于stable状态的情况下，这时有消费者心跳操作超时或有新的消费者发送了JoinGroupRequest请求，
    // 都会导致重平衡，然后消费者组状态切换为PreparingRebalance，组中其他成员发送心跳时，协调者发现消费者组处于PreparingRebalance状态，就会给消费者组发送错误信息，消费者接收到心跳请求，看到错误信息，就会发送JoinGroupRequest请求，重新申请加入组
    // 至于为什么处于CompletingRebalance状态不利用心跳去通知消费者重新加入组，我觉得是因为消费者组处于CompletingRebalance状态，根本就没有心跳机制，心跳是在stable状态的消费者组才有的，所以为了通知到已经加入组中成员，所以才有了这里代码。
    if (group.is(CompletingRebalance))
    // 重置MemberMetadata.assignment字段，调用MemberMetadata.awaitingSyncCallback向消费者返回REBALANCE_IN_PROGRESS的错误码
      resetAndPropagateAssignmentError(group, Errors.REBALANCE_IN_PROGRESS)

    val delayedRebalance = if (group.is(Empty))
      new InitialDelayedJoin(this,
        joinPurgatory,
        group,
        groupConfig.groupInitialRebalanceDelayMs,
        groupConfig.groupInitialRebalanceDelayMs,
        max(group.rebalanceTimeoutMs - groupConfig.groupInitialRebalanceDelayMs, 0))
    else
    // DelayedJoin的超时时长是GroupMetadata中所有Member设置的超时时长的最大值
      new DelayedJoin(this, group, group.rebalanceTimeoutMs)
    // 切换为PreparingRebalance状态，表示准备执行Rebalance操作
    group.transitionTo(PreparingRebalance)

    info(s"Preparing to rebalance group ${group.groupId} in state ${group.currentState} with old generation " +
      s"${group.generationId} (${Topic.GROUP_METADATA_TOPIC_NAME}-${partitionFor(group.groupId)}) (reason: $reason)")
    // 创建DelayedJoin的Watcher Key
    val groupKey = GroupKey(group.groupId)
    // 尝试立即完成DelayedJoin，如果不能完成就添加到joinPurgatory炼狱
    joinPurgatory.tryCompleteElseWatch(delayedRebalance, Seq(groupKey))
  }

  private def removeMemberAndUpdateGroup(group: GroupMetadata, member: MemberMetadata, reason: String): Unit = {
    // New members may timeout with a pending JoinGroup while the group is still rebalancing, so we have
    // to invoke the callback before removing the member. We return UNKNOWN_MEMBER_ID so that the consumer
    // will retry the JoinGroup request if is still active.
    group.maybeInvokeJoinCallback(member, JoinGroupResult(JoinGroupRequest.UNKNOWN_MEMBER_ID, Errors.UNKNOWN_MEMBER_ID))
    // 将Member从对应的GroupMetadata中移除
    group.remove(member.memberId)
    group.removeStaticMember(member.groupInstanceId)

    group.currentState match {
      // 无操作
      case Dead | Empty =>
      // 之前的分区分配可能已经失效了，将GroupMetadata切换成PreparingRebalance状态，进行重平衡
      case Stable | CompletingRebalance => maybePrepareRebalance(group, reason)
      // GroupMetadata中的Member减少，可能满足DelayedJoin的执行条件，尝试执行
      case PreparingRebalance => joinPurgatory.checkAndComplete(GroupKey(group.groupId))
    }
  }

  private def removePendingMemberAndUpdateGroup(group: GroupMetadata, memberId: String): Unit = {
    group.removePendingMember(memberId)

    if (group.is(PreparingRebalance)) {
      joinPurgatory.checkAndComplete(GroupKey(group.groupId))
    }
  }

  def tryCompleteJoin(group: GroupMetadata, forceComplete: () => Boolean) = {
    group.inLock {
      // GroupMetadata#hasAllMembersJoined 判断条件如下：
      // 1、判断已知的Member是否已经申请加入，从这里看到numMembersAwaitingJoin这个变量扮演着很重要的角色，这里这个变量和成员数相等，则用来判断是否已经申请加入组。
      // 我觉得  members.size == numMembersAwaitingJoin 的条件是和所有成员的 MemberMetadata.awaitingJoinCallback 不为空是等价的，因为所有 MemberMetadata.awaitingJoinCallback字段
      // 赋值回调函数的地方都伴随着 numMembersAwaitingJoin + 1 TODO MemberMetadata.awaitingJoinCallback字段不为空到底代表着什么呢？是成员加入了组，还是还没有加入组
      // 2、待决成员列表为空
      if (group.hasAllMembersJoined)
      // 尝试强制完成
        forceComplete()
      else false
    }
  }

  def onExpireJoin(): Unit = {
    // TODO: add metrics for restabilize timeouts
  }
  // 当已知Member都已申请重新加入或DelayedJoin到期时执行该方法
  def onCompleteJoin(group: GroupMetadata): Unit = {
    group.inLock {
      // 获取未重新加入（MemberMetadata.awaitingJoinCallback为空，并且不是静态成员）的已知 Member 集合
      val notYetRejoinedDynamicMembers = group.notYetRejoinedMembers.filterNot(_._2.isStaticMember)
      // 如果未加入的成员集合不为空
      if (notYetRejoinedDynamicMembers.nonEmpty) {
        info(s"Group ${group.groupId} remove dynamic members " +
          s"who haven't joined: ${notYetRejoinedDynamicMembers.keySet}")

        notYetRejoinedDynamicMembers.values foreach { failedMember =>
          removeHeartbeatForLeavingMember(group, failedMember)
          // 移除未加入的已知Member
          group.remove(failedMember.memberId)
          // TODO: cut the socket connection to the client
        }
      }

      if (group.is(Dead)) {
        info(s"Group ${group.groupId} is dead, skipping rebalance stage")
      } else if (!group.maybeElectNewJoinedLeader() && group.allMembers.nonEmpty) {
        // If all members are not rejoining, we will postpone the completion
        // of rebalance preparing stage, and send out another delayed operation
        // until session timeout removes all the non-responsive members.
        error(s"Group ${group.groupId} could not complete rebalance because no members rejoined")
        joinPurgatory.tryCompleteElseWatch(
          new DelayedJoin(this, group, group.rebalanceTimeoutMs),
          Seq(GroupKey(group.groupId)))
      } else {
        // 递增generationId，选择该Consumer Group最终使用的PartitionAssignor， 此处还会将GroupMetadata的状态转换为CompletingRebalance，这样接下来，就会进入rebalance的第二步SyncGroup，等待SyncGroupRequest请求
        group.initNextGeneration()
        if (group.is(Empty)) {
          info(s"Group ${group.groupId} with generation ${group.generationId} is now empty " +
            s"(${Topic.GROUP_METADATA_TOPIC_NAME}-${partitionFor(group.groupId)})")
          // 如果消费者为空，则存储空的分区分配数据到消费者组的位移主题日志（应该是TimeStone消息吧），即向Offsets Topic中对应的分区写入“删除标记”
          groupManager.storeGroup(group, Map.empty, error => {
            if (error != Errors.NONE) {
              // we failed to write the empty group metadata. If the broker fails before another rebalance,
              // the previous generation written to the log will become active again (and most likely timeout).
              // This should be safe since there are no active members in an empty generation, so we just warn.
              warn(s"Failed to write empty metadata for group ${group.groupId}: ${error.message}")
            }
          })
        } else {
          info(s"Stabilized group ${group.groupId} generation ${group.generationId} " +
            s"(${Topic.GROUP_METADATA_TOPIC_NAME}-${partitionFor(group.groupId)})")

          // trigger the awaiting join group response callback for all the members after rebalancing
          // 向GroupMetadata中所有的Member发送JoinGroupResponse响应
          for (member <- group.allMemberMetadata) {
            // 构造响应结果，Leader和Follower是不同的
            val joinResult = JoinGroupResult(
              members = if (group.isLeader(member.memberId)) {
                // Consumer Group的Consumer Leader会收到相应的分配结果
                group.currentMemberMetadata
              } else {
                List.empty
              },
              memberId = member.memberId,
              generationId = group.generationId,
              protocolType = group.protocolType,
              protocolName = group.protocolName,
              leaderId = group.leaderOrNull,
              error = Errors.NONE)
            // 调用回调函数，其实就是发送JoinGroupResponse响应给消费者成员
            group.maybeInvokeJoinCallback(member, joinResult)
            // 重置心跳延迟任务
            completeAndScheduleNextHeartbeatExpiration(group, member)
            member.isNew = false
          }
        }
      }
    }
  }

  def tryCompleteHeartbeat(group: GroupMetadata,
                           memberId: String,
                           isPending: Boolean,
                           forceComplete: () => Boolean): Boolean = {
    group.inLock {
      // The group has been unloaded and invalid, we should complete the heartbeat.
      // 组已经Dead，完成心跳即可
      if (group.is(Dead)) {
        forceComplete()
      } else if (isPending) {
        // complete the heartbeat if the member has joined the group
        // 如果消费者已经加入消费者组，则完成这次心跳
        if (group.has(memberId)) {
          forceComplete()
        } else false
        // isPending = false时，成员已经完成入组（重平衡），则需要成员已经收到了心跳，或者它即将离开，通过shouldCompleteNonPendingHeartbeat判断是否完成这次心跳
      } else if (shouldCompleteNonPendingHeartbeat(group, memberId)) {
        forceComplete()
      } else false
    }
  }

  /**
   * * 四个条件满足一个即可完成心跳：
   * 1. awaitingJoinCallback不为null，即消费者正在等待JoinGroupResponse
   * 2. awaitingSyncCallback不为null，即消费者正在等待SyncGroupResponse
   * 3. 消费者心跳是否超时
   * 4. 消费者已经离开了Consumer Group
   * @param group
   * @param memberId
   * @return
   */
  def shouldCompleteNonPendingHeartbeat(group: GroupMetadata, memberId: String): Boolean = {
    if (group.has(memberId)) {
      val member = group.get(memberId)
      member.hasSatisfiedHeartbeat || member.isLeaving
    } else {
      info(s"Member id $memberId was not found in ${group.groupId} during heartbeat completion check")
      true
    }
  }
  // 将对应的消费者（memberId）从group中删除，并按照当前group所处的状态进行分类处理：
  def onExpireHeartbeat(group: GroupMetadata, memberId: String, isPending: Boolean): Unit = {
    group.inLock {
      if (group.is(Dead)) {
        info(s"Received notification of heartbeat expiration for member $memberId after group ${group.groupId} had already been unloaded or deleted.")
      } else if (isPending) {
        info(s"Pending member $memberId in group ${group.groupId} has been removed after session timeout expiration.")
        // 若成员没有入组，超时则直接移除该成员到组外
        removePendingMemberAndUpdateGroup(group, memberId)
      } else if (!group.has(memberId)) {
        debug(s"Member $memberId has already been removed from the group.")
      } else {
        // 若成员入组，且没收到心跳，超时，则将其移出组外，根据当前组状态，决定是否进行重平衡
        val member = group.get(memberId)
        if (!member.hasSatisfiedHeartbeat) {
          info(s"Member ${member.memberId} in group ${group.groupId} has failed, removing it from the group")
          removeMemberAndUpdateGroup(group, member, s"removing member ${member.memberId} on heartbeat expiration")
        }
      }
    }
  }

  def onCompleteHeartbeat(): Unit = {
    // TODO: add metrics for complete heartbeats
  }

  def partitionFor(group: String): Int = groupManager.partitionFor(group)

  private def groupIsOverCapacity(group: GroupMetadata): Boolean = {
    group.size > groupConfig.groupMaxSize
  }

  private def isCoordinatorForGroup(groupId: String) = groupManager.isGroupLocal(groupId)

  private def isCoordinatorLoadInProgress(groupId: String) = groupManager.isGroupLoading(groupId)
}

object GroupCoordinator {

  val NoState = ""
  val NoProtocolType = ""
  val NoProtocol = ""
  val NoLeader = ""
  val NoGeneration = -1
  val NoMembers = List[MemberSummary]()
  val EmptyGroup = GroupSummary(NoState, NoProtocolType, NoProtocol, NoMembers)
  val DeadGroup = GroupSummary(Dead.toString, NoProtocolType, NoProtocol, NoMembers)
  val NewMemberJoinTimeoutMs: Int = 5 * 60 * 1000

  def apply(config: KafkaConfig,
            zkClient: KafkaZkClient,
            replicaManager: ReplicaManager,
            time: Time,
            metrics: Metrics): GroupCoordinator = {
    val heartbeatPurgatory = DelayedOperationPurgatory[DelayedHeartbeat]("Heartbeat", config.brokerId)
    val joinPurgatory = DelayedOperationPurgatory[DelayedJoin]("Rebalance", config.brokerId)
    apply(config, zkClient, replicaManager, heartbeatPurgatory, joinPurgatory, time, metrics)
  }

  private[group] def offsetConfig(config: KafkaConfig) = OffsetConfig(
    maxMetadataSize = config.offsetMetadataMaxSize,
    loadBufferSize = config.offsetsLoadBufferSize,
    offsetsRetentionMs = config.offsetsRetentionMinutes * 60L * 1000L,
    offsetsRetentionCheckIntervalMs = config.offsetsRetentionCheckIntervalMs,
    offsetsTopicNumPartitions = config.offsetsTopicPartitions,
    offsetsTopicSegmentBytes = config.offsetsTopicSegmentBytes,
    offsetsTopicReplicationFactor = config.offsetsTopicReplicationFactor,
    offsetsTopicCompressionCodec = config.offsetsTopicCompressionCodec,
    offsetCommitTimeoutMs = config.offsetCommitTimeoutMs,
    offsetCommitRequiredAcks = config.offsetCommitRequiredAcks
  )

  def apply(config: KafkaConfig,
            zkClient: KafkaZkClient,
            replicaManager: ReplicaManager,
            heartbeatPurgatory: DelayedOperationPurgatory[DelayedHeartbeat],
            joinPurgatory: DelayedOperationPurgatory[DelayedJoin],
            time: Time,
            metrics: Metrics): GroupCoordinator = {
    val offsetConfig = this.offsetConfig(config)
    val groupConfig = GroupConfig(groupMinSessionTimeoutMs = config.groupMinSessionTimeoutMs,
      groupMaxSessionTimeoutMs = config.groupMaxSessionTimeoutMs,
      groupMaxSize = config.groupMaxSize,
      groupInitialRebalanceDelayMs = config.groupInitialRebalanceDelay)

    val groupMetadataManager = new GroupMetadataManager(config.brokerId, config.interBrokerProtocolVersion,
      offsetConfig, replicaManager, zkClient, time, metrics)
    new GroupCoordinator(config.brokerId, groupConfig, offsetConfig, groupMetadataManager, heartbeatPurgatory, joinPurgatory, time, metrics)
  }

  private def memberLeaveError(memberIdentity: MemberIdentity,
                               error: Errors): LeaveMemberResponse = {
    LeaveMemberResponse(
      memberId = memberIdentity.memberId,
      groupInstanceId = Option(memberIdentity.groupInstanceId),
      error = error)
  }

  private def leaveError(topLevelError: Errors,
                         memberResponses: List[LeaveMemberResponse]): LeaveGroupResult = {
    LeaveGroupResult(
      topLevelError = topLevelError,
      memberResponses = memberResponses)
  }
}

case class GroupConfig(groupMinSessionTimeoutMs: Int, // Consumer Group中消费者的Session过期的最小时长
                       groupMaxSessionTimeoutMs: Int, // Consumer Group中消费者的Session过期的最大时长
                       groupMaxSize: Int,
                       groupInitialRebalanceDelayMs: Int)

case class JoinGroupResult(members: List[JoinGroupResponseMember],
                           memberId: String,
                           generationId: Int,
                           protocolType: Option[String],
                           protocolName: Option[String],
                           leaderId: String,
                           error: Errors)

object JoinGroupResult {
  def apply(memberId: String, error: Errors): JoinGroupResult = {
    JoinGroupResult(
      members = List.empty,
      memberId = memberId,
      generationId = GroupCoordinator.NoGeneration,
      protocolType = None,
      protocolName = None,
      leaderId = GroupCoordinator.NoLeader,
      error = error)
  }
}

case class SyncGroupResult(protocolType: Option[String],
                           protocolName: Option[String],
                           memberAssignment: Array[Byte],
                           error: Errors)

object SyncGroupResult {
  def apply(error: Errors): SyncGroupResult = {
    SyncGroupResult(None, None, Array.empty, error)
  }
}

case class LeaveMemberResponse(memberId: String,
                               groupInstanceId: Option[String],
                               error: Errors)

case class LeaveGroupResult(topLevelError: Errors,
                            memberResponses : List[LeaveMemberResponse])
