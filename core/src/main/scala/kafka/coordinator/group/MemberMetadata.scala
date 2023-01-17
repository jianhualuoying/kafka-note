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

import java.util

import kafka.utils.nonthreadsafe

/**
 * case class 类似于 Java 中的 POJO 类
 * 组成员概要数据，提取了最核心的元数据信息。
 * @param memberId              成员ID，由Kafka自动生成，consumer- 组 ID-< 序号 >-
 * @param groupInstanceId       消费者组静态成员的 ID（Consumer端参数group.instance.id值）。静态成员机制的引入能够规避不必要的消费者组 Rebalance 操作。
 * @param clientId              client.id参数值。由于 memberId 不能被设置，因此，你可以用这个字段来区分消费者组下的不同成员。
 * @param clientHost            Consumer端程序主机名，它记录了这个客户端是从哪台机器发出的消费请求。
 * @param metadata              标识消费者组成员分区分配策略的字节数组，由消费者端参数 partition.assignment.strategy 值设定，默认的 RangeAssignor 策略是按照主题平均分配分区
 * @param assignment            保存分配给该成员的订阅分区。每个消费者组都要选出一个 Leader 消费者组成员，负责给所有成员分配消费方案。之后，Kafka 将制定好的分配方案序列化成字节数组，赋值给 assignment，分发给各个成员。
 */
case class MemberSummary(memberId: String,
                         groupInstanceId: Option[String],
                         clientId: String,
                         clientHost: String,
                         metadata: Array[Byte],
                         assignment: Array[Byte])

private object MemberMetadata {
  /**
   * 提取分区分配策略集合
   * 从一组给定的分区分配策略详情中提取出分区分配策略的名称，并将其封装成一个集合对象返回
   * eg：消费者组下有 3 个成员，它们的 partition.assignment.strategy(supportedProtocols列表中元组的第一个元素) 参数分别设置成 RangeAssignor、RangeAssignor 和 RoundRobinAssignor，
   * 那么，plainProtocolSet 方法的返回值就是集合[RangeAssignor，RoundRobinAssignor]
   * @param supportedProtocols
   * @return
   */
  def plainProtocolSet(supportedProtocols: List[(String, Array[Byte])]) = supportedProtocols.map(_._1).toSet
}

/**
 *
 * 消费者组成员的元数据。
 * Member metadata contains the following metadata:
 * Heartbeat metadata:
 * 1. negotiated heartbeat session timeout
 * 2. timestamp of the latest heartbeat
 *
 * Protocol metadata:
 * 1. the list of supported protocols (ordered by preference)
 * 2. the metadata associated with each protocol
 *
 * In addition, it also contains the following state information:
 *
 * 1. Awaiting rebalance callback: when the group is in the prepare-rebalance state,
 *                                 its rebalance callback will be kept in the metadata if the
 *                                 member has sent the join group request
 * 2. Awaiting sync callback: when the group is in the awaiting-sync state, its sync callback
 *                            is kept in metadata until the leader provides the group assignment
 *                            and the group transitions to stable
 *
 * @param memberId                成员ID，由Kafka自动生成，consumer- 组 ID-< 序号 >-
 * @param groupId
 * @param groupInstanceId         消费者组静态成员的 ID（Consumer端参数group.instance.id值）。静态成员机制的引入能够规避不必要的消费者组 Rebalance 操作。
 * @param clientId                client.id参数值。由于 memberId 不能被设置，因此，你可以用这个字段来区分消费者组下的不同成员。
 * @param clientHost
 * @param rebalanceTimeoutMs       Rebalance 操作的超时时间，即一次 Rebalance 操作必须在这个时间内完成，否则被视为超时。这个字段的值是 Consumer 端参数 max.poll.interval.ms 的值。
 * @param sessionTimeoutMs        会话超时时间。当前消费者组成员依靠心跳机制“保活”。如果在会话超时时间之内未能成功发送心跳，组成员就被判定成“下线”，从而触发新一轮的 Rebalance。这个字段的值是 Consumer 端参数 session.timeout.ms 的值。
 * @param protocolType            直译就是协议类型。它实际上标识的是消费者组被用在了哪个场景。这里的场景具体有两个：
 *                                第一个是作为普通的消费者组使用，该字段对应的值就是consumer；
 *                                第二个是供 Kafka Connect 组件中的消费者使用，该字段对应的值是connect。
 * @param supportedProtocols      标识成员配置的多组分区分配策略。目前，Consumer 端参数 partition.assignment.strategy 的类型是 List，说明你可以为消费者组成员设置多组分配策略，
 *                                因此，这个字段也是一个 List 类型，每个元素是一个元组（Tuple）。元组的第一个元素是策略名称，第二个元素是序列化后的策略详情。
 */
@nonthreadsafe
private[group] class MemberMetadata(var memberId: String,
                                    val groupId: String,
                                    val groupInstanceId: Option[String],
                                    val clientId: String,
                                    val clientHost: String,
                                    val rebalanceTimeoutMs: Int,
                                    val sessionTimeoutMs: Int,
                                    val protocolType: String,
                                    var supportedProtocols: List[(String, Array[Byte])]) {
  // 保存分配给该成员的分区分配方案
  var assignment: Array[Byte] = Array.empty[Byte]
  // 表示组成员是否正在等待加入组，为空则表示成员已经加入消费者组，不为空，则表示成员还没有加入消费者组
  var awaitingJoinCallback: JoinGroupResult => Unit = null
  // 表示组成员是否正在等待 GroupCoordinator 发送分配方案
  var awaitingSyncCallback: SyncGroupResult => Unit = null
  // 表示组成员是否发起“退出组”的操作。
  var isLeaving: Boolean = false
  // 表示是否是消费者组下的新成员。
  var isNew: Boolean = false
  val isStaticMember: Boolean = groupInstanceId.isDefined

  // This variable is used to track heartbeat completion through the delayed
  // heartbeat purgatory. When scheduling a new heartbeat expiration, we set
  // this value to `false`. Upon receiving the heartbeat (or any other event
  // indicating the liveness of the client), we set it to `true` so that the
  // delayed heartbeat can be completed.
  // 这个变量用来跟踪消费者心跳是否超时，如果心跳时间内，消费者内没有发送心跳，则设置该变量为 false，如果收到消费者发送的心跳，则设置该变量为true
  var heartbeatSatisfied: Boolean = false

  def isAwaitingJoin = awaitingJoinCallback != null
  def isAwaitingSync = awaitingSyncCallback != null

  /**
   *
   * Get metadata corresponding to the provided protocol.
   * 从配置的分区分配策略中寻找给定策略
   *
   * @param protocol  给定的策略
   * @return
   */
  def metadata(protocol: String): Array[Byte] = {
    supportedProtocols.find(_._1 == protocol) match {
      case Some((_, metadata)) => metadata
      case None =>
        throw new IllegalArgumentException("Member does not support protocol")
    }
  }

  def hasSatisfiedHeartbeat: Boolean = {
    // isNew 用来标识消费者是否是消费者组下的新成员，heartbeatSatisfied用来标识消费者心跳是否超时
    // 如果消费者是消费者组下的新成员，则返回消费者心跳是否超时
    if (isNew) {
      // New members can be expired while awaiting join, so we have to check this first
      heartbeatSatisfied
    } else if (isAwaitingJoin || isAwaitingSync) {
      // Members that are awaiting a rebalance automatically satisfy expected heartbeats
      // 消费者正在等待JoinGroupResponse（awaitingJoinCallback不为空）或者正在等待SyncGroupResponse（awaitingSyncCallback不为空），则返回true
      true
    } else {
      // Otherwise we require the next heartbeat
      // 其他情况，返回消费者心跳是否超时
      heartbeatSatisfied
    }
  }

  /**
   * Check if the provided protocol metadata matches the currently stored metadata.
   * 检查协议元数据是否匹配
   */
  def matches(protocols: List[(String, Array[Byte])]): Boolean = {
    if (protocols.size != this.supportedProtocols.size)
      return false
    // indices方法可以返回指定列表的所有有效索引值组成的列表
    for (i <- protocols.indices) {
      val p1 = protocols(i)
      val p2 = supportedProtocols(i)
      if (p1._1 != p2._1 || !util.Arrays.equals(p1._2, p2._2))
        return false
    }
    true
  }

  def summary(protocol: String): MemberSummary = {
    MemberSummary(memberId, groupInstanceId, clientId, clientHost, metadata(protocol), assignment)
  }

  def summaryNoMetadata(): MemberSummary = {
    MemberSummary(memberId, groupInstanceId, clientId, clientHost, Array.empty[Byte], Array.empty[Byte])
  }

  /**
   * Vote for one of the potential group protocols. This takes into account the protocol preference as
   * indicated by the order of supported protocols and returns the first one also contained in the set
   * candidates 字段的值是[“策略 A”，“策略 B”]，
   * 成员 1 支持[“策略 B”，“策略 A”]，
   * 成员 2 支持[“策略 A”，“策略 B”，“策略 C”]，
   * 成员 3 支持[“策略D”，“策略 B”，“策略 A”]
   * 那么，vote 方法会将 candidates 与每个成员的支持列表进行比对，找出成员支持列表中第一个包含在 candidates 中的策略。
   * 因此，对于这个例子来说，
   * 成员 1 投票策略 B，
   * 成员 2 投票策略 A，
   * 成员 3 投票策略 B。
   * 可以看到，投票的结果是，策略 B 是两票，策略 A 是 1 票。所以，返回策略 B 作为新的策略
   * 成员支持列表中的策略是有顺序的，成员会倾向于选择靠前的策略。
   */
  def vote(candidates: Set[String]): String = {
    supportedProtocols.find({ case (protocol, _) => candidates.contains(protocol)}) match {
      case Some((protocol, _)) => protocol
      case None =>
        throw new IllegalArgumentException("Member does not support any of the candidate protocols")
    }
  }

  override def toString: String = {
    "MemberMetadata(" +
      s"memberId=$memberId, " +
      s"groupInstanceId=$groupInstanceId, " +
      s"clientId=$clientId, " +
      s"clientHost=$clientHost, " +
      s"sessionTimeoutMs=$sessionTimeoutMs, " +
      s"rebalanceTimeoutMs=$rebalanceTimeoutMs, " +
      s"supportedProtocols=${supportedProtocols.map(_._1)}, " +
      ")"
  }
}
