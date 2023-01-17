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

import kafka.server.{DelayedOperation, DelayedOperationPurgatory, GroupKey}

import scala.math.{max, min}

/**
 * Delayed rebalance operations that are added to the purgatory when group is preparing for rebalance
 *
 * Whenever a join-group request is received, check if all known group members have requested
 * to re-join the group; if yes, complete this operation to proceed rebalance.
 *
 * When the operation has expired, any known members that have not requested to re-join
 * the group are marked as failed, and complete this operation to proceed rebalance with
 * the rest of the group.
 * 等待 Consumer Group 中所有的消费者发送 JoinGroupRequest 申请加入。
 * 每当处理完新收到的 JoinGroupRequest 时，都会检测相关的DelayedJoin是否能够完成，
 * 经过一段时间的等待，DelayedJoin也会到期执行。
 * 延迟加入组
 */
private[group] class DelayedJoin(coordinator: GroupCoordinator,
                                 group: GroupMetadata,
                                 rebalanceTimeout: Long) extends DelayedOperation(rebalanceTimeout, Some(group.lock)) {
  /**
   * 注意这里的 forceComplete 就是 DelayedOperation#forceComplete()方法，forceComplete方法中又调用了DelayedJoin实现的 DelayedJoin#onComplete()方法
   * @return
   */
  override def tryComplete(): Boolean = coordinator.tryCompleteJoin(group, forceComplete _)
  override def onExpiration(): Unit = {
    coordinator.onExpireJoin()
    // try to complete delayed actions introduced by coordinator.onCompleteJoin
    tryToCompleteDelayedAction()
  }
  // 当已知Member都已申请重新加入或DelayedJoin到期时执行该方法
  override def onComplete(): Unit = coordinator.onCompleteJoin(group)

  // TODO: remove this ugly chain after we move the action queue to handler thread
  private def tryToCompleteDelayedAction(): Unit = coordinator.groupManager.replicaManager.tryCompleteActions()
}

/**
  * Delayed rebalance operation that is added to the purgatory when a group is transitioning from
  * Empty to PreparingRebalance
  *
  * When onComplete is triggered we check if any new members have been added and if there is still time remaining
  * before the rebalance timeout. If both are true we then schedule a further delay. Otherwise we complete the
  * rebalance.
  */
private[group] class InitialDelayedJoin(coordinator: GroupCoordinator,
                                        purgatory: DelayedOperationPurgatory[DelayedJoin],
                                        group: GroupMetadata,
                                        configuredRebalanceDelay: Int,
                                        delayMs: Int,
                                        remainingMs: Int) extends DelayedJoin(coordinator, group, delayMs) {

  override def tryComplete(): Boolean = false

  override def onComplete(): Unit = {
    group.inLock {
      if (group.newMemberAdded && remainingMs != 0) {
        group.newMemberAdded = false
        val delay = min(configuredRebalanceDelay, remainingMs)
        val remaining = max(remainingMs - delayMs, 0)
        purgatory.tryCompleteElseWatch(new InitialDelayedJoin(coordinator,
          purgatory,
          group,
          configuredRebalanceDelay,
          delay,
          remaining
        ), Seq(GroupKey(group.groupId)))
      } else
        super.onComplete()
    }
  }

}
