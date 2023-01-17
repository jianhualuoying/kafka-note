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

import kafka.server.DelayedOperation

/**
 *
 * Delayed heartbeat operations that are added to the purgatory for session timeout checking.
 * Heartbeats are paused during rebalance.
 * 除了正常的心跳请求外，JoinGroupRequest和SyncGroupRequest请求也被视作心跳请求处理，
 * 当处理、响应“加入组”，“同步组”，“离开组”的请求时，协调者会触发延迟心跳的完成操作，调用 completeAndScheduleNextHeartbeatExpiration方法
 * @param coordinator coordinator GroupCoordinator对象，DelayedHeartbeat中方法的实现方式是调用GroupCoordinator中对应的方法
 * @param group       group 对应的GroupMetadata对象
 * @param memberId    member 对应的MemberMetadata对象
 * @param isPending
 * @param timeoutMs   指定了DelayedHeartbeat的到期时长，此时间是消费者在JoinGroupRequest中设置的，且符合GroupConfig指定的合法区间
 */
private[group] class DelayedHeartbeat(coordinator: GroupCoordinator,
                                      group: GroupMetadata,
                                      memberId: String,
                                      isPending: Boolean,
                                      timeoutMs: Long)
  extends DelayedOperation(timeoutMs, Some(group.lock)) {
 // 在添加DelayedHeartbeat任务时就会尝试一次执行，调用的是tryComplete()方法
 // forceComplete 就是 DelayedOperation#forceComplete()方法，forceComplete方法中又调用了DelayedHeartbeat实现的 DelayedHeartbeat#onComplete()方法，以及 TimerTask.cancel 取消任务
 // DelayedHeartbeat#onComplete最终还是调用的GroupCoordinator的onCompleteHeartbeat()，而该方法是空实现，没有任何动作。所以心跳检查最终只是取消该任务
  override def tryComplete(): Boolean = coordinator.tryCompleteHeartbeat(group, memberId, isPending, forceComplete _)
 // 当DelayedHeartbeat到期时会调用onExpiration()方法
  override def onExpiration() = coordinator.onExpireHeartbeat(group, memberId, isPending)
  override def onComplete() = coordinator.onCompleteHeartbeat()
}
