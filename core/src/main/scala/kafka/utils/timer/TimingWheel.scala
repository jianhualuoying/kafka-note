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
package kafka.utils.timer

import kafka.utils.nonthreadsafe

import java.util.concurrent.DelayQueue
import java.util.concurrent.atomic.AtomicInteger

/*
 * Hierarchical Timing Wheels
 *
 * A simple timing wheel is a circular list of buckets of timer tasks. Let u be the time unit.
 * A timing wheel with size n has n buckets and can hold timer tasks in n * u time interval.
 * Each bucket holds timer tasks that fall into the corresponding time range. At the beginning,
 * the first bucket holds tasks for [0, u), the second bucket holds tasks for [u, 2u), …,
 * the n-th bucket for [u * (n -1), u * n). Every interval of time unit u, the timer ticks and
 * moved to the next bucket then expire all timer tasks in it. So, the timer never insert a task
 * into the bucket for the current time since it is already expired. The timer immediately runs
 * the expired task. The emptied bucket is then available for the next round, so if the current
 * bucket is for the time t, it becomes the bucket for [t + u * n, t + (n + 1) * u) after a tick.
 * A timing wheel has O(1) cost for insert/delete (start-timer/stop-timer) whereas priority queue
 * based timers, such as java.util.concurrent.DelayQueue and java.util.Timer, have O(log n)
 * insert/delete cost.
 *
 * A major drawback of a simple timing wheel is that it assumes that a timer request is within
 * the time interval of n * u from the current time. If a timer request is out of this interval,
 * it is an overflow. A hierarchical timing wheel deals with such overflows. It is a hierarchically
 * organized timing wheels. The lowest level has the finest time resolution. As moving up the
 * hierarchy, time resolutions become coarser. If the resolution of a wheel at one level is u and
 * the size is n, the resolution of the next level should be n * u. At each level overflows are
 * delegated to the wheel in one level higher. When the wheel in the higher level ticks, it reinsert
 * timer tasks to the lower level. An overflow wheel can be created on-demand. When a bucket in an
 * overflow bucket expires, all tasks in it are reinserted into the timer recursively. The tasks
 * are then moved to the finer grain wheels or be executed. The insert (start-timer) cost is O(m)
 * where m is the number of wheels, which is usually very small compared to the number of requests
 * in the system, and the delete (stop-timer) cost is still O(1).
 *
 * Example
 * Let's say that u is 1 and n is 3. If the start time is c,
 * then the buckets at different levels are:
 *
 * level    buckets
 * 1        [c,c]   [c+1,c+1]  [c+2,c+2]
 * 2        [c,c+2] [c+3,c+5]  [c+6,c+8]
 * 3        [c,c+8] [c+9,c+17] [c+18,c+26]
 *
 * The bucket expiration is at the time of bucket beginning.
 * So at time = c+1, buckets [c,c], [c,c+2] and [c,c+8] are expired.
 * Level 1's clock moves to c+1, and [c+3,c+3] is created.
 * Level 2 and level3's clock stay at c since their clocks move in unit of 3 and 9, respectively.
 * So, no new buckets are created in level 2 and 3.
 *
 * Note that bucket [c,c+2] in level 2 won't receive any task since that range is already covered in level 1.
 * The same is true for the bucket [c,c+8] in level 3 since its range is covered in level 2.
 * This is a bit wasteful, but simplifies the implementation.
 *
 * 1        [c+1,c+1]  [c+2,c+2]  [c+3,c+3]
 * 2        [c,c+2]    [c+3,c+5]  [c+6,c+8]
 * 3        [c,c+8]    [c+9,c+17] [c+18,c+26]
 *
 * At time = c+2, [c+1,c+1] is newly expired.
 * Level 1 moves to c+2, and [c+4,c+4] is created,
 *
 * 1        [c+2,c+2]  [c+3,c+3]  [c+4,c+4]
 * 2        [c,c+2]    [c+3,c+5]  [c+6,c+8]
 * 3        [c,c+8]    [c+9,c+17] [c+18,c+18]
 *
 * At time = c+3, [c+2,c+2] is newly expired.
 * Level 2 moves to c+3, and [c+5,c+5] and [c+9,c+11] are created.
 * Level 3 stay at c.
 *
 * 1        [c+3,c+3]  [c+4,c+4]  [c+5,c+5]
 * 2        [c+3,c+5]  [c+6,c+8]  [c+9,c+11]
 * 3        [c,c+8]    [c+9,c+17] [c+8,c+11]
 *
 * The hierarchical timing wheels works especially well when operations are completed before they time out.
 * Even when everything times out, it still has advantageous when there are many items in the timer.
 * Its insert cost (including reinsert) and delete cost are O(m) and O(1), respectively while priority
 * queue based timers takes O(log N) for both insert and delete where N is the number of items in the queue.
 *
 * This class is not thread-safe. There should not be any add calls while advanceClock is executing.
 * It is caller's responsibility to enforce it. Simultaneous add calls are thread-safe.
 */

/**
 *
 * @param tickMs 滴答一次的时长，类似于手表的例子中向前推进一格的时间
 * @param wheelSize 每一层时间轮上的 Bucket 数量，第 1 层的 Bucket 数量是 20。
 * @param startMs 时间轮对象被创建时的起始时间戳
 * @param taskCounter 这一层时间轮上的总定时任务数
 * @param queue 将所有 Bucket 按照过期时间排序的延迟队列
 */
@nonthreadsafe
private[timer] class TimingWheel(tickMs: Long, wheelSize: Int, startMs: Long, taskCounter: AtomicInteger, queue: DelayQueue[TimerTaskList]) {
  // 这层时间轮总时长，等于滴答时长乘以 wheelSize。以第 1 层为例，interval 就是 20 毫秒。由于下一层时间轮的滴答时长就是上一层的总时长，因此，第 2 层的滴答时长就是 20 毫秒，总时长是 400 毫秒，以此类推。
  private[this] val interval = tickMs * wheelSize
  // 时间轮下的所有 Bucket 对象，也就是所有 TimerTaskList 对象。
  private[this] val buckets = Array.tabulate[TimerTaskList](wheelSize) { _ => new TimerTaskList(taskCounter) }
  // 当前时间戳，只是源码对它进行了一些微调整，将它设置成小于当前时间的最大滴答时长的整数倍。举个例子，假设滴答时长是 20 毫秒，当前时间戳是 123 毫秒，那么，currentTime 会被调整为 120 毫秒。
  private[this] var currentTime = startMs - (startMs % tickMs) // rounding down to multiple of tickMs

  // overflowWheel can potentially be updated and read by two concurrent threads through add().
  // Therefore, it needs to be volatile due to the issue of Double-Checked Locking pattern with JVM
  // 上层时间轮
  // Kafka 是按需创建上层时间轮的。这也就是说，当有新的定时任务到达时，会尝试将其放入第 1 层时间轮。如果第 1 层的 interval 无法容纳定时任务的超时时间，就现场创建并配置好第 2 层时间轮
  // 它可能被两个并发的线程同时访问。一个线程更新该字段，另一个线程读取它，因此代码声明该字段是volatile型以保证它的内存可见性。
  @volatile private[this] var overflowWheel: TimingWheel = null

  /**
   * 创建新的上层时间轮
   */
  private[this] def addOverflowWheel(): Unit = {
    synchronized {
      if (overflowWheel == null) {
        overflowWheel = new TimingWheel(
          // 下一层时间轮的tickMs时长设置为这层时间轮总时长interval
          tickMs = interval,
          wheelSize = wheelSize,
          startMs = currentTime,
          taskCounter = taskCounter,
          queue
        )
      }
    }
  }

  def add(timerTaskEntry: TimerTaskEntry): Boolean = {
    // 获取定时任务的过期时间戳
    val expiration = timerTaskEntry.expirationMs
    // 如果该任务已然被取消了，则无需添加，直接返回
    if (timerTaskEntry.cancelled) {
      // Cancelled
      false
      // 如果该任务超时时间已过期
    } else if (expiration < currentTime + tickMs) {
      // Already expired
      false
      // 如果该任务超时时间在本层时间轮覆盖时间范围内
    } else if (expiration < currentTime + interval) {
      // Put in its own bucket
      val virtualId = expiration / tickMs
      // 计算要被放入到哪个Bucket中
      val bucket = buckets((virtualId % wheelSize.toLong).toInt)
      // 添加到Bucket中
      bucket.add(timerTaskEntry)

      // Set the bucket expiration time
      // 设置Bucket过期时间
      // 如果该时间变更过（kafka.utils.timer.TimerTaskList.setExpiration方法会返回kafka.utils.timer.TimerTaskList.expiration原时长），说明Bucket是新建或被重用，将其加回到DelayQueue
      if (bucket.setExpiration(virtualId * tickMs)) {
        // The bucket needs to be enqueued because it was an expired bucket
        // We only need to enqueue the bucket when its expiration time has changed, i.e. the wheel has advanced
        // and the previous buckets gets reused; further calls to set the expiration within the same wheel cycle
        // will pass in the same value and hence return false, thus the bucket with the same expiration will not
        // be enqueued multiple times.
        queue.offer(bucket)
      }
      true
      // 本层时间轮无法容纳该任务，交由上层时间轮处理
    } else {
      // Out of the interval. Put it into the parent timer
      // 按需创建上层时间轮
      if (overflowWheel == null) addOverflowWheel()
      // 加入到上层时间轮中
      overflowWheel.add(timerTaskEntry)
    }
  }

  // Try to advance the clock
  // 向前驱动时钟
  def advanceClock(timeMs: Long): Unit = {
    // 向前驱动到的时点要超过Bucket的时间范围，才是有意义的推进，否则什么都不做
    // 更新当前时间currentTime到下一个Bucket的起始时点
    if (timeMs >= currentTime + tickMs) {
      // currentTime设置成小于当前时间的最大滴答时长的整数倍。举个例子，假设滴答时长是 20 毫秒，当前时间戳是 123 毫秒，那么，currentTime 会被调整为 120 毫秒。
      // TODO currentTime只是减少了余数，但是没有减少一个tickMs，为什么？
      currentTime = timeMs - (timeMs % tickMs)

      // Try to advance the clock of the overflow wheel if present
      // 同时尝试为上一层时间轮做向前推进动作
      if (overflowWheel != null) overflowWheel.advanceClock(currentTime)
    }
  }
}
