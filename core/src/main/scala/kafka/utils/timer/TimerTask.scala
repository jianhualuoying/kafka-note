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

/**
 *  Kafka 中的定时任务
 */
trait TimerTask extends Runnable {
  // 通常是request.timeout.ms参数值
  val delayMs: Long // timestamp in millisecond
  // 每个 TimerTask 实例关联一个 TimerTaskEntry
  // 就是说每个定时任务需要知道它在哪个Bucket链表下的哪个链表元素上
  // TimingWheel是时间轮，时间轮下的每个元素是Bucket，每个 Bucket 下是一个双向循环链表 TimerTaskList，用来保存延迟请求 TimerTaskEntry，
  // TimerTaskList 下包含多个 TimerTaskEntry，每个TimerTaskEntry都包含一个TimerTask
  private[this] var timerTaskEntry: TimerTaskEntry = null

  /**
   * 取消定时任务
   */
  def cancel(): Unit = {
    synchronized {
      if (timerTaskEntry != null) timerTaskEntry.remove()
      timerTaskEntry = null
    }
  }

  /**
   * 在给 timerTaskEntry 赋值之前，它必须要先考虑这个定时任务是否已经绑定了其他的 timerTaskEntry，如果是的话，就必须先取消绑定。
   * @param entry
   */
  private[timer] def setTimerTaskEntry(entry: TimerTaskEntry): Unit = {
    synchronized {
      // if this timerTask is already held by an existing timer task entry,
      // we will remove such an entry first.
      // 这里 TimerTask 已经有关联的 TimerTaskEntry，既然要换 TimerTaskEntry，而原来的 TimerTaskEntry 也已经放到了某个 TimerTaskList
      // 所以，原来的TimerTaskEntry也就没有必要放在原来的TimerTaskList上了，因为TimerTaskEntry 是依托于 TimerTask 而存在的
      if (timerTaskEntry != null && timerTaskEntry != entry)
        timerTaskEntry.remove()

      timerTaskEntry = entry
    }
  }

  private[timer] def getTimerTaskEntry: TimerTaskEntry = timerTaskEntry

}
