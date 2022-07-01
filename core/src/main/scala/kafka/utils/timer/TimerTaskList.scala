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

import java.util.concurrent.{Delayed, TimeUnit}
import java.util.concurrent.atomic.{AtomicInteger, AtomicLong}

import kafka.utils.threadsafe
import org.apache.kafka.common.utils.Time

import scala.math._

/**
 * 任务链表相当于 Bucket，它有起始时间和结束时间，因而也就有时间间隔的概念，即“结束时间 - 起始时间 = 时间间隔”
 * @param taskCounter 用于标识当前这个链表中的总定时任务数
 */
@threadsafe
private[timer] class TimerTaskList(taskCounter: AtomicInteger) extends Delayed {

  // TimerTaskList forms a doubly linked cyclic list using a dummy root entry
  // root.next points to the head
  // root.prev points to the tail
  private[this] val root = new TimerTaskEntry(null, -1)
  root.next = root
  root.prev = root
  // 表示这个链表所在 Bucket 的过期时间戳
  private[this] val expiration = new AtomicLong(-1L)

  // Set the bucket's expiration time
  // Returns true if the expiration time is changed
  // 随着时钟不断向前推进，原有 Bucket 会不断地过期，然后失效。当这些 Bucket 失效后，源码会重用这些 Bucket。重用的方式就是重新设置 Bucket 的过期时间，并把它们加回到 DelayQueue 中
  def setExpiration(expirationMs: Long): Boolean = {
    expiration.getAndSet(expirationMs) != expirationMs
  }

  // Get the bucket's expiration time
  def getExpiration: Long = expiration.get

  // Apply the supplied function to each of tasks in this list
  def foreach(f: (TimerTask)=>Unit): Unit = {
    synchronized {
      var entry = root.next
      while (entry ne root) {
        val nextEntry = entry.next

        if (!entry.cancelled) f(entry.timerTask)

        entry = nextEntry
      }
    }
  }

  // Add a timer task entry to this list
  def add(timerTaskEntry: TimerTaskEntry): Unit = {
    var done = false
    while (!done) {
      // Remove the timer task entry if it is already in any other list
      // We do this outside of the sync block below to avoid deadlocking.
      // We may retry until timerTaskEntry.list becomes null.
      // 在添加之前尝试移除该定时任务，保证该任务没有在其他链表中
      timerTaskEntry.remove()

      synchronized {
        timerTaskEntry.synchronized {
          if (timerTaskEntry.list == null) {
            // put the timer task entry to the end of the list. (root.prev points to the tail entry)
            val tail = root.prev
            timerTaskEntry.next = root
            timerTaskEntry.prev = tail
            timerTaskEntry.list = this
            // 把timerTaskEntry添加到链表末尾 TODO 这里操作tail的next会不会存在并发问题？
            tail.next = timerTaskEntry
            // TODO 这里操作root的prev会不会存在并发问题？
            root.prev = timerTaskEntry
            taskCounter.incrementAndGet()
            done = true
          }
        }
      }
    }
  }

  // Remove the specified timer task entry from this list
  def remove(timerTaskEntry: TimerTaskEntry): Unit = {
    synchronized {
      timerTaskEntry.synchronized {
        if (timerTaskEntry.list eq this) {
          timerTaskEntry.next.prev = timerTaskEntry.prev
          timerTaskEntry.prev.next = timerTaskEntry.next
          timerTaskEntry.next = null
          timerTaskEntry.prev = null
          // list置空，这样这个timerTaskEntry元素就不属于任何链表了，可以安全地认为此链表元素被成功移除
          timerTaskEntry.list = null
          taskCounter.decrementAndGet()
        }
      }
    }
  }

  // Remove all task entries and apply the supplied function to each of them

  /**
   *
   * @param f 用于将高层次时间轮 Bucket 上的定时任务重新插入回低层次的 Bucket 中
   */
  def flush(f: (TimerTaskEntry)=>Unit): Unit = {
    synchronized {
      // 找到链表第一个元素
      var head = root.next
      while (head ne root) {
        // 移除遍历到的链表元素
        remove(head)
        // 执行传入参数f的逻辑
        f(head)
        head = root.next
      }
      // 清空过期时间设置
      expiration.set(-1L)
    }
  }

  def getDelay(unit: TimeUnit): Long = {
    unit.convert(max(getExpiration - Time.SYSTEM.hiResClockMs, 0), TimeUnit.MILLISECONDS)
  }

  def compareTo(d: Delayed): Int = {
    val other = d.asInstanceOf[TimerTaskList]
    java.lang.Long.compare(getExpiration, other.getExpiration)
  }

}

private[timer] class TimerTaskEntry(val timerTask: TimerTask, val expirationMs: Long) extends Ordered[TimerTaskEntry] {
  // 绑定的Bucket链表实例。这里设置TimerTaskList为volatile，只能保证这个引用的修改对其他线程可见，但是不能保证对TimerTaskList中元素的修改会对其他线程可见，
  // 但是这里也没有必要，因为TimerTaskEntry中对TimerTaskList的操作只限制为引用的修改，没有对其元素的修改
  @volatile
  var list: TimerTaskList = null
  var next: TimerTaskEntry = null  // next指针
  var prev: TimerTaskEntry = null  // prev指针

  // if this timerTask is already held by an existing timer task entry,
  // setTimerTaskEntry will remove it.
  // 关联给定的定时任务
  if (timerTask != null) timerTask.setTimerTaskEntry(this)
  // 关联定时任务是否已经被取消了
  def cancelled: Boolean = {
    timerTask.getTimerTaskEntry != this
  }
  // 从Bucket链表中移除自己
  // TODO 这里的设计有点不明白，TimerTaskEntry包含TimerTaskList的引用为了从链表中移除自己，这个方法只在kafka.utils.timer.TimerTaskList.add和kafka.utils.timer.TimerTask.setTimerTaskEntry方法中调用
  //  kafka.utils.timer.TimerTaskList.add方法主要是在TimerTaskList添加元素时，防止TimeTaskEntry已经添加到其他List，从而一个元素放到多个链表上；
  //  kafka.utils.timer.TimerTask.setTimerTaskEntry方法主要是在
  def remove(): Unit = {
    var currentList = list
    // If remove is called when another thread is moving the entry from a task entry list to another,
    // this may fail to remove the entry due to the change of value of list. Thus, we retry until the list becomes null.
    // In a rare case, this thread sees null and exits the loop, but the other thread insert the entry to another list later.
    while (currentList != null) {
      currentList.remove(this)
      currentList = list
    }
  }

  override def compare(that: TimerTaskEntry): Int = {
    java.lang.Long.compare(expirationMs, that.expirationMs)
  }
}

