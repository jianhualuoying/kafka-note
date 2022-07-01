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

package kafka.server

import java.util.concurrent._
import java.util.concurrent.atomic._
import java.util.concurrent.locks.{Lock, ReentrantLock}

import kafka.metrics.KafkaMetricsGroup
import kafka.utils.CoreUtils.inLock
import kafka.utils._
import kafka.utils.timer._

import scala.collection._
import scala.collection.mutable.ListBuffer

/**
 * An operation whose processing needs to be delayed for at most the given delayMs. For example
 * a delayed produce operation could be waiting for specified number of acks; or
 * a delayed fetch operation could be waiting for a given number of bytes to accumulate.
 *
 * The logic upon completing a delayed operation is defined in onComplete() and will be called exactly once.
 * Once an operation is completed, isCompleted() will return true. onComplete() can be triggered by either
 * forceComplete(), which forces calling onComplete() after delayMs if the operation is not yet completed,
 * or tryComplete(), which first checks if the operation can be completed or not now, and if yes calls
 * forceComplete().
 *
 * A subclass of DelayedOperation needs to provide an implementation of both onComplete() and tryComplete().
 *
 * Noted that if you add a future delayed operation that calls ReplicaManager.appendRecords() in onComplete()
 * like DelayedJoin, you must be aware that this operation's onExpiration() needs to call actionQueue.tryCompleteAction().
 */

/**
 *
 * @param delayMs 超时时间
 * @param lockOpt
 */
abstract class DelayedOperation(override val delayMs: Long,
                                lockOpt: Option[Lock] = None)
  extends TimerTask with Logging {
  // 标识该延迟操作是否已经完成
  private val completed = new AtomicBoolean(false)
  // Visible for testing
  private[server] val lock: Lock = lockOpt.getOrElse(new ReentrantLock)

  /*
   * Force completing the delayed operation, if not already completed.
   * This function can be triggered when
   *
   * 1. The operation has been verified to be completable inside tryComplete()
   * 2. The operation has expired and hence needs to be completed right now
   *
   * Return true iff the operation is completed by the caller: note that
   * concurrent threads can try to complete the same operation, but only
   * the first thread will succeed in completing the operation and return
   * true, others will still return false
   * 强制完成延迟操作，不管它是否满足完成条件。每当操作满足完成条件或已经过期了，就需要调用该方法完成该操作。
   */
  def forceComplete(): Boolean = {
    if (completed.compareAndSet(false, true)) {
      // cancel the timeout timer
      cancel()
      onComplete()
      true
    } else {
      false
    }
  }

  /**
   * Check if the delayed operation is already completed
   * 检查延迟操作是否已经完成。源码使用这个方法来决定后续如何处理该操作。比如如果操作已经完成了，那么通常需要取消该操作。
   */
  def isCompleted: Boolean = completed.get()

  /**
   * Call-back to execute when a delayed operation gets expired and hence forced to complete.
   * 强制完成之后执行的过期逻辑回调方法。只有真正完成操作的那个线程才有资格调用这个方法。
   */
  def onExpiration(): Unit

  /**
   * Process for completing an operation; This function needs to be defined
   * in subclasses and will be called exactly once in forceComplete()
   * 完成延迟操作所需的处理逻辑。这个方法只会在 forceComplete 方法中被调用
   */
  def onComplete(): Unit

  /**
   * Try to complete the delayed operation by first checking if the operation
   * can be completed by now. If yes execute the completion logic by calling
   * forceComplete() and return true iff forceComplete returns true; otherwise return false
   *
   * This function needs to be defined in subclasses
   * 尝试完成延迟操作的顶层方法，内部会调用 forceComplete 方法
   */
  def tryComplete(): Boolean

  /**
   * Thread-safe variant of tryComplete() and call extra function if first tryComplete returns false
   * @param f else function to be executed after first tryComplete returns false
   * @return result of tryComplete
   */
  private[server] def safeTryCompleteOrElse(f: => Unit): Boolean = inLock(lock) {
    // 如果该延迟请求是由本线程完成的，直接返回true即可
    if (tryComplete()) true
    else {
      // 否则，执行函数f
      f
      // last completion check
      tryComplete()
    }
  }

  /**
   * Thread-safe variant of tryComplete()
   * 线程安全版本的 tryComplete 方法。这个方法其实是社区后来才加入的，不过已经慢慢地取代了 tryComplete，现在外部代码调用的都是这个方法了
   */
  private[server] def safeTryComplete(): Boolean = inLock(lock)(tryComplete())

  /*
   * run() method defines a task that is executed on timeout
   * 调用延迟操作超时后的过期逻辑，也就是组合调用 forceComplete + onExpiration。
   */
  override def run(): Unit = {
    if (forceComplete())
      onExpiration()
  }
}

object DelayedOperationPurgatory {

  private val Shards = 512 // Shard the watcher list to reduce lock contention

  def apply[T <: DelayedOperation](purgatoryName: String,
                                   brokerId: Int = 0,
                                   purgeInterval: Int = 1000,
                                   reaperEnabled: Boolean = true,
                                   timerEnabled: Boolean = true): DelayedOperationPurgatory[T] = {
    val timer = new SystemTimer(purgatoryName)
    new DelayedOperationPurgatory[T](purgatoryName, timer, brokerId, purgeInterval, reaperEnabled, timerEnabled)
  }

}

/**
 *  A helper purgatory class for bookkeeping delayed operations with a timeout, and expiring timed out operations.
 * 每一类延迟请求都对应于一个DelayedOperationPurgatory 实例，。这些实例一般都保存在上层的管理器中。比如，与消费者组相关的心跳请求、加入组请求的 Purgatory 实例，就保存在 GroupCoordinator 组件中，
 * 而与生产者相关的 PRODUCE 请求的 Purgatory 实例，被保存在分区对象或副本状态机中。
 *
 * DelayedOperation 调用 SystemTimer 类，DelayedOperationPurgatory 管理 DelayedOperation。它们共同实现了 Broker 端对于延迟请求的处理，
 * 基本思想就是，能立即完成的请求马上完成，否则就放入到名为Purgatory 的缓冲区中。后续，DelayedOperationPurgatory 类的方法会自动地处理这些延迟请求
 * @param purgatoryName  Purgatory 的名字
 * @param timeoutTimer
 * @param brokerId      Broker 的序号
 * @param purgeInterval 用于控制删除线程移除 Bucket 中的过期延迟请求的频率
 * @param reaperEnabled 是否启动删除线程，默认启动
 * @param timerEnabled  是否启用分层时间轮，默认启动
 * @tparam T
 */
final class DelayedOperationPurgatory[T <: DelayedOperation](purgatoryName: String,
                                                             timeoutTimer: Timer,
                                                             brokerId: Int = 0,
                                                             purgeInterval: Int = 1000,
                                                             reaperEnabled: Boolean = true,
                                                             timerEnabled: Boolean = true)
        extends Logging with KafkaMetricsGroup {
  /* a list of operation watching keys */

  /**
   * 实现延迟请求后续自动处理的关键数据结构
   */
  private class WatcherList {
    // 定义一组按照Key分组的Watchers对象，Pool封装了ConcurrentHashMap
    val watchersByKey = new Pool[Any, Watchers](Some((key: Any) => new Watchers(key)))

    val watchersLock = new ReentrantLock()

    /*
     * Return all the current watcher lists,
     * note that the returned watchers may be removed from the list by other threads
     * 返回所有Watchers对象
     */
    def allWatchers = {
      watchersByKey.values
    }
  }

  private val watcherLists = Array.fill[WatcherList](DelayedOperationPurgatory.Shards)(new WatcherList)
  private def watcherList(key: Any): WatcherList = {
    watcherLists(Math.abs(key.hashCode() % watcherLists.length))
  }

  // the number of estimated total operations in the purgatory
  private[this] val estimatedTotalOperations = new AtomicInteger(0)

  /* background thread expiring operations that have timed out */
  private val expirationReaper = new ExpiredOperationReaper()

  private val metricsTags = Map("delayedOperation" -> purgatoryName)
  newGauge("PurgatorySize", () => watched, metricsTags)
  newGauge("NumDelayedOperations", () => numDelayed, metricsTags)

  if (reaperEnabled)
    expirationReaper.start()

  /**
   * Check if the operation can be completed, if not watch it based on the given watch keys
   *
   * Note that a delayed operation can be watched on multiple keys. It is possible that
   * an operation is completed after it has been added to the watch list for some, but
   * not all of the keys. In this case, the operation is considered completed and won't
   * be added to the watch list of the remaining keys. The expiration reaper thread will
   * remove this operation from any watcher list in which the operation exists.
   * 检查操作是否能够完成
   * 1. 先尝试完成延迟请求；
   * 2. 如果不行，就加入到 WatcherList，等待后面再试。
   * @param operation the delayed operation to be checked
   * @param watchKeys keys for bookkeeping the operation
   * @return true iff the delayed operations can be completed by the caller
   */
  def tryCompleteElseWatch(operation: T, watchKeys: Seq[Any]): Boolean = {
    assert(watchKeys.nonEmpty, "The watch key list can't be empty")

    // The cost of tryComplete() is typically proportional to the number of keys. Calling tryComplete() for each key is
    // going to be expensive if there are many keys. Instead, we do the check in the following way through safeTryCompleteOrElse().
    // If the operation is not completed, we just add the operation to all keys. Then we call tryComplete() again. At
    // this time, if the operation is still not completed, we are guaranteed that it won't miss any future triggering
    // event since the operation is already on the watcher list for all keys.
    //
    // ==============[story about lock]==============
    // Through safeTryCompleteOrElse(), we hold the operation's lock while adding the operation to watch list and doing
    // the tryComplete() check. This is to avoid a potential deadlock between the callers to tryCompleteElseWatch() and
    // checkAndComplete(). For example, the following deadlock can happen if the lock is only held for the final tryComplete()
    // 1) thread_a holds readlock of stateLock from TransactionStateManager
    // 2) thread_a is executing tryCompleteElseWatch()
    // 3) thread_a adds op to watch list
    // 4) thread_b requires writelock of stateLock from TransactionStateManager (blocked by thread_a)
    // 5) thread_c calls checkAndComplete() and holds lock of op
    // 6) thread_c is waiting readlock of stateLock to complete op (blocked by thread_b)
    // 7) thread_a is waiting lock of op to call the final tryComplete() (blocked by thread_c)
    //
    // Note that even with the current approach, deadlocks could still be introduced. For example,
    // 1) thread_a calls tryCompleteElseWatch() and gets lock of op
    // 2) thread_a adds op to watch list
    // 3) thread_a calls op#tryComplete and tries to require lock_b
    // 4) thread_b holds lock_b and calls checkAndComplete()
    // 5) thread_b sees op from watch list
    // 6) thread_b needs lock of op
    // To avoid the above scenario, we recommend DelayedOperationPurgatory.checkAndComplete() be called without holding
    // any exclusive lock. Since DelayedOperationPurgatory.checkAndComplete() completes delayed operations asynchronously,
    // holding a exclusive lock to make the call is often unnecessary.
    if (operation.safeTryCompleteOrElse {
      // 如果延迟操作延迟请求不是由本线程完成，将该operation加入到Key 所在的 WatcherList  中进行监控
      watchKeys.foreach(key => watchForOperation(key, operation))
      // 更新Purgatory中总请求数
      if (watchKeys.nonEmpty) estimatedTotalOperations.incrementAndGet()
    }) return true

    // if it cannot be completed by now and hence is watched, add to the expire queue also
    // kafka.server.DelayedOperation.safeTryCompleteOrElse 函数中会再次尝试完成该延迟请求，如果依然不能完成此请求，将其加入到过期队列
    if (!operation.isCompleted) {
      if (timerEnabled)
        timeoutTimer.add(operation)
      if (operation.isCompleted) {
        // cancel the timer task
        // 如果完成取消该操作
        operation.cancel()
      }
    }

    false
  }

  /**
   * Check if some delayed operations can be completed with the given watch key,
   * and if yes complete them.
   * 检查给定 Key 所在的 WatcherList 中的延迟请求是否满足完成条件
   * @return the number of completed operations during this process
   */
  def checkAndComplete(key: Any): Int = {
    // 获取给定Key的WatcherList
    val wl = watcherList(key)
    // 获取WatcherList中Key对应的Watchers对象实例
    val watchers = inLock(wl.watchersLock) { wl.watchersByKey.get(key) }
    // 尝试完成满足完成条件的延迟请求并返回成功完成的请求数
    val numCompleted = if (watchers == null)
      0
    else
      watchers.tryCompleteWatched()
    debug(s"Request key $key unblocked $numCompleted $purgatoryName operations")
    numCompleted
  }

  /**
   * Return the total size of watch lists the purgatory. Since an operation may be watched
   * on multiple lists, and some of its watched entries may still be in the watch lists
   * even when it has been completed, this number may be larger than the number of real operations watched
   */
  def watched: Int = {
    watcherLists.foldLeft(0) { case (sum, watcherList) => sum + watcherList.allWatchers.map(_.countWatched).sum }
  }

  /**
   * Return the number of delayed operations in the expiry queue
   */
  def numDelayed: Int = timeoutTimer.size

  /**
    * Cancel watching on any delayed operations for the given key. Note the operation will not be completed
    */
  def cancelForKey(key: Any): List[T] = {
    val wl = watcherList(key)
    inLock(wl.watchersLock) {
      val watchers = wl.watchersByKey.remove(key)
      if (watchers != null)
        watchers.cancel()
      else
        Nil
    }
  }

  /*
   * Return the watch list of the given key, note that we need to
   * grab the removeWatchersLock to avoid the operation being added to a removed watcher list
   */
  private def watchForOperation(key: Any, operation: T): Unit = {
    val wl = watcherList(key)
    inLock(wl.watchersLock) {
      val watcher = wl.watchersByKey.getAndMaybePut(key)
      watcher.watch(operation)
    }
  }

  /*
   * Remove the key from watcher lists if its list is empty
   */
  private def removeKeyIfEmpty(key: Any, watchers: Watchers): Unit = {
    val wl = watcherList(key)
    inLock(wl.watchersLock) {
      // if the current key is no longer correlated to the watchers to remove, skip
      if (wl.watchersByKey.get(key) != watchers)
        return

      if (watchers != null && watchers.isEmpty) {
        wl.watchersByKey.remove(key)
      }
    }
  }

  /**
   * Shutdown the expire reaper thread
   */
  def shutdown(): Unit = {
    if (reaperEnabled)
      expirationReaper.shutdown()
    timeoutTimer.shutdown()
    removeMetric("PurgatorySize", metricsTags)
    removeMetric("NumDelayedOperations", metricsTags)
  }

  /**
   *  A linked list of watched delayed operations based on some key
   * 每个 Watchers 实例都定义了一个延迟请求链表,Kafka 利用它来监控保存其中的延迟请求的可完成状态
   *
   * @param key Key 可以是任何类型，比如表示消费者组的字符串类型、表示主题分区的 TopicPartitionOperationKey 类型
   */
  private class Watchers(val key: Any) {
    private[this] val operations = new ConcurrentLinkedQueue[T]()

    // count the current number of watched operations. This is O(n), so use isEmpty() if possible
    def countWatched: Int = operations.size

    def isEmpty: Boolean = operations.isEmpty

    // add the element to watch
    // 将延迟请求加入到链表中
    def watch(t: T): Unit = {
      operations.add(t)
    }

    // traverse the list and try to complete some watched elements
    def tryCompleteWatched(): Int = {
      var completed = 0

      val iter = operations.iterator()
      //  遍历整个链表，并尝试完成其中的延迟请求
      while (iter.hasNext) {
        val curr = iter.next()
        if (curr.isCompleted) {
          // another thread has completed this operation, just remove it
          iter.remove()
        } else if (curr.safeTryComplete()) {
          iter.remove()
          completed += 1
        }
      }

      if (operations.isEmpty)
        removeKeyIfEmpty(key, this)

      completed
    }

    def cancel(): List[T] = {
      val iter = operations.iterator()
      val cancelled = new ListBuffer[T]()
      // 遍历链表，再取消掉里面的延迟请求
      while (iter.hasNext) {
        val curr = iter.next()
        curr.cancel()
        iter.remove()
        cancelled += curr
      }
      cancelled.toList
    }

    // traverse the list and purge elements that are already completed by others
    def purgeCompleted(): Int = {
      var purged = 0

      val iter = operations.iterator()
      while (iter.hasNext) {
        val curr = iter.next()
        if (curr.isCompleted) {
          iter.remove()
          purged += 1
        }
      }

      if (operations.isEmpty)
        removeKeyIfEmpty(key, this)

      purged
    }
  }

  def advanceClock(timeoutMs: Long): Unit = {
    timeoutTimer.advanceClock(timeoutMs)

    // Trigger a purge if the number of completed but still being watched operations is larger than
    // the purge threshold. That number is computed by the difference btw the estimated total number of
    // operations and the number of pending delayed operations.
    // 如果已经完成但是仍被监听的操作（estimatedTotalOperations.get - numDelayed）大于阈值purgeInterval，就是触发清理
    if (estimatedTotalOperations.get - numDelayed > purgeInterval) {
      // now set estimatedTotalOperations to delayed (the number of pending operations) since we are going to
      // clean up watchers. Note that, if more operations are completed during the clean up, we may end up with
      // a little overestimated total number of operations.
      estimatedTotalOperations.getAndSet(numDelayed)
      debug("Begin purging watch lists")
      // 计算watcherList中总的已完成的请求数。另外在遍历watcherList的同时，代码还会将这些已完成的任务从链表中移除（purgeCompleted方法中完成）。
      val purged = watcherLists.foldLeft(0) {
        case (sum, watcherList) => sum + watcherList.allWatchers.map(_.purgeCompleted()).sum
      }
      debug("Purged %d elements from watch lists.".format(purged))
    }
  }

  /**
   * A background reaper to expire delayed operations that have timed out
   */
  private class ExpiredOperationReaper extends ShutdownableThread(
    "ExpirationReaper-%d-%s".format(brokerId, purgatoryName),
    false) {

    override def doWork(): Unit = {
      advanceClock(200L)
    }
  }
}
