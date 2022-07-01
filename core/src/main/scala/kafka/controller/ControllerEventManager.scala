/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
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

import java.util.ArrayList
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.{CountDownLatch, LinkedBlockingQueue, TimeUnit}
import java.util.concurrent.locks.ReentrantLock

import kafka.metrics.{KafkaMetricsGroup, KafkaTimer}
import kafka.utils.CoreUtils.inLock
import kafka.utils.ShutdownableThread
import org.apache.kafka.common.utils.Time

import scala.collection._

/**
 * 保存一些字符串常量，比如线程名字
 */
object ControllerEventManager {
  val ControllerEventThreadName = "controller-event-thread"
  val EventQueueTimeMetricName = "EventQueueTimeMs"
  val EventQueueSizeMetricName = "EventQueueSize"
}

/**
 * Controller 端的事件处理器接口
 */
trait ControllerEventProcessor {
  // 接收一个 Controller 事件，并进行处理
  def process(event: ControllerEvent): Unit
  // 接收一个 Controller 事件，并抢占队列之前的事件进行优先处理
  def preempt(event: ControllerEvent): Unit
}

/**
 * 事件队列上的事件对象
 * QueuedEvent 使用它的唯一目的，是确保 Expire 事件在建立 ZooKeeper 会话前被处理。
 * @param event  ControllerEvent类，表示Controller事件
 * @param enqueueTimeMs 表示Controller事件被放入到事件队列的时间戳
 */
class QueuedEvent(val event: ControllerEvent,
                  val enqueueTimeMs: Long) {
  // 标识事件是否开始被处理
  val processingStarted = new CountDownLatch(1)
  // 标识事件是否被处理过
  val spent = new AtomicBoolean(false)
  // 处理事件
  def process(processor: ControllerEventProcessor): Unit = {
    // 这里是设置为true，返回false（修改前的值），才能通过，如果以前值就是true，说明事件对象已经处理过了。
    if (spent.getAndSet(true))
      return
    processingStarted.countDown()
    // 这里的processor是ControllerEventProcessor，它只有一个实现类KafkaController
    processor.process(event)
  }
  // 抢占式处理事件
  def preempt(processor: ControllerEventProcessor): Unit = {
    if (spent.getAndSet(true))
      return
    processor.preempt(event)
  }
  // 阻塞等待事件被处理完成
  def awaitProcessing(): Unit = {
    processingStarted.await()
  }

  override def toString: String = {
    s"QueuedEvent(event=$event, enqueueTimeMs=$enqueueTimeMs)"
  }
}

/**
 * 主要用于创建和管理事件处理线程和事件队列。这个类中定义了重要的 ControllerEventThread 线程类
 * @param controllerId
 * @param processor
 * @param time
 * @param rateAndTimeMetrics
 * @param eventQueueTimeTimeoutMs
 */
class ControllerEventManager(controllerId: Int,
                             processor: ControllerEventProcessor,
                             time: Time,
                             rateAndTimeMetrics: Map[ControllerState, KafkaTimer],
                             eventQueueTimeTimeoutMs: Long = 300000) extends KafkaMetricsGroup {
  import ControllerEventManager._

  @volatile private var _state: ControllerState = ControllerState.Idle
  private val putLock = new ReentrantLock()
  // 事件队列
  private val queue = new LinkedBlockingQueue[QueuedEvent]
  // Visible for test
  // ControllerEventManager 类一开始就启动了一个 ControllerEventThread 线程，用于处理 controller 事件
  private[controller] var thread = new ControllerEventThread(ControllerEventThreadName)

  private val eventQueueTimeHist = newHistogram(EventQueueTimeMetricName)

  newGauge(EventQueueSizeMetricName, () => queue.size)

  def state: ControllerState = _state

  def start(): Unit = thread.start()

  def close(): Unit = {
    try {
      thread.initiateShutdown()
      clearAndPut(ShutdownEventThread)
      thread.awaitShutdown()
    } finally {
      removeMetric(EventQueueTimeMetricName)
      removeMetric(EventQueueSizeMetricName)
    }
  }

  /**
   * 把指定 ControllerEvent 插入到事件队列
   * @param event
   * @return
   */
  def put(event: ControllerEvent): QueuedEvent = inLock(putLock) {
    // 构建QueuedEvent实例
    val queuedEvent = new QueuedEvent(event, time.milliseconds())
    // 插入到事件队列
    queue.put(queuedEvent)
    // 返回新建QueuedEvent实例
    queuedEvent
  }

  /**
   * 先执行具有高优先级的抢占式事件，再插入指定的事件
   * @param event
   * @return
   */
  def clearAndPut(event: ControllerEvent): QueuedEvent = inLock(putLock){
    val preemptedEvents = new ArrayList[QueuedEvent]()
    queue.drainTo(preemptedEvents)
    // 优先处理抢占式事件
    preemptedEvents.forEach(_.preempt(processor))
    // 调用上面的put方法将给定事件插入到事件队列
    put(event)
  }

  def isEmpty: Boolean = queue.isEmpty

  class ControllerEventThread(name: String) extends ShutdownableThread(name = name, isInterruptible = false) {
    logIdent = s"[ControllerEventThread controllerId=$controllerId] "

    override def doWork(): Unit = {
      // 从事件队列中获取待处理的Controller事件，否则等待
      val dequeued = pollFromEventQueue()
      dequeued.event match {
        // 如果是关闭线程事件，什么都不用做。关闭线程由外部来执行
        case ShutdownEventThread => // The shutting down of the thread has been initiated at this point. Ignore this event.
        case controllerEvent =>
          _state = controllerEvent.state
          // 更新对应事件在队列中保存的时间
          eventQueueTimeHist.update(time.milliseconds() - dequeued.enqueueTimeMs)

          try {
            // 这里的 processor 是 ControllerEventManager中processor（ControllerEventProcessor），ControllerEventThread是ControllerEventManager的内部类
            def process(): Unit = dequeued.process(processor)
            // 处理事件，同时计算处理速率
            rateAndTimeMetrics.get(state) match {
              case Some(timer) => timer.time { process() }
              case None => process()
            }
          } catch {
            case e: Throwable => error(s"Uncaught error processing event $controllerEvent", e)
          }

          _state = ControllerState.Idle
      }
    }
  }

  private def pollFromEventQueue(): QueuedEvent = {
    val count = eventQueueTimeHist.count()
    if (count != 0) {
      val event  = queue.poll(eventQueueTimeTimeoutMs, TimeUnit.MILLISECONDS)
      if (event == null) {
        eventQueueTimeHist.clear()
        // 如果没有获取到事件，则调用take方法，一直阻塞等待队列中的事件
        queue.take()
      } else {
        event
      }
    } else {
      queue.take()
    }
  }

}
