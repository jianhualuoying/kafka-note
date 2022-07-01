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

package kafka.network

import java.io.IOException
import java.net._
import java.nio.ByteBuffer
import java.nio.channels._
import java.nio.channels.{Selector => NSelector}
import java.util
import java.util.Optional
import java.util.concurrent._
import java.util.concurrent.atomic._

import kafka.cluster.{BrokerEndPoint, EndPoint}
import kafka.metrics.KafkaMetricsGroup
import kafka.network.RequestChannel.{CloseConnectionResponse, EndThrottlingResponse, NoOpResponse, SendResponse, StartThrottlingResponse}
import kafka.network.Processor._
import kafka.network.SocketServer._
import kafka.security.CredentialProvider
import kafka.server.{BrokerReconfigurable, KafkaConfig}
import kafka.utils._
import kafka.utils.Implicits._
import org.apache.kafka.common.config.ConfigException
import org.apache.kafka.common.errors.InvalidRequestException
import org.apache.kafka.common.{Endpoint, KafkaException, MetricName, Reconfigurable}
import org.apache.kafka.common.memory.{MemoryPool, SimpleMemoryPool}
import org.apache.kafka.common.metrics._
import org.apache.kafka.common.metrics.stats.{Avg, CumulativeSum, Meter, Rate}
import org.apache.kafka.common.network.ClientInformation
import org.apache.kafka.common.network.KafkaChannel.ChannelMuteEvent
import org.apache.kafka.common.network.{ChannelBuilder, ChannelBuilders, KafkaChannel, ListenerName, ListenerReconfigurable, Selectable, Send, Selector => KSelector}
import org.apache.kafka.common.protocol.ApiKeys
import org.apache.kafka.common.requests.ApiVersionsRequest
import org.apache.kafka.common.requests.{RequestContext, RequestHeader}
import org.apache.kafka.common.security.auth.SecurityProtocol
import org.apache.kafka.common.utils.{KafkaThread, LogContext, Time}
import org.slf4j.event.Level

import scala.collection._
import scala.jdk.CollectionConverters._
import scala.collection.mutable.{ArrayBuffer, Buffer}
import scala.util.control.ControlThrowable

/**
 * Handles new connections, requests and responses to and from broker.
 * Kafka supports two types of request planes :
 *  - data-plane :
 *    - Handles requests from clients and other brokers in the cluster.
 *    - The threading model is
 *      1 Acceptor thread per listener, that handles new connections.
 *      It is possible to configure multiple data-planes by specifying multiple "," separated endpoints for "listeners" in KafkaConfig.
 *      Acceptor has N Processor threads that each have their own selector and read requests from sockets
 *      M Handler threads that handle requests and produce responses back to the processor threads for writing.
 *  - control-plane :
 *    - Handles requests from controller. This is optional and can be configured by specifying "control.plane.listener.name".
 *      If not configured, the controller requests are handled by the data-plane.
 *    - The threading model is
 *      1 Acceptor thread that handles new connections
 *      Acceptor has 1 Processor thread that has its own selector and read requests from the socket.
 *      1 Handler thread that handles requests and produce responses back to the processor thread for writing.
 */
class SocketServer(val config: KafkaConfig,
                   val metrics: Metrics,
                   val time: Time,
                   val credentialProvider: CredentialProvider)
// SocketServer实现BrokerReconfigurable trait表明SocketServer的一些参数配置是允许动态修改的，即在Broker不停机的情况下修改它们
  extends Logging with KafkaMetricsGroup with BrokerReconfigurable {
  // SocketServer的请求队列长度，由Broker端参数queued.max.requests值而定，默认值是500
  private val maxQueuedRequests = config.queuedMaxRequests

  private val logContext = new LogContext(s"[SocketServer brokerId=${config.brokerId}] ")
  this.logIdent = logContext.logPrefix

  private val memoryPoolSensor = metrics.sensor("MemoryPoolUtilization")
  private val memoryPoolDepletedPercentMetricName = metrics.metricName("MemoryPoolAvgDepletedPercent", MetricsGroup)
  private val memoryPoolDepletedTimeMetricName = metrics.metricName("MemoryPoolDepletedTimeTotal", MetricsGroup)
  memoryPoolSensor.add(new Meter(TimeUnit.MILLISECONDS, memoryPoolDepletedPercentMetricName, memoryPoolDepletedTimeMetricName))
  private val memoryPool = if (config.queuedMaxBytes > 0) new SimpleMemoryPool(config.queuedMaxBytes, config.socketRequestMaxBytes, false, memoryPoolSensor) else MemoryPool.NONE
  // data-plane
  // Processor 线程池：即网络线程池，负责将请求高速地放入到请求队列中。
  private val dataPlaneProcessors = new ConcurrentHashMap[Int, Processor]()
  // 处理数据类请求的Acceptor线程池，每套监听器对应一个Acceptor线程
  // 保存了 SocketServer 为每个监听器EndPoint定义的 Acceptor 线程，此线程负责分发该监听器上的入站连接建立请求。
  private[network] val dataPlaneAcceptors = new ConcurrentHashMap[EndPoint, Acceptor]()
  // 处理数据类请求专属的RequestChannel对象，承载请求队列的请求处理通道。
  val dataPlaneRequestChannel = new RequestChannel(maxQueuedRequests, DataPlaneMetricPrefix, time)
  // control-plane

  // Control plane 的配套资源只有 1 个 Acceptor 线程 + 1 个 Processor 线程 + 1 个深度是 20 的请求队列而已。
  // 一旦你开启了 Control plane 设置，其 Processor 线程就只有 1 个，Acceptor 线程也是 1 个。另外，你要注意，它对应的 RequestChannel 里面的请求队列长度被硬编码成了 20，而不是一个可配置的值。
  // 这揭示了社区在这里所做的一个假设：即控制类请求的数量应该远远小于数据类请求，因而不需要为它创建线程池和较深的请求队列。

  // 用于处理控制类请求的Processor线程
  // 注意：目前定义了专属的Processor线程而非线程池处理控制类请求
  private var controlPlaneProcessorOpt : Option[Processor] = None
  // 用于处理控制类请求的Acceptor线程
  private[network] var controlPlaneAcceptorOpt : Option[Acceptor] = None
  // 处理控制类请求专属的RequestChannel对象
  val controlPlaneRequestChannelOpt: Option[RequestChannel] = config.controlPlaneListenerName.map(_ =>
    new RequestChannel(20, ControlPlaneMetricPrefix, time))
  private var nextProcessorId = 0
  private var connectionQuotas: ConnectionQuotas = _
  private var startedProcessingRequests = false
  private var stoppedProcessingRequests = false

  /**
   * Starts the socket server and creates all the Acceptors and the Processors. The Acceptors
   * start listening at this stage so that the bound port is known when this method completes
   * even when ephemeral ports are used. Acceptors and Processors are started if `startProcessingRequests`
   * is true. If not, acceptors and processors are only started when [[kafka.network.SocketServer#startProcessingRequests()]]
   * is invoked. Delayed starting of acceptors and processors is used to delay processing client
   * connections until server is fully initialized, e.g. to ensure that all credentials have been
   * loaded before authentications are performed. Incoming connections on this server are processed
   * when processors start up and invoke [[org.apache.kafka.common.network.Selector#poll]].
   *
   * @param startProcessingRequests Flag indicating whether `Processor`s must be started.
   */
  def startup(startProcessingRequests: Boolean = true): Unit = {
    this.synchronized {
      connectionQuotas = new ConnectionQuotas(config, time, metrics)
      // 调用config#controlPlaneListener方法，获取Control plane 设置的专属监听器
      createControlPlaneAcceptorAndProcessor(config.controlPlaneListener)
      createDataPlaneAcceptorsAndProcessors(config.numNetworkThreads, config.dataPlaneListeners)
      if (startProcessingRequests) {
        this.startProcessingRequests()
      }
    }

    newGauge(s"${DataPlaneMetricPrefix}NetworkProcessorAvgIdlePercent", () => SocketServer.this.synchronized {
      val ioWaitRatioMetricNames = dataPlaneProcessors.values.asScala.iterator.map { p =>
        metrics.metricName("io-wait-ratio", MetricsGroup, p.metricTags)
      }
      ioWaitRatioMetricNames.map { metricName =>
        Option(metrics.metric(metricName)).fold(0.0)(m => Math.min(m.metricValue.asInstanceOf[Double], 1.0))
      }.sum / dataPlaneProcessors.size
    })
    newGauge(s"${ControlPlaneMetricPrefix}NetworkProcessorAvgIdlePercent", () => SocketServer.this.synchronized {
      val ioWaitRatioMetricName = controlPlaneProcessorOpt.map { p =>
        metrics.metricName("io-wait-ratio", "socket-server-metrics", p.metricTags)
      }
      ioWaitRatioMetricName.map { metricName =>
        Option(metrics.metric(metricName)).fold(0.0)(m => Math.min(m.metricValue.asInstanceOf[Double], 1.0))
      }.getOrElse(Double.NaN)
    })
    newGauge("MemoryPoolAvailable", () => memoryPool.availableMemory)
    newGauge("MemoryPoolUsed", () => memoryPool.size() - memoryPool.availableMemory)
    newGauge(s"${DataPlaneMetricPrefix}ExpiredConnectionsKilledCount", () => SocketServer.this.synchronized {
      val expiredConnectionsKilledCountMetricNames = dataPlaneProcessors.values.asScala.iterator.map { p =>
        metrics.metricName("expired-connections-killed-count", "socket-server-metrics", p.metricTags)
      }
      expiredConnectionsKilledCountMetricNames.map { metricName =>
        Option(metrics.metric(metricName)).fold(0.0)(m => m.metricValue.asInstanceOf[Double])
      }.sum
    })
    newGauge(s"${ControlPlaneMetricPrefix}ExpiredConnectionsKilledCount", () => SocketServer.this.synchronized {
      val expiredConnectionsKilledCountMetricNames = controlPlaneProcessorOpt.map { p =>
        metrics.metricName("expired-connections-killed-count", "socket-server-metrics", p.metricTags)
      }
      expiredConnectionsKilledCountMetricNames.map { metricName =>
        Option(metrics.metric(metricName)).fold(0.0)(m => m.metricValue.asInstanceOf[Double])
      }.getOrElse(0.0)
    })
  }

  /**
   * Start processing requests and new connections. This method is used for delayed starting of
   * all the acceptors and processors if [[kafka.network.SocketServer#startup]] was invoked with
   * `startProcessingRequests=false`.
   *
   * Before starting processors for each endpoint, we ensure that authorizer has all the metadata
   * to authorize requests on that endpoint by waiting on the provided future. We start inter-broker
   * listener before other listeners. This allows authorization metadata for other listeners to be
   * stored in Kafka topics in this cluster.
   *
   * @param authorizerFutures Future per [[EndPoint]] used to wait before starting the processor
   *                          corresponding to the [[EndPoint]]
   */
  def startProcessingRequests(authorizerFutures: Map[Endpoint, CompletableFuture[Void]] = Map.empty): Unit = {
    info("Starting socket server acceptors and processors")
    this.synchronized {
      if (!startedProcessingRequests) {
        // 启动处理控制类请求的Processor和Acceptor线程
        startControlPlaneProcessorAndAcceptor(authorizerFutures)
        // 启动处理数据类请求的Processor和Acceptor线程
        startDataPlaneProcessorsAndAcceptors(authorizerFutures)
        startedProcessingRequests = true
      } else {
        info("Socket server acceptors and processors already started")
      }
    }
    info("Started socket server acceptors and processors")
  }

  /**
   * Starts processors of the provided acceptor and the acceptor itself.
   *
   * Before starting them, we ensure that authorizer has all the metadata to authorize
   * requests on that endpoint by waiting on the provided future.
   */
  private def startAcceptorAndProcessors(threadPrefix: String,
                                         endpoint: EndPoint,
                                         acceptor: Acceptor,
                                         authorizerFutures: Map[Endpoint, CompletableFuture[Void]] = Map.empty): Unit = {
    debug(s"Wait for authorizer to complete start up on listener ${endpoint.listenerName}")
    waitForAuthorizerFuture(acceptor, authorizerFutures)
    debug(s"Start processors on listener ${endpoint.listenerName}")
    // 启动Processors中所有的Processor线程
    acceptor.startProcessors(threadPrefix)
    debug(s"Start acceptor thread on listener ${endpoint.listenerName}")
    // 获取闭锁是否等于0 TODO 关注闭锁用法
    if (!acceptor.isStarted()) {
      KafkaThread.nonDaemon(
        s"${threadPrefix}-kafka-socket-acceptor-${endpoint.listenerName}-${endpoint.securityProtocol}-${endpoint.port}",
        acceptor
      ).start()
      // 调用闭锁的await方法，等待线程启动？TODO 这里不明白，主要是Processor线程和Acceptor线程的run方法调用了kafka.network.AbstractServerThread#startupComplete方法，方法中调用了启动闭锁的countDown方法，而且闭锁数量为1
      acceptor.awaitStartup()
    }
    info(s"Started $threadPrefix acceptor and processor(s) for endpoint : ${endpoint.listenerName}")
  }

  /**
   * Starts processors of all the data-plane acceptors and all the acceptors of this server.
   *
   * We start inter-broker listener before other listeners. This allows authorization metadata for
   * other listeners to be stored in Kafka topics in this cluster.
   */
  private def startDataPlaneProcessorsAndAcceptors(authorizerFutures: Map[Endpoint, CompletableFuture[Void]]): Unit = {
    // 获取Broker间通讯所用的监听器，默认是PLAINTEXT
    val interBrokerListener = dataPlaneAcceptors.asScala.keySet
      .find(_.listenerName == config.interBrokerListenerName)
      .getOrElse(throw new IllegalStateException(s"Inter-broker listener ${config.interBrokerListenerName} not found, endpoints=${dataPlaneAcceptors.keySet}"))
    val orderedAcceptors = List(dataPlaneAcceptors.get(interBrokerListener)) ++
      dataPlaneAcceptors.asScala.filter { case (k, _) => k != interBrokerListener }.values
    orderedAcceptors.foreach { acceptor =>
      val endpoint = acceptor.endPoint
      // 启动Processor和Acceptor线程
      startAcceptorAndProcessors(DataPlaneThreadPrefix, endpoint, acceptor, authorizerFutures)
    }
  }

  /**
   * Start the processor of control-plane acceptor and the acceptor of this server.
   */
  private def startControlPlaneProcessorAndAcceptor(authorizerFutures: Map[Endpoint, CompletableFuture[Void]]): Unit = {
    controlPlaneAcceptorOpt.foreach { controlPlaneAcceptor =>
      val endpoint = config.controlPlaneListener.get
      startAcceptorAndProcessors(ControlPlaneThreadPrefix, endpoint, controlPlaneAcceptor, authorizerFutures)
    }
  }

  private def endpoints = config.listeners.map(l => l.listenerName -> l).toMap

  private def createDataPlaneAcceptorsAndProcessors(dataProcessorsPerListener: Int,
                                                    endpoints: Seq[EndPoint]): Unit = {
    // 遍历监听器集合
    endpoints.foreach { endpoint =>
      // 将监听器纳入到连接配额管理之下
      connectionQuotas.addListener(config, endpoint.listenerName)
      // 为监听器创建对应的Acceptor线程
      val dataPlaneAcceptor = createAcceptor(endpoint, DataPlaneMetricPrefix)
      // 为监听器创建多个Processor线程。具体数目由num.network.threads决定
      addDataPlaneProcessors(dataPlaneAcceptor, endpoint, dataProcessorsPerListener)
      // 将<监听器，Acceptor线程>对保存起来统一管理
      dataPlaneAcceptors.put(endpoint, dataPlaneAcceptor)
      info(s"Created data-plane acceptor and processors for endpoint : ${endpoint.listenerName}")
    }
  }

  private def createControlPlaneAcceptorAndProcessor(endpointOpt: Option[EndPoint]): Unit = {
    // 如果为Control plane配置了监听器
    endpointOpt.foreach { endpoint =>
      // 将监听器纳入到连接配额管理之下
      connectionQuotas.addListener(config, endpoint.listenerName)
      // 为监听器创建对应的Acceptor线程
      val controlPlaneAcceptor = createAcceptor(endpoint, ControlPlaneMetricPrefix)
      // 为监听器创建对应的Processor线程
      val controlPlaneProcessor = newProcessor(nextProcessorId, controlPlaneRequestChannelOpt.get, connectionQuotas, endpoint.listenerName, endpoint.securityProtocol, memoryPool)
      controlPlaneAcceptorOpt = Some(controlPlaneAcceptor)
      controlPlaneProcessorOpt = Some(controlPlaneProcessor)
      val listenerProcessors = new ArrayBuffer[Processor]()
      listenerProcessors += controlPlaneProcessor
      // 将Processor线程添加到控制类请求专属RequestChannel中
      // 即添加到RequestChannel实例保存的Processor线程池中
      // TODO 这里的foreach是干嘛用的，是因为controlPlaneRequestChannelOpt 是 Option类型，所以可以用foreach判空？
      controlPlaneRequestChannelOpt.foreach(_.addProcessor(controlPlaneProcessor))
      nextProcessorId += 1
      // 把Processor对象也添加到Acceptor线程管理的Processor线程池中
      controlPlaneAcceptor.addProcessors(listenerProcessors, ControlPlaneThreadPrefix)
      info(s"Created control-plane acceptor and processor for endpoint : ${endpoint.listenerName}")
    }
  }

  private def createAcceptor(endPoint: EndPoint, metricPrefix: String) : Acceptor = {
    val sendBufferSize = config.socketSendBufferBytes
    val recvBufferSize = config.socketReceiveBufferBytes
    val brokerId = config.brokerId
    new Acceptor(endPoint, sendBufferSize, recvBufferSize, brokerId, connectionQuotas, metricPrefix)
  }

  private def addDataPlaneProcessors(acceptor: Acceptor, endpoint: EndPoint, newProcessorsPerListener: Int): Unit = {
    val listenerName = endpoint.listenerName
    val securityProtocol = endpoint.securityProtocol
    val listenerProcessors = new ArrayBuffer[Processor]()
    for (_ <- 0 until newProcessorsPerListener) {
      // nextProcessorId，dataPlaneRequestChannel，memoryPool在类初始化时初始化，connectionQuotas在kafka.network.SocketServer#startup方法中初始化
      val processor = newProcessor(nextProcessorId, dataPlaneRequestChannel, connectionQuotas, listenerName, securityProtocol, memoryPool)
      listenerProcessors += processor
      // dataPlaneRequestChannel和dataPlaneProcessors是一致的，只不过一个存在SocketServer中，一个存在RequestChannel中。而且Processor线程池应该放到Acceptor线程中管理
      // 所以在Acceptor线程中也是一致的
      // 这几个Processor线程池底层都是引用的相同的Processor对象，所以修改其中一个元素，会影响到其他几个线程池。但是线程池是不同的对象
      dataPlaneRequestChannel.addProcessor(processor)
      nextProcessorId += 1
    }
    listenerProcessors.foreach(p => dataPlaneProcessors.put(p.id, p))
    acceptor.addProcessors(listenerProcessors, DataPlaneThreadPrefix)
  }

  /**
   * Stop processing requests and new connections.
   */
  def stopProcessingRequests(): Unit = {
    info("Stopping socket server request processors")
    this.synchronized {
      dataPlaneAcceptors.asScala.values.foreach(_.initiateShutdown())
      dataPlaneAcceptors.asScala.values.foreach(_.awaitShutdown())
      controlPlaneAcceptorOpt.foreach(_.initiateShutdown())
      controlPlaneAcceptorOpt.foreach(_.awaitShutdown())
      dataPlaneRequestChannel.clear()
      controlPlaneRequestChannelOpt.foreach(_.clear())
      stoppedProcessingRequests = true
    }
    info("Stopped socket server request processors")
  }

  def resizeThreadPool(oldNumNetworkThreads: Int, newNumNetworkThreads: Int): Unit = synchronized {
    info(s"Resizing network thread pool size for each data-plane listener from $oldNumNetworkThreads to $newNumNetworkThreads")
    if (newNumNetworkThreads > oldNumNetworkThreads) {
      dataPlaneAcceptors.forEach { (endpoint, acceptor) =>
        addDataPlaneProcessors(acceptor, endpoint, newNumNetworkThreads - oldNumNetworkThreads)
      }
    } else if (newNumNetworkThreads < oldNumNetworkThreads)
      dataPlaneAcceptors.asScala.values.foreach(_.removeProcessors(oldNumNetworkThreads - newNumNetworkThreads, dataPlaneRequestChannel))
  }

  /**
   * Shutdown the socket server. If still processing requests, shutdown
   * acceptors and processors first.
   */
  def shutdown(): Unit = {
    info("Shutting down socket server")
    this.synchronized {
      if (!stoppedProcessingRequests)
        stopProcessingRequests()
      dataPlaneRequestChannel.shutdown()
      controlPlaneRequestChannelOpt.foreach(_.shutdown())
      connectionQuotas.close()
    }
    info("Shutdown completed")
  }

  def boundPort(listenerName: ListenerName): Int = {
    try {
      val acceptor = dataPlaneAcceptors.get(endpoints(listenerName))
      if (acceptor != null) {
        acceptor.serverChannel.socket.getLocalPort
      } else {
        controlPlaneAcceptorOpt.map (_.serverChannel.socket().getLocalPort).getOrElse(throw new KafkaException("Could not find listenerName : " + listenerName + " in data-plane or control-plane"))
      }
    } catch {
      case e: Exception =>
        throw new KafkaException("Tried to check server's port before server was started or checked for port of non-existing protocol", e)
    }
  }

  def addListeners(listenersAdded: Seq[EndPoint]): Unit = synchronized {
    info(s"Adding data-plane listeners for endpoints $listenersAdded")
    createDataPlaneAcceptorsAndProcessors(config.numNetworkThreads, listenersAdded)
    listenersAdded.foreach { endpoint =>
      val acceptor = dataPlaneAcceptors.get(endpoint)
      startAcceptorAndProcessors(DataPlaneThreadPrefix, endpoint, acceptor)
    }
  }

  def removeListeners(listenersRemoved: Seq[EndPoint]): Unit = synchronized {
    info(s"Removing data-plane listeners for endpoints $listenersRemoved")
    listenersRemoved.foreach { endpoint =>
      connectionQuotas.removeListener(config, endpoint.listenerName)
      dataPlaneAcceptors.asScala.remove(endpoint).foreach { acceptor =>
        acceptor.initiateShutdown()
        acceptor.awaitShutdown()
      }
    }
  }

  override def reconfigurableConfigs: Set[String] = SocketServer.ReconfigurableConfigs

  override def validateReconfiguration(newConfig: KafkaConfig): Unit = {

  }

  override def reconfigure(oldConfig: KafkaConfig, newConfig: KafkaConfig): Unit = {
    val maxConnectionsPerIp = newConfig.maxConnectionsPerIp
    if (maxConnectionsPerIp != oldConfig.maxConnectionsPerIp) {
      info(s"Updating maxConnectionsPerIp: $maxConnectionsPerIp")
      connectionQuotas.updateMaxConnectionsPerIp(maxConnectionsPerIp)
    }
    val maxConnectionsPerIpOverrides = newConfig.maxConnectionsPerIpOverrides
    if (maxConnectionsPerIpOverrides != oldConfig.maxConnectionsPerIpOverrides) {
      info(s"Updating maxConnectionsPerIpOverrides: ${maxConnectionsPerIpOverrides.map { case (k, v) => s"$k=$v" }.mkString(",")}")
      connectionQuotas.updateMaxConnectionsPerIpOverride(maxConnectionsPerIpOverrides)
    }
    val maxConnections = newConfig.maxConnections
    if (maxConnections != oldConfig.maxConnections) {
      info(s"Updating broker-wide maxConnections: $maxConnections")
      connectionQuotas.updateBrokerMaxConnections(maxConnections)
    }
    val maxConnectionRate = newConfig.maxConnectionCreationRate
    if (maxConnectionRate != oldConfig.maxConnectionCreationRate) {
      info(s"Updating broker-wide maxConnectionCreationRate: $maxConnectionRate")
      connectionQuotas.updateBrokerMaxConnectionRate(maxConnectionRate)
    }
  }

  private def waitForAuthorizerFuture(acceptor: Acceptor,
                                      authorizerFutures: Map[Endpoint, CompletableFuture[Void]]): Unit = {
    //we can't rely on authorizerFutures.get() due to ephemeral ports. Get the future using listener name
    authorizerFutures.forKeyValue { (endpoint, future) =>
      if (endpoint.listenerName == Optional.of(acceptor.endPoint.listenerName.value))
        future.join()
    }
  }

  // `protected` for test usage
  protected[network] def newProcessor(id: Int, requestChannel: RequestChannel, connectionQuotas: ConnectionQuotas, listenerName: ListenerName,
                                      securityProtocol: SecurityProtocol, memoryPool: MemoryPool): Processor = {
    new Processor(id,
      time,
      config.socketRequestMaxBytes,
      requestChannel,
      connectionQuotas,
      config.connectionsMaxIdleMs,
      config.failedAuthenticationDelayMs,
      listenerName,
      securityProtocol,
      config,
      metrics,
      credentialProvider,
      memoryPool,
      logContext,
    )
  }

  // For test usage
  private[network] def connectionCount(address: InetAddress): Int =
    Option(connectionQuotas).fold(0)(_.get(address))

  // For test usage
  private[network] def dataPlaneProcessor(index: Int): Processor = dataPlaneProcessors.get(index)

}

object SocketServer {
  val MetricsGroup = "socket-server-metrics"
  val DataPlaneThreadPrefix = "data-plane"
  val ControlPlaneThreadPrefix = "control-plane"
  val DataPlaneMetricPrefix = ""
  val ControlPlaneMetricPrefix = "ControlPlane"
  // 以下属性是允许动态修改的
  val ReconfigurableConfigs = Set(
    //  max.connections.per.ip
    KafkaConfig.MaxConnectionsPerIpProp,
    // max.connections.per.ip.overrides
    KafkaConfig.MaxConnectionsPerIpOverridesProp,
    // max.connections
    KafkaConfig.MaxConnectionsProp,
    // max.connections.creation.rate
    KafkaConfig.MaxConnectionCreationRateProp)

  val ListenerReconfigurableConfigs = Set(KafkaConfig.MaxConnectionsProp, KafkaConfig.MaxConnectionCreationRateProp)
}

/**
 * A base class with some helper variables and methods
 */
private[kafka] abstract class AbstractServerThread(connectionQuotas: ConnectionQuotas) extends Runnable with Logging {

  private val startupLatch = new CountDownLatch(1)

  // `shutdown()` is invoked before `startupComplete` and `shutdownComplete` if an exception is thrown in the constructor
  // (e.g. if the address is already in use). We want `shutdown` to proceed in such cases, so we first assign an open
  // latch and then replace it in `startupComplete()`.
  @volatile private var shutdownLatch = new CountDownLatch(0)

  private val alive = new AtomicBoolean(true)

  def wakeup(): Unit

  /**
   * Initiates a graceful shutdown by signaling to stop
   */
  def initiateShutdown(): Unit = {
    if (alive.getAndSet(false))
      wakeup()
  }

  /**
   * Wait for the thread to completely shutdown
   */
  def awaitShutdown(): Unit = shutdownLatch.await

  /**
   * Returns true if the thread is completely started
   */
  def isStarted(): Boolean = startupLatch.getCount == 0

  /**
   * Wait for the thread to completely start up
   */
  def awaitStartup(): Unit = startupLatch.await

  /**
   * Record that the thread startup is complete
   */
  protected def startupComplete(): Unit = {
    // Replace the open latch with a closed one
    shutdownLatch = new CountDownLatch(1)
    startupLatch.countDown()
  }

  /**
   * Record that the thread shutdown is complete
   */
  protected def shutdownComplete(): Unit = shutdownLatch.countDown()

  /**
   * Is the server still running?
   */
  protected def isRunning: Boolean = alive.get

  /**
   * Close `channel` and decrement the connection count.
   */
  def close(listenerName: ListenerName, channel: SocketChannel): Unit = {
    if (channel != null) {
      debug(s"Closing connection from ${channel.socket.getRemoteSocketAddress()}")
      connectionQuotas.dec(listenerName, channel.socket.getInetAddress)
      CoreUtils.swallow(channel.socket().close(), this, Level.ERROR)
      CoreUtils.swallow(channel.close(), this, Level.ERROR)
    }
  }
}

/**
 * Thread that accepts and configures new connections. There is one of these per endpoint.
 * 接收和创建外部 TCP 连接的线程。每个 SocketServer 实例只会创建一个 Acceptor 线程。它的唯一目的就是创建连接，并将接收到的 Request 传递给下游的 Processor 线程处理。
 * 经典的 Reactor 模式有个 【Dispatcher】 的角色，接收外部请求并分发给下面的实际处理线程。在 Kafka 中，这个 【Dispatcher】 就是 Acceptor 线程。
 * @param endPoint 它就是你定义的 Kafka Broker 连接信息，比如PLAINTEXT://localhost:9092。Acceptor 需要用到 endPoint 包含的主机名和端口信息创建 Server Socket
 * @param sendBufferSize 它设置的是 SocketOptions 的 SO_SNDBUF，即用于设置出站（Outbound）网络 I/O 的底层缓冲区大小。该值默认是 Broker 端参数socket.send.buffer.bytes 的值，即 100KB
 * @param recvBufferSize 它设置的是 SocketOptions 的 SO_RCVBUF，即用于设置入站（Inbound）网络 I/O 的底层缓冲区大小。该值默认是 Broker 端参数socket.receive.buffer.bytes 的值，即 100KB。
 * @param brokerId
 * @param connectionQuotas
 * @param metricPrefix
 *
 * 除了上面类定义的字段，Acceptor 线程还有两个非常关键的自定义属性nioSelector和processors，自定义属性不出现在构造函数中。
 */
private[kafka] class Acceptor(val endPoint: EndPoint,
                              val sendBufferSize: Int,
                              val recvBufferSize: Int,
                              brokerId: Int,
                              connectionQuotas: ConnectionQuotas,
                              metricPrefix: String) extends AbstractServerThread(connectionQuotas) with KafkaMetricsGroup {
  // 1. 是 Java NIO 库的 Selector 对象实例，也是后续所有网络通信组件实现 Java NIO 机制的基础。
  private val nioSelector = NSelector.open()
  // 2. Broker端创建对应的ServerSocketChannel实例,然后将Channel注册到Selector对象上
  val serverChannel = openServerSocket(endPoint.host, endPoint.port)
  // 3. 创建网络 Processor 线程池，由此可以得出，Processor 线程是在 Acceptor 线程中管理和维护的。
  private val processors = new ArrayBuffer[Processor]()
  private val processorsStarted = new AtomicBoolean
  private val blockedPercentMeter = newMeter(s"${metricPrefix}AcceptorBlockedPercent",
    "blocked time", TimeUnit.NANOSECONDS, Map(ListenerMetricTag -> endPoint.listenerName.value))

  private[network] def addProcessors(newProcessors: Buffer[Processor], processorThreadPrefix: String): Unit = synchronized {
    processors ++= newProcessors // 添加一组新的Processor线程
    if (processorsStarted.get) // 如果Processor线程池已经启动
      startProcessors(newProcessors, processorThreadPrefix) // 启动新的Processor线程
  }

  private[network] def startProcessors(processorThreadPrefix: String): Unit = synchronized {
    // 如果Processor线程池未启动
    if (!processorsStarted.getAndSet(true)) {
      startProcessors(processors, processorThreadPrefix) // 启动给定的Processor线程
    }
  }

  private def startProcessors(processors: Seq[Processor], processorThreadPrefix: String): Unit = synchronized {
    // 依次创建并启动Processor线程
    // 线程命名规范：processor线程前缀-kafka-network-thread-broker序号-监听器名称-安全协议-Processor序号
    // 假设为序号为0的Broker设置PLAINTEXT://localhost:9092作为连接信息，那么3个Processor线程名称分别为：
    processors.foreach { processor =>
      KafkaThread.nonDaemon(
        s"${processorThreadPrefix}-kafka-network-thread-$brokerId-${endPoint.listenerName}-${endPoint.securityProtocol}-${processor.id}",
        processor
      ).start()
    }
  }

  private[network] def removeProcessors(removeCount: Int, requestChannel: RequestChannel): Unit = synchronized {
    // Shutdown `removeCount` processors. Remove them from the processor list first so that no more
    // connections are assigned. Shutdown the removed processors, closing the selector and its connections.
    // The processors are then removed from `requestChannel` and any pending responses to these processors are dropped.
    // 获取Processor线程池中最后removeCount个线程
    val toRemove = processors.takeRight(removeCount)
    // 移除最后removeCount个线程
    processors.remove(processors.size - removeCount, removeCount)
    // 关闭最后removeCount个线程
    toRemove.foreach(_.initiateShutdown())
    toRemove.foreach(_.awaitShutdown())
    // 在RequestChannel中移除这些Processor
    toRemove.foreach(processor => requestChannel.removeProcessor(processor.id))
  }

  override def initiateShutdown(): Unit = {
    super.initiateShutdown()
    synchronized {
      processors.foreach(_.initiateShutdown())
    }
  }

  override def awaitShutdown(): Unit = {
    super.awaitShutdown()
    synchronized {
      processors.foreach(_.awaitShutdown())
    }
  }

  /**
   * Accept loop that checks for new connection attempts
   * 它是处理 Reactor 模式中分发逻辑的主要实现方法。
   */
  def run(): Unit = {
    //注册OP_ACCEPT事件
    serverChannel.register(nioSelector, SelectionKey.OP_ACCEPT)
    // 等待Acceptor线程启动完成
    startupComplete()
    try {
      // 当前使用的Processor序号，从0开始，最大值是num.network.threads - 1
      var currentProcessorIndex = 0
      while (isRunning) {
        try {
          // 每500毫秒获取一次就绪I/O事件
          val ready = nioSelector.select(500)
          // 如果有I/O事件准备就绪
          if (ready > 0) {
            val keys = nioSelector.selectedKeys()
            val iter = keys.iterator()
            while (iter.hasNext && isRunning) {
              try {
                val key = iter.next
                // 从nioSelector中，移除已经处理过的事件
                iter.remove()

                if (key.isAcceptable) {
                  // 调用accept方法创建Socket连接 socketChannel
                  accept(key).foreach { socketChannel =>
                    // Assign the channel to the next processor (using round-robin) to which the
                    // channel can be added without blocking. If newConnections queue is full on
                    // all processors, block until the last one is able to accept a connection.
                    var retriesLeft = synchronized(processors.length)
                    var processor: Processor = null
                    do {
                      retriesLeft -= 1
                      // 指定由哪个Processor线程进行处理
                      processor = synchronized {
                        // adjust the index (if necessary) and retrieve the processor atomically for
                        // correct behaviour in case the number of processors is reduced dynamically
                        currentProcessorIndex = currentProcessorIndex % processors.length
                        processors(currentProcessorIndex)
                      }
                      // 更新Processor线程序号
                      currentProcessorIndex += 1
                      // 将新Socket连接加入到Processor线程待处理连接队列
                      // 等待Processor线程后续处理，可以看到，这里仅仅只是放到队列，至于后续的socketChannel注册读写事件，
                      // 分发读写请求以及请求处理完的响应都是Processor线程的逻辑，放到了Processor#run方法中。
                      // TODO processors线程池中的元素何时初始化，初始化后，线程就不释放了吗？
                    } while (!assignNewConnection(socketChannel, processor, retriesLeft == 0))
                  }
                } else
                  throw new IllegalStateException("Unrecognized key state for acceptor thread.")
              } catch {
                case e: Throwable => error("Error while accepting connection", e)
              }
            }
          }
        }
        catch {
          // We catch all the throwables to prevent the acceptor thread from exiting on exceptions due
          // to a select operation on a specific channel or a bad request. We don't want
          // the broker to stop responding to requests from other clients in these scenarios.
          case e: ControlThrowable => throw e
          case e: Throwable => error("Error occurred", e)
        }
      }
    } finally {
      // 执行各种资源关闭逻辑
      debug("Closing server socket and selector.")
      CoreUtils.swallow(serverChannel.close(), this, Level.ERROR)
      CoreUtils.swallow(nioSelector.close(), this, Level.ERROR)
      shutdownComplete()
    }
  }

  /**
  * Create a server socket to listen for connections on.
  */
  private def openServerSocket(host: String, port: Int): ServerSocketChannel = {
    val socketAddress =
      if (host == null || host.trim.isEmpty)
        new InetSocketAddress(port)
      else
        new InetSocketAddress(host, port)
    val serverChannel = ServerSocketChannel.open()
    serverChannel.configureBlocking(false)
    if (recvBufferSize != Selectable.USE_DEFAULT_BUFFER_SIZE)
      serverChannel.socket().setReceiveBufferSize(recvBufferSize)

    try {
      serverChannel.socket.bind(socketAddress)
      info(s"Awaiting socket connections on ${socketAddress.getHostString}:${serverChannel.socket.getLocalPort}.")
    } catch {
      case e: SocketException =>
        throw new KafkaException(s"Socket server failed to bind to ${socketAddress.getHostString}:$port: ${e.getMessage}.", e)
    }
    serverChannel
  }

  /**
   * Accept a new connection
   */
  private def accept(key: SelectionKey): Option[SocketChannel] = {
    // 创建 SocketChannel 对象
    val serverSocketChannel = key.channel().asInstanceOf[ServerSocketChannel]
    val socketChannel = serverSocketChannel.accept()
    try {
      // 增加对应 IP 上的连接数，如果连接数超过阈值，则抛 TooManyConnectionsException 异常
      connectionQuotas.inc(endPoint.listenerName, socketChannel.socket.getInetAddress, blockedPercentMeter)
      // 配置 SocketChannel 对象，非阻塞模式
      socketChannel.configureBlocking(false)
      socketChannel.socket().setTcpNoDelay(true)
      socketChannel.socket().setKeepAlive(true)
      if (sendBufferSize != Selectable.USE_DEFAULT_BUFFER_SIZE)
        socketChannel.socket().setSendBufferSize(sendBufferSize)
      // 返回 SocketChannel 交给 Processor 进行处理
      Some(socketChannel)
    } catch {
      case e: TooManyConnectionsException =>
      info(s"Rejected connection from ${e.ip}, address already has the configured maximum of ${e.count} connections.")
        // 连接数过多，关闭当前通道上的连接，并将连接计数减 1
        close(endPoint.listenerName, socketChannel)
        None
    }
  }

  private def assignNewConnection(socketChannel: SocketChannel, processor: Processor, mayBlock: Boolean): Boolean = {
    if (processor.accept(socketChannel, mayBlock, blockedPercentMeter)) {
      debug(s"Accepted connection from ${socketChannel.socket.getRemoteSocketAddress} on" +
        s" ${socketChannel.socket.getLocalSocketAddress} and assigned it to processor ${processor.id}," +
        s" sendBufferSize [actual|requested]: [${socketChannel.socket.getSendBufferSize}|$sendBufferSize]" +
        s" recvBufferSize [actual|requested]: [${socketChannel.socket.getReceiveBufferSize}|$recvBufferSize]")
      true
    } else
      false
  }

  /**
   * Wakeup the thread for selection.
   */
  @Override
  def wakeup(): Unit = nioSelector.wakeup()

}

private[kafka] object Processor {
  val IdlePercentMetricName = "IdlePercent"
  val NetworkProcessorMetricTag = "networkProcessor"
  val ListenerMetricTag = "listener"

  val ConnectionQueueSize = 20
}

/**
 * Thread that processes all requests from a single connection. There are N of these running in parallel
 * each of which has its own selector
 * 每个Processor线程都有自己的Selector和newConnections（新连接队列）
 * 从Acceptor里面创建的新连接，都会放入某一个process的连接队列，然后processor的selector对自己的连接队列不断注册事件并处理。收到的请求和处理后的返回，都会放到requestChannel列表里。
 * Processor线程有自己的响应队列responseQueue，但是请求队列是共享的：resquest共享这样才能实线程间的负载均衡，
 * response专属的是因为对应的request已经分配给对应的线程处理已，为了避免线程上下文上下文切换，理应也由这个线程处理响应，作为一个线程内部的变量更加合理
 * @param id
 * @param time
 * @param maxRequestSize
 * @param requestChannel
 * @param connectionQuotas
 * @param connectionsMaxIdleMs
 * @param failedAuthenticationDelayMs
 * @param listenerName
 * @param securityProtocol
 * @param config
 * @param metrics
 * @param credentialProvider
 * @param memoryPool
 * @param logContext
 * @param connectionQueueSize
 */
private[kafka] class Processor(val id: Int,
                               time: Time,
                               maxRequestSize: Int,
                               requestChannel: RequestChannel,
                               connectionQuotas: ConnectionQuotas,
                               connectionsMaxIdleMs: Long,
                               failedAuthenticationDelayMs: Int,
                               listenerName: ListenerName,
                               securityProtocol: SecurityProtocol,
                               config: KafkaConfig,
                               metrics: Metrics,
                               credentialProvider: CredentialProvider,
                               memoryPool: MemoryPool,
                               logContext: LogContext,
                               connectionQueueSize: Int = ConnectionQueueSize) extends AbstractServerThread(connectionQuotas) with KafkaMetricsGroup {

  private object ConnectionId {
    def fromString(s: String): Option[ConnectionId] = s.split("-") match {
      case Array(local, remote, index) => BrokerEndPoint.parseHostPort(local).flatMap { case (localHost, localPort) =>
        BrokerEndPoint.parseHostPort(remote).map { case (remoteHost, remotePort) =>
          ConnectionId(localHost, localPort, remoteHost, remotePort, Integer.parseInt(index))
        }
      }
      case _ => None
    }
  }

  private[network] case class ConnectionId(localHost: String, localPort: Int, remoteHost: String, remotePort: Int, index: Int) {
    override def toString: String = s"$localHost:$localPort-$remoteHost:$remotePort-$index"
  }
  // 保存的是要创建的新连接信息，就是 SocketChannel 对象。队列长度硬编码为20
  // 每当 Acceptor 线程接收新的连接请求时，都会通过Processor对象将对应的 SocketChannel 放入这个队列。后面在创建连接时（也就是调用 configureNewConnections 时），
  // 就从该队列中取出SocketChannel，然后注册新的连接。
  private val newConnections = new ArrayBlockingQueue[SocketChannel](connectionQueueSize)
  // 临时 Response 队列。当 Processor 线程将 Response 返还给Request 发送方之后，还要将 Response 放入这个临时队列。
  // 有些 Response 回调逻辑要在 Response 被发送回发送方之后，才能执行，因此需要暂存在一个临时队列里面。
  private val inflightResponses = mutable.Map[String, RequestChannel.Response]()
  // 每个 Processor 线程都会维护自己的 Response 队列
  private val responseQueue = new LinkedBlockingDeque[RequestChannel.Response]()

  private[kafka] val metricTags = mutable.LinkedHashMap(
    ListenerMetricTag -> listenerName.value,
    NetworkProcessorMetricTag -> id.toString
  ).asJava

  newGauge(IdlePercentMetricName, () => {
    Option(metrics.metric(metrics.metricName("io-wait-ratio", MetricsGroup, metricTags))).fold(0.0)(m =>
      Math.min(m.metricValue.asInstanceOf[Double], 1.0))
    },
    // for compatibility, only add a networkProcessor tag to the Yammer Metrics alias (the equivalent Selector metric
    // also includes the listener name)
    Map(NetworkProcessorMetricTag -> id.toString)
  )

  val expiredConnectionsKilledCount = new CumulativeSum()
  private val expiredConnectionsKilledCountMetricName = metrics.metricName("expired-connections-killed-count", "socket-server-metrics", metricTags)
  metrics.addMetric(expiredConnectionsKilledCountMetricName, expiredConnectionsKilledCount)
  // 创建kafka自定义的Selector： org.apache.kafka.common.network.Selector
  private val selector = createSelector(
    ChannelBuilders.serverChannelBuilder(listenerName,
      listenerName == config.interBrokerListenerName,
      securityProtocol,
      config,
      credentialProvider.credentialCache,
      credentialProvider.tokenCache,
      time,
      logContext))
  // Visible to override for testing
  protected[network] def createSelector(channelBuilder: ChannelBuilder): KSelector = {
    channelBuilder match {
      case reconfigurable: Reconfigurable => config.addReconfigurable(reconfigurable)
      case _ =>
    }
    new KSelector(
      maxRequestSize,
      connectionsMaxIdleMs,
      failedAuthenticationDelayMs,
      metrics,
      time,
      "socket-server",
      metricTags,
      false,
      true,
      channelBuilder,
      memoryPool,
      logContext)
  }

  // Connection ids have the format `localAddr:localPort-remoteAddr:remotePort-index`. The index is a
  // non-negative incrementing value that ensures that even if remotePort is reused after a connection is
  // closed, connection ids are not reused while requests from the closed connection are being processed.
  private var nextConnectionIndex = 0

  override def run(): Unit = {
    // 等待Processor线程启动完成
    startupComplete()
    try {
      while (isRunning) {
        try {
          // setup any new connections that have been queued up
          // 读取newConnections队列中的请求Request（其实没有真正读取，真正读取是在poll方法中），将newConnections里面的channel取出来注册到selector上。
          configureNewConnections()
          // register any new responses for writing
          // 发送IO处理线程存入队列responseQueue中的Response（其实没有真正发送，真正发送是在poll方法中），并将Response放入到inflightResponses临时队列
          processNewResponses()
          // 执行NIO poll，获取对应SocketChannel上准备就绪的I/O操作
          // 上面提到的所有读写逻辑都不是执行真正的读写，这里才是
          poll()
          // 将poll方法中接收到的Request（实际上是NetworkReceive，然后在该方法中转换成Request）放入Request队列
          processCompletedReceives()
          // 为临时Response队列（inflightResponses）中的Response执行回调逻辑
          processCompletedSends()
          // 处理因发送失败而导致的连接断开（把Response从inflightResponses中移除）
          processDisconnected()
          // 关闭超过配额限制部分的连接
          closeExcessConnections()
        } catch {
          // We catch all the throwables here to prevent the processor thread from exiting. We do this because
          // letting a processor exit might cause a bigger impact on the broker. This behavior might need to be
          // reviewed if we see an exception that needs the entire broker to stop. Usually the exceptions thrown would
          // be either associated with a specific socket channel or a bad request. These exceptions are caught and
          // processed by the individual methods above which close the failing channel and continue processing other
          // channels. So this catch block should only ever see ControlThrowables.
          case e: Throwable => processException("Processor got uncaught exception.", e)
        }
      }
    } finally {
      debug(s"Closing selector - processor $id")
      // 关闭底层资源
      CoreUtils.swallow(closeAll(), this, Level.ERROR)
      shutdownComplete()
    }
  }

  private[network] def processException(errorMessage: String, throwable: Throwable): Unit = {
    throwable match {
      case e: ControlThrowable => throw e
      case e => error(errorMessage, e)
    }
  }

  private def processChannelException(channelId: String, errorMessage: String, throwable: Throwable): Unit = {
    if (openOrClosingChannel(channelId).isDefined) {
      error(s"Closing socket for $channelId because of error", throwable)
      close(channelId)
    }
    processException(errorMessage, throwable)
  }

  private def processNewResponses(): Unit = {
    var currentResponse: RequestChannel.Response = null
    // 处理器还有响应要发送
    while ({currentResponse = dequeueResponse(); currentResponse != null}) { // Response队列中存在待处理Response
      val channelId = currentResponse.request.context.connectionId // 获取连接通道ID
      try {
        currentResponse match {
          case response: NoOpResponse => // 无需发送Response
            // There is no response to send to the client, we need to read more pipelined requests
            // that are sitting in the server's socket buffer
            updateRequestMetrics(response)
            trace(s"Socket server received empty response to send, registering for read: $response")
            // Try unmuting the channel. If there was no quota violation and the channel has not been throttled,
            // it will be unmuted immediately. If the channel has been throttled, it will be unmuted only if the
            // throttling delay has already passed by now.
            // 没有响应发送给客户端，需要读取更多请求，注册读事件
            handleChannelMuteEvent(channelId, ChannelMuteEvent.RESPONSE_SENT)
            tryUnmuteChannel(channelId)

          case response: SendResponse =>  // 发送Response并将Response放入inflightResponses
            // 有响应要发送给客户端，注册写事件
            sendResponse(response, response.responseSend)
          case response: CloseConnectionResponse =>  // 关闭对应的连接
            updateRequestMetrics(response)
            trace("Closing socket connection actively according to the response code.")
            close(channelId)
          case _: StartThrottlingResponse =>
            handleChannelMuteEvent(channelId, ChannelMuteEvent.THROTTLE_STARTED)
          case _: EndThrottlingResponse =>
            // Try unmuting the channel. The channel will be unmuted only if the response has already been sent out to
            // the client.
            handleChannelMuteEvent(channelId, ChannelMuteEvent.THROTTLE_ENDED)
            tryUnmuteChannel(channelId)
          case _ =>
            throw new IllegalArgumentException(s"Unknown response type: ${currentResponse.getClass}")
        }
      } catch {
        case e: Throwable =>
          processChannelException(channelId, s"Exception while processing response for $channelId", e)
      }
    }
  }

  // `protected` for test usage
  protected[network] def sendResponse(response: RequestChannel.Response, responseSend: Send): Unit = {
    val connectionId = response.request.context.connectionId
    trace(s"Socket server received response to send to $connectionId, registering for write and sending data: $response")
    // `channel` can be None if the connection was closed remotely or if selector closed it for being idle for too long
    // 根据connectionId获取KafkaChannel对象，并判断返回的是否为空
    // Scala Option#isEmpty 检测可选类型值是否为 None，是的话返回 true，否则返回 false
    if (channel(connectionId).isEmpty) {
      warn(s"Attempting to send response via channel for which there is no open connection, connection id $connectionId")
      response.request.updateRequestMetrics(0L, response)
    }
    // Invoke send for closingChannel as well so that the send is failed and the channel closed properly and
    // removed from the Selector after discarding any pending staged receives.
    // `openOrClosingChannel` can be None if the selector closed the connection because it was idle for too long
    // Scala Option#isDefined 如果可选值是 Some 的实例返回 true，否则返回 false。
    // 如果该连接处于可连接状态
    if (openOrClosingChannel(connectionId).isDefined) {
      // 将响应通过Selector标记为Send，实际发送通过poll轮询完成
      // 这里只是将Send缓存到KafkaChannel（通过org.apache.kafka.common.network.Send.destination识别是哪个KafkaChannel），并且在selector上注册通道的写事件（OP_WRITE）
      selector.send(responseSend)
      // 添加响应到临时队列inflightResponses
      inflightResponses += (connectionId -> response)
    }
  }

  private def poll(): Unit = {
    val pollTimeout = if (newConnections.isEmpty) 300 else 0
    try selector.poll(pollTimeout)
    catch {
      case e @ (_: IllegalStateException | _: IOException) =>
        // The exception is not re-thrown and any completed sends/receives/connections/disconnections
        // from this poll will be processed.
        error(s"Processor $id poll failed", e)
    }
  }

  protected def parseRequestHeader(buffer: ByteBuffer): RequestHeader = {
    val header = RequestHeader.parse(buffer)
    if (!header.apiKey.isEnabled) {
      throw new InvalidRequestException("Received request for disabled api key " + header.apiKey)
    }
    header
  }

  /**
   * Processor 从底层 Socket 通道不断读取已接收到的网络请求，然后转换成 Request 实例，并将其放入到 Request 队列，交给 RequestChannel 处理
   */
  private def processCompletedReceives(): Unit = {
    // 遍历所有已接收的Request，org.apache.kafka.common.network.Selector#attemptRead方法=>org.apache.kafka.common.network.Selector#addToCompletedReceives方法
    // 中，通过jdk nio SocketChannel 读取取到数据到 selector.completedReceives中
    selector.completedReceives.forEach { receive =>
      try {
        // 保证对应连接通道已经建立，receive.source是connectionId？
        openOrClosingChannel(receive.source) match {
          case Some(channel) =>
            // 解析ByteBuffer 到ReuestHeader
            val header = parseRequestHeader(receive.payload)
            if (header.apiKey == ApiKeys.SASL_HANDSHAKE && channel.maybeBeginServerReauthentication(receive,
              () => time.nanoseconds()))
              trace(s"Begin re-authentication: $channel")
            else {
              val nowNanos = time.nanoseconds()
              // 如果认证会话已过期，则关闭连接
              if (channel.serverAuthenticationSessionExpired(nowNanos)) {
                // be sure to decrease connection count and drop any in-flight responses
                debug(s"Disconnecting expired channel: $channel : $header")
                close(channel.id)
                expiredConnectionsKilledCount.record(null, 1, 0)
              } else {
                val connectionId = receive.source
                // 组装RequestContext对象
                val context = new RequestContext(header, connectionId, channel.socketAddress,
                  channel.principal, listenerName, securityProtocol,
                  channel.channelMetadataRegistry.clientInformation)
                // 构建 RequestChannel.Request对象包含了处理器的编号，响应对象中可以获取到，这样就能保证请求和响应的处理都是在同一个处理器中完成
                val req = new RequestChannel.Request(processor = id, context = context,
                  startTimeNanos = nowNanos, memoryPool, receive.payload, requestChannel.metrics)
                // KIP-511: ApiVersionsRequest is intercepted here to catch the client software name
                // and version. It is done here to avoid wiring things up to the api layer.
                if (header.apiKey == ApiKeys.API_VERSIONS) {
                  val apiVersionsRequest = req.body[ApiVersionsRequest]
                  if (apiVersionsRequest.isValid) {
                    channel.channelMetadataRegistry.registerClientInformation(new ClientInformation(
                      apiVersionsRequest.data.clientSoftwareName,
                      apiVersionsRequest.data.clientSoftwareVersion))
                  }
                }
                // 核心代码：将Request添加到Request队列，交给 RequestChannel 处理
                requestChannel.sendRequest(req)
                // 移除OP_READ读事件，已经接收到请求了，所以就不用再读了
                selector.mute(connectionId)
                handleChannelMuteEvent(connectionId, ChannelMuteEvent.REQUEST_RECEIVED)
              }
            }
          case None =>
            // This should never happen since completed receives are processed immediately after `poll()`
            throw new IllegalStateException(s"Channel ${receive.source} removed from selector before processing completed receive")
        }
      } catch {
        // note that even though we got an exception, we can assume that receive.source is valid.
        // Issues with constructing a valid receive object were handled earlier
        case e: Throwable =>
          processChannelException(receive.source, s"Exception while processing request from ${receive.source}", e)
      }
    }
    selector.clearCompletedReceives()
  }

  private def processCompletedSends(): Unit = {
    // 遍历底层SocketChannel已发送的Response
    selector.completedSends.forEach { send =>
      try {
        // 结束的写请求要从inflightResponses移出
        val response = inflightResponses.remove(send.destination).getOrElse {
          throw new IllegalStateException(s"Send for ${send.destination} completed, but not in `inflightResponses`")
        }
        updateRequestMetrics(response)

        // Invoke send completion callback TODO 这里没有看懂 onComplete => onComplete(send) ， 而且为什么能用foreach
        response.onComplete.foreach(onComplete => onComplete(send))

        // Try unmuting the channel. If there was no quota violation and the channel has not been throttled,
        // it will be unmuted immediately. If the channel has been throttled, it will unmuted only if the throttling
        // delay has already passed by now.
        handleChannelMuteEvent(send.destination, ChannelMuteEvent.RESPONSE_SENT)
        // 添加 OP_READ 读事件，这样就可以继续读取客户端发来的请求
        tryUnmuteChannel(send.destination)
      } catch {
        case e: Throwable => processChannelException(send.destination,
          s"Exception while processing completed send to ${send.destination}", e)
      }
    }
    selector.clearCompletedSends()
  }

  private def updateRequestMetrics(response: RequestChannel.Response): Unit = {
    val request = response.request
    val networkThreadTimeNanos = openOrClosingChannel(request.context.connectionId).fold(0L)(_.getAndResetNetworkThreadTimeNanos())
    request.updateRequestMetrics(networkThreadTimeNanos, response)
  }

  private def processDisconnected(): Unit = {
    // 遍历底层SocketChannel的那些已经断开的连接
    selector.disconnected.keySet.forEach { connectionId =>
      try {
        // 获取断开连接的远端主机名信息
        val remoteHost = ConnectionId.fromString(connectionId).getOrElse {
          throw new IllegalStateException(s"connectionId has unexpected format: $connectionId")
        }.remoteHost
        // remove方法返回Option类型，为什么还用foreach方法
        // 将该连接从inflightResponses中移除，同时更新一些监控指标
        inflightResponses.remove(connectionId).foreach(updateRequestMetrics)
        // the channel has been closed by the selector but the quotas still need to be updated
        // 更新配额数据
        connectionQuotas.dec(listenerName, InetAddress.getByName(remoteHost))
      } catch {
        case e: Throwable => processException(s"Exception while processing disconnection of $connectionId", e)
      }
    }
  }

  private def closeExcessConnections(): Unit = {
    // 如果配额超限了
    if (connectionQuotas.maxConnectionsExceeded(listenerName)) {
      // 找出优先关闭的那个连接
      // 所谓优先关闭，是指在诸多 TCP 连接中找出最近未被使用的那个。这里“未被使用”就是说，在最近一段时间内，没有任何 Request 经由这个连接被发送到 Processor 线程。
      val channel = selector.lowestPriorityChannel()
      if (channel != null)
        close(channel.id) // 关闭该连接
    }
  }

  /**
   * Close the connection identified by `connectionId` and decrement the connection count.
   * The channel will be immediately removed from the selector's `channels` or `closingChannels`
   * and no further disconnect notifications will be sent for this channel by the selector.
   * If responses are pending for the channel, they are dropped and metrics is updated.
   * If the channel has already been removed from selector, no action is taken.
   */
  private def close(connectionId: String): Unit = {
    openOrClosingChannel(connectionId).foreach { channel =>
      debug(s"Closing selector connection $connectionId")
      val address = channel.socketAddress
      if (address != null)
        connectionQuotas.dec(listenerName, address)
      selector.close(connectionId)

      inflightResponses.remove(connectionId).foreach(response => updateRequestMetrics(response))
    }
  }

  /**
   * Queue up a new connection for reading
   */
  def accept(socketChannel: SocketChannel,
             mayBlock: Boolean,
             acceptorIdlePercentMeter: com.yammer.metrics.core.Meter): Boolean = {
    val accepted = {
      if (newConnections.offer(socketChannel))
        true
      else if (mayBlock) {
        val startNs = time.nanoseconds
        newConnections.put(socketChannel)
        acceptorIdlePercentMeter.mark(time.nanoseconds() - startNs)
        true
      } else
        false
    }
    // 唤醒选择器线程开始轮询，原来的轮询因为没有事件到来被阻塞
    if (accepted)
      wakeup()
    accepted
  }

  /**
   * Register any new connections that have been queued up. The number of connections processed
   * in each iteration is limited to ensure that traffic and connection close notifications of
   * existing channels are handled promptly.
   */
  private def configureNewConnections(): Unit = {
    // 当前已配置的连接数计数器
    var connectionsProcessed = 0
    // newConnections 中的元素在 kafka.network.Acceptor.assignNewConnection函数中填充
    while (connectionsProcessed < connectionQueueSize && !newConnections.isEmpty) {
      // 从连接队列中取出SocketChannel
      val channel = newConnections.poll()
      try {
        debug(s"Processor $id listening to new connection from ${channel.socket.getRemoteSocketAddress}")
        // selector 是 kafka 自定义的 selector（封装了nioSelector），每个Processor 线程都维护了一个 Selector 类实例， org.apache.kafka.common.network.Selector，在这个客户端连接上注册读事件（OP_READ），通过org.apache.kafka.common.network.Selector.nioSelector注册
        // connectionId -> $localHost:$localPort-$remoteHost:$remotePort-$index
        // 创建 kafka自定义的 org.apache.kafka.common.network.KafkaChannel，创建好后，放到 org.apache.kafka.common.network.Selector.channels Map中， key->connectionId val->KafkaChannel
        selector.register(connectionId(channel.socket), channel)
        // 更新计数器
        connectionsProcessed += 1
      } catch {
        // We explicitly catch all exceptions and close the socket to avoid a socket leak.
        case e: Throwable =>
          val remoteAddress = channel.socket.getRemoteSocketAddress
          // need to close the channel here to avoid a socket leak.
          close(listenerName, channel)
          processException(s"Processor $id closed connection from $remoteAddress", e)
      }
    }
  }

  /**
   * Close the selector and all open connections
   */
  private def closeAll(): Unit = {
    while (!newConnections.isEmpty) {
      newConnections.poll().close()
    }
    selector.channels.forEach { channel =>
      close(channel.id)
    }
    selector.close()
    removeMetric(IdlePercentMetricName, Map(NetworkProcessorMetricTag -> id.toString))
  }

  // 'protected` to allow override for testing
  protected[network] def connectionId(socket: Socket): String = {
    val localHost = socket.getLocalAddress.getHostAddress
    val localPort = socket.getLocalPort
    val remoteHost = socket.getInetAddress.getHostAddress
    val remotePort = socket.getPort
    val connId = ConnectionId(localHost, localPort, remoteHost, remotePort, nextConnectionIndex).toString
    nextConnectionIndex = if (nextConnectionIndex == Int.MaxValue) 0 else nextConnectionIndex + 1
    connId
  }

  private[network] def enqueueResponse(response: RequestChannel.Response): Unit = {
    responseQueue.put(response)
    wakeup()
  }

  private def dequeueResponse(): RequestChannel.Response = {
    val response = responseQueue.poll()
    if (response != null)
      response.request.responseDequeueTimeNanos = Time.SYSTEM.nanoseconds
    response
  }

  private[network] def responseQueueSize = responseQueue.size

  // Only for testing
  private[network] def inflightResponseCount: Int = inflightResponses.size

  // Visible for testing
  // Only methods that are safe to call on a disconnected channel should be invoked on 'openOrClosingChannel'.
  private[network] def openOrClosingChannel(connectionId: String): Option[KafkaChannel] =
    Option(selector.channel(connectionId)).orElse(Option(selector.closingChannel(connectionId)))

  // Indicate the specified channel that the specified channel mute-related event has happened so that it can change its
  // mute state.
  private def handleChannelMuteEvent(connectionId: String, event: ChannelMuteEvent): Unit = {
    openOrClosingChannel(connectionId).foreach(c => c.handleChannelMuteEvent(event))
  }

  private def tryUnmuteChannel(connectionId: String) = {
    openOrClosingChannel(connectionId).foreach(c => selector.unmute(c.id))
  }

  /* For test usage */
  private[network] def channel(connectionId: String): Option[KafkaChannel] =
    Option(selector.channel(connectionId))

  /**
   * Wakeup the thread for selection.
   */
  override def wakeup() = selector.wakeup()

  override def initiateShutdown(): Unit = {
    super.initiateShutdown()
    removeMetric("IdlePercent", Map("networkProcessor" -> id.toString))
    metrics.removeMetric(expiredConnectionsKilledCountMetricName)
  }
}

class ConnectionQuotas(config: KafkaConfig, time: Time, metrics: Metrics) extends Logging with AutoCloseable {

  @volatile private var defaultMaxConnectionsPerIp: Int = config.maxConnectionsPerIp
  @volatile private var maxConnectionsPerIpOverrides = config.maxConnectionsPerIpOverrides.map { case (host, count) => (InetAddress.getByName(host), count) }
  @volatile private var brokerMaxConnections = config.maxConnections
  private val counts = mutable.Map[InetAddress, Int]()

  // Listener counts and configs are synchronized on `counts`
  private val listenerCounts = mutable.Map[ListenerName, Int]()
  private[network] val maxConnectionsPerListener = mutable.Map[ListenerName, ListenerConnectionQuota]()
  @volatile private var totalCount = 0

  // sensor that tracks broker-wide connection creation rate and limit (quota)
  private val brokerConnectionRateSensor = createConnectionRateQuotaSensor(config.maxConnectionCreationRate)
  private val maxThrottleTimeMs = TimeUnit.SECONDS.toMillis(config.quotaWindowSizeSeconds.toLong)

  def inc(listenerName: ListenerName, address: InetAddress, acceptorBlockedPercentMeter: com.yammer.metrics.core.Meter): Unit = {
    counts.synchronized {
      waitForConnectionSlot(listenerName, acceptorBlockedPercentMeter)

      val count = counts.getOrElseUpdate(address, 0)
      counts.put(address, count + 1)
      totalCount += 1
      if (listenerCounts.contains(listenerName)) {
        listenerCounts.put(listenerName, listenerCounts(listenerName) + 1)
      }
      val max = maxConnectionsPerIpOverrides.getOrElse(address, defaultMaxConnectionsPerIp)
      if (count >= max)
        throw new TooManyConnectionsException(address, max)
    }
  }

  private[network] def updateMaxConnectionsPerIp(maxConnectionsPerIp: Int): Unit = {
    defaultMaxConnectionsPerIp = maxConnectionsPerIp
  }

  private[network] def updateMaxConnectionsPerIpOverride(overrideQuotas: Map[String, Int]): Unit = {
    maxConnectionsPerIpOverrides = overrideQuotas.map { case (host, count) => (InetAddress.getByName(host), count) }
  }

  private[network] def updateBrokerMaxConnections(maxConnections: Int): Unit = {
    counts.synchronized {
      brokerMaxConnections = maxConnections
      counts.notifyAll()
    }
  }

  private[network] def updateBrokerMaxConnectionRate(maxConnectionRate: Int): Unit = {
    // if there is a connection waiting on the rate throttle delay, we will let it wait the original delay even if
    // the rate limit increases, because it is just one connection per listener and the code is simpler that way
    updateConnectionRateQuota(maxConnectionRate)
  }

  private[network] def addListener(config: KafkaConfig, listenerName: ListenerName): Unit = {
    counts.synchronized {
      if (!maxConnectionsPerListener.contains(listenerName)) {
        // TODO 没看懂 counts用来初始化所有类型listenerName的配额？而且map也没初始化
        val newListenerQuota = new ListenerConnectionQuota(counts, listenerName)
        maxConnectionsPerListener.put(listenerName, newListenerQuota)
        listenerCounts.put(listenerName, 0)
        config.addReconfigurable(newListenerQuota)
        newListenerQuota.configure(config.valuesWithPrefixOverride(listenerName.configPrefix))
      }
      counts.notifyAll()
    }
  }

  private[network] def removeListener(config: KafkaConfig, listenerName: ListenerName): Unit = {
    counts.synchronized {
      maxConnectionsPerListener.remove(listenerName).foreach { listenerQuota =>
        listenerCounts.remove(listenerName)
        // once listener is removed from maxConnectionsPerListener, no metrics will be recorded into listener's sensor
        // so it is safe to remove sensor here
        listenerQuota.close()
        counts.notifyAll() // wake up any waiting acceptors to close cleanly
        config.removeReconfigurable(listenerQuota)
      }
    }
  }

  def dec(listenerName: ListenerName, address: InetAddress): Unit = {
    counts.synchronized {
      val count = counts.getOrElse(address,
        throw new IllegalArgumentException(s"Attempted to decrease connection count for address with no connections, address: $address"))
      if (count == 1)
        counts.remove(address)
      else
        counts.put(address, count - 1)

      if (totalCount <= 0)
        error(s"Attempted to decrease total connection count for broker with no connections")
      totalCount -= 1

      if (maxConnectionsPerListener.contains(listenerName)) {
        val listenerCount = listenerCounts(listenerName)
        if (listenerCount == 0)
          error(s"Attempted to decrease connection count for listener $listenerName with no connections")
        else
          listenerCounts.put(listenerName, listenerCount - 1)
      }
      counts.notifyAll() // wake up any acceptors waiting to process a new connection since listener connection limit was reached
    }
  }

  def get(address: InetAddress): Int = counts.synchronized {
    counts.getOrElse(address, 0)
  }

  private def waitForConnectionSlot(listenerName: ListenerName,
                                    acceptorBlockedPercentMeter: com.yammer.metrics.core.Meter): Unit = {
    counts.synchronized {
      val startThrottleTimeMs = time.milliseconds
      val throttleTimeMs = math.max(recordConnectionAndGetThrottleTimeMs(listenerName, startThrottleTimeMs), 0)

      if (throttleTimeMs > 0 || !connectionSlotAvailable(listenerName)) {
        val startNs = time.nanoseconds
        val endThrottleTimeMs = startThrottleTimeMs + throttleTimeMs
        var remainingThrottleTimeMs = throttleTimeMs
        do {
          counts.wait(remainingThrottleTimeMs)
          remainingThrottleTimeMs = math.max(endThrottleTimeMs - time.milliseconds, 0)
        } while (remainingThrottleTimeMs > 0 || !connectionSlotAvailable(listenerName))
        acceptorBlockedPercentMeter.mark(time.nanoseconds - startNs)
      }
    }
  }

  // This is invoked in every poll iteration and we close one LRU connection in an iteration
  // if necessary
  def maxConnectionsExceeded(listenerName: ListenerName): Boolean = {
    totalCount > brokerMaxConnections && !protectedListener(listenerName)
  }

  private def connectionSlotAvailable(listenerName: ListenerName): Boolean = {
    if (listenerCounts(listenerName) >= maxListenerConnections(listenerName))
      false
    else if (protectedListener(listenerName))
      true
    else
      totalCount < brokerMaxConnections
  }

  private def protectedListener(listenerName: ListenerName): Boolean =
    config.interBrokerListenerName == listenerName && config.listeners.size > 1

  private def maxListenerConnections(listenerName: ListenerName): Int =
    maxConnectionsPerListener.get(listenerName).map(_.maxConnections).getOrElse(Int.MaxValue)

  /**
   * Calculates the delay needed to bring the observed connection creation rate to listener-level limit or to broker-wide
   * limit, whichever the longest. The delay is capped to the quota window size defined by QuotaWindowSizeSecondsProp
   *
   * @param listenerName listener for which calculate the delay
   * @param timeMs current time in milliseconds
   * @return delay in milliseconds
   */
  private def recordConnectionAndGetThrottleTimeMs(listenerName: ListenerName, timeMs: Long): Long = {
    def recordAndGetListenerThrottleTime(minThrottleTimeMs: Int): Int = {
      maxConnectionsPerListener
      .get(listenerName)
      .map { listenerQuota =>
        val listenerThrottleTimeMs = recordAndGetThrottleTimeMs(listenerQuota.connectionRateSensor, timeMs)
        val throttleTimeMs = math.max(minThrottleTimeMs, listenerThrottleTimeMs)
        // record throttle time due to hitting connection rate quota
        if (throttleTimeMs > 0) {
          listenerQuota.connectionRateThrottleSensor.record(throttleTimeMs.toDouble, timeMs)
        }
        throttleTimeMs
      }
      .getOrElse(0)
    }

    if (protectedListener(listenerName)) {
      recordAndGetListenerThrottleTime(0)
    } else {
      val brokerThrottleTimeMs = recordAndGetThrottleTimeMs(brokerConnectionRateSensor, timeMs)
      recordAndGetListenerThrottleTime(brokerThrottleTimeMs)
    }
  }

  /**
   * Records a new connection into a given connection acceptance rate sensor 'sensor' and returns throttle time
   * in milliseconds if quota got violated
   * @param sensor sensor to record connection
   * @param timeMs current time in milliseconds
   * @return throttle time in milliseconds if quota got violated, otherwise 0
   */
  private def recordAndGetThrottleTimeMs(sensor: Sensor, timeMs: Long): Int = {
    try {
      sensor.record(1.0, timeMs)
      0
    } catch {
      case e: QuotaViolationException =>
        val throttleTimeMs = QuotaUtils.boundedThrottleTime(e, maxThrottleTimeMs, timeMs).toInt
        debug(s"Quota violated for sensor (${sensor.name}). Delay time: $throttleTimeMs ms")
        throttleTimeMs
    }
  }

  /**
   * Creates sensor for tracking the connection creation rate and corresponding connection rate quota for a given
   * listener or broker-wide, if listener is not provided.
   * @param quotaLimit connection creation rate quota
   * @param listenerOpt listener name if sensor is for a listener
   */
  private def createConnectionRateQuotaSensor(quotaLimit: Int, listenerOpt: Option[String] = None): Sensor = {
    val sensorName = listenerOpt.map(listener => s"ConnectionAcceptRate-$listener").getOrElse("ConnectionAcceptRate")
    val sensor = metrics.sensor(sensorName, rateQuotaMetricConfig(quotaLimit))
    sensor.add(connectionRateMetricName(listenerOpt), new Rate, null)
    info(s"Created $sensorName sensor, quotaLimit=$quotaLimit")
    sensor
  }

  /**
   * Updates quota configuration for a given listener or broker-wide (if 'listenerOpt' is None)
   */
  private def updateConnectionRateQuota(quotaLimit: Int, listenerOpt: Option[String] = None): Unit = {
    val metric = metrics.metric(connectionRateMetricName(listenerOpt))
    metric.config(rateQuotaMetricConfig(quotaLimit))
    info(s"Updated ${listenerOpt.getOrElse("broker-wide")} max connection creation rate to $quotaLimit")
  }

  private def connectionRateMetricName(listenerOpt: Option[String]): MetricName = {
    val tags = listenerOpt.map(listener => Map("listener" -> listener)).getOrElse(Map())
    val namePrefix = listenerOpt.map(_ => "").getOrElse("broker-")
    metrics.metricName(
      s"${namePrefix}connection-accept-rate",
      MetricsGroup,
      s"Tracking rate of accepting new connections (per second)",
      tags.asJava)
  }

  private def rateQuotaMetricConfig(quotaLimit: Int): MetricConfig = {
    new MetricConfig()
      .timeWindow(config.quotaWindowSizeSeconds.toLong, TimeUnit.SECONDS)
      .samples(config.numQuotaSamples)
      .quota(new Quota(quotaLimit, true))
  }

  def close(): Unit = {
    metrics.removeSensor("ConnectionAcceptRate")
    maxConnectionsPerListener.values.foreach(_.close())
  }

  class ListenerConnectionQuota(lock: Object, listener: ListenerName) extends ListenerReconfigurable with AutoCloseable {
    @volatile private var _maxConnections = Int.MaxValue
    private[network] val connectionRateSensor = createConnectionRateQuotaSensor(Int.MaxValue, Some(listener.value))
    private[network] val connectionRateThrottleSensor = createConnectionRateThrottleSensor()

    def maxConnections: Int = _maxConnections

    override def listenerName(): ListenerName = listener

    override def configure(configs: util.Map[String, _]): Unit = {
      _maxConnections = maxConnections(configs)
      updateConnectionRateQuota(maxConnectionCreationRate(configs), Some(listener.value))
    }

    override def reconfigurableConfigs(): util.Set[String] = {
      SocketServer.ListenerReconfigurableConfigs.asJava
    }

    override def validateReconfiguration(configs: util.Map[String, _]): Unit = {
      val value = maxConnections(configs)
      if (value <= 0)
        throw new ConfigException(s"Invalid ${KafkaConfig.MaxConnectionsProp} $value")

      val rate = maxConnectionCreationRate(configs)
      if (rate <= 0)
        throw new ConfigException(s"Invalid ${KafkaConfig.MaxConnectionCreationRateProp} $rate")
    }

    override def reconfigure(configs: util.Map[String, _]): Unit = {
      lock.synchronized {
        _maxConnections = maxConnections(configs)
        updateConnectionRateQuota(maxConnectionCreationRate(configs), Some(listener.value))
        lock.notifyAll()
      }
    }

    def close(): Unit = {
      metrics.removeSensor(connectionRateSensor.name)
      metrics.removeSensor(connectionRateThrottleSensor.name)
    }

    private def maxConnections(configs: util.Map[String, _]): Int = {
      Option(configs.get(KafkaConfig.MaxConnectionsProp)).map(_.toString.toInt).getOrElse(Int.MaxValue)
    }

    private def maxConnectionCreationRate(configs: util.Map[String, _]): Int = {
      Option(configs.get(KafkaConfig.MaxConnectionCreationRateProp)).map(_.toString.toInt).getOrElse(Int.MaxValue)
    }

    /**
     * Creates sensor for tracking the average throttle time on this listener due to hitting connection rate quota.
     * The average is out of all throttle times > 0, which is consistent with the bandwidth and request quota throttle
     * time metrics.
     */
    private def createConnectionRateThrottleSensor(): Sensor = {
      val sensor = metrics.sensor(s"ConnectionRateThrottleTime-${listener.value}")
      val metricName = metrics.metricName("connection-accept-throttle-time",
        MetricsGroup,
        "Tracking average throttle-time, out of non-zero throttle times, per listener",
        Map(ListenerMetricTag -> listener.value).asJava)
      sensor.add(metricName, new Avg)
      sensor
    }
  }
}

class TooManyConnectionsException(val ip: InetAddress, val count: Int) extends KafkaException(s"Too many connections from $ip (maximum = $count)")
