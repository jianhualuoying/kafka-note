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

import kafka.message.{CompressionCodec, NoCompressionCodec}

/**
 * Configuration settings for in-built offset management
 * @param maxMetadataSize The maximum allowed metadata for any offset commit.
 * @param loadBufferSize Batch size for reading from the offsets segments when loading offsets into the cache.
 * @param offsetsRetentionMs After a consumer group loses all its consumers (i.e. becomes empty) its offsets will be kept for this retention period before getting discarded.
 *                           For standalone consumers (using manual assignment), offsets will be expired after the time of last commit plus this retention period.
 * @param offsetsRetentionCheckIntervalMs Frequency at which to check for expired offsets.
 * @param offsetsTopicNumPartitions The number of partitions for the offset commit topic (should not change after deployment).
 * @param offsetsTopicSegmentBytes The offsets topic segment bytes should be kept relatively small to facilitate faster
 *                                 log compaction and faster offset loads
 * @param offsetsTopicReplicationFactor The replication factor for the offset commit topic (set higher to ensure availability).
 * @param offsetsTopicCompressionCodec Compression codec for the offsets topic - compression should be turned on in
 *                                     order to achieve "atomic" commits.
 * @param offsetCommitTimeoutMs The offset commit will be delayed until all replicas for the offsets topic receive the
 *                              commit or this timeout is reached. (Similar to the producer request timeout.)
 * @param offsetCommitRequiredAcks The required acks before the commit can be accepted. In general, the default (-1)
 *                                 should not be overridden.
 */
case class OffsetConfig(maxMetadataSize: Int = OffsetConfig.DefaultMaxMetadataSize,  // OffsetMetadata中metadata携带信息的最大大小，默认为4KB
                        loadBufferSize: Int = OffsetConfig.DefaultLoadBufferSize,    // 用于读取Offsets Topic中Offset的读缓冲大小，默认为5MB
                        offsetsRetentionMs: Long = OffsetConfig.DefaultOffsetRetentionMs, // Offsets Topic中Offset保留的最大时长。默认为24小时
                        offsetsRetentionCheckIntervalMs: Long = OffsetConfig.DefaultOffsetsRetentionCheckIntervalMs, // 检查Offset是否过期的间隔时长，默认为600秒
                        offsetsTopicNumPartitions: Int = OffsetConfig.DefaultOffsetsTopicNumPartitions, // Offsets Topic的分区个数，默认为50个
                        offsetsTopicSegmentBytes: Int = OffsetConfig.DefaultOffsetsTopicSegmentBytes, // Offsets Topic的Segment大小，默认为100MB
                        offsetsTopicReplicationFactor: Short = OffsetConfig.DefaultOffsetsTopicReplicationFactor, // Offsets Topic的副本因子，默认为3个
                        offsetsTopicCompressionCodec: CompressionCodec = OffsetConfig.DefaultOffsetsTopicCompressionCodec, // Offsets Topic的压缩器，默认没有压缩器
                        offsetCommitTimeoutMs: Int = OffsetConfig.DefaultOffsetCommitTimeoutMs,  // Offsets Topic的处理OffsetCommitRequest的超时时间，默认为5秒
                        offsetCommitRequiredAcks: Short = OffsetConfig.DefaultOffsetCommitRequiredAcks) // Offsets Topic处理生产消息的ACK，默认为-1

object OffsetConfig {
  val DefaultMaxMetadataSize = 4096
  val DefaultLoadBufferSize = 5*1024*1024
  val DefaultOffsetRetentionMs = 24*60*60*1000L
  val DefaultOffsetsRetentionCheckIntervalMs = 600000L
  val DefaultOffsetsTopicNumPartitions = 50
  val DefaultOffsetsTopicSegmentBytes = 100*1024*1024
  val DefaultOffsetsTopicReplicationFactor = 3.toShort
  val DefaultOffsetsTopicCompressionCodec = NoCompressionCodec
  val DefaultOffsetCommitTimeoutMs = 5000
  val DefaultOffsetCommitRequiredAcks = (-1).toShort
}