package org.apache.spark.streaming.kafka

import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.serializer.Decoder
import org.apache.spark.SparkException
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka.KafkaCluster.LeaderOffset

import scala.reflect.ClassTag

/**
  * Created by bigdata on 17-7-26.
  */
class KafkaManager(val kafkaParams:Map[String,String]) {

  private val kc = new KafkaCluster(kafkaParams)

  def createDirectStream[K: ClassTag, V: ClassTag, KD <: Decoder[K] : ClassTag, VD <: Decoder[V] : ClassTag]
  (ssc: StreamingContext, kafkaParams: Map[String, String], topics: Set[String]): InputDStream[(K, V, String)] = {
    val groupId = kafkaParams.get("group.id").get
    // 在zookeeper上读取offsets前先根据实际情况更新offsets
    setOrUpdateOffsets(topics, groupId)
    //从zookeeper上读取offset开始消费message
    val messages = {
      // Either 类型
      val partitionsE = kc.getPartitions(topics)
      if (partitionsE.isLeft)
      // s"xx ${}" 字符串插值
        throw new SparkException(s"get kafka partition failed: ${partitionsE.left.get}")
      val partitions = partitionsE.right.get
      val consumerOffsetsE = kc.getConsumerOffsets(groupId, partitions)
      if (consumerOffsetsE.isLeft)
        throw new SparkException(s"get kafka consumer offsets failed: ${consumerOffsetsE.left.get}")
      val consumerOffsets = consumerOffsetsE.right.get
      // 从指定offsets处消费kafka
      // messageHandler = (mmd: MessageAndMetadata[String, String]) => (mmd.key(), mmd.message())
      // MessageAndMetadata里包含message的topic message 等等信息
      KafkaUtils.createDirectStream[K, V, KD, VD, (K, V, String)](
        ssc, kafkaParams, consumerOffsets, (mmd: MessageAndMetadata[K, V]) => ( mmd.key, mmd.message, mmd.topic))
    }
    messages
  }
  private def setOrUpdateOffsets(topics: Set[String], groupId: String): Unit = {
    topics.foreach(topic => {
      var hasConsumed = true
      val partitionsE = kc.getPartitions(Set(topic))
      if (partitionsE.isLeft)
        throw new SparkException(s"get kafka partition failed: ${partitionsE.left.get}")
      val partitions = partitionsE.right.get
      val consumerOffsetsE = kc.getConsumerOffsets(groupId, partitions)
      if (consumerOffsetsE.isLeft) hasConsumed = false
      // 某个groupid首次没有offset信息，会报错，从头开始读
      if (hasConsumed) {
        // 消费过
        /**
          * 如果streaming程序执行的时候出现kafka.common.OffsetOutOfRangeException，
          * 说明zk上保存的offsets已经过时了，即kafka的定时清理策略已经将包含该offsets的文件删除。
          * 针对这种情况，只要判断一下zk上的consumerOffsets和earliestLeaderOffsets的大小，
          * 如果consumerOffsets比earliestLeaderOffsets还小的话，说明consumerOffsets已过时,
          * 这时把consumerOffsets更新为earliestLeaderOffsets
          */
        val earliestLeaderOffsetsE = kc.getEarliestLeaderOffsets(partitions)
        if (earliestLeaderOffsetsE.isLeft)
          throw new SparkException(s"get earliest leader offsets failed: ${earliestLeaderOffsetsE.left.get}")
        val earliestLeaderOffsets = earliestLeaderOffsetsE.right.get
        val consumerOffsets = consumerOffsetsE.right.get
        // 可能只是存在部分分区consumerOffsets过时，所以只更新过时分区的consumerOffsets为earliestLeaderOffsets
        var offsets: Map[TopicAndPartition, Long] = Map()
        consumerOffsets.foreach({ case (tp, n) =>
          val earliestLeaderOffset = earliestLeaderOffsets(tp).offset
          //println("topic: "+tp.topic + "--earliestLeaderOffset: " +earliestLeaderOffset + "--n: " + n)

          if (n < earliestLeaderOffset) {
            //println("consumer group:" + groupId + ",topic:" + tp.topic + ",partition:" + tp.partition +" offsets已经过时，更新为" + earliestLeaderOffset)
            offsets += (tp -> earliestLeaderOffset)
          }
        })
        //println("offsets.isEmpty: " + offsets.isEmpty)
        if (!offsets.isEmpty) {
          kc.setConsumerOffsets(groupId, offsets)
        }
      } else {
        // 没有消费过
        //println("没有消费过")
        val reset = kafkaParams.get("auto.offset.reset").map(_.toLowerCase)
        var leaderOffsets: Map[TopicAndPartition, LeaderOffset] = null
        if (reset == Some("smallest")) {
          // 从头消费
          val leaderOffsetsE = kc.getEarliestLeaderOffsets(partitions)
          if (leaderOffsetsE.isLeft)
            throw new SparkException(s"get earliest leader offsets failed: ${leaderOffsetsE.left.get}")
          leaderOffsets = leaderOffsetsE.right.get
        } else {
          // 从最新offset处消费
          val leaderOffsetsE = kc.getLatestLeaderOffsets(partitions)
          if (leaderOffsetsE.isLeft)
            throw new SparkException(s"get latest leader offsets failed: ${leaderOffsetsE.left.get}")
          leaderOffsets = leaderOffsetsE.right.get
        }
        val offsets = leaderOffsets.map {
          case (tp, offset) => (tp, offset.offset)
        }
        //println("offsets: "+offsets)
        kc.setConsumerOffsets(groupId, offsets)
      }
    })
  }
  def updateZKOffsets(rdd: RDD[(String, String, String)]): Unit = {
    val groupId = kafkaParams.get("group.id").get

    val offsetsList = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
    for (offsets <- offsetsList) {
      val topicAndPartition = TopicAndPartition(offsets.topic, offsets.partition)
      val o = kc.setConsumerOffsets(groupId, Map((topicAndPartition, offsets.untilOffset)))
      if (o.isLeft) {
        println(s"Error updating the offset to Kafka cluster: ${o.left.get}")
      }
    }
  }
}
