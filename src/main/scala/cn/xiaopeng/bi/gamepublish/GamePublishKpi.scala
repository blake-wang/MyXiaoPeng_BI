package cn.xiaopeng.bi.gamepublish

import cn.wanglei.bi.ConfigurationUtil
import cn.xiaopeng.bi.utils.{HiveContextSingleton, SparkUtils}
import kafka.serializer.StringDecoder
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka.KafkaUtils

/**
  * Created by bigdata on 17-8-31.
  */
object GamePublishKpi {
  var arg = "60"

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)
    if (arg.length > 0) {
      arg = args(0)
    }
    val ssc = StreamingContext.getOrCreate(ConfigurationUtil.getProperty("spark.checkpoint.kpi"), getStreamingContext _)
    ssc.start()
    ssc.awaitTermination()
  }

  def getStreamingContext(): StreamingContext = {
    val sparkConf = new SparkConf().setAppName(this.getClass.getName.replace("$", ""))
      .set("spark.default.parallelism", "60")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.shuffle.consolidateFiles", "true")
      .set("spark.sql.shuffle.partitions", "60")
    SparkUtils.setMaster(sparkConf)
    val sc = new SparkContext(sparkConf);
    val ssc = new StreamingContext(sc, Seconds(arg.toInt));
    // 获取kafka的数据
    val Array(brokers, topics) = Array(ConfigurationUtil.getProperty("kafka.metadata.broker.list"), ConfigurationUtil.getProperty("kafka.topics.kpi"));
    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers);
    val topicsSet = topics.split(",").toSet;
    val dslogs: DStream[String] = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topicsSet).map(_._2)
    dslogs.foreachRDD(rdd=>{
      val sc = rdd.sparkContext
      val hiveContext: HiveContext = HiveContextSingleton.getInstance(sc)

      //基本维度信息
      //把以前的日志bi_pubgame和本次实时的日志bi_pubgame 相加




    })



    ssc.checkpoint(ConfigurationUtil.getProperty("spark.checkpoint.kpi"))
    ssc
  }
}
