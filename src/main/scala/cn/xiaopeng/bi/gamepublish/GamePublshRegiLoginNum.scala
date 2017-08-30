package cn.xiaopeng.bi.gamepublish

import cn.wanglei.bi.ConfigurationUtil
import cn.xiaopeng.bi.utils.{HiveContextSingleton, SparkUtils}
import kafka.serializer.StringDecoder
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by bigdata on 17-8-28.
  * 运营报表 - 实时  -  注册登录帐号数
  */
object GamePublshRegiLoginNum {
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


  def getStreamingContext: StreamingContext = {
    val sparkConf = new SparkConf().setAppName(this.getClass.getName.replace("$", ""))
      .set("spark.default.parallelism", "60")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.shuffle.consolidateFiles", "true")
      .set("spark.sql.shuffle.partitions", "60")
    SparkUtils.setMaster(sparkConf)
    val sparkContext = new SparkContext(sparkConf)
    val streamingContext = new StreamingContext(sparkContext, Seconds(arg.toInt))
    val kafkaParams = Map[String, String]("metadata.broker.list" -> ConfigurationUtil.getProperty("kafka.metadata.broker.list"))
    val topics = ConfigurationUtil.getProperty("kafka.topics.kpi").split(",").toSet
    val dStream: DStream[String] = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](streamingContext, kafkaParams, topics).map(_._2)
    dStream.foreachRDD(rdd => {
      val sparkContext = rdd.sparkContext
      val hiveContext = HiveContextSingleton.getInstance(sparkContext)
      regiLoginNumInfo(rdd, hiveContext)

    })

    streamingContext.checkpoint(ConfigurationUtil.getProperty("spark.checkpoint.kpi"))
    streamingContext
  }

  def regiLoginNumInfo(rdd: RDD[String], hiveContext: HiveContext): Unit = {
    val regiRdd = rdd.filter(line => {
      val fields = line.split("\\|", -1)

      fields(0).contains("bi_login") && (!fields(3).equals("")) && fields.length >= 12
    }).map(line => {
      val fields = line.split("\\|", -1)
      var imei = ""
      if (fields.length >= 15) {
        imei = fields(14)
      } else {
        imei = ""
      }

      Row(fields(3).trim.toLowerCase, fields(4).toInt, fields(5), imei)
    })
    if (!regiRdd.isEmpty()) {
      val regiStruct = new StructType()
        .add("game_account", StringType)
        .add("game_id", IntegerType)
        .add("reg_time", StringType)
        .add("imei", StringType)
      val regiDataFrame = hiveContext.createDataFrame(regiRdd, regiStruct)
      regiDataFrame.registerTempTable("ods_regi")
    }

  }

}
