package cn.xiaopeng.bi.gamepublish

import cn.wanglei.bi.ConfigurationUtil
import cn.wanglei.bi.utils.PublicFxGgameTbPush2Redis
import cn.xiaopeng.bi.utils.{GamePublicRegiUtil, HiveContextSingleton, SparkUtils, StreamingUtils}
import kafka.serializer.StringDecoder
import org.apache.log4j.{Level, Logger}
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by bigdata on 17-8-1.
  * 1、实时注册，登录统计
  */
object GamePublishKpi {
  var arg = "600"

  def main(args: Array[String]): Unit = {
//    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
//    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)
    if (args.length == 1) {
      arg = args(0)
    }

    //生产环境需要checkpoint
//    val ssc = StreamingContext.getOrCreate(ConfigurationUtil.getProperty("spark.checkpoint.kpi"), getStreamingContext _);

    //测试环境不许要checkpoint
    val ssc = getStreamingContext

    ssc.start()
    ssc.awaitTermination()
  }

  def getStreamingContext(): StreamingContext = {
    //创建各种上下文
    val conf = new SparkConf().setAppName(this.getClass.getName.replace("$", ""))
      .set("spark.default.parallelism", "60")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.shuffle.consolidateFiles", "true")
      .set("spark.sql.shuffle.partitions", "60")
    SparkUtils.setMaster(conf)
    val sc = new SparkContext(conf)
    //时间间隔是60秒
    val ssc = new StreamingContext(sc, Seconds(arg.toInt))
    //获取kafka的数据
    val Array(brokers, topics) = Array(ConfigurationUtil.getProperty("kafka.metadata.broker.list"), ConfigurationUtil.getProperty("kafka.topics.kpi"))
    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers)
    val topicsSet = topics.split(",").toSet


    val dsLogs: DStream[String] = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topicsSet).map(_._2)
    dsLogs.print()
    dsLogs.foreachRDD(rdd => {
      if(rdd.count()>0)
        {
          //打印
          rdd.collect().foreach(println(_))
          val sc = rdd.sparkContext
          val hiveContext = HiveContextSingleton.getInstance(sc)

          //基本维度信息
          PublicFxGgameTbPush2Redis.publicGgameTbPush2Redis()
          //把以前的日志 bi_pubgame 和本次实时的日志  bi_pubgame  相加
          StreamingUtils.convertPubGameLogsToDfTmpTable(rdd, hiveContext)
          //处理注册日志
          GamePublicRegiUtil.loadRegiInfo(rdd, hiveContext)
          //
        }


    })

    //测试环境不许要checkpoint
    //ssc.checkpoint(ConfigurationUtil.getProperty("spark.checkpoint.kpi"))
    ssc
  }
}

