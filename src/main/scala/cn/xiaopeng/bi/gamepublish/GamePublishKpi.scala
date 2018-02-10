package cn.xiaopeng.bi.gamepublish

import cn.wanglei.bi.ConfigurationUtil
import cn.wanglei.bi.utils.publicFxGameTbPush2Redis
import cn.xiaopeng.bi.utils._
import cn.xiaopeng.bi.utils.action.GamePublicActs2
import kafka.serializer.StringDecoder
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
    if (arg.length > 0) {
      arg = args(0)
    }
    val ssc = StreamingContext.getOrCreate(ConfigurationUtil.getProperty("spark.checkpoint.kpi"), getStreamingContext _)
    ssc.start()
    ssc.awaitTermination()
  }

  def getStreamingContext(): StreamingContext = {
    //创建上下文
    val sparkConf = new SparkConf().setAppName(this.getClass.getName.replace("$", ""))
      .set("spark.serializer", "org.apache.spark.serializer,KyroSerializer")
      .set("spark.shuffle.consolidateFiles", "true")
      .set("spark.sql.shuffle.partitions", "60")
    SparkUtils.setMaster(sparkConf)
    val sc = new SparkContext(sparkConf)
    val ssc = new StreamingContext(sc, Seconds(60))
    //获取kafka的数据
    val brokers = "master-yyft:9092,slaves01-yyft:9092,slaves02-yyft:9092"
    val topics = "regi,order,login,active,pubgame,channel,request,thirddata".split(",").toSet
    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers)
    val dslogs: DStream[String] = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topics).map(_._2)
    dslogs.foreachRDD(rdd => {
      val sc = rdd.sparkContext
      val hiveContext = HiveContextSingleton.getInstance(sc)

      //基本维度信息
      publicFxGameTbPush2Redis.publicGameTbPush2Redis();
      //把以前的日志 bi_pubgame 和 本次实时的日志 bi_pubgame 相加
      StreamingUtils.convertPubGameLogsToDfTmpTable(rdd, hiveContext)
      //处理注册日志
      GamePublicRegiUtil.loadRegiInfo(rdd, hiveContext)
      //处理激活日志
      GamePublicActiveUtil.loadActiveInfo(rdd, hiveContext)
      //处理登录数据
      GamePublicActs2.loadLoginInfo(rdd)
      //投放报表支付数据
      GamePublicActs2.loadOrderInfo(rdd)
      //运营报表支付数据
      GamePublicPayUtil.loadPayInfo(rdd,hiveContext)
      //处理点击数据
      GamePublicClickUtil.loadClickInfo(rdd,hiveContext)



    })
    ssc.checkpoint(ConfigurationUtil.getProperty("spark.checkpoint.kpi"))
    ssc
  }


}
