package cn.xiaopeng.bi.centurioncard

import cn.wanglei.bi.ConfigurationUtil
import cn.xiaopeng.bi.utils.{AccountUtil, SparkUtils, StringUtils}
import kafka.serializer.StringDecoder
import org.apache.log4j.{Level, Logger}
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * 保存 每个游戏 = 游戏id，游戏名称
  * 每个帐号的帐号类型，注册时间，最近登录时间，是否交易
  *
  */
object bi_centurioncard_accountdetails {
  var arg = "10"

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)
    arg = args(0)
    val ssc = StreamingContext.getOrCreate(ConfigurationUtil.getProperty("spark.checkpoint.account"), recently _)
    ssc.start()
    ssc.awaitTermination()
  }

  def recently(): StreamingContext = {
    val sparkConf = new SparkConf().setAppName(this.getClass.getName.replace("$", ""))
    SparkUtils.setMaster(sparkConf)
    val sc = new SparkContext(sparkConf)
    val ssc = new StreamingContext(sc, Seconds(arg.toInt))

    val Array(brokers, topics) = Array(ConfigurationUtil.getProperty("kafka.metadata.broker.list"), ConfigurationUtil.getProperty("kafka.topics.account"))
    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers)
    val topicsSet = topics.split(",").toSet

    //从kafka中获取到原始日志
    val valuesDstream: DStream[(String)] = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topicsSet).map(_._2)

    //注册日志
    val regiLogs = valuesDstream.filter(x => {
      val splitlog = x.split("\\|")
      splitlog(0).contains("bi_regi") && splitlog.length > 6 && (!splitlog(3).equals("")) && (!splitlog(8).equals("") && StringUtils.isTime(splitlog(4)))
    }).map(x => {
      val splitlog = x.split("\\|")
      //元组
      (splitlog(0), splitlog(3), splitlog(4), splitlog(5), splitlog(6), splitlog(8))
    })
    dealInfo(regiLogs)

    //登录日志
    val loginLogs = valuesDstream.filter(x => {
      val splitlog = x.split("\\|")
      splitlog(0).contains("bi_login") && splitlog.length > 8 && (!splitlog(3).equals("")) && (!splitlog(8).equals("") && StringUtils.isTime(splitlog(4)))
    }).map(x => {
      val splitlog = x.split("\\|")
      (splitlog(0), splitlog(3), splitlog(8), splitlog(4), "", "")
    })
    dealInfo(loginLogs)

    //支付日志
    val orderLogs = valuesDstream.filter(x => {
      val splitlog = x.split("\\|")
      splitlog(0).contains("bi_order") && splitlog.length > 7 && (!splitlog(5).equals("")) && (!splitlog(7).equals(""))
    })


    ssc.checkpoint(ConfigurationUtil.getProperty("spark.checkpoint.account"))
    ssc
  }

  def dealInfo(valuesDstream: DStream[(String, String, String, String, String, String)]) = {
    valuesDstream.foreachRDD(rdd => {
      AccountUtil.deallogs(rdd)
    })
  }

}
