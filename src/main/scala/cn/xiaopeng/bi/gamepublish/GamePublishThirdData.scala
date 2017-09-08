package cn.xiaopeng.bi.gamepublish

import javax.security.auth.login.Configuration

import cn.wanglei.bi.ConfigurationUtil
import cn.xiaopeng.bi.utils.Hadoop
import cn.xiaopeng.bi.utils.action.ThirdDataActs
import kafka.serializer.StringDecoder
import org.apache.log4j.{Level, Logger}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by bigdata on 17-9-6.
  * 第三方广告平台监测
  */
object GamePublishThirdData {
  val checkdir = "file:///home/hduser/spark/spark-1.6.1/checkpointdir/thirddata"
  var batch: Int = 6
  val topic = "thirddata,active,regi,order"

  def batchInt(bt: Int): Unit = {
    batch = bt
  }

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)
    Hadoop.hd
    batchInt(args(0).toInt)
    val ssc: StreamingContext = StreamingContext.getOrCreate(checkdir, statActions _)
    ssc.start()
    ssc.awaitTermination()
  }

  //游戏内部数据
  def statActions: StreamingContext = {
    val Array(brokers, topics) = Array(ConfigurationUtil.getProperty("kafka.metadata.broker.list"), topic)
    val sparkConf = new SparkConf().setAppName(this.getClass.getName.replace("$", ""))
    sparkConf.set("spark.streaming.backpressure.enabled", "true") //开启后，spark自动根据系统负载
    sparkConf.set("spark.streaming.backpressure.initialRate", "1000")
    sparkConf.set("spark.streaming.stopGracefullyOnShutdown", "true") //启动优雅关闭
    sparkConf.set("spark.streaming.kafka.maxRatePerPartition", "1000")
    sparkConf.set("spark.streaming.unpersist", "true")
    sparkConf.set("spark.locality.wait", "500")
    //数据本地化模式转换等待时间
    val sparkContext = new SparkContext(sparkConf)
    val ssc: StreamingContext = new StreamingContext(sparkContext, Seconds(batch))
    val topicsSet = topics.split(",").toSet
    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers)
    //从kafka中获取所有游戏日志数据
    val message = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topicsSet)
    //获取人tuple的values
    val valuesDStream = message.map(_._2)
    //角色数据
    val thirdData = valuesDStream.filter(x => x.contains("bi_thirddata")).filter(x => (!x.contains("bi_adv_money"))) //排除钱大师
      .map(line => line.substring(line.indexOf("{"), line.length))
    val ownerData = valuesDStream.filter(x => (!x.contains("bi_thirddata")))
    //加载陌陌数据
    ThirdDataActs.adClick(thirdData)

    ssc.checkpoint(checkdir)
    ssc

  }
}
