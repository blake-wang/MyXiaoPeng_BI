package cn.xiaopeng.bi.gamepublish

import cn.wanglei.bi.ConfigurationUtil
import org.apache.spark.SparkConf
import org.apache.spark.streaming.StreamingContext

/**
  * Created by bigdata on 17-9-6.
  */
object GamePublishThirdData {
  val checkdir = "file:///home/hduser/spark/spark-1.6.1/checkpointdir/thirddata"
  var batch: Int = 6
  val topic = "thirddata,active"

  def batchInt(bt: Int): Unit = {
    batch = bt
  }

  def main(args: Array[String]) = {
    batchInt(args(0).toInt)
//    val ssc: StreamingContext = StreamingContext.getOrCreate(checkdir, startActions _)
//    ssc.start()
//    ssc.awaitTermination()
  }

//  def startActions(): StreamingContext = {
//    val Array(brokers,topics) = Array(ConfigurationUtil.getProperty("kafka.metadata.broker.list"),topic)
//    val sparkConf = new SparkConf().setAppName(this.getClass.getName.replace("$",""))
//    sparkConf.set("spark.streaming.backpressure.enabled","true")
//    sparkConf.set("spark.streaming.backpressure.inititalRate","1000")
//    sparkConf.set("spark.streaming.stopGracefullyOnShutdown","true")
//    sparkConf.set("spark.streaming.kafka.maxRatePerPartition","1000")
//    sparkConf.set("spark.streaming.unpersist","true")
//    sparkConf.set("spark.locality.wait","1000")
//  }
}
