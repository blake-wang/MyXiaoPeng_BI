package cn.xiaopeng.bi.gamepublish

import cn.wanglei.bi.ConfigurationUtil
import cn.xiaopeng.bi.utils.Hadoop
import com.sun.xml.internal.bind.v2.TODO
import kafka.serializer.StringDecoder
import org.apache.log4j.{Level, Logger}
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * Created by JSJSB-0071 on 2017/7/13.
 */
object GamePublishActions {
  //检查点目录
  val checkdir = "file:///home/hduser/spark/spark-1.6.1/checkpointdir/gamepublishactions1"
  //topic
  val topic: String = "order,login"

  def main(args: Array[String]) {
    Hadoop.hd
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)
    val ssc: StreamingContext = statActions
    ssc.start()
    ssc.awaitTermination()
  }

  def statActions(): StreamingContext = {
    val Array(brokers, topics) = Array(ConfigurationUtil.getProperty("kafka.metadata.broker.list"), "login,order")
    val sparkConf = new SparkConf().setAppName(this.getClass.getName.replace("$", "")).setMaster("local[2]")
    sparkConf.set("spark.streaming.backpressure.enabled", "true") //开启后spark自动根据系统负载选择最优消费速率
    sparkConf.set("spark.streaming.backpressure.initialRate", "500")
    sparkConf.set("spark.streaming.stopGracefullyOnShutdown", "true") //启动优雅关闭
    sparkConf.set("spark.streaming.unpersist", "true")

    val sparkContext = new SparkContext(sparkConf)
    val ssc: StreamingContext = new StreamingContext(sparkContext, Seconds(5))
    val topicsSet = topics.split(",").toSet
    val kafkaParams = Map[String,String]("metadata.broker.list"-> brokers)
    val dslogs: DStream[String] = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc,kafkaParams,topicsSet).map(_._2)
    dslogs.print()
    dslogs.foreachRDD(rdd=>{
      //加载登录数据

      //加载流水数据
    })

    ssc
  }
}
