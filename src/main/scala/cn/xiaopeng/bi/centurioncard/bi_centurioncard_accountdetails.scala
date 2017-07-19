package cn.xiaopeng.bi.centurioncard

import cn.wanglei.bi.ConfigurationUtil
import cn.xiaopeng.bi.utils.{SparkUtils, StringUtils}
import kafka.serializer.StringDecoder
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SQLContext
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by kequan on 2/13/17.
  * 保存 每个游戏（游戏id,游戏名称），每个账号的 账号类型，注册时间，最近登录时间，是否交易
  */
object bi_centurioncard_accountdetails {
  var arg = "10"
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)
    arg = args(0)
    val ssc = StreamingContext.getOrCreate(ConfigurationUtil.getProperty("spark.checkpoint.account"),recently )
    ssc.start()
    ssc.awaitTermination()

  }

  def dealInfo(valuesDstream: DStream[(String, String, String, String, String, String)])= {
    valuesDstream.foreachRDD(rdd=>{
      AccountUtil.deallogs(rdd)
    })
  }

  def recently():StreamingContext={
    val sparkConf = new SparkConf().setAppName(this.getClass.getName.replace("$",""))
    SparkUtils.setMaster(sparkConf);
    val sc = new SparkContext(sparkConf)
    val ssc = new StreamingContext(sc,Seconds(arg.toInt))
    val sqlContext = new SQLContext(sc)

    // 获取kafka的数据
    val Array(brokers,topics) = Array(ConfigurationUtil.getProperty("kafka.metadata.broker.list"),"kafka.topics.account")
    val kafkaParams = Map[String,String]("metadata.broker.list" -> brokers)
    val topicsSet = topics.split(",").toSet

    val valuesDstream:DStream[(String)] = KafkaUtils.createDirectStream[String,String,StringDecoder, StringDecoder](ssc,kafkaParams,topicsSet).map(x=>{
      x._2
    }).cache()
    //注册日志  header 游戏帐号 游戏id 注册时间  user_type 绑定的
    val regiLogs:DStream[(String,String,String,String,String,String)] = valuesDstream.filter(x=>{
      val splitlog = x.split("\\|");
      splitlog(0).contains("bi_regi") && splitlog.length > 6 && (!splitlog(3).equals(""))&&StringUtils.isTime(splitlog(5))

    }).map(x=>{
      val splitlog = x.split("\\|")
      (splitlog(0),splitlog(3),splitlog(4),splitlog(5),splitlog(6),splitlog(8))
    })
    dealInfo(regiLogs)



  }

}
