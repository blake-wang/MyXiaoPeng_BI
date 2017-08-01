package cn.xiaopeng.bi.gamepublish

import cn.wanglei.bi.ConfigurationUtil
import cn.xiaopeng.bi.utils.{SQLContextSingleton, SparkUtils}
import kafka.serializer.StringDecoder
import org.apache.log4j.Logger
import org.apache.spark.sql.catalyst.expressions.Minute
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka.{KafkaManager, KafkaUtils}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Minutes, StreamingContext}

/**
  * Created by bigdata on 17-7-26.
  */
object NewPubBackPay {
  val logger = Logger.getLogger(NewPubBackClick.getClass)
  var arg = "60"

  def main(args: Array[String]): Unit = {
    if(arg.length>0){
      arg=args(0)
    }
    //第二个参数 方法名后面的 _ ，是把方法转换成函数
//    val ssc = StreamingContext.getOrCreate(ConfigurationUtil.getProperty("spark.checkpoint.payclick"),getStreamingContext _)

  }
//  def getStreamingContext:StreamingContext ={
//    val sparkConf = new SparkConf().setAppName(this.getClass.getName.replace("$",""))
//        .set("spark.sql.shuffle.partitions",ConfigurationUtil.getProperty("spark.sql.shuffle.partitions"))
//        .set("spark.serializer","org.apache.spark.serializer.KyroSerializer")
//    SparkUtils.setMaster(sparkConf)
//    val sparkContext = new SparkContext(sparkConf)
//    val ssc = new StreamingContext(sparkContext,Minutes(Integer.parseInt(arg)))
//
//    //获取kafka的数据
//    val Array(brokers,topics)=Array(ConfigurationUtil.getProperty("kafka.metadata.broker.list"),ConfigurationUtil.getProperty("kafka.topics.payclick"))
//    val topicsSet = topics.split(",").toSet
//    val kafkaParams = Map[String,String]("metadata.broker.list" -> brokers)
//    val km = new KafkaManager(kafkaParams)
//    val messages :DStream[String] = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc,kafkaParams,topicsSet).map(_._2)
//    messages.foreachRDD(rdd=>{
//      if(!rdd.isEmpty()){
//        val sc = rdd.sparkContext
//        val sqlContext = SQLContextSingleton.getInstance(sc)
//        //----写不下去了
//      }
//    })
//
//  }
}
