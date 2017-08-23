package cn.xiaopeng.bi.gamepublish

import java.sql.Struct
import javax.security.auth.login.Configuration

import cn.wanglei.bi.ConfigurationUtil
import cn.xiaopeng.bi.utils.{GameDayLoginUtil, JdbcUtil, SparkUtils}
import kafka.serializer.StringDecoder
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.{StringType, StructType}
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}


/**
  * Created by bigdata on 17-8-15.
  *
  * --登录日志 按帐号  按天去重
  * 每个游戏帐号，每天不管登录多少次，只计算当天第一次登录时间
  * 一个字段一个字段的去处理，一个一个字段处理的熟练了，再一起去处理多个字段
  * 每日    登录帐号数
  * 每日    新增登录帐号数
  * 每日    登录设备数
  * 每日    新增登录设备数
  */
object DayLoginKpi2 {
  var arg = "60"

  def main(args: Array[String]): Unit = {
    if (args.length > 0) {
      arg = args(0)
    }
    val conf = new SparkConf().setAppName(this.getClass.getName.replace("$", ""))
      .set("spark.default.parallelism", "60")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.shuffle.consolidateFiles", "true")
      .set("spark.sql.shuffle.partitions", "60")
      .setMaster("local[*]")
    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc, Seconds(arg.toInt))
    val kafkaparms = Map[String, String]("metadata.broker.list" -> "master-yyft:9092,slaves01-yyft:9092,slaves02-yyft:9092")
    val topicSet = Array("login").toSet
    val dStream: DStream[String] = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaparms, topicSet).map(_._2)

    //遍历DStream中的每一个rdd
    dStream.foreachRDD(rdd => {
      val sc = rdd.sparkContext
      val hiveContext = new HiveContext(sc)

      //每日登录帐号数
      GameDayLoginUtil.loadLoginInfo(rdd, hiveContext)
      //每日新增登录帐号数
      GameDayLoginUtil.loadNewLoginInfo(rdd, hiveContext)

    })
    ssc.start()
    ssc.awaitTermination()
  }


  def getArrayChannel(channelId: String): Array[String] = {
    val splited = channelId.split("_")
    if (channelId == null) {
      Array[String]("no_acc", "", "")
    } else if (channelId.equals("") || channelId.equals("0")) {
      Array[String]("21", "", "")
    } else if (splited.length == 1 || splited.length == 2) {
      Array[String](splited(0), "", "")
    } else {
      Array[String](splited(0), splited(1), splited(2))
    }
  }

}
