package cn.xiaopeng.bi.gamepublish

import java.sql.Struct
import javax.security.auth.login.Configuration

import cn.wanglei.bi.ConfigurationUtil
import cn.xiaopeng.bi.utils.SparkUtils
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
  */
object DayLoginKpi2 {
  var arg = "60"

  def main(args: Array[String]): Unit = {
    if (args.length > 0) {
      arg = args(0)
    }
    val conf = new SparkConf().setAppName(this.getClass.getName.replace("$", ""))
    val sc = new SparkContext(conf)
    SparkUtils.setMaster(conf)
    val ssc = new StreamingContext(sc, Seconds(arg.toInt))
    val kafkaparms = Map[String, String]("metedata.broker.list" -> "master-yyft:9092,slaves01-yyft:9092,slaves02-yyft:9092")
    val topicSet = "regi,login,order,active,pubgame,channel,request".split(",").toSet
    val dStream: DStream[String] = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaparms, topicSet).map(_._2)
    //dStream里面是一连串的rdd
    dStream.foreachRDD(rdd => {
      val sc = rdd.sparkContext
      val hiveContext = new HiveContext(sc)

      loadLoginInfo(rdd, hiveContext)
    })
  }


  def loadLoginInfo(rdd: RDD[String], hiveContext: HiveContext): Unit = {
    val loginRdd = rdd.filter(line => {
      val fields = line.split("\\|", -1)
      fields(0).contains("bi_login") && fields.length >= 5
    }).map(line => {
      val fields = line.split("\\|", -1)
      //game_account,login_time,expand_channel,imei,game_id
      //                        parent_channel,child_channel,pkg_code
      Row(fields(3), fields(4), getArrayChannel(fields(6))(0), getArrayChannel(fields(6))(1), getArrayChannel(fields(6))(2), fields(7), fields(9))
    })
    if (!loginRdd.isEmpty()) {
      val loginStruct = new StructType()
        .add("game_account", StringType)
        .add("login_time", StringType)
        .add("parent_channel", StringType)
        .add("child_channel", StringType)
        .add("ad_label", StringType)
        .add("imei", StringType)
        .add("game_id", StringType)
      val loginRddDf = hiveContext.createDataFrame(loginRdd,loginStruct)
      loginRddDf.registerTempTable("ods_login")


    }


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
