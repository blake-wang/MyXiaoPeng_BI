package cn.xiaopeng.bi

import cn.wanglei.bi.bean.MoneyMasterBean
import cn.wanglei.bi.utils.AnalysisJsonUtil
import cn.xiaopeng.bi.utils.{HiveContextSingleton, JdbcUtil, SparkUtils}
import kafka.serializer.StringDecoder
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer

/**
  * Created by bigdata on 17-9-6.
  */
object TestMapMethod {


  // 打印rdd中内容的 2种方式：
  //  1    rdd.collect().foreach{println}
  //  2    rdd.take(10).foreach{println}
  //  //take(10) 取前10个
  //  二、例子
  //  val logData = sparkcontext.textFile(logFile, 2).cache()
  //  logData.collect().foreach {println}
  //  logData.take(10).foreach { println }
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName(this.getClass.getName.replace("$", ""))
      .set("spark.default.parallelism", "60")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.shuffle.consolidateFiles", "true")
      .set("spark.sql.shuffle.partitions", "60")
    SparkUtils.setMaster(sparkConf);
    val sparkContext = new SparkContext(sparkConf)

    val ssc = new StreamingContext(sparkContext, Seconds(60))
    val brokers = "master-yyft:9092,slaves01-yyft:9092,slaves02-yyft:9092"
    val topicSet = Array("regi").toSet
    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers)
    val message = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topicSet)
    val valueDStream = message.map(_._2)

    val newDStream = valueDStream.filter(line => {
      println("filter - line : " + line)
      val fields = line.split("\\|", -1)
      println("filter - game_id : " + fields(4))
      val game_id = fields(4)
      val conn = JdbcUtil.getConn()
      val testSql = "select publish_date,parent_game_id from bi_gamepublic_opera_regi_pay where child_game_id = '" + game_id + "' limit 1"
      val stat = conn.createStatement()
      val resultSet = stat.executeQuery(testSql)


      var publish_date = ""
      while (resultSet.next()) {
        publish_date = resultSet.getString(1)
        val parent_game_id = resultSet.getString(2)

        println("publish_date : " + publish_date + "   ,   parent_game_id : " + parent_game_id)
      }

      //      fields(0).contains("bi_regi")
      !publish_date.equals("")
    }).map(line => {
      println("map - line : " + line)
      val fields = line.split("\\|", -1)
      println(fields(0) + " - " + fields(1) + " - " + fields(2) + " - " + fields(3) + " - " + fields(4) + " - " + fields(5) + " - " + fields(6))
      println("map - game_id : " + fields(4))
      val child_game_id = fields(4)
      val conn = JdbcUtil.getConn()
      val testSql = "select publish_date,parent_game_id from bi_gamepublic_opera_regi_pay where child_game_id = '" + child_game_id + "' limit 1"
      val stat = conn.createStatement()
      val resultSet = stat.executeQuery(testSql)
      val ab = ArrayBuffer[Array[Any]]()
      while (resultSet.next()) {
        val publis_date = resultSet.getString("publish_date")
        val parent_game_id = resultSet.getString("parent_game_id")

        println("publis_date : " + publis_date + "   ,   parent_game_id : " + parent_game_id)
        ab.+=(Array(publis_date, parent_game_id))
      }
      ab
    })

    newDStream.foreachRDD(rdd => {
      rdd.foreachPartition(part => {

        part.foreach { line => {
          val arr = line(0)
          val publish_date = arr(0)
          val parent_game_id = arr(1)
          println("newDStream -  publish_date : " + publish_date + " , parent_game_id : " + parent_game_id)

        }
        }
      }
      )
    })
    ssc.start()
    ssc.awaitTermination()
  }


}

