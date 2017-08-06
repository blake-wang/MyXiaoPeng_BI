package cn.xiaopeng.bi.gamepublish

import java.sql.{Connection, PreparedStatement}

import cn.xiaopeng.bi.utils.{JdbcUtil, StringUtils}
import kafka.serializer.StringDecoder
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable.ArrayBuffer

/**
  * Created by bigdata on 17-8-3.
  *
  * 对登录的第一次实时调优,跑同数据流程，从数据源取数据，经过处理，再把数据存入数据库
  */
object DayLoginDevNum_Simple {
  var arg = "60"

  def main(args: Array[String]): Unit = {
    if (args.length > 0) {
      arg = args(0)
    }
    val ssc = getStreamingContext()
    ssc.start()
    ssc.awaitTermination
  }


  def getStreamingContext(): StreamingContext = {
    val sparkConf = new SparkConf().setAppName(this.getClass.getName.replace("$", ""))
      .set("spark.default.parallelism", "60")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.shuffle.consolidateFiles", "true")
      .set("spark.sql.shuffle.partitions", "60")
      .setMaster("local[*]")
    val sc = new SparkContext(sparkConf)
    val ssc = new StreamingContext(sc, Seconds(arg.toInt))
    val kafkaParams = Map[String, String]("metadata.broker.list" -> "master-yyft:9092,slaves01-yyft:9092,slaves02-yyft:9092")
    val topicSet = Array("login").toSet
    val dsLogin: DStream[String] = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topicSet).map(_._2)

    dsLogin.foreachRDD(rdd => {
      val sc = rdd.sparkContext
      val sqlContext = new HiveContext(sc)

      dayDevNumber(rdd, sqlContext)
    })
    ssc
  }


  def dayDevNumber(loginRdd: RDD[String], hiveContext: HiveContext) = {
    //过滤并生成新的rdd
    val rdd = loginRdd.filter(row => {
      val fields = row.split("\\|")
      fields.length > 8
    }).map(row => {
      //打印
      println(row)

      val fields = row.split("\\|")
      //取出字段 game_account,logintime,game_id,expand_channel,imei
      Row(fields(3), fields(4), fields(8).toInt, if (fields(6) == null || fields(6).equals("")) "21" else fields(6), fields(7))
    })

    //判断过滤后的rdd是否为空
    if (!rdd.isEmpty()) {
      //注册成表
      val loginStruct = (new StructType())
        .add("game_account", StringType)
        .add("login_time", StringType)
        .add("game_id", IntegerType)
        .add("expand_channel", StringType)
        .add("imei", StringType)
      val loginDF = hiveContext.createDataFrame(rdd, loginStruct)
      loginDF.registerTempTable("ods_login_cache")
      hiveContext.sql("use yyft")
      val df = hiveContext.sql("select distinct login_time,ods_login_cache.game_id as game_id,if(expand_channel='' or expand_channel is null,'21',expand_channel) expand_channel,sum(imei) dev_num from  ods_login_cache join game_sdk on ods_login_cache.game_id = game_sdk.old_game_id group by login_time,ods_login_cache.game_id,expand_channel")
      updateDevLoginNumToMySql(df, hiveContext)
    }
  }


  def updateDevLoginNumToMySql(df: DataFrame, hiveContext: HiveContext) = {
    df.foreachPartition(iter => {
      if (!iter.isEmpty) {
        val conn = JdbcUtil.getConn()

        //更新明细表 bi_gamepublic_base_day_kpi
        val sql_login = "insert into bi_gamepublic_base_day_kpi (publish_date,child_game_id,medium_channel,ad_site_channel,pkg_code,dau_device_num)  values (?,?,?,?,?,?) on duplicate key update dau_device_num=dau_device_num+values(dau_device_num)"
        val ptmt = conn.prepareStatement(sql_login)
        val sql_params = new ArrayBuffer[Array[Any]]()


        //login_time,game_id,if(expand_channel='' or expand_channel is null,'21',expand_channel) expand_channel,sum(imei) dev_num
        iter.foreach(row => {
          val login_time = row.getAs[String]("login_time")
          val game_id = row.getAs[Int]("game_id")

          val expand_channel = row.getAs[String]("expand_channel")

          val medium_channel = StringUtils.getArrayChannel(expand_channel)(0)
          val ad_site_channel = StringUtils.getArrayChannel(expand_channel)(1)
          val pkg_code = StringUtils.getArrayChannel(expand_channel)(2)

          var dev_num = row.getAs[String]("dev_num")
          if (dev_num == null || dev_num.equals("")) {
              dev_num = "99"
          }

          sql_params.+=(Array[Any](login_time, game_id, medium_channel, ad_site_channel, pkg_code, dev_num))

          //插入数据库
          executeUpdate(ptmt, sql_params, conn)
        })
        //关闭数据库链接
        ptmt.close()
        conn.close()
      }
    })
  }

  def executeUpdate(pstat: PreparedStatement, params: ArrayBuffer[Array[Any]], conn: Connection): Unit = {
    if (params.length > 0) {
      for (param <- params) {
        for (index <- 0 to param.length - 1) {
          pstat.setObject(index + 1, param(index))
        }
        pstat.executeUpdate()
      }
      params.clear
    }
  }

}
