package cn.xiaopeng.bi.gamepublish

import java.sql.{Connection, PreparedStatement}

import cn.xiaopeng.bi.utils.{JdbcUtil, JedisPoolSingleton, StringUtils}
import kafka.serializer.StringDecoder
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import redis.clients.jedis.{Jedis, JedisPool}

import scala.collection.mutable.ArrayBuffer

/**
  * Created by bigdata on 17-8-3.
  *
  * 在第一版的基础上，增加redis读取数据和增加唯独数据
  */
object DayLoginDevNum_two {
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
      fields(0).contains("bi_login") && fields.length > 8
    }).map(row => {
      //打印
      println(row)

      val fields = row.split("\\|")
      //取出字段 game_account,login_time,game_id,expand_channel,imei
      Row(fields(3).trim.toLowerCase, fields(4), fields(8).toInt, if (fields(6) == null || fields(6).equals("")) "21" else fields(6), fields(7))
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
        //获取数据库的连接
        val conn = JdbcUtil.getConn()
        val connFx: Connection = JdbcUtil.getXiaopeng2FXConn()
        //jedis
        val pool: JedisPool = JedisPoolSingleton.getJedisPool
        val jedis0: Jedis = pool.getResource
        jedis0.select(0)
        val jedis3: Jedis = pool.getResource
        jedis3.select(3)


        //更新明细表 bi_gamepublic_base_day_kpi
        val sql_login = "insert into bi_gamepublic_base_day_kpi (publish_date,child_game_id,medium_channel,ad_site_channel,pkg_code,dau_device_num)  values (?,?,?,?,?,?) on duplicate key update dau_device_num=dau_device_num+values(dau_device_num)"
        val ptmt = conn.prepareStatement(sql_login)
        val sql_params = new ArrayBuffer[Array[Any]]()


        //login_time,game_id,if(expand_channel='' or expand_channel is null,'21',expand_channel) expand_channel,sum(imei) dev_num
        iter.foreach(row => {
          //是否为当日新增注册设备

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

  //更新  game_id   把历史pubgame 中的 game_id 和 当前实时pubgame中的game_id 和并
  def convertPubGameLogsToDfTmpTable(rdd: RDD[String], hiveContext: HiveContext) {
    //过滤pubgame日志
    val pubGameRdd = rdd.filter(line => {
      val fields = line.split("\\|")
      fields(0).contains("bi_pubgame") && fields.length > 5
    }
    ).map(line => {
      //打印过滤后的日志
      println(line)

      val fields = line.split("\\|")
      if (fields(1) != null && !fields(1).equals("")) {
        Row(Integer.parseInt(fields(1)))
      } else {
        Row(0)
      }
    })
    val pubGameStruct = (new StructType()).add("game_id", IntegerType)
    val pubGameDF = hiveContext.createDataFrame(pubGameRdd, pubGameStruct)
    pubGameDF.registerTempTable("pubgame")
    print("select distinct game_id from (select game_id from pubgame union select distinct game_id from yyft.ods_publish_game) t")
    val lastPubGame = hiveContext.sql("select distinct game_id from (select game_id from pubgame union select distinct game_id from yyft.ods_publish_game) t")
    //最终game_id表
    lastPubGame.registerTempTable("lastPubGame")

  }

}
