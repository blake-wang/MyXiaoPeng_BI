package cn.xiaopeng.bi.gamepublish

import java.sql.{Connection, PreparedStatement}

import cn.xiaopeng.bi.utils.{Commons, JdbcUtil, JedisPoolSingleton, StringUtils}
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
      //从日志中取出字段 game_account(3),login_time(4),game_id(8),expand_channel(6),imei(7)

      //重新包装成Row
      //      game_account(0),    login_time(1), game_id(2),     parent_channel(3),             child_channel(4),               pkg_code(6),                    imei(6)
      Row(fields(3).trim.toLowerCase, fields(4), fields(8).toInt, getArrayChannel(fields(6))(0), getArrayChannel(fields(6))(1), getArrayChannel(fields(6))(2), fields(7))
    })

    //判断过滤后的rdd是否为空
    if (!rdd.isEmpty()) {
      //注册成表
      val loginStruct = (new StructType())
        .add("game_account", StringType)
        .add("login_time", StringType)
        .add("game_id", IntegerType)
        .add("parent_channel", StringType)
        .add("child_channel", StringType)
        .add("ad_label", StringType)
        .add("imei", StringType)
      val loginDF = hiveContext.createDataFrame(rdd, loginStruct)
      loginDF.registerTempTable("ods_login_cache")

      hiveContext.sql("use yyft")
      val sql_bi_login_hour = "select distinct \nlastPubGame.child_game_id as child_game_id,\nfilter_login.parent_channel as parent_channel,\nfilter_login.child_channel as child_channel,\nfilter_login.ad_label as ad_label,\nfilter_login.imei as imei,\nfilter_login.count_acc as count_acc\nfrom\n (select distinct game_id,parent_channel,child_channel,ad_label,imei,count(distinct game_account) count_acc,substr(min(login_time),0,13) as login_time  from ods_login_rz_cache where game_account is not null and game_account != '' group by game_id,parent_channel,child_channel,ad_label,imei,to_date(login_time)) filter_login \n join (select distinct game_id as child_game_id from lastPubGame) lastPubGame on filter_login.game_id = lastPubGame.child_game_id"

      val df_bi_login_hour = hiveContext.sql(sql_bi_login_hour)

      foreachLoginDataFrame(df_bi_login_hour)
    }
  }


  def foreachLoginDataFrame(df_bi_login_hour: DataFrame) = {
    //遍历每一个分区
    df_bi_login_hour.foreachPartition(iter => {
      //判断分区不为空
      if (!iter.isEmpty) {
        //获取数据库的连接
        val conn = JdbcUtil.getConn()
        val stmt = conn.createStatement()
        val connFx: Connection = JdbcUtil.getXiaopeng2FXConn()
        //jedis
        val pool: JedisPool = JedisPoolSingleton.getJedisPool
        val jedis: Jedis = pool.getResource
        jedis.select(0)

        //一、投放报表：

        //1、投放日表 bi_gamepublic_base_day_kpi  dau_device_num
        val day_dau_device_num = "INSERT INTO bi_gamepublic_base_day_kpi(parent_game_id,child_game_id,medium_channel,ad_site_channel,pkg_code,publish_date,dau_device_num) VALUES (?,?,?,?,?,?,?) ON DUPLICATE KEY update os=?,group_id=?,medium_account=?,promotion_channel=?,promotion_mode=?,head_people=?,dau_device_num=dau_device_num + VALUES(dau_device_num)"
        val pstmt_day_dau_device_num = conn.prepareStatement(day_dau_device_num)
        var day_dau_device_num_params = new ArrayBuffer[Array[Any]]()

        //2、投放日表 bi_gamepublic_base_day_kpi  dau_account_num
        val day_dau_account_num = "INSERT INTO bi_gamepublic_base_day_kpi(parent_game_id,child_game_id,medium_channel,ad_site_channel,pkg_code,publish_date,dau_account_num) VALUES (?,?,?,?,?,?,?) ON DUPLICATE KEY update os=?,group_id=?,medium_account=?,promotion_channel=?,promotion_mode=?,head_people=?,dau_account_num=dau_account_num + VALUES(dau_account_num)"
        val pstmt_day_dau_account_num = conn.prepareStatement(day_dau_account_num)
        var day_dau_account_num_params = new ArrayBuffer[Array[Any]]()

        //3、投放日表  新增登录设备数  new_login_device_num
        val day_new_login_device_num = "INSERT INTO bi_gamepublic_base_day_kpi(parent_game_id,child_game_id,medium_channel,ad_site_channel,pkg_code,publish_date,new_login_device_num) VALUES (?,?,?,?,?,?,?) ON DUPLICATE KEY update os=?,group_id=?,medium_account=?,promotion_channel=?,promotion_mode=?,head_people=?,new_login_device_num=new_login_device_num + VALUES(new_login_device_num)"
        val pstmt_day_new_login_device_num = conn.prepareStatement(day_new_login_device_num)
        var day_new_login_device_num_params = new ArrayBuffer[Array[Any]]()

        //4、投放日表  新增登录帐号数  new_login_account_num
        val day_new_login_account_num = "INSERT INTO bi_gamepublic_base_day_kpi(parent_game_id,child_game_id,medium_channel,ad_site_channel,pkg_code,publish_date,new_login_account_num) VALUES (?,?,?,?,?,?,?) ON DUPLICATE KEY update os=?,group_id=?,medium_account=?,promotion_channel=?,promotion_mode=?,head_people=?,new_login_account_num=new_login_account_num + VALUES(new_login_account_num)"
        val pstmt_day_new_login_account_num = conn.prepareStatement(day_new_login_account_num)
        var day_new_login_account_num_params = new ArrayBuffer[Array[Any]]()


        iter.foreach(row => {
          //拿出dataframe中的数据
          val child_game_id = row.getAs[Int]("child_game_id")
          val parent_channel = row.getAs[String]("parent_channel")
          val child_channel = row.getAs[String]("child_channel")
          val ad_label = row.getAs[String]("ad_label")
          val imei = row.getAs[String]("imei")
          val login_time = row.getAs[String]("login_time")
          val game_account_num = row.getAs[String]("count_acc")

          //拿出jedis中的数据
          val redisValue = getRedisValue(child_game_id, ad_label, login_time, jedis, connFx)
          val parent_game_id = redisValue(0)
          val os = redisValue(1)
          val medium_account = redisValue(2)
          val promotion_channel = redisValue(3)
          val promotion_mode = redisValue(4)
          val head_people = redisValue(5)
          val group_id = redisValue(6)

          //1、查询注册明细表，获取最早的注册时间
          val select_first_regi = "select left(regi_hour,10) as publish_date from bi_gamepublic_regi_detail where ad_label='" + ad_label + "' and imei='" + imei + "' and parent_channel='" + parent_channel + "' and child_channel='" + child_channel + "' and game_id='" + child_game_id + "' and order by regi_hour asc limit 1"
          val result_first_regi = stmt.executeQuery(select_first_regi)
          if (result_first_regi.next()) {
            //注册过,并且第一次注册时间是今天
            val first_regi_date = result_first_regi.getString("publish_date")
            if (first_regi_date.equals(login_time.substring(0, 10))) {
              //新增登录设备数
              day_new_login_device_num_params.+=(Array[Any](parent_game_id, child_game_id, parent_channel,
                child_channel, ad_label, login_time.substring(0, 10), 1, os, group_id,
                medium_account, promotion_channel, promotion_mode, head_people))
              //新增登录帐号数
              day_new_login_account_num_params.+=(Array[Any](parent_game_id, child_game_id, parent_channel, child_channel, ad_label,
                login_time.substring(0, 10), game_account_num, os, group_id,
                medium_account, promotion_channel, promotion_mode, head_people))
            }else{
              //第一次注册时间是今天以前
              //登录设备数 dau_device_num = dau_device_num + 1
              day_dau_device_num_params.+=(Array[Any](parent_game_id, child_game_id, parent_channel,
                child_channel, ad_label, login_time.substring(0, 10), 1,
                os, group_id, medium_account, promotion_channel, promotion_mode, head_people))

              //登录帐号数 dau_account_num
              day_dau_account_num_params.+=(Array[Any](parent_game_id, child_game_id, parent_channel,
                child_channel, ad_label, login_time.substring(0, 10), game_account_num,
                os, group_id, medium_account, promotion_channel, promotion_mode, head_people))
            }
          }
        })


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

    val lastPubGame = hiveContext.sql("select distinct game_id from (select game_id from pubgame union select distinct game_id from yyft.ods_publish_game) t")
    //最终game_id表
    lastPubGame.registerTempTable("lastPubGame")

  }


  //从expand_channel 中 拆分出parent_channel,child_channel,ad_label
  def getArrayChannel(channelId: String): Array[String] = {
    val channelArr = channelId.split("_")
    if (channelId == null || channelId.equals("") || channelId.equals("0")) {
      Array[String]("21", "", "")
    } else if (channelArr.length < 3) {
      Array[String](channelId, "", "")
    } else {
      Array[String](channelArr(0), channelArr(1), channelArr(2))
    }
  }

  //取出redis中的数据
  def getRedisValue(game_id: Int, pkg_code: String, order_date: String, jedis: Jedis, connFx: Connection) = {
    var parent_game_id = jedis.hget(game_id.toString + "_publish_game", "mainid")
    if (parent_game_id == null) parent_game_id = "0"
    var medium_account = jedis.hget(pkg_code + "_pkgcode", "medium_account")
    if (medium_account == null) medium_account = ""
    var promotion_channel = jedis.hget(pkg_code + "_pkgcode", "promotion_channel")
    if (promotion_channel == null) promotion_channel = ""
    var promotion_mode = jedis.hget(pkg_code + "_" + order_date + "_pkgcode", "promotion_mode")
    if (promotion_mode == null) promotion_mode = ""
    var head_people = jedis.hget(pkg_code + "_" + order_date + "_pkgcode", "head_people")
    if (head_people == null) head_people = ""
    val os = Commons.getPubGameGroupIdAndOs(game_id, connFx)(1)
    val groupid = Commons.getPubGameGroupIdAndOs(game_id, connFx)(0)

    Array[String](parent_game_id, os, medium_account, promotion_channel, promotion_mode, head_people, groupid)
  }

}
