package cn.xiaopeng.bi.gamepublish

import java.sql.{Connection, PreparedStatement}

import cn.xiaopeng.bi.utils._
import kafka.serializer.StringDecoder
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import redis.clients.jedis.{Jedis, JedisPool}

import scala.collection.mutable.ArrayBuffer

/**
  * Created by bigdata on 17-8-3.
  */
object DayRegiKpi {
  var arg = "60"

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)
    if (args.length > 0) {
      arg = args(0)
    }
    val sparkConf = new SparkConf().setAppName(this.getClass.getName.replace("$", ""))
      .set("spark.default.parallelism", "60")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.shuffle.consolidateFiles", "true")
      .set("spark.sql.shuffle.partitions", "60")
      .setMaster("local[*]")
    val sparkContext = new SparkContext(sparkConf)

    val ssc = new StreamingContext(sparkContext, Seconds(arg.toInt))
    val kafkaParams = Map[String, String]("metadata.broker.list" -> "master-yyft:9092,slaves01-yyft:9092,slaves02-yyft:9092")
    val topicSet = "regi,pubgame".split(",").toSet

    val dsRegi: DStream[String] = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topicSet).map(_._2)


    dsRegi.foreachRDD(rdd => {
      val sc = rdd.sparkContext
      val hiveContext = new HiveContext(sc)

      //加载媒介帐号，推广渠道信息到redis


      //更新  game_id   把历史pubgame 中的 game_id 和 当前实时pubgame中的game_id 和并
      unionPubgameGameid(rdd, hiveContext)

      //处理注册日志
      loadRegiInfo(rdd, hiveContext)

    })

    ssc.start()
    ssc.awaitTermination()

  }

  //更新  game_id   把历史pubgame 中的 game_id 和 当前实时pubgame中的game_id 和并
  def unionPubgameGameid(rdd: RDD[String], hiveContext: HiveContext) {
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


  def loadRegiInfo(rdd: RDD[String], hiveContext: HiveContext) = {
    val regiRdd = rdd.filter(line => {
      //过滤
      val fields = line.split("\\|")
      fields(0).contains("bi_regi") && fields.length > 14
    }).map(line => {
      //打印过滤后的登录日志
      println(line)
      //转换
      val fields = line.split("\\|")
      //game_account,game_id,parent_channel,child_channel,ad_label,reg_time,imei
      Row(fields(3), fields(4).toInt, getArrayChannel(fields(13))(0), getArrayChannel(fields(13))(1), getArrayChannel(fields(13))(2), fields(5), fields(14))
    })
    //判断转换后的regiRdd是否为空
    if (!regiRdd.isEmpty()) {
      val regiStruct = new StructType()
        .add("game_account", StringType)
        .add("game_id", IntegerType)
        .add("parent_channel", StringType)
        .add("child_channel", StringType)
        .add("ad_label", StringType)
        .add("reg_time", StringType)
        .add("imei", StringType)
      val regiDataFrame = hiveContext.createDataFrame(regiRdd, regiStruct)
      //注册临时表
      regiDataFrame.registerTempTable("ods_regi_rz_cache")

      //按小时去重   --想一想，为什么这里要把hive中的历史数据全部取出来
      hiveContext.sql("use yyft");
      val sql_bi_regi_Hour = "select distinct\nlastPubGame.child_game_id as child_game_id,\nrz.parent_channel as parent_channel,\nrz.child_channel as child_channel,\nrz.ad_label as ad_label,\nrz.reg_time as reg_time,\nrz.imei as imei,\nrz.count_acc as count_acc\nfrom\n(select distinct game_id,parent_channel,child_channel,ad_label,imei,count(distinct game_account) count_acc,substr(min(reg_time),0,13) as reg_time from\nods_regi_rz_cache where game_account is not null and game_account != '' group by game_id,parent_channel,child_channel,ad_label,imei,to_date(reg_time)) rz join (select distinct game_id as child_game_id from lastPubGame) lastPubGame on rz.game_id = lastPubGame.child_game_id"
      val df_bi_regi_Hour: DataFrame = hiveContext.sql(sql_bi_regi_Hour);

      foreachRegiDataFrame(df_bi_regi_Hour)

      //关闭 redis链接
      JedisPoolSingleton.destroy()
    }
  }


  def foreachRegiDataFrame(regiDataFrame: DataFrame) = {
    //遍历每一个分区
    regiDataFrame.foreachPartition(iter => {
      if (!iter.isEmpty) {
        //数据库链接
        val conn = JdbcUtil.getConn()
        val stmt = conn.createStatement()
        val connFX = JdbcUtil.getXiaopeng2FXConn()

        //jedis链接
        val pool: JedisPool = JedisPoolSingleton.getJedisPool()
        val jedis = pool.getResource
        jedis.select(0)


        //一、投放报表：

        //1、更新明细表   按小时去重  -------注册有明细表 ，登录没有明细表
        val detail_regi = "INSERT INTO bi_gamepublic_regi_detail(game_id,parent_channel,child_channel,ad_label,regi_hour,imei) VALUES (?,?,?,?,?,?)"
        val pstmt_detail_regi = conn.prepareStatement(detail_regi)
        val detail_regi_params = new ArrayBuffer[Array[Any]]()

        //2、更新投放小时表  按小时去重
        // ---1、新增注册设备数  new_regi_device_num
        val hour_new_regi_device = "INSERT INTO bi_gamepublic_basekpi(parent_game_id,game_id,parent_channel,child_channel,ad_label,publish_time,new_regi_device_num) VALUES (?,?,?,?,?,?,?) ON DUPLICATE KEY update os=?,group_id=?,medium_account=?,promotion_channel=?,promotion_mode=?,head_people=?,new_regi_device_num=new_regi_device_num + VALUES(new_regi_device_num)"
        val pstmt_hour_new_regi_device = conn.prepareStatement(hour_new_regi_device)
        var hour_new_regi_device_params = new ArrayBuffer[Array[Any]]()

        //3、更新投放天表
        // ---1、新增注册设备数  new_regi_device_num
        val day_new_regi_device = "INSERT INTO bi_gamepublic_base_day_kpi(parent_game_id,child_game_id,medium_channel,ad_site_channel,pkg_code,publish_date,new_regi_device_num) VALUES (?,?,?,?,?,?,?) ON DUPLICATE KEY update os=?,group_id=?,medium_account=?,promotion_channel=?,promotion_mode=?,head_people=?,new_regi_device_num=new_regi_device_num + VALUES(new_regi_device_num)"
        val pstmt_day_new_regi_device = conn.prepareStatement(day_new_regi_device)
        var day_new_regi_device_params = new ArrayBuffer[Array[Any]]()

        // ---2、新增注册帐号数 new_regi_account_num
        val day_new_regi_account = "INSERT INTO bi_gamepublic_base_day_kpi(parent_game_id,child_game_id,medium_channel,ad_site_channel,pkg_code,publish_date,new_regi_account_num) VALUES (?,?,?,?,?,?,?) ON DUPLICATE KEY update os=?,group_id=?,medium_account=?,promotion_channel=?,promotion_mode=?,head_people=?,new_regi_account_num=new_regi_account_num + VALUES(new_regi_account_num)"
        val pstmt_day_new_regi_account = conn.prepareStatement(day_new_regi_account)
        var day_new_regi_account_params = new ArrayBuffer[Array[Any]]()

        // ---3、注册设备数   regi_device_num
        // 更新  bi_gamepublic_base_day_kpi  每天数据   按天去重
        val day_regi_device = "INSERT INTO bi_gamepublic_base_day_kpi(parent_game_id,child_game_id,medium_channel,ad_site_channel,pkg_code,publish_date,regi_device_num) VALUES (?,?,?,?,?,?,?) ON DUPLICATE KEY update os=?,group_id=?,medium_account=?,promotion_channel=?,promotion_mode=?,head_people=?,regi_device_num=regi_device_num + VALUES(regi_device_num)"
        val pstmt_day_regi_device = conn.prepareStatement(day_regi_device)
        var day_regi_device_params = new ArrayBuffer[Array[Any]]()


        //遍历每一行数据
        iter.foreach(line => {
          //获取dataframe的数据
          val child_game_id = line.getAs[Int]("child_game_id")
          val parent_channel = line.getAs[String]("parent_channel")
          val child_channel = line.getAs[String]("child_channel")
          val ad_label = line.getAs[String]("ad_label")
          val publish_time = line.getAs[String]("reg_time")
          val imei = line.getAs[String]("imei")
          val count_game_account = line.getAs[Long]("count_acc").toInt

          //获取redis中的数据
          val redisValue = getRedisValue(child_game_id, ad_label, publish_time.substring(0, 10), jedis, connFX);
          val parent_game_id = redisValue(0)
          val os = redisValue(1)
          val medium_account = redisValue(2)
          val promotion_channel = redisValue(3)
          val promotion_mode = redisValue(4)
          val head_people = redisValue(5)
          val group_id = redisValue(6)


          if (parent_channel.length <= 10 && child_channel.length <= 10 && ad_label.length <= 12) {
            //1、按天去重，先查看注册明细表
            val select_today_regi = "select regi_hour from bi_gamepublic_regi_detail WHERE game_id = '" + child_game_id + "'and parent_channel='" + parent_channel + "' and child_channel='" + child_channel + "'and ad_label= '" + ad_label + "' and  imei='" + imei + "' and date(regi_hour)='" + publish_time.substring(0, 10) + "'";
            val result_today_regi = stmt.executeQuery(select_today_regi)
            //判断是否今天注册过   按天去重
            if (!result_today_regi.next()) {
              //今天没有注册过
              // 注册详情表  bi_gamepublic_regi_detail
              detail_regi_params.+=(Array[Any](child_game_id, parent_channel, child_channel, ad_label, publish_time, imei))

              // 投放天表  bi_gamepublic_base_day_kpi   注册设备数    按天去重
              day_regi_device_params.+=(Array[Any](parent_game_id, child_game_id, parent_channel, child_channel, ad_label, publish_time + ":00:00", 1, os, group_id, medium_account, promotion_channel, promotion_mode, head_people))
            }

            //2、查看设备所有的注册信息，并取最早的注册时间信息
            val select_all = "select left(regi_hour,10) as publish_time from bi_gamepublic_regi_detail WHERE game_id = '" + child_game_id + "'and parent_channel='" + parent_channel + "' and child_channel='" + child_channel + "'and ad_label= '" + ad_label + "' and  imei='" + imei + "' order by regi_hour asc limit 1";
            val result_all = stmt.executeQuery(select_all)

            //判断设备以前是否注册过
            if (!result_all.next()) {
              //没有注册过

              //投放日表  新增注册设备数
              day_new_regi_device_params.+=(Array[Any](parent_game_id, child_game_id, parent_channel, child_channel, ad_label, publish_time.substring(0, 10), 1, os, group_id, medium_account, promotion_channel, promotion_mode, head_people))

              //投放日表  新增注册帐号数
              day_new_regi_account_params.+=(Array[Any](parent_game_id, child_game_id, parent_channel, child_channel, ad_label, publish_time.substring(0, 10), count_game_account, os, group_id, medium_account, promotion_channel, promotion_mode, head_people))
            } else {
              //注册过

              val selected_publish_time = result_all.getString("publish_time")
              if (selected_publish_time.equals(publish_time.substring(0, 10))) {
                //投放日表  新增注册帐号数
                day_new_regi_account_params.+=(Array[Any](parent_game_id, child_game_id, parent_channel, child_channel, ad_label, publish_time.substring(0, 10), count_game_account, os, group_id, medium_account, promotion_channel, promotion_mode, head_people))
              }

            }

            //3、查看今天以前的注册明细表
            val select_before_regi = "select regi_hour from bi_gamepublic_regi_detail WHERE game_id = '" + child_game_id + "'and parent_channel='" + parent_channel + "' and child_channel='" + child_channel + "'and ad_label= '" + ad_label + "' and  imei='" + imei + "' and date(regi_hour)<'" + publish_time.substring(0, 10) + "'";
            val result_before_regi = stmt.executeQuery(select_before_regi)
            //判断是否今天以前注册过
            if (!result_before_regi.next()) {
              //投放小时表   新增注册设备数   按小时去重
              hour_new_regi_device_params.+=(Array[Any](parent_game_id, child_game_id, parent_channel, child_channel, ad_label, publish_time + ":00:00", 1, os, group_id, medium_account, promotion_channel, promotion_mode, head_people))
            }


          }
          //投放报表数据库更新
          executeUpdate(pstmt_detail_regi, detail_regi_params, conn) //注册详情表
          executeUpdate(pstmt_day_regi_device, day_regi_device_params, conn) //投放日表，注册设备数
          executeUpdate(pstmt_day_new_regi_device, day_new_regi_device_params, conn) //投放日表，新增注册设备数
          executeUpdate(pstmt_day_new_regi_account, day_new_regi_account_params, conn) //投放日表，新增注册帐号数
          executeUpdate(pstmt_hour_new_regi_device, hour_new_regi_device_params, conn) //小时表，新增注册设备数


        })
        stmt.close()
        pstmt_detail_regi.close()
        pstmt_day_regi_device.close()
        pstmt_day_new_regi_device.close()
        pstmt_day_new_regi_account.close()
        pstmt_hour_new_regi_device.close()
        conn.close()
        connFX.close()
      }
    })

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
  def getRedisValue(game_id:Int,pkg_code:String,order_date:String,jedis:Jedis,connFx:Connection) = {
    var parent_game_id = jedis.hget(game_id.toString + "_publish_game", "mainid")
    if(parent_game_id==null) parent_game_id="0"
    var medium_account =jedis.hget(pkg_code+"_pkgcode","medium_account")
    if(medium_account==null) medium_account=""
    var promotion_channel = jedis.hget(pkg_code+"_pkgcode","promotion_channel")
    if(promotion_channel==null) promotion_channel=""
    var promotion_mode =jedis.hget(pkg_code+ "_" + order_date+"_pkgcode","promotion_mode")
    if(promotion_mode==null) promotion_mode=""
    var head_people =jedis.hget(pkg_code+ "_" + order_date+"_pkgcode","head_people")
    if(head_people==null) head_people=""
    val os =Commons.getPubGameGroupIdAndOs(game_id,connFx)(1)
    val groupid =Commons.getPubGameGroupIdAndOs(game_id,connFx)(0)

    Array[String](parent_game_id,os,medium_account,promotion_channel,promotion_mode,head_people,groupid)
  }


  def executeUpdate(pstat: PreparedStatement, params: ArrayBuffer[Array[Any]], conn: Connection): Unit = {
    if (params.length > 0) {
      for (param <- params) {
        for (index <- 0 to param.length - 1) {
          pstat.setObject(index + 1, param(index))
        }
        pstat.executeUpdate()
      }
      params.clear()
    }
  }
}
