//package cn.xiaopeng.bi.utils
//
//import java.sql.{Connection, PreparedStatement}
//
//
//import org.apache.spark.rdd.RDD
//import org.apache.spark.sql.{DataFrame, Row}
//import org.apache.spark.sql.hive.HiveContext
//import org.apache.spark.sql.types.{IntegerType, StringType, StructType}
//import redis.clients.jedis.{Jedis, JedisPool}
//
//import scala.collection.mutable.ArrayBuffer
//
///**
//  * Created by bigdata on 17-8-1.
//  * 1、
//  */
//object GamePublicRegiUtil_old {
//
//
//  //加载注册信息
//  def loadRegiInfo(rdd: RDD[String], hiveContext: HiveContext): Unit = {
//    //一、数据准备  将日志转化为表
//    val regiRdd = rdd.filter(t => { //filter的返回值是boolean   用来过滤
//      val arr = t.split("\\|")
//      arr(0).contains("bi_regi") && arr.length > 14 && StringUtils.isNumber(arr(4))
//    }).map(t => { //map会生成一个新的rdd
//      val arr: Array[String] = t.split("\\|")
//      // game_account  game_id      parent_channel   child_channel   ad_label  reg_time  imei
//      Row(arr(3), arr(4).toInt, StringUtils.getArrayChannel(arr(13))(0), StringUtils.getArrayChannel(arr(13))(1), StringUtils.getArrayChannel(arr(13))(2), arr(5), arr(14))
//    });
//    if (!regiRdd.isEmpty()) {
//      //将过滤后的rdd数据转换为dataframe
//      //这里表头的数据类型，和Row中的字段的类型要一样，否则解析不正确
//      val regiStruct = (new StructType)
//        .add("game_account", StringType)
//        .add("game_id", IntegerType)
//        .add("parent_channel", StringType)
//        .add("child_channel", StringType)
//        .add("ad_label", StringType)
//        .add("reg_time", StringType)
//        .add("imei", StringType)
//      val regiDF = hiveContext.createDataFrame(regiRdd, regiStruct)
//      regiDF.registerTempTable("ods_regi_rz_cache")
//
//      //二、按小时去重的字段
//      hiveContext.sql("use yyft")
//      val sql_bi_regi_Hour = "select distinct   lastPubGame.child_game_id as child_game_id,   \nods_regi_rz_cache.parent_channel as parent_channel,   \nods_regi_rz_cache.child_channel as child_channel,   \nods_regi_rz_cache.ad_label as ad_label,   \nods_regi_rz_cache.reg_time as reg_time,   \nods_regi_rz_cache.imei as imei,  \nods_regi_rz_cache.game_account as game_account   \nfrom   \n(select distinct game_id,parent_channel,child_channel,ad_label,imei,game_account,substr(min(reg_time),0,13) as reg_time from ods_regi_rz_cache group by game_id,parent_channel,child_channel,ad_label,imei,game_account) ods_regi_rz_cache   join (select distinct game_id as child_game_id  from lastPubGame) lastPubGame on ods_regi_rz_cache.game_id=lastPubGame.child_game_id --注册按小时去重"
//      val df_bi_regi_Hour: DataFrame = hiveContext.sql(sql_bi_regi_Hour)
//      updateDataTomysql(df_bi_regi_Hour, "realTime")
//    }
//  }
//
//  def updateDataTomysql(df: DataFrame, mode: String): Unit = {
//    df.foreachPartition(iter => {
//      //先做非空判断
//      if (!iter.isEmpty) {
//        //数据库链接
//        val conn: Connection = JdbcUtil.getConn()
//        val stmt = conn.createStatement()
//
//        val connFx: Connection = JdbcUtil.getXiaopeng2Conn()
//        //一，更新明细表，按小时去重
//        val sql_detail = "INSERT INTO bi_gamepublic_regi_detail(game_id,parent_channel,child_channel,ad_label,regi_hour,imei) VALUES (?,?,?,?,?,?) "
//        val pstat_sql_detail: PreparedStatement = conn.prepareStatement(sql_detail)
//        var ps_sql_detail_params = new ArrayBuffer[Array[Any]]()
//
//        //二、更新投放小时表  按小时去重
//        //1、新增注册设备数  new_regi_device_num  永久去重
//        val sqlkpi_hour_new_regi_device = "INSERT INTO bi_gamepublic_basekpi(parent_game_id,game_id,parent_channel,child_channel,ad_label,publish_time,new_regi_device_num) VALUES (?,?,?,?,?,?,?) ON DUPLICATE KEY update os=?,group_id=?,medium_account=?,promotion_channel=?,promotion_mode=?,head_people=?,new_regi_device_num=new_regi_device_num + VALUES(new_regi_device_num)"
//        val pstat_sqlkpi_detail = conn.prepareStatement(sqlkpi_hour_new_regi_device)
//        var kpi_hour_new_regi_device_params = new ArrayBuffer[Array[Any]]()
//
//        //2、新增注册帐号数  new_regi_account_num
//        //更新  bi_gamepublic_base_day_kpi   每天数据  特殊
//        val sqlkpi_day_new_regi_account = "INSERT INTO bi_gamepublic_base_day_kpi(parent_game_id,child_game_id,medium_channel,ad_site_channel,pkg_code,publish_date,new_regi_account_num) VALUES (?,?,?,?,?,?,?) ON DUPLICATE KEY update os=?,group_id=?,medium_account=?,promotion_channel=?,promotion_mode=?,head_people=?,new_regi_account_num=new_regi_account_num + VALUES(new_regi_account_num)"
//        val pstat_sqlkpi_day_new_regi_device = conn.prepareStatement(sqlkpi_day_new_regi_account)
//        var pskpi_day_new_regi_device_params = new ArrayBuffer[Array[Any]]()
//
//        //3、注册设备数 regi_device_num
//        //更新 bi_gamepublic_base_day_kpi  每天数据  按天去重
//        val sqlkpi_day_regi_device = "INSERT INTO bi_gamepublic_base_day_kpi(parent_game_id,child_game_id,medium_channel,ad_site_channel,pkg_code,publish_date,regi_device_num) VALUES (?,?,?,?,?,?,?) ON DUPLICATE KEY update os=?,group_id=?,medium_account=?,promotion_channel=?,promotion_mode=?,head_people=?,regi_device_num=regi_device_num + VALUES(regi_device_num)"
//        val pstat_sqlkpi_day_regi_device = conn.prepareStatement(sqlkpi_day_regi_device)
//        var pskpi_day_regi_device_params = new ArrayBuffer[Array[Any]]
//
//        //四、更新 运营天表（日报）
//        //1、新增注册设备数
//        val sql_opera_kpi_day_new_regi_device = "INSERT INTO bi_gamepublic_base_opera_kpi(parent_game_id,child_game_id,publish_date,new_regi_device_num,os,group_id) VALUES (?,?,?,?,?,?) ON DUPLICATE KEY update new_regi_device_num=new_regi_device_num + VALUES(new_regi_device_num)"
//        val pstat_sql_opera_kpi_day_new_regi_device = conn.prepareStatement(sql_opera_kpi_day_new_regi_device)
//        var ps_opera_kpi_day_new_regi_device_params = new ArrayBuffer[Array[Any]]()
//
//        //2、新增注册帐号数
//        val sql_opera_kpi_day_new_regi_account = "INSERT INTO bi_gamepublic_base_opera_kpi(parent_game_id,child_game_id,publish_date,new_regi_account_num,os,group_id) VALUES (?,?,?,?,?,?) ON DUPLICATE KEY update new_regi_account_num=new_regi_account_num + VALUES(new_regi_account_num)"
//        val pstat_sql_opera_kpi_day_new_regi_account: PreparedStatement = conn.prepareStatement(sql_opera_kpi_day_new_regi_account);
//        var ps_opera_kpi_day_new_regi_account_params = new ArrayBuffer[Array[Any]]()
//
//
//
//        //redis 链接
//        val pool: JedisPool = JedisPoolSingleton.getJedisPool
//        val jedis: Jedis = pool.getResource
//        jedis.select(0)
//
//        iter.foreach(t => {
//          //获取dataframe的数据
//          val child_game_id = t.getAs[Int]("child_game_id")
//          val parent_channel = t.getAs[String]("parent_channel")
//          val child_channel = t.getAs[String]("child_channel")
//          val ad_label = t.getAs[String]("ad_label")
//          val publish_time = t.getAs[String]("reg_time")
//          val imei = t.getAs[String]("imei")
//          var count_game_account = 1;
//          println("dataframe row  :  " + child_game_id + " : " + parent_channel + " : " + child_channel + " : " + ad_label + " : " + publish_time + " : " + imei)
//          //获取redis的数据
//          val redisValue = JedisUtil.getRedisValue(child_game_id, ad_label, publish_time.substring(0, 10), jedis, connFx)
//          val parent_game_id = redisValue(0)
//          val os = redisValue(1)
//          val medium_account = redisValue(2)
//          val promotion_channel = redisValue(3)
//          val promotion_mode = redisValue(4)
//          val head_people = redisValue(5)
//          val group_id = redisValue(6)
//
//          println("redis row  :  " + parent_game_id + " : " + os + " : " + medium_account + " : " + promotion_channel + " : " + promotion_mode + " : " + head_people + " : " + group_id)
//          if (parent_channel.length <= 10 && child_channel.length <= 10 && ad_label.length <= 12) {
//            if (mode.equals("realTime")) {
//              val sqlselect_hour = "select regi_hour from bi_gamepublic_regi_detail WHERE game_id = '" + child_game_id + "'and parent_channel='" + parent_channel + "' and child_channel='" + child_channel + "'and ad_label= '" + ad_label + "' and  imei='" + imei + "' and regi_hour='" + publish_time + "'";
//              val result_hour = stmt.executeQuery(sqlselect_hour)
//              //只要这个小时，不存在就插入
//              if (!result_hour.next()) {
//                //明细表
//                ps_sql_detail_params.+=(Array[Any](child_game_id, parent_channel, child_channel, ad_label, publish_time, imei))
//                //              TODO 新增注册设备数  投放小时表
//
//              }
//            }
//          }
//          //插入数据库
//          JdbcUtil.executeUpdate(pstat_sql_detail, ps_sql_detail_params, conn)
//        })
//        //关闭数据库链接
//        stmt.close()
//        pstat_sql_detail.close()
//        conn.close()
//        pool.returnResource(jedis)
//      }
//    })
//    //关闭jedis
//    JedisPoolSingleton.destroy()
//  }
//
//}
