package cn.xiaopeng.bi.utils

import java.sql.{Connection, PreparedStatement}

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.hive.HiveContext
import redis.clients.jedis.{Jedis, JedisPool}

import scala.collection.mutable.ArrayBuffer

/**
  * Created by bigdata on 17-7-26.
  */
object GamePublicUtil3 {


  /*
      * 离线处理
      * */
  def loadRegiInfoOffline(hiveContext: HiveContext, startTime: String, endTime: String) = {
    //1、获取注册日志
    val sql_regi = "select distinct game_account,game_id,getchannel(expand_channel,'0') parent_channel,getchannel(expand_channel,'1') child_channel,getchannel(expand_channel,'2') ad_label,reg_time, imei from ods_regi_rz where to_date(reg_time)>='startTime' and to_date(reg_time)<'endTime' and imei is not null"
      .replace("startTime", startTime).replace("endTime", endTime)
    val regiDF: DataFrame = hiveContext.sql(sql_regi)
    regiDF.registerTempTable("ods_regi_rz_cache")
    hiveContext.sql("use yyft")

    //2、按小时去重的字段
    val sql_bi_regi_Hour = "select distinct\nods_publish_game.child_game_id as child_game_id,\nods_regi_rz_cache.parent_channel as parent_channel,\nods_regi_rz_cache.child_channel as child_channel,\nods_regi_rz_cache.ad_label as ad_label,\nods_regi_rz_cache.reg_time as reg_time,\nods_regi_rz_cache.imei as imei,\nods_regi_rz_cache.count_game_account as count_game_account\nfrom\n(select game_id,parent_channel,child_channel,ad_label,substr(reg_time,0,13) as reg_time,imei,count(distinct game_account) as count_game_account from ods_regi_rz_cache group by game_id,parent_channel,child_channel,ad_label,imei,substr(reg_time,0,13)) ods_regi_rz_cache\njoin (select distinct game_id as child_game_id  from ods_publish_game) ods_publish_game on ods_regi_rz_cache.game_id=ods_publish_game.child_game_id and ods_regi_rz_cache.game_id is not null--过滤发行游戏"
    val df_bi_regi_Hour: DataFrame = hiveContext.sql(sql_bi_regi_Hour)
    resetRegiData(startTime, endTime)
    updateDataTomysql(df_bi_regi_Hour, "offLine_one")

  }

  def resetRegiData(startTime: String, endTime: String) = {
    val conn: Connection = JdbcUtil.getConn()
    val stmt = conn.createStatement()
    var sql1 = "UPDATE  bi_gamepublic_base_opera_kpi set new_regi_device_num =0,new_regi_account_num=0 where publish_date>='startTime' and publish_date<'endTime'".replace("startTime", startTime).replace("endTime", endTime)
    var sql2 = "UPDATE  bi_gamepublic_base_day_kpi set new_regi_device_num =0, new_regi_account_num=0 where date(publish_date)>='startTime' and date(publish_date)<'endTime'".replace("startTime", startTime).replace("endTime", endTime)
    stmt.execute(sql1)
    stmt.execute(sql2)
    stmt.close()
    conn.close()
  }

  def updateDataTomysql(df: DataFrame, mode: String) = {
    df.foreachPartition(iter => {
      if (!iter.isEmpty) {
        //数据库链接
        val conn: Connection = JdbcUtil.getConn()
        val stmt = conn.createStatement()
        //1、更新明细表 按小时去重
        val sql_detail = "INSERT INTO bi_gamepublic_regi_detail(game_id,parent_channel,child_channel,ad_label,regi_hour,imei) VALUES (?,?,?,?,?,?) "
        val pstat_sql_detail: PreparedStatement = conn.prepareStatement(sql_detail)
        var ps_sql_detail_params = new ArrayBuffer[Array[Any]]()

        //2、更新投放小时表  按小时去重
        //  1、新增注册设备数   new_regi_device_num
        val sqlkpi_hour_new_regi_device = "INSERT INTO bi_gamepublic_basekpi(parent_game_id,game_id,parent_channel,child_channel,ad_label,publish_time,new_regi_device_num) VALUES (?,?,?,?,?,?,?) ON DUPLICATE KEY update os=?,group_id=?,medium_account=?,promotion_channel=?,promotion_mode=?,head_people=?,new_regi_device_num=new_regi_device_num + VALUES(new_regi_device_num)"
        val pstat_sqlkpi_hour_new_regi_device: PreparedStatement = conn.prepareStatement(sqlkpi_hour_new_regi_device)
        var pskpi_hour_new_regi_device_params = new ArrayBuffer[Array[Any]]()

        //3、更新投放天表
        //  1、新增注册设备数  new_regi_device_num  永久去重
        val sqlkpi_day_new_regi_device = "INSERT INTO bi_gamepublic_base_day_kpi(parent_game_id,child_game_id,medium_channel,ad_site_channel,pkg_code,publish_date,new_regi_device_num) VALUES (?,?,?,?,?,?,?) ON DUPLICATE KEY update os=?,group_id=?,medium_account=?,promotion_channel=?,promotion_mode=?,head_people=?,new_regi_device_num=new_regi_device_num + VALUES(new_regi_device_num)"
        val pstat_sqlkpi_day_new_regi_device: PreparedStatement = conn.prepareStatement(sqlkpi_day_new_regi_device)
        var pskpi_day_new_regi_device_params = new ArrayBuffer[Array[Any]]()
        //  2、新增注册帐号数  new_regi_account_num
        // 更新bi_gamepublic_base_day_kpi  每天数据
        val sqlkpi_day_new_regi_account = "INSERT INTO bi_gamepublic_base_day_kpi(parent_game_id,child_game_id,medium_channel,ad_site_channel,pkg_code,publish_date,new_regi_account_num) VALUES (?,?,?,?,?,?,?) ON DUPLICATE KEY update os=?,group_id=?,medium_account=?,promotion_channel=?,promotion_mode=?,head_people=?,new_regi_account_num=new_regi_account_num + VALUES(new_regi_account_num)"
        val pstat_sqlkpi_day_new_regi_account: PreparedStatement = conn.prepareStatement(sqlkpi_day_new_regi_account)
        var sqlkpi_day_new_regi_account_params = new ArrayBuffer[Array[Any]]()
        //  3、注册设备数  regi_device_num
        //更新 bi_gamepublic_base_day_kpi  每天数据
        val sqlkpi_day_regi_device = "INSERT INTO bi_gamepublic_base_day_kpi(parent_game_id,child_game_id,medium_channel,ad_site_channel,pkg_code,publish_date,regi_device_num) VALUES (?,?,?,?,?,?,?) ON DUPLICATE KEY update os=?,group_id=?,medium_account=?,promotion_channel=?,promotion_mode=?,head_people=?,regi_device_num=regi_device_num + VALUES(regi_device_num)"
        val pstat_sqlkpi_day_regi_device: PreparedStatement = conn.prepareStatement(sqlkpi_day_regi_device)
        var pskpi_day_regi_device_params = new ArrayBuffer[Array[Any]]()

        //4、更新  运营天表(日报)
        //1、新增注册设备数
        val sql_opera_kpi_day_new_regi_device = "INSERT INTO bi_gamepublic_base_opera_kpi(parent_game_id,child_game_id,publish_date,new_regi_device_num,os,group_id) VALUES (?,?,?,?,?,?) ON DUPLICATE KEY update new_regi_device_num=new_regi_device_num + VALUES(new_regi_device_num)"
        val pstat_sql_opera_kpi_day_new_regi_device: PreparedStatement = conn.prepareStatement(sql_opera_kpi_day_new_regi_device);
        var ps_opera_kpi_day_new_regi_device_params = new ArrayBuffer[Array[Any]]()
        //2、新增注册帐号数
        val sql_opera_kpi_day_new_regi_account = "INSERT INTO bi_gamepublic_base_opera_kpi(parent_game_id,child_game_id,publish_date,new_regi_account_num,os,group_id) VALUES (?,?,?,?,?,?) ON DUPLICATE KEY update new_regi_account_num=new_regi_account_num + VALUES(new_regi_account_num)"
        val pstat_sql_opera_kpi_day_new_regi_account: PreparedStatement = conn.prepareStatement(sql_opera_kpi_day_new_regi_account);
        var ps_opera_kpi_day_new_regi_account_params = new ArrayBuffer[Array[Any]]()
        //3、注册设备数
        val sql_opera_kpi_day_regi_device = "INSERT INTO bi_gamepublic_base_opera_kpi(parent_game_id,child_game_id,publish_date,regi_device_num,os,group_id) VALUES (?,?,?,?,?,?) ON DUPLICATE KEY update regi_device_num=regi_device_num + VALUES(regi_device_num)"
        val pstat_sql_opera_kpi_day_regi_device: PreparedStatement = conn.prepareStatement(sql_opera_kpi_day_regi_device);
        var ps_opera_kpi_day_regi_device_params = new ArrayBuffer[Array[Any]]()

        // redis 链接
        val pool: JedisPool = JedisPoolSingleton.getJedisPool()
        val jedis: Jedis = pool.getResource
        jedis.select(0)

        iter.foreach(t=>{
          // 获取dataframe的数据
          val child_game_id = t.getAs[Int]("child_game_id");
          val parent_channel = t.getAs[String]("parent_channel");
          val child_channel = t.getAs[String]("child_channel");
          val ad_label = t.getAs[String]("ad_label");
          val publish_time = t.getAs[String]("reg_time");
          val imei = t.getAs[String]("imei");
          var count_game_account = 1;

          //获取redis的数据
          var parent_game_id = jedis.hget(child_game_id.toString + "_publish_game", "mainid")
          if(parent_game_id==null) parent_game_id = "0"
          var medium_account = jedis.hget(ad_label + "_pkgcode", "medium_account")
          if (medium_account == null) medium_account = ""
          var promotion_channel = jedis.hget(ad_label + "_pkgcode", "promotion_channel")
          if (promotion_channel == null) promotion_channel = ""
          var promotion_mode = jedis.hget(ad_label + "_" + publish_time.substring(0, 10) + "_pkgcode", "promotion_mode")
          if (promotion_mode == null) promotion_mode = ""
          var head_people = jedis.hget(ad_label + "_" + publish_time.substring(0, 10) + "_pkgcode", "head_people")
          if (head_people == null) head_people = ""
          var os = jedis.hget(child_game_id.toString + "_publish_game", "system_type")
          if (os == null) os = "1"
          var group_id = jedis.hget(child_game_id + "_publish_game", "publish_group_id")
          if (group_id == null) group_id = "0"


          if (parent_channel.length <= 10 && child_channel.length <= 10 && ad_label.length <= 12) {
              if(mode.equals("realTime"))  {
                val sqlselct_hour = "select regi_hour from bi_gamepublic_regi_detail WHERE game_id = '" + child_game_id + "'and parent_channel='" + parent_channel + "' and child_channel='" + child_channel + "'and ad_label= '" + ad_label + "' and  imei='" + imei + "' and regi_hour='" + publish_time + "'";
                val results_hour = stmt.executeQuery(sqlselct_hour)
                //只要这个小时，不存在就插入8
                if(!results_hour.next()){
                  //明细表
                  ps_sql_detail_params.+=(Array[Any](child_game_id, parent_channel, child_channel, ad_label, publish_time, imei))
                  //新增注册设备数，投放小时表
                  pskpi_hour_new_regi_device_params.+=(Array[Any](parent_game_id, child_game_id, parent_channel, child_channel, ad_label, publish_time + ":00:00", 1, os, group_id, medium_account, promotion_channel, promotion_mode, head_people))
                }
                val sqlselect_oper_day = "select regi_hour from bi_gamepublic_regi_detail WHERE game_id = '" + child_game_id + "'and  imei='" + imei + "' and date(regi_hour)='" + publish_time.substring(0, 10) + "'";
                val results_oper_day = stmt.executeQuery(sqlselect_oper_day);
                if(!results_oper_day.next()){
                  //注册设备数  投放天表
                  pskpi_day_regi_device_params.+=(Array[Any](parent_game_id, child_game_id, parent_channel, child_channel, ad_label, publish_time.substring(0, 10), 1, os, group_id, medium_account, promotion_channel, promotion_mode, head_people))
                }

              }

          }


        })
      }
    })
  }


}
