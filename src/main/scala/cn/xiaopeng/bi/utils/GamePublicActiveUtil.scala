package cn.xiaopeng.bi.utils

import java.sql.PreparedStatement

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}
import redis.clients.jedis.{Jedis, JedisPool}

import scala.collection.mutable.ArrayBuffer

/**
  * Created by bigdata on 17-8-2.
  */
object GamePublicActiveUtil {


  def loadActiveInfo(rdd: RDD[String], hiveContext: HiveContext) = {
    //一、数据准备  将日志转化为表
    val activeRdd = rdd.filter(t => {
      val arr = t.split("\\|", -1)
      arr(0).contains("bi_active") && arr.length >= 9 && arr(5).length >= 13
    }).map(t => {
      val arr = t.split("\\|", -1)
      Row(arr(1).toInt, StringUtils.getArrayChannel(arr(4))(0), StringUtils.getArrayChannel(arr(4))(1), StringUtils.getArrayChannel(arr(4))(2), arr(5), arr(8))

    })

    if (!activeRdd.isEmpty) {
      val activeStruct = new StructType()
        .add("game_id", IntegerType)
        .add("parent_channel", StringType)
        .add("child_channel", StringType)
        .add("ad_label", StringType)
        .add("active_time", StringType)
        .add("imei", StringType)

      val activeDF = hiveContext.createDataFrame(activeRdd, activeStruct)
      activeDF.registerTempTable("ods_active_cache")
      //二、按照业务逻辑去重和过滤

      hiveContext.sql("use yyft")
      val sql_bi_active = "select distinct \nlastPubGame.child_game_id as child_game_id,  \nods_active_cache.parent_channel as parent_channel,  \nods_active_cache.child_channel as child_channel,  \nods_active_cache.ad_label as ad_label,  \nods_active_cache.active_time as active_time,  \nods_active_cache.imei as imei  \nfrom  \n(select distinct game_id,parent_channel,child_channel,ad_label, substr(min(active_time),0,13)  as active_time,imei from ods_active_cache group by game_id,parent_channel,child_channel,ad_label,imei) ods_active_cache  \njoin (select distinct game_id as child_game_id  from lastPubGame) lastPubGame on ods_active_cache.game_id=lastPubGame.child_game_id"
      val df_bi_active = hiveContext.sql(sql_bi_active)
      foreachRealPartition(df_bi_active)


    }

  }

  def foreachRealPartition(df_bi_active: DataFrame) = {
    df_bi_active.foreachPartition(iter => {
      if (!iter.isEmpty) {
        //数据库链接
        val conn = JdbcUtil.getConn()
        val stmt = conn.createStatement()

        //一、明细表
        val sql_detail = "INSERT INTO bi_gamepublic_active_detail(game_id,parent_channel,child_channel,ad_label,active_hour,imei) VALUES (?,?,?,?,?,?) "
        val ps_detail = conn.prepareStatement(sql_detail)
        val params_detail = new ArrayBuffer[Array[Any]]()

        val sql_detail_update = "update bi_gamepublic_active_detail set active_hour =?  WHERE game_id = ? and parent_channel=? and child_channel=? and ad_label=? and  imei=?"
        val ps_detail_update = conn.prepareStatement(sql_detail_update)
        val params_detail_update = new ArrayBuffer[Array[Any]]()

        //二、更新投放小时表
        val sql_hour_active_num = "INSERT INTO bi_gamepublic_basekpi(parent_game_id,game_id,parent_channel,child_channel,ad_label,publish_time,active_num,os,group_id,medium_account,promotion_channel,promotion_mode,head_people) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?) ON DUPLICATE KEY update os=values(os),group_id=values(group_id),medium_account=values(medium_account),promotion_channel=values(promotion_channel),promotion_mode=values(promotion_mode),head_people=values(head_people),active_num=active_num + VALUES(active_num)"
        val ps_hour_active_num = conn.prepareStatement(sql_hour_active_num)
        val params_hour_active = new ArrayBuffer[Array[Any]]()

        //三、更新投放天表
        val sql_day_active_num = "INSERT INTO bi_gamepublic_base_day_kpi(parent_game_id,child_game_id,medium_channel,ad_site_channel,pkg_code,publish_date,active_num,os,group_id,medium_account,promotion_channel,promotion_mode,head_people) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?) ON DUPLICATE KEY update os=values(os),group_id=values(group_id),medium_account=values(medium_account),promotion_channel=values(promotion_channel),promotion_mode=values(promotion_mode),head_people=values(head_people),active_num=active_num + VALUES(active_num)"
        val ps_day_active_num = conn.prepareStatement(sql_day_active_num)
        val params_day_active = new ArrayBuffer[Array[Any]]()

        //四、更新运营小时表
        val sql_opera_hour_kpi = "insert into bi_gamepublic_base_opera_hour_kpi(parent_game_id,child_game_id,publish_time,active_num,os,group_id) values(?,?,?,?,?,?) on duplicate key update active_num=active_num+VALUES(active_num)"
        val ps_opera_hour_kpi = conn.prepareStatement(sql_opera_hour_kpi)
        val params_opera_kpi_hour = new ArrayBuffer[Array[Any]]()

        //五、更新运营天表（日报）
        val sql_opera_kpi = "insert into bi_gamepublic_base_opera_kpi(parent_game_id,child_game_id,publish_date,active_num,os,group_id) values(?,?,?,?,?,?) on duplicate key update active_num=active_num+VALUES(active_num)"
        val ps_opera_kpi = conn.prepareStatement(sql_opera_kpi)
        val params_opera_kpi = new ArrayBuffer[Array[Any]]()

        //redis 链接
        val pool = JedisUtil.getJedisPool
        val jedis = pool.getResource
        jedis.select(0)

        iter.foreach(row => {
          val child_game_id = row.getAs[Int]("child_game_id")
          val parent_channel = row.getAs[String]("parent_channel")
          val child_channel = row.getAs[String]("child_channel")
          val ad_label = row.getAs[String]("ad_label")
          val publish_time = row.getAs[String]("active_time")
          val imei = row.getAs[String]("imei")

          //获取redis中的数据
          val redisValue = JedisUtil.getRedisValue(child_game_id, ad_label, publish_time.substring(0, 10), jedis)
          val parent_game_id = redisValue(0);
          val medium_account = redisValue(2);
          val promotion_channel = redisValue(3);
          val promotion_mode = redisValue(4);
          val head_people = redisValue(5);
          val os = redisValue(1);
          val group_id = redisValue(6);

          if (parent_channel.length <= 10 && child_channel.length <= 10 && ad_label.length <= 12) {
            val sqlselect_all = "select active_hour from bi_gamepublic_active_detail WHERE game_id = '" + child_game_id + "'and parent_channel='" + parent_channel + "' and child_channel='" + child_channel + "'and ad_label= '" + ad_label + "' and  imei='" + imei + "' "
            val results_all = stmt.executeQuery(sqlselect_all)
            //这个设备没有激活过
            if (!results_all.next()) {
              //明细表
              params_detail.+=(Array[Any](child_game_id, parent_channel, child_channel, ad_label, publish_time, imei))
              //投放小时表
              params_hour_active.+=(Array[Any](parent_game_id, child_game_id, parent_channel, child_channel, ad_label, publish_time, 1, os, group_id, medium_account, promotion_channel, promotion_mode, head_people))
              //投放天表
              params_day_active.+=(Array[Any](parent_game_id, child_game_id, parent_channel, child_channel, ad_label, publish_time.substring(0, 10), 1, os, group_id, medium_account, promotion_channel, promotion_mode, head_people))
            } else {
              //存在就看时间
              val active_hour_old = results_all.getString("active_hour")
              if (DateUtils.beforeHour(publish_time, active_hour_old)) {
                //新数据的激活时间靠前
                //更新明细表
                params_detail_update.++(Array[Any](publish_time, child_game_id, parent_channel, child_channel, ad_label, imei))
                //投放小时表
                params_hour_active.+=(Array[Any](parent_game_id, child_game_id, parent_channel, child_channel, ad_label, publish_time, 1, os, group_id, medium_account, promotion_channel, promotion_mode, head_people))
                params_hour_active.+=(Array[Any](parent_game_id, child_game_id, parent_channel, child_channel, ad_label, active_hour_old, -1, os, group_id, medium_account, promotion_channel, promotion_mode, head_people))
                //投放天表
                params_day_active.+=(Array[Any](parent_game_id, child_game_id, parent_channel, ad_label, publish_time.substring(0, 10), 1, os, group_id, medium_account, promotion_channel, promotion_mode, head_people))
                params_day_active.+=(Array[Any](parent_game_id, child_game_id, parent_channel, child_channel, ad_label, active_hour_old.substring(0, 10), -1, os, group_id, medium_account, promotion_channel, promotion_mode, head_people))

              }
            }
            val sql_game_all = "select active_hour from bi_gamepublic_active_detail WHERE game_id = '\" + child_game_id + \"' and  imei='\" + imei + \"' "
            val results_game_all = stmt.executeQuery(sql_game_all)
            if (!results_game_all.next()) {
              //运营小时表
              params_opera_kpi_hour.+=(Array[Any](parent_game_id, child_game_id, publish_time.substring(0, 13), 1, os, group_id))
              //运营天表（日报）
              params_opera_kpi.+=(Array[Any](parent_game_id, child_game_id, publish_time.substring(0, 10), 1, os, group_id))
            } else {
              //运营天表（日报）
              val active_hour_old = results_game_all.getString("active_hour")
              if (DateUtils.beforeHour(publish_time, active_hour_old)) {
                //运营小时表
                params_opera_kpi_hour.+=(Array[Any](parent_game_id, child_game_id, publish_time.substring(0, 13), 1, os, group_id))
                params_opera_kpi_hour.+=(Array[Any](parent_game_id, child_game_id, active_hour_old.substring(0, 13), -1, os, group_id))
                //运营天表（日报）
                params_opera_kpi.+=(Array[Any](parent_game_id, child_game_id, publish_time.substring(0, 10), 1, os, group_id))
                params_opera_kpi.+=(Array[Any](parent_game_id, child_game_id))
              }
            }
          }

          //插入数据库
          JdbcUtil.executeUpdate(ps_detail, params_detail, conn)
          JdbcUtil.executeUpdate(ps_detail_update, params_detail_update, conn)
          JdbcUtil.executeUpdate(ps_hour_active_num, params_hour_active, conn)
          JdbcUtil.executeUpdate(ps_day_active_num, params_day_active, conn)
          JdbcUtil.executeUpdate(ps_opera_hour_kpi, params_opera_kpi_hour, conn)
          JdbcUtil.executeUpdate(ps_opera_kpi, params_opera_kpi, conn)

        })
        //关闭数据库链接
        ps_detail.close()
        ps_detail_update.close()
        ps_hour_active_num.close()
        ps_day_active_num.close()
        ps_opera_hour_kpi.close()
        ps_opera_kpi.close()
        stmt.close()
        conn.close()
        //关闭redis
        pool.returnResource(jedis)
        pool.destroy()
      }
    })
  }


}
