package cn.xiaopeng.bi.utils

import java.sql.PreparedStatement

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}

import scala.collection.mutable.ArrayBuffer

/**
  * Created by bigdata on 17-8-4.
  * 用来处理注册日志
  */
object GamePublicRegiUtil {



  def loadRegiInfo(rdd: RDD[String], hiveContext: HiveContext): Unit = {

    val regiRdd =rdd.filter(t=>{
      val arr =t.split("\\|",-1)
      arr(0).contains("bi_regi") &&arr.length > 14 && StringUtils.isNumber(arr(4))
    }).map(t=>{
      val arr =t.split("\\|",-1)
      Row(arr(3), arr(4).toInt, StringUtils.getArrayChannel(arr(13))(0), StringUtils.getArrayChannel(arr(13))(1), StringUtils.getArrayChannel(arr(13))(2), arr(5), arr(14))
    })
    //转化完成之后判断一次rdd是否为空
    if(!regiRdd.isEmpty()){
      // 将数据转化为 dataframe
      val regiStruct = (new StructType)
        .add("game_account", StringType)
        .add("game_id", IntegerType)
        .add("parent_channel", StringType)
        .add("child_channel", StringType)
        .add("ad_label", StringType)
        .add("reg_time", StringType)
        .add("imei", StringType);
      val regiDF = hiveContext.createDataFrame(regiRdd, regiStruct);
      regiDF.registerTempTable("ods_regi_rz_cache")

      //按业务需求去重和过滤
      hiveContext.sql("use yyft")
      val sql_bi_regi_Hour ="select distinct  \nlastPubGame.child_game_id as child_game_id,  \nrz.parent_channel as parent_channel,  \nrz.child_channel as child_channel,  \nrz.ad_label as ad_label,  \nrz.reg_time as reg_time,  \nrz.imei as imei,  \nrz.count_acc as count_acc  \nfrom  \n(select game_id,parent_channel,child_channel,ad_label,imei,count(distinct game_account) count_acc,substr(reg_time,0,13) as reg_time from ods_regi_rz_cache where game_account!='' group by game_id,parent_channel,child_channel,ad_label,imei,substr(reg_time,0,13)) rz  \njoin (select distinct game_id as child_game_id  from lastPubGame) lastPubGame on rz.game_id=lastPubGame.child_game_id"
      val df_bi_regi_Hour: DataFrame = hiveContext.sql(sql_bi_regi_Hour);
      foreachRealPartition(df_bi_regi_Hour)
      //关闭redis
      JedisPoolSingleton.destroy()
    }
  }

  def foreachRealPartition(df: DataFrame) = {
    df.foreachPartition(iter =>{
      if(!iter.isEmpty){
        //数据库链接
        val conn = JdbcUtil.getConn()
        val stmt = conn.createStatement()

        // 一.更新明细表  按小时去重
        val sql_deatil = "INSERT INTO bi_gamepublic_regi_detail(game_id,parent_channel,child_channel,ad_label,regi_hour,imei) VALUES (?,?,?,?,?,?) "
        //因为在每一个分区中都建立一个conn链接，所以这里用prepareStatement比较好
        val pstat_sql_deatil: PreparedStatement = conn.prepareStatement(sql_deatil);
        var ps_sql_deatil_params = new ArrayBuffer[Array[Any]]()

        // 二.更新投放小时表  按小时去重
        // 新增注册设备数  new_regi_device_num
        val sql_basekpi = "INSERT INTO bi_gamepublic_basekpi(parent_game_id,game_id,parent_channel,child_channel,ad_label,publish_time,new_regi_device_num,os,group_id,medium_account,promotion_channel,promotion_mode,head_people) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?) ON DUPLICATE KEY update os=values(os),group_id=values(group_id),medium_account=values(medium_account),promotion_channel=values(promotion_channel),promotion_mode=values(promotion_mode),head_people=values(head_people),new_regi_device_num=new_regi_device_num + VALUES(new_regi_device_num)"
        val ps_basekpi: PreparedStatement = conn.prepareStatement(sql_basekpi);
        var params_basekpi = new ArrayBuffer[Array[Any]]()

        // 三.更新投放天表
        //  新增注册设备数  new_regi_device_num   永久去重 ; 新增注册帐号数 new_regi_account_num 特殊; regi_device_num  按天去重
        val sql_base_day_kpi = "INSERT INTO bi_gamepublic_base_day_kpi(parent_game_id,child_game_id,medium_channel,ad_site_channel,pkg_code,publish_date,new_regi_device_num,new_regi_account_num,regi_device_num,os,group_id,medium_account,promotion_channel,promotion_mode,head_people) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) ON DUPLICATE KEY update os=values(os),group_id=values(group_id),medium_account=values(medium_account),promotion_channel=values(promotion_channel),promotion_mode=values(promotion_mode),head_people=values(head_people),new_regi_device_num=new_regi_device_num + VALUES(new_regi_device_num),new_regi_account_num=new_regi_account_num + VALUES(new_regi_account_num),regi_device_num=regi_device_num + VALUES(regi_device_num)"
        val ps_base_day_kpi: PreparedStatement = conn.prepareStatement(sql_base_day_kpi);
        var params_base_day_kpi = new ArrayBuffer[Array[Any]]()

        // 四.更新 运营小时表
        // 新增注册设备数  new_regi_device_num
        val sql_base_opera_hour_kpi = "INSERT INTO bi_gamepublic_base_opera_hour_kpi(parent_game_id,child_game_id,publish_time,new_regi_device_num,os,group_id) VALUES (?,?,?,?,?,?) ON DUPLICATE KEY update os=values(os),group_id=values(group_id),new_regi_device_num=new_regi_device_num + VALUES(new_regi_device_num)";
        val ps_base_opera_hour_kpi: PreparedStatement = conn.prepareStatement(sql_base_opera_hour_kpi);
        var params_base_opera_hour_kpi = new ArrayBuffer[Array[Any]]()

        // 五.更新 运营天表（日报）
        // 1.新增注册设备数,新增注册帐号数,注册设备数 ; 2.新增注册帐号数 ;3.注册设备数
        val sql_base_opera_kpi = "INSERT INTO bi_gamepublic_base_opera_kpi(parent_game_id,child_game_id,publish_date,new_regi_device_num,new_regi_account_num,regi_device_num,os,group_id) VALUES (?,?,?,?,?,?,?,?) ON DUPLICATE KEY update os=values(os),group_id=values(group_id),new_regi_device_num=new_regi_device_num + VALUES(new_regi_device_num),new_regi_account_num=new_regi_account_num + VALUES(new_regi_account_num),regi_device_num=regi_device_num + VALUES(regi_device_num)"
        val ps_base_opera_kpi: PreparedStatement = conn.prepareStatement(sql_base_opera_kpi);
        var params_base_opera_kpi = new ArrayBuffer[Array[Any]]()

        //redis链接
        val pool = JedisPoolSingleton.getJedisPool()
        val jedis = pool.getResource
        //选择jedis 0号库
        jedis.select(0)
        iter.foreach(row=>{
          // 获取dataframe的数据
          val child_game_id = row.getAs[Int]("child_game_id");
          val parent_channel = row.getAs[String]("parent_channel");
          val child_channel = row.getAs[String]("child_channel");
          val ad_label = row.getAs[String]("ad_label");
          val publish_time = row.getAs[String]("reg_time");
          val imei = row.getAs[String]("imei");
          var count_game_account = row.getAs[Long]("count_acc").toInt;

          // 获取redis的数据
          val redisValue = JedisUtil.getRedisValue(child_game_id, ad_label, publish_time.substring(0, 10), jedis)
          val parent_game_id = redisValue(0);
          val os = redisValue(1);
          val medium_account = redisValue(2);
          val promotion_channel = redisValue(3);
          val promotion_mode = redisValue(4);
          val head_people = redisValue(5);
          val group_id = redisValue(6);

          //二：投放小时表 bi_gamepublic_basekpi
          //新增注册设备数
          var basekpi_new_regi_device_num = 0

          //三：投放天表 bi_gamepublic_base_day_kpi
          //  新增注册设备数
          var base_day_kpi_new_regi_device_num = 0;
          //  新增注册账户数
          var base_day_kpi_new_regi_account_num = 0;
          //  注册设备数
          var base_day_kpi_regi_device_num = 0;

          // 四.运营小时表
          var base_opera_hour_kpi_new_regi_device_num = 0;

          // 五.运营天表的一些字段  bi_gamepublic_base_opera_kpi
          //  新增注册设备数
          var base_opera_kpi_new_regi_device_num = 0;
          //  新增注册账户数
          var base_opera_kpi_new_regi_account_num = 0;
          //  注册设备数
          var base_opera_kpi_regi_device_num = 0;

          if(parent_channel.length <= 10 && child_channel.length <= 10 && ad_label.length <= 12){
            //一：明细表 二：投放小时表
            val sql_hour_day = "select regi_hour from bi_gamepublic_regi_detail WHERE game_id = '" + child_game_id + "'and parent_channel='" + parent_channel + "' and child_channel='" + child_channel + "'and ad_label= '" + ad_label + "' and  imei='" + imei + "' and regi_hour='" + publish_time + "'";
            val results_hour_day =stmt.executeQuery(sql_hour_day);
            //按小时去重
            //这个小时不存在
            if(!results_hour_day.next()){
              //明细表
              ps_sql_deatil_params.+=(Array[Any](child_game_id, parent_channel, child_channel, ad_label, publish_time, imei))
              val sql_beforetoay = "select regi_hour from bi_gamepublic_regi_detail WHERE game_id = '" + child_game_id + "'and parent_channel='" + parent_channel + "' and child_channel='" + child_channel + "'and ad_label= '" + ad_label + "' and  imei='" + imei + "' and date(regi_hour)<'" + publish_time.substring(0, 10) + "'";
              val results_beforetoay = stmt.executeQuery(sql_beforetoay);
              //今天以前不存在
              if(!results_beforetoay.next()){
                //新增注册设备数  投放小时表
                basekpi_new_regi_device_num =1
              }
            }
            // =========== 三.投放天表
            val sql_day = "select regi_hour from bi_gamepublic_regi_detail WHERE game_id = '" + child_game_id + "'and parent_channel='" + parent_channel + "' and child_channel='" + child_channel + "'and ad_label= '" + ad_label + "' and  imei='" + imei + "' and date(regi_hour)='" + publish_time.substring(0, 10) + "'";
            val results_day = stmt.executeQuery(sql_day);
            //今天不存在  包
            if(!results_day.next()){
              //注册设备数 投放天表
              base_day_kpi_regi_device_num = 1;
            }

            val sql_all = "select left(regi_hour,10) as publish_time from bi_gamepublic_regi_detail WHERE game_id = '" + child_game_id + "'and parent_channel='" + parent_channel + "' and child_channel='" + child_channel + "'and ad_label= '" + ad_label + "' and  imei='" + imei + "' order by TIMESTAMP(regi_hour) asc limit 1";
            val results_all = stmt.executeQuery(sql_all);
            //历史不存在
            if(!results_all.next()){
              //新增注册设备数  投放天表
              base_day_kpi_new_regi_device_num = 1;
              //新增注册帐号数  投放天表
              base_day_kpi_new_regi_account_num = count_game_account;
            }else{
              //有设备注册记录，再去查注册日期，看注册日期是否今天
              val sql_publish_time = results_all.getString("publish_time");
              if(sql_publish_time.equals(publish_time.substring(0, 10))){
                //一个设备，就算不是新增设备，可有可能在今天注册新的帐号，那这些帐号就是 新增帐号
                base_day_kpi_new_regi_account_num = count_game_account;
              }

            }

            //运营小时表
            val sql_oper_hour = "select regi_hour from bi_gamepublic_regi_detail WHERE game_id = '" + child_game_id + "' and  imei='" + imei + "' and regi_hour='" + publish_time.substring(0, 13) + "'";
            val results_oper_hour = stmt.executeQuery(sql_oper_hour);
            // 这个小时不存在
            if (!results_oper_hour.next()) {
              val sql_oper_beforetoay = "select regi_hour from bi_gamepublic_regi_detail WHERE game_id = '" + child_game_id + "' and  imei='" + imei + "' and date(regi_hour)<'" + publish_time.substring(0, 10) + "'";
              val results_oper_beforetoay = stmt.executeQuery(sql_oper_beforetoay);
              // 今天以前不存在 包
              if (!results_oper_beforetoay.next()) {
                //新增注册设备数 投放小时表
                base_opera_hour_kpi_new_regi_device_num = 1;
              }
            }

            //运营天表
            val sqlselect_oper_day = "select regi_hour from bi_gamepublic_regi_detail WHERE game_id = '" + child_game_id + "'and  imei='" + imei + "' and date(regi_hour)='" + publish_time.substring(0, 10) + "'";
            val results_oper_day = stmt.executeQuery(sqlselect_oper_day);
            if (!results_oper_day.next()) {
              // 注册设备数  运营天表（日报）
              base_opera_kpi_regi_device_num = 1;
            }
            val sql_oper_all = "select left(regi_hour,10) as publish_time from bi_gamepublic_regi_detail WHERE game_id = '" + child_game_id + "'and  imei='" + imei + "' order by TIMESTAMP(regi_hour) asc limit 1";
            val results_oper_all = stmt.executeQuery(sql_oper_all);
            // 历史不存在  游戏
            if (!results_oper_all.next()) {
              //新增注册设备数  运营天表（日报）
              base_opera_kpi_new_regi_device_num = 1;
              //新增注册帐号数  运营天表（日报）
              base_opera_kpi_new_regi_account_num = count_game_account;
            } else {
              val sql_publish_time = results_oper_all.getString("publish_time");
              if (sql_publish_time.equals(publish_time.substring(0, 10))) {
                //新增注册帐号数 运营天表（日报）
                base_opera_kpi_new_regi_account_num = count_game_account;
              }
            }

          }
          //插入数据库
          JdbcUtil.executeUpdate(pstat_sql_deatil, ps_sql_deatil_params, conn)
          if (basekpi_new_regi_device_num > 0) {
            params_basekpi.+=(Array[Any](parent_game_id, child_game_id, parent_channel, child_channel, ad_label, publish_time + ":00:00", basekpi_new_regi_device_num, os, group_id, medium_account, promotion_channel, promotion_mode, head_people))
            JdbcUtil.executeUpdate(ps_basekpi, params_basekpi, conn)
          }
          if (!(base_day_kpi_new_regi_device_num == 0 && base_day_kpi_new_regi_account_num == 0 && base_day_kpi_regi_device_num == 0)) {
            params_base_day_kpi.+=(Array[Any](parent_game_id, child_game_id, parent_channel, child_channel, ad_label, publish_time.substring(0, 10), base_day_kpi_new_regi_device_num, base_day_kpi_new_regi_account_num, base_day_kpi_regi_device_num, os, group_id, medium_account, promotion_channel, promotion_mode, head_people))
            JdbcUtil.executeUpdate(ps_base_day_kpi, params_base_day_kpi, conn)
          }
          if (base_opera_hour_kpi_new_regi_device_num > 0) {
            params_base_opera_hour_kpi.+=(Array[Any](parent_game_id, child_game_id, publish_time + ":00:00", base_opera_hour_kpi_new_regi_device_num, os, group_id))
            JdbcUtil.executeUpdate(ps_base_opera_hour_kpi, params_base_opera_hour_kpi, conn)
          }

          if (!(base_opera_kpi_new_regi_device_num == 0 && base_opera_kpi_new_regi_account_num == 0 && base_opera_kpi_regi_device_num == 0)) {
            params_base_opera_kpi.+=(Array[Any](parent_game_id, child_game_id, publish_time.substring(0, 10), base_opera_kpi_new_regi_device_num, base_opera_kpi_new_regi_account_num, base_opera_kpi_regi_device_num, os, group_id))
            JdbcUtil.executeUpdate(ps_base_opera_kpi, params_base_opera_kpi, conn)
          }

        })
        //关闭数据库链接
        stmt.close()
        pstat_sql_deatil.close()
        ps_basekpi.close()
        ps_base_day_kpi.close()
        ps_base_opera_hour_kpi.close()
        ps_base_opera_kpi.close()
        conn.close
        pool.returnResource(jedis)

      }
    })
  }


}
