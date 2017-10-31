package cn.xiaopeng.bi.gamepublish

import java.sql.PreparedStatement

import cn.xiaopeng.bi.utils.{DimensionUtil, JdbcUtil}
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.hive.HiveContext

/**
  * Created by bigdata on 17-8-23.
  */
object GamePublish2Active {




  def main(args: Array[String]): Unit = {
    val currentday = args(0)
    /**
      * 新增注册-活跃分析
      */
    var fxGameLoginSql = "select lg.game_account,\nlg.game_id,\nlg.login_time,\nif((split(channel_expand,'_')[0] is null or split(channel_expand,'_')[0]=''),'21',split(channel_expand,'_')[0]) as parent_channel   ,\nif(split(lg.channel_expand,'_')[1] is null,'',split(lg.channel_expand,'_')[1]) child_channel,\nif(split(lg.channel_expand,'_')[2] is null,'',split(lg.channel_expand,'_')[2]) ad_label,\nlg.imei\nfrom ods_login lg join (select distinct game_id from ods_publish_game) pg on lg.game_id=pg.game_id where to_date(lg.login_time)='currentday' "
      .replace("currentday",currentday)
    val sparkConf = new SparkConf().setAppName(this.getClass.getName.replace("$", ""))
    val sc = new SparkContext(sparkConf)
    val sqlContext = new HiveContext(sc)
    //加载活跃数据
    sqlContext.sql("use yyft")
    sqlContext.sql(fxGameLoginSql).coalesce(10).registerTempTable("tb_fxgamelogin")
    sqlContext.cacheTable("tb_fxgamelogin")
    //加载 新增设备并登录 数据到临时表
    var newDevLoginSql = "select grd.game_id,grd.parent_channel,grd.child_channel,grd.ad_label,grd.imei,count(lg.game_account) cs\nfrom bi_gamepublic_regi_detail grd join tb_fxgamelogin lg on lg.imei=grd.imei and grd.ad_label=lg.ad_label and lg.parent_channel=grd.parent_channel and lg.child_channel=grd.child_channel\nwhere substring(regi_hour,0,10)='currentday' and substring(regi_hour,0,10)>'2017-06-01'\ngroup by grd.game_id,grd.ad_label,grd.imei,grd.parent_channel,grd.child_channel"
    newDevLoginSql = newDevLoginSql.replace("currentday", currentday)
    sqlContext.sql(newDevLoginSql).registerTempTable("tb_newdevlogin")
    //用户登录次数分布
    val devFbSql = "select \n'currentday' as publish_date,\nrs.game_id child_game_id,\nrs.parent_channel medium_channel,\nrs.child_channel ad_site_channel,\nrs.ad_label pkg_code,\nif(sdk.game_id is null,0,sdk.game_id) as parent_game_id,\nif(sdk.group_id is null,0,sdk.group_id) group_id,\nif(sdk.system_type is null,0,sdk.system_type)  as os,\nsum(case when cs=1 then 1 else 0 end) as dev_add_lg_1,\nsum(case when cs=2 then 1 else 0 end) as dev_add_lg_2,\nsum(case when cs=3 then 1 else 0 end) as dev_add_lg_3,\nsum(case when cs=4 then 1 else 0 end) as dev_add_lg_4,\nsum(case when cs=5 then 1 else 0 end) as dev_add_lg_5,\nsum(case when cs>=6 and cs<=10 then 1 else 0 end) as dev_add_lg_6_10,\nsum(case when cs>=11 then 1 else 0 end) as dev_add_lg_11\nfrom \ntb_newdevlogin rs join yyft.game_sdk sdk on sdk.old_game_id=rs.game_id \ngroup by rs.game_id,rs.ad_label,rs.parent_channel,rs.child_channel,sdk.game_id,sdk.group_id,sdk.system_type".replace("currentday", currentday)
    val istDevFbSql = "insert into bi_gamepublic_actions(publish_date,child_game_id,medium_channel,ad_site_channel,pkg_code,parent_game_id,group_id,os,dev_add_lg_1,dev_add_lg_2,dev_add_lg_3,dev_add_lg_4,dev_add_lg_5,dev_add_lg_6_10,dev_add_lg_11)\n values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) on duplicate key update dev_add_lg_1=?,dev_add_lg_2=?,dev_add_lg_3=?,dev_add_lg_4=?,dev_add_lg_5=?,dev_add_lg_6_10=?,dev_add_lg_11=?"
    val devFbDf = sqlContext.sql(devFbSql)
    processDbFct(devFbDf,istDevFbSql)

    //总次数
    val totalDevsSql = "select \n'currentday' as publish_date,\nrs.game_id child_game_id,\nrs.parent_channel medium_channel,\nrs.child_channel ad_site_channel,\nrs.ad_label pkg_code,\nif(sdk.game_id is null,0,sdk.game_id) as parent_game_id,\nif(sdk.group_id is null,0,sdk.group_id) group_id,\nif(sdk.system_type is null,0,sdk.system_type)  as os,\nsum(cs)   dev_add_lg_times\nfrom \ntb_newdevlogin rs join yyft.game_sdk sdk on sdk.old_game_id=rs.game_id \ngroup by rs.game_id,rs.ad_label,rs.parent_channel,rs.child_channel,sdk.game_id,sdk.group_id,sdk.system_type".replace("currentday", currentday)
    val istTotalDevsSql = "insert into bi_gamepublic_actions(publish_date,child_game_id,medium_channel,ad_site_channel,pkg_code,parent_game_id,group_id,os,dev_add_lg_times)\n values(?,?,?,?,?,?,?,?,?) on duplicate key update dev_add_lg_times=?"
    val totalDevDf = sqlContext.sql(totalDevsSql)
    processDbFct(totalDevDf, istTotalDevsSql)

    /**
      * 所有用户-活跃分析
      */
    /*按天统计分布*/
    val allDevLoginSql = "select \n'currentday' as publish_date,\nrs.game_id child_game_id,\nrs.parent_channel medium_channel,\nrs.child_channel ad_site_channel,\nrs.ad_label pkg_code,\nif(sdk.game_id is null,0,sdk.game_id) as parent_game_id,\nif(sdk.group_id is null,0,sdk.group_id) group_id,\nif(sdk.system_type is null,0,sdk.system_type)  as os,\nsum(case when cs=1 then 1 else 0 end) as dau_lg_1,\nsum(case when cs=2 then 1 else 0 end) as dau_lg_2,\nsum(case when cs=3 then 1 else 0 end) as dau_lg_3,\nsum(case when cs=4 then 1 else 0 end) as dau_lg_4,\nsum(case when cs=5 then 1 else 0 end) as dau_lg_5,\nsum(case when cs>=6 and cs<=10 then 1 else 0 end) as dau_lg_6_10,\nsum(case when cs>=11 then 1 else 0 end) as dau_lg_11\nfrom \n(select game_id,parent_channel,child_channel,ad_label,imei,count(game_account) cs from tb_fxgamelogin group by game_id,parent_channel,child_channel,ad_label,imei) rs \njoin yyft.game_sdk sdk on sdk.old_game_id=rs.game_id \ngroup by rs.game_id,rs.ad_label,rs.parent_channel,rs.child_channel,sdk.game_id,sdk.group_id,sdk.system_type".replace("currentday", currentday)
    val istAllDevLogin = "insert into bi_gamepublic_actions(publish_date,child_game_id,medium_channel,ad_site_channel,pkg_code,parent_game_id,group_id,os,dau_lg_1,dau_lg_2,dau_lg_3,dau_lg_4,dau_lg_5,dau_lg_6_10,dau_lg_11)\n values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) on duplicate key update dau_lg_1=?,dau_lg_2=?,dau_lg_3=?,dau_lg_4=?,dau_lg_5=?,dau_lg_6_10=?,dau_lg_11=?"
    val allDevLoginDf = sqlContext.sql(allDevLoginSql)
    processDbFct(allDevLoginDf, istAllDevLogin)

    val allDevLoginTimesSql = "select \n'currentday' as publish_date,\nrs.game_id child_game_id,\nrs.parent_channel medium_channel,\nrs.child_channel ad_site_channel,\nrs.ad_label pkg_code,\nif(sdk.game_id is null,0,sdk.game_id) as parent_game_id,\nif(sdk.group_id is null,0,sdk.group_id) group_id,\nif(sdk.system_type is null,0,sdk.system_type)  as os,\ncount(game_account) dau_lg_times \nfrom tb_fxgamelogin rs\njoin  yyft.game_sdk sdk on sdk.old_game_id=rs.game_id \n group by rs.game_id,rs.ad_label,rs.parent_channel,rs.child_channel,sdk.game_id,sdk.group_id,sdk.system_type ".replace("currentday", currentday)
    val istAllDevLoginTimes = "insert into bi_game/*总登陆次数*/public_actions(publish_date,child_game_id,medium_channel,ad_site_channel,pkg_code,parent_game_id,group_id,os,dau_lg_times)\n values(?,?,?,?,?,?,?,?,?) on duplicate key update dau_lg_times=?"
    val allDevLoginTimesDf = sqlContext.sql(allDevLoginTimesSql)
    processDbFct(allDevLoginTimesDf, istAllDevLoginTimes)

    //新增注册设备，DAU设备数，从基础表来
    newRegiDevLogin(currentday)

    //推送维度表
    DimensionUtil.processDbDim(sqlContext,currentday)

    System.clearProperty("spark.driver.port")
    sc.stop()
    sc.clearCallSite()

  }

  def processDbFct(dataf: DataFrame, insertSql: String): Unit = {
    //全部转为小写，后面好判断
    val sql2Mysql = insertSql.replace("|", " ").toLowerCase
    //获取values（）里面有多少个?参数，有利于后面的循环
    val startValuesIndex = sql2Mysql.indexOf("(?") + 1
    val endValuesIndex = sql2Mysql.indexOf("?)") + 1
    //values中的个数
    val valueArray: Array[String] = sql2Mysql.substring(startValuesIndex, endValuesIndex).split(",")
    //两个（？？）中间的值
    //条件中的参数个数
    val wh: Array[String] = sql2Mysql.substring(sql2Mysql.indexOf("update") + 6).split(",")
    //找update后面的字符串再判断
    //查找需要insert的字段
    val cols_ref = sql2Mysql.substring(0, sql2Mysql.lastIndexOf("(?"))
    //获取（?特殊字符前的字符串，然后再找字段
    val cols: Array[String] = cols_ref.substring(cols_ref.lastIndexOf("(") + 1, cols_ref.lastIndexOf(")")).split(",")

    /** ******************数据库操作 *******************/
    dataf.foreachPartition((rows: Iterator[Row]) => {
      val conn = JdbcUtil.getConn()
      val ps: PreparedStatement = conn.prepareStatement(sql2Mysql)
      for (x <- rows) {
        //补充value值
        for (rs <- 0 to valueArray.length - 1) {
          ps.setString(rs.toInt + 1, x.get(rs).toString)
        }
        //补充条件
        for (i <- 0 to wh.length - 1) {
          val rs = wh(i).trim.substring(0, wh(i).trim.lastIndexOf("="))
          for (ii <- 0 to cols.length - 1) {
            if (cols(ii).trim.equals(rs)) {
              ps.setString(i.toInt + valueArray.length.toInt + 1, x.get(ii).toString)
            }
          }
        }
        ps.executeUpdate()
      }
      conn.close()
    }
    )
  }

  //新增注册设备，DAU设备数
  def newRegiDevLogin(currentday: String): Unit = {
    val sql = " insert into bi_gamepublic_actions(publish_date,parent_game_id,child_game_id,medium_channel,ad_site_channel,pkg_code,medium_account,\n              promotion_channel,promotion_mode,head_people,group_id,os,dau_dev_num,dev_add_regi) \n              select publish_date,parent_game_id,child_game_id,medium_channel,ad_site_channel,pkg_code,medium_account,\n              promotion_channel,promotion_mode,head_people,group_id,os,dau_device_num ,new_regi_device_num \n              from bi_gamepublic_base_day_kpi rs where publish_date='currentday' on DUPLICATE key update dau_dev_num=dau_device_num,dev_add_regi=new_regi_device_num,\n                  os=rs.os,child_game_id=rs.child_game_id,medium_channel=rs.medium_channel,\n                ad_site_channel=rs.ad_site_channel,medium_account=rs.medium_account,promotion_channel=rs.promotion_channel,promotion_mode=rs.promotion_mode,head_people=rs.head_people,ad_site_channel=rs.ad_site_channel".replace("currentday", currentday)
    val conn = JdbcUtil.getConn()
    val sta = conn.createStatement()
    sta.executeUpdate(sql)
    sta.close()
    conn.close()
  }
}
