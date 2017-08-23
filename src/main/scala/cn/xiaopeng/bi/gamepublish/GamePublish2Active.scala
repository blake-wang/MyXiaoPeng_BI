package cn.xiaopeng.bi.gamepublish

import org.apache.spark.sql.DataFrame
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.hive.HiveContext

/**
  * Created by bigdata on 17-8-23.
  */
object GamePublish2Active {


  def main(args: Array[String]): Unit = {
    val currentday = args(0)

    //加载发行游戏的登录数据，然后注册成临时表
    var fxGameLoginSql = "select lg.game_account,\nlg.game_id,\nlg.login_time,\nif((split(channel_expand,'_')[0] is null or split(channel_expand,'_')[0]=''),'21',split(channel_expand,'_')[0]) as parent_channel   ,\nif(split(lg.channel_expand,'_')[1] is null,'',split(lg.channel_expand,'_')[1]) child_channel,\nif(split(lg.channel_expand,'_')[2] is null,'',split(lg.channel_expand,'_')[2]) ad_label,\nlg.imei\nfrom ods_login lg join (select distinct game_id from ods_publish_game) pg on lg.game_id=pg.game_id where to_date(lg.login_time)='currentday' "
    fxGameLoginSql = fxGameLoginSql.replace("currentday", currentday)
    val sparkConf = new SparkConf().setAppName(this.getClass.getName.replace("$", ""))
    val sparkContext = new SparkContext(sparkConf)
    val sqlContext = new HiveContext(sparkContext)

    //加载活跃数据
    sqlContext.sql("use yyft")
    sqlContext.sql(fxGameLoginSql).coalesce(10).registerTempTable("tb_fxgamelogin")
    sqlContext.cacheTable("tb_fxgamelogin")

    //加载新增设备并登录数据到临时表
    var newDevLoginSql = "select grd.game_id,grd.parent_channel,grd.child_channel,grd.ad_label,grd.imei,count(lg.game_account) cs\nfrom bi_gamepublic_regi_detail grd join tb_fxgamelogin lg on lg.imei=grd.imei and grd.ad_label=lg.ad_label and lg.parent_channel=grd.parent_channel and lg.child_channel=grd.child_channel\nwhere substring(regi_hour,0,10)='currentday' and substring(regi_hour,0,10)>'2017-06-01'\ngroup by grd.game_id,grd.ad_label,grd.imei,grd.parent_channel,grd.child_channel"
    newDevLoginSql = newDevLoginSql.replace("currentday",currentday)
    sqlContext.sql(newDevLoginSql).registerTempTable("tb_newdevlogin")

    //用户登录次数分布
    val devFbSql = "select \n'currentday' as publish_date,\nrs.game_id child_game_id,\nrs.parent_channel medium_channel,\nrs.child_channel ad_site_channel,\nrs.ad_label pkg_code,\nif(sdk.game_id is null,0,sdk.game_id) as parent_game_id,\nif(sdk.group_id is null,0,sdk.group_id) group_id,\nif(sdk.system_type is null,0,sdk.system_type)  as os,\nsum(case when cs=1 then 1 else 0 end) as dev_add_lg_1,\nsum(case when cs=2 then 1 else 0 end) as dev_add_lg_2,\nsum(case when cs=3 then 1 else 0 end) as dev_add_lg_3,\nsum(case when cs=4 then 1 else 0 end) as dev_add_lg_4,\nsum(case when cs=5 then 1 else 0 end) as dev_add_lg_5,\nsum(case when cs>=6 and cs<=10 then 1 else 0 end) as dev_add_lg_6_10,\nsum(case when cs>=11 then 1 else 0 end) as dev_add_lg_11\nfrom \ntb_newdevlogin rs join yyft.game_sdk sdk on sdk.old_game_id=rs.game_id \ngroup by rs.game_id,rs.ad_label,rs.parent_channel,rs.child_channel,sdk.game_id,sdk.group_id,sdk.system_type".replace("currentday",currentday)
    val istDevFbSql = "c"
    val devFbDf = sqlContext.sql(devFbSql)
    processDbFct(devFbDf,istDevFbSql)


  }

  def processDbFct(dataf: DataFrame, insertSql: String): Unit = {

  }

}
