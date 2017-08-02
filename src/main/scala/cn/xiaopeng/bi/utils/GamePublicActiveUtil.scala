package cn.xiaopeng.bi.utils

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}

/**
  * Created by bigdata on 17-8-2.
  */
object GamePublicActiveUtil {


  def loadActiveInfo(rdd: RDD[String], hiveContext: HiveContext) = {
    //一、数据准备  将日志转化为表
    val activeRdd = rdd.filter(t => {
      val arr = t.split("\\|");
      arr(0).contains("bi_active") && arr.length >= 9 && arr(5).length >= 13
    }).map(t => {
      val arr: Array[String] = t.split("\\|");
      Row(arr(1).toInt, StringUtils.getArrayChannel(arr(4))(0), StringUtils.getArrayChannel(arr(4))(1), StringUtils.getArrayChannel(arr(4))(2), arr(5), arr(8))
    }).cache()
    val activeStruct = (new StructType).add("game_id", IntegerType).add("parent_channel", StringType).add("child_channel", StringType).add("ad_label", StringType).add("active_time", StringType).add("imei", StringType);
    val activeDF = hiveContext.createDataFrame(activeRdd, activeStruct)
    activeDF.registerTempTable("ods_active_cache")

    //二、过滤和补全字段
    hiveContext.sql("use yyft")
    val sql_bi_active = "select distinct \nlastPubGame.child_game_id as child_game_id,  \nods_active_cache.parent_channel as parent_channel,  \nods_active_cache.child_channel as child_channel,  \nods_active_cache.ad_label as ad_label,  \nods_active_cache.active_time as active_time,  \nods_active_cache.imei as imei  \nfrom  \n(select distinct game_id,parent_channel,child_channel,ad_label, substr(min(active_time),0,13)  as active_time,imei from ods_active_cache group by game_id,parent_channel,child_channel,ad_label,imei) ods_active_cache  \njoin (select distinct game_id as child_game_id  from lastPubGame) lastPubGame on ods_active_cache.game_id=lastPubGame.child_game_id"
    val df_bi_active: DataFrame = hiveContext.sql(sql_bi_active)
    foreachRealPartition(df_bi_active)

  }

  def foreachRealPartition(df_bi_active: DataFrame) = {
    df_bi_active.foreachPartition(iter => {
      //TODO 激活表先停下来
    })

  }

}
