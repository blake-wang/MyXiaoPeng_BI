package cn.xiaopeng.bi.gamepublish

import cn.xiaopeng.bi.utils.{JdbcUtil, SparkUtils}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer

/**
  * Created by bigdata on 17-7-24.
  * 注册设备统计  --运营报表
  *
  */
object GamePublishDeviceRetained {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)
    val currentday = args(0)
    // 创建各种上下文
    val sparkConf = new SparkConf().setAppName(this.getClass.getName.replace("$", ""))
      .set("spark.default.parallelism", "60")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.shuffle.consolidateFiles", "true")
      .set("spark.storage.memoryFraction", "0.4")
      .set("spark.streaming.stopGracefullyOnShutdown", "true");
    SparkUtils.setMaster(sparkConf);
    val sc = new SparkContext(sparkConf)
    val sqlContext = new HiveContext(sc)

    sqlContext.sql("use yyft")
    val devRetainedSql = " select    \n rs.reg_time,   \n rs.game_id, \n rs.parent_game_id,\n rs.system_type,\n rs.group_id,\n sum(CASE rs.dur WHEN 1 THEN 1 ELSE 0 END ) as retained_1day,\n sum(CASE rs.dur WHEN 2 THEN 1 ELSE 0 END ) as retained_2day,\n sum(CASE rs.dur WHEN 3 THEN 1 ELSE 0 END ) as retained_3day,\n sum(CASE rs.dur WHEN 4 THEN 1 ELSE 0 END ) as retained_4day,\n sum(CASE rs.dur WHEN 5 THEN 1 ELSE 0 END ) as retained_5day,\n sum(CASE rs.dur WHEN 6 THEN 1 ELSE 0 END ) as retained_6day,\n sum(CASE rs.dur WHEN 7 THEN 1 ELSE 0 END ) as retained_7day,\n sum(CASE rs.dur WHEN 8 THEN 1 ELSE 0 END ) as retained_8day,\n sum(CASE rs.dur WHEN 9 THEN 1 ELSE 0 END ) as retained_9day,\n sum(CASE rs.dur WHEN 10 THEN 1 ELSE 0 END ) as retained_10day,\n sum(CASE rs.dur WHEN 11 THEN 1 ELSE 0 END ) as retained_11day,\n sum(CASE rs.dur WHEN 12 THEN 1 ELSE 0 END ) as retained_12day,\n sum(CASE rs.dur WHEN 13 THEN 1 ELSE 0 END ) as retained_13day,\n sum(CASE rs.dur WHEN 14 THEN 1 ELSE 0 END ) as retained_14day,\n sum(CASE rs.dur WHEN 29 THEN 1 ELSE 0 END ) as retained_29day \n FROM (  \n select distinct sdk.parent_game_id,sdk.system_type,sdk.group_id,filterOdsR.reg_time,filterOdsR.game_id, datediff(odsl.login_time,filterOdsR.reg_time) dur,filterOdsR.imei from \n (select to_date(odsr.reg_time) reg_time,odsr.game_id,odsr.imei from ods_regi_rz odsr where to_date(odsr.reg_time)<='currentday' and to_date (odsr.reg_time) >= date_add('currentday',-29) and game_id is not null and game_account is not null )\n filterOdsR    \n join (select  distinct game_id as parent_game_id ,old_game_id,system_type,group_id from game_sdk  where state=0) sdk on sdk.old_game_id=filterOdsR.game_id       \n join (select distinct to_date(login_time) login_time,imei,game_id from ods_login) odsl on filterOdsR.game_id = odsl.game_id  and filterOdsR.imei = odsl.imei where to_date(odsl.login_time) > filterOdsR.reg_time  and to_date(odsl.login_time)<'currentday'  and datediff(odsl.login_time,filterOdsR.reg_time) in(1,2,3,4,5,6,7,8,9,10,11,12,13,14,29) \n ) rs  \n group BY rs.reg_time,rs.game_id,rs.parent_game_id,rs.system_type,rs.group_id"
    val devRetainedexecSql = devRetainedSql.replace("currentday", currentday)
    val operaDeviceRetainedDf = sqlContext.sql(devRetainedexecSql)
    operaDeviceRetainedForeachPartition(operaDeviceRetainedDf)

    sc.stop()

  }


  private def operaDeviceRetainedForeachPartition(deviceRetainedDf: DataFrame) = {
    deviceRetainedDf.foreachPartition(rows => {

      val conn = JdbcUtil.getConn()
      val statement = conn.createStatement

      val sqlText = "insert into bi_gamepublic_opera_actions(\npublish_date,\nchild_game_id,\nparent_game_id,\nos,\ngroup_id,\ndev_retained_2day,\ndev_retained_3day,\ndev_retained_4day,\ndev_retained_5day,\ndev_retained_6day,\ndev_retained_7day,\ndev_retained_8day,\ndev_retained_9day,\ndev_retained_10day,\ndev_retained_11day,\ndev_retained_12day,\ndev_retained_13day,\ndev_retained_14day,\ndev_retained_15day,\ndev_retained_30day)\nvalues(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)\non duplicate key update \nparent_game_id=VALUES(parent_game_id),\nos=VALUES(os),\ngroup_id=VALUES(group_id),\ndev_retained_2day=VALUES(dev_retained_2day),\ndev_retained_3day=VALUES(dev_retained_3day),\ndev_retained_4day=VALUES(dev_retained_4day),\ndev_retained_5day=VALUES(dev_retained_5day),\ndev_retained_6day=VALUES(dev_retained_6day),\ndev_retained_7day=VALUES(dev_retained_7day),\ndev_retained_8day=VALUES(dev_retained_8day),\ndev_retained_9day=VALUES(dev_retained_9day),\ndev_retained_10day=VALUES(dev_retained_10day),\ndev_retained_11day=VALUES(dev_retained_11day),\ndev_retained_12day=VALUES(dev_retained_12day),\ndev_retained_13day=VALUES(dev_retained_13day),\ndev_retained_14day=VALUES(dev_retained_14day),\ndev_retained_15day=VALUES(dev_retained_15day),\ndev_retained_30day=VALUES(dev_retained_30day)"
      val params = new ArrayBuffer[Array[Any]]()
      for (insertedRow <- rows) {
        params.+=(Array[Any](insertedRow.get(0), insertedRow.get(1), insertedRow.get(2),
          insertedRow.get(3), insertedRow.get(4), insertedRow.get(5), insertedRow.get(6),
          insertedRow.get(7), insertedRow.get(8), insertedRow.get(9), insertedRow(10),
          insertedRow(11), insertedRow(12), insertedRow(13), insertedRow(14), insertedRow(15), insertedRow(16), insertedRow(17), insertedRow(18), insertedRow(19)
        ))
      }
      try {
        JdbcUtil.doBatch(sqlText, params, conn)
      } finally {
        statement.close()
        conn.close
      }
    })
  }
}
