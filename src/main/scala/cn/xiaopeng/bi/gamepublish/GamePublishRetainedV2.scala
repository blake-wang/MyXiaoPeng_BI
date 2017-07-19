package cn.xiaopeng.bi.gamepublish

import javax.security.auth.login.Configuration

import cn.wanglei.bi.ConfigurationUtil
import cn.xiaopeng.bi.utils.{StringUtils, JdbcUtil, DateUtils}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkContext, SparkConf}

import scala.collection.mutable.ArrayBuffer

/**
 * Created by JSJSB-0071 on 2017/7/13.
 * 游戏发布数据报表统计 -- 留存
 */
object GamePublishRetainedV2 {
  def main(args: Array[String]) {
    if (args.length < 1) {
      System.err.println("Usage + <currentday>")
      System.exit(1)
    }
    if (args.length > 1) {
      System.err.println("参数传入个数太多，固定为1个：<currentday>")
      System.exit(1)
    }

    //跑数日期
    val currentday = args(0)
    val hivesql = "select \n  " +
      "retained_tmp.reg_time,\n  " +
      "retained_tmp.game_id,\n  " +
      "if(retained_tmp.channel_id = '','21',retained_tmp.channel_id) ,\n  " +
      "reg_count.reg_sum add_user_count,\n  retained_tmp.retained_1day,\n  " +
      "retained_tmp.retained_2day,\n  " +
      "retained_tmp.retained_3day,\n  " +
      "retained_tmp.retained_4day,\n  " +
      "retained_tmp.retained_5day,\n  " +
      "retained_tmp.retained_6day,\n  " +
      "retained_tmp.retained_14day,\n  " +
      "retained_tmp.retained_29day,\n  " +
      "retained_tmp.retained_59day\n " +
      "from(select rs.reg_time rs.game_id,expand_channel channel_id, count(distinct rs.game_account) add_user_count," +
      "sum(CASE rs.dur WHEN 1 THEN 1 ELSE 0 END ) as retained_1day," +
      "sum(CASE rs.dur WHEN 2 THEN 1 ELSE 0 END ) as retained_2day," +
      "sum(CASE rs.dur WHEN 3 THEN 1 ELSE 0 END ) as retained_3day," +
      "sum(CASE rs.dur WHEN 4 THEN 1 ELSE 0 END ) as retained_4day," +
      "sum(CASE rs.dur WHEN 5 THEN 1 ELSE 0 END ) as retained_5day," +
      "sum(CASE rs.dur WHEN 6 THEN 1 ELSE 0 END ) as retained_6day," +
      "sum(CASE rs.dur WHEN 14 THEN 1 ELSE 0 END ) as retained_14day," +
      "sum(CASE rs.dur WHEN 29 THEN 1 ELSE 0 END ) as retained_29day," +
      "sum(CASE rs.dur WHEN 59 THEN 1 ELSE 0 END ) as retained_59day" +
      "FROM" +
      "(select distinct filterOdsR.reg_time,filterOdsR.game_id,if(filterOdsR.expand_channel = '','21',filterOdsR.expand_channel) expand_channel, datediff(odsl.login_time,filterOdsR.reg_time) dur,filterOdsR.game_account from (\n    select to_date(odsr.reg_time) reg_time,odsr.game_id,odsr.expand_channel,odsr.game_account from ods_regi_rz odsr " +
      "where to_date(odsr.reg_time)<='currentday' and to_date (odsr.reg_time) >= date_add(\"currentday\",-59)\n    )filterOdsR\n      join ods_login odsl on filterOdsR.game_account = odsl.game_account\n    where to_date(odsl.login_time) > filterOdsR.reg_time and datediff(odsl.login_time,filterOdsR.reg_time) in(1,2,3,4,5,6,14,29,59)\n) rs \ngroup BY rs.reg_time,rs.game_id,rs.expand_channel\n) retained_tmp\n  join (select to_date(odsr.reg_time) reg_time,odsr.game_id,if(odsr.expand_channel = '','21',odsr.expand_channel) expand_channel,count(odsr.game_account) reg_sum from ods_regi_rz odsr \n         where to_date(odsr.reg_time)<='currentday' and to_date (odsr.reg_time) >= date_add(\"currentday\",-59)\n         group by to_date(odsr.reg_time),game_id,if(odsr.expand_channel = '','21',odsr.expand_channel)) reg_count\n  on retained_tmp.reg_time = reg_count.reg_time and retained_tmp.game_id = reg_count.game_id and retained_tmp.channel_id = reg_count.expand_channel\n  join (select distinct game_id from ods_publish_game) pg on retained_tmp.game_id = pg.game_id;"
    val execSql = hivesql.replace("currentday", currentday)


    //hive库操作
    val sparkConf = new SparkConf().setAppName(this.getClass.getName.replace("$", ""))
      .set("spark.memory.storageFraction", ConfigurationUtil.getProperty("spark.memory.storageFraction"))
      .set("spark.sql.shuffle.partitions", ConfigurationUtil.getProperty("spark.memory.storageFraction"))
    val sc = new SparkContext(sparkConf)
    val sqlContext = new HiveContext(sc)
    sqlContext.sql("use yyft;")
    val dataf = sqlContext.sql(execSql)

    //数据库操作
    val date = DateUtils.getNowDate
    dataf.foreachPartition(rows => {
      val conn = JdbcUtil.getConn()
      val statement = conn.createStatement()
      val sqlText = " insert into bi_gamepublic_retainedltv(reg_time,game_id,parent_channel,child_channel,ad_label,add_user_num,retained_1day,retained_2day,retained_3day,retained_4day,retained_5day," +
        "retained_6day,retained_14day, retained_29day,retained_59day)" +
        " values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)" +
        " on duplicate key update add_user_num=?,retained_1day=?,retained_2day=?,retained_3day=?,retained_4day=?,retained_5day=?,retained_6day=?,retained_14day=?,retained_29day=?," +
        "retained_59day=?;"
      val params = new ArrayBuffer[Array[Any]]()
      for (insertedRow <- rows) {
        val channelArray = StringUtils.getArrayChannel(insertedRow.get(2).toString)
        if (channelArray(0).length <= 10 && channelArray(1).length <= 10 && channelArray(2).length <= 12) {
          params.+=(Array[Any](insertedRow.get(0),insertedRow.get(1),
          channelArray(0),channelArray(1),channelArray(2),
          insertedRow.get(3),insertedRow.get(4),insertedRow.get(5),insertedRow.get(6),
          insertedRow.get(7),insertedRow.get(8), insertedRow.get(9), insertedRow.get(10),
            insertedRow.get(11), insertedRow.get(12),
            insertedRow.get(3),insertedRow.get(4), insertedRow.get(5), insertedRow.get(6)
            , insertedRow.get(7), insertedRow.get(8), insertedRow.get(9), insertedRow.get(10),
            insertedRow.get(11), insertedRow.get(12)
          ))
        }
      }
      JdbcUtil.doBatch(sqlText, params, conn)
      statement.close()
      conn.close
    })

    System.clearProperty("spark.driver.port")
    sc.stop
  }


}
