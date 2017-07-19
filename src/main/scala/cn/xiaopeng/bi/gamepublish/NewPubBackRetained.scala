package cn.xiaopeng.bi.gamepublish

import cn.wanglei.bi.ConfigurationUtil
import cn.xiaopeng.bi.utils.{StringUtils, JdbcUtil}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkContext, SparkConf}

import scala.collection.mutable.ArrayBuffer

/**
 * Created by JSJSB-0071 on 2017/7/13.
 */
object NewPubBackRetained {
  def main(args: Array[String]) {
    val currentday = args(0)

    //hive操作
    val sparkConf = new SparkConf().setAppName(this.getClass.getName.replace("$", ""))
      .set("spark.memory.storageFraction", ConfigurationUtil.getProperty("spark.memory.storageFraction"))
      .set("spark.sql.shuffle.partitions", ConfigurationUtil.getProperty("spark.sql.shuffle.partitions"))
    val sc = new SparkContext(sparkConf)
    val sqlContext = new HiveContext(sc)

    sqlContext.read.parquet(ConfigurationUtil.getProperty("fxdim.parquet")).registerTempTable("fxdim")
    sqlContext.sql("use yyft;")
    sqlContext.sql("select distinct game_id from ods_publish_game").registerTempTable("publish_game_tmp")
    sqlContext.cacheTable("publish_game_tmp")

    //获取30天注册
    sqlContext.sql("select r.game_id,max(if(expand_channel = '' or expand_channel is null,'21',expand_channel)) expand_channel,imei,game_account,min(reg_time) reg_time " +
      "from ods_regi_rz r " +
      "join publish_game_tmp pg on r.game_id=pg.game_id " +
      "where to_date(reg_time) <= '" + currentday + "' and to_date (reg_time) >= date_add('" + currentday + "',-29)" + " group by r.game_id,imei,game_account")
      .registerTempTable("regi_30day")
    //获取30登录
    sqlContext.sql("select l.* " +
      "from " +
      "(select game_id, if(channel_expand = ''  or channel_expand is null ,'21',channel_expand) expand_channel,imei,game_account,to_date(login_time) login_time from ods_login) l " +
      "join publish_game_tmp pg on l.game_id = pg.game_id where to_date(login_time) <= '" + currentday + "' and to_date (login_time) >= date_add('" + currentday + "',-29)")
      .registerTempTable("login_30day")

    //按设备统计留存
    //获取近30天注册设备
    sqlContext.sql("select game_id,expand_channel,imei,to_date(min(reg_time)) reg_time from regi_30day group by game_id,expand_channel,imei")
      .registerTempTable("new_regi_device_tmp")

    //获取设备存留明细
    sqlContext.sql("select distinct r.reg_time,r.game_id,r.expand_channel,r.imei,datediff(l.login_time,r.reg_time) dur from new_regi_device_tmp r join login_30day l on r.imei=l.imei where l.login_time>r.reg_time " +
      "and datediff(l.login_time,r.reg_time) in (1,2,3,4,5,6,7,8,9,10,11,12,13,14,29)").registerTempTable("new_regi_device_diff_tmp")
    sqlContext.sql("select rd.reg_time,rd.game_id,rd.expand_channel,\nsum(CASE rd.dur WHEN 1 THEN 1 ELSE 0 END ) as retained_1day,\nsum(CASE rd.dur WHEN 2 THEN 1 ELSE 0 END ) as retained_2day,\nsum(CASE rd.dur WHEN 3 THEN 1 ELSE 0 END ) as retained_3day,\nsum(CASE rd.dur WHEN 4 THEN 1 ELSE 0 END ) as retained_4day,\nsum(CASE rd.dur WHEN 5 THEN 1 ELSE 0 END ) as retained_5day,\nsum(CASE rd.dur WHEN 6 THEN 1 ELSE 0 END ) as retained_6day,\nsum(CASE rd.dur WHEN 7 THEN 1 ELSE 0 END ) as retained_7day,\nsum(CASE rd.dur WHEN 8 THEN 1 ELSE 0 END ) as retained_8day,\nsum(CASE rd.dur WHEN 9 THEN 1 ELSE 0 END ) as retained_9day,\nsum(CASE rd.dur WHEN 10 THEN 1 ELSE 0 END ) as retained_10day,\nsum(CASE rd.dur WHEN 11 THEN 1 ELSE 0 END ) as retained_11day,\nsum(CASE rd.dur WHEN 12 THEN 1 ELSE 0 END ) as retained_12day,\nsum(CASE rd.dur WHEN 13 THEN 1 ELSE 0 END ) as retained_13day,\nsum(CASE rd.dur WHEN 14 THEN 1 ELSE 0 END ) as retained_14day,\nsum(CASE rd.dur WHEN 29 THEN 1 ELSE 0 END ) as retained_29day\nfrom new_regi_device_diff_tmp rd join game_sdk sdk on sdk.old_game_id=rd.game_id group by rd.reg_time,rd.game_id,rd.expand_channel")
      .registerTempTable("result_retained_tmp")

    val deviceRetainedDf = sqlContext.sql("select rrt.*,f.promotion_channel,f.medium_account,f.promotion_mode,f.head_people,gs.game_id parent_game_id,gs.system_type os,gs.group_id " +
      "from result_retained_tmp rrt left join fxdim f on split(rrt.expand_channel,'_')[2] = f.pkg_code and rrt.reg_time>=f.mstart_date and rrt.reg_time<=f.mend_date and rrt.reg_time>=f.astart_date and rrt.reg_time<=f.aend_date " +
      "left join game_sdk gs on rrt.game_id = gs.old_game_id ")
    deviceRetainedForeachPartition(deviceRetainedDf)

    //按帐号统计留存
    val accountRetainedSql = "select \n  rs.reg_time,\n  rs.game_id,\n  expand_channel expand_channel,\n  sum(CASE rs.dur WHEN 1 THEN 1 ELSE 0 END ) as retained_1day,\n  sum(CASE rs.dur WHEN 2 THEN 1 ELSE 0 END ) as retained_2day,\n  sum(CASE rs.dur WHEN 3 THEN 1 ELSE 0 END ) as retained_3day,\n  sum(CASE rs.dur WHEN 4 THEN 1 ELSE 0 END ) as retained_4day,\n  sum(CASE rs.dur WHEN 5 THEN 1 ELSE 0 END ) as retained_5day,\n  sum(CASE rs.dur WHEN 6 THEN 1 ELSE 0 END ) as retained_6day,\n  sum(CASE rs.dur WHEN 7 THEN 1 ELSE 0 END ) as retained_7day,\n  sum(CASE rs.dur WHEN 8 THEN 1 ELSE 0 END ) as retained_8day,\n  sum(CASE rs.dur WHEN 9 THEN 1 ELSE 0 END ) as retained_9day,\n  sum(CASE rs.dur WHEN 10 THEN 1 ELSE 0 END ) as retained_10day,\n  sum(CASE rs.dur WHEN 11 THEN 1 ELSE 0 END ) as retained_11day,\n  sum(CASE rs.dur WHEN 12 THEN 1 ELSE 0 END ) as retained_12day,\n  sum(CASE rs.dur WHEN 13 THEN 1 ELSE 0 END ) as retained_13day,\n  sum(CASE rs.dur WHEN 14 THEN 1 ELSE 0 END ) as retained_14day,  \n  sum(CASE rs.dur WHEN 29 THEN 1 ELSE 0 END ) as retained_29day\nFROM\n(\n select distinct filterOdsR.reg_time,filterOdsR.game_id,if(filterOdsR.expand_channel = '' or expand_channel is null,'21',filterOdsR.expand_channel) expand_channel, \n datediff(odsl.login_time,filterOdsR.reg_time) dur,filterOdsR.game_account from \n    (\n    select to_date(odsr.reg_time) reg_time,odsr.game_id,odsr.expand_channel,odsr.game_account from ods_regi_rz odsr \n      where to_date(odsr.reg_time)<='currentday' and to_date (odsr.reg_time) >= date_add(\"currentday\",-29) and game_id is not null \n    )filterOdsR\n\t  join (select old_game_id game_id from yyft.game_sdk) sdk on sdk.game_id=filterOdsR.game_id\n      join ods_login odsl on filterOdsR.game_account = odsl.game_account\n    where to_date(odsl.login_time) > filterOdsR.reg_time and to_date(odsl.login_time)<=date_add(\"currentday\",30) and datediff(odsl.login_time,filterOdsR.reg_time) in(1,2,3,4,5,6,7,8,9,10,11,12,13,14,29)\n) rs \ngroup BY rs.reg_time,rs.game_id,rs.expand_channel"
    val execSql = accountRetainedSql.replace("currentday", currentday)
    sqlContext.sql(execSql).registerTempTable("account_retained_tmp")
    val accountRetainedDf = sqlContext.sql("select art.*,f.promotion_channel,f.medium_account,f.promotion_mode,f.head_people,gs.game_id parent_game_id,gs.system_type os,gs.group_id from account_retained_tmp art left join fxdim f on split(art.expand_channel,'_')[2] = f.pkg_code " +
      "and art.reg_time>=f.mstart_date and art.reg_time<=f.mend_date and art.reg_time>=f.astart_date and art.reg_time<=f.aend_date left join game_sdk gs on art.game_id = gs.old_game_id ")


  }

  private def deviceRetainedForeachPartition(deviceRetainedDf: DataFrame) = {
    deviceRetainedDf.foreachPartition(rows => {
      val conn = JdbcUtil.getConn()
      val statement = conn.createStatement
      val sqlText = " insert into bi_gamepublic_actions(publish_date,child_game_id,medium_channel,ad_site_channel,pkg_code" +
        ",dev_retained_2day,dev_retained_3day,dev_retained_4day,dev_retained_5day,dev_retained_6day,dev_retained_7day,dev_retained_8day,dev_retained_9day," +
        "dev_retained_10day,dev_retained_11day,dev_retained_12day,dev_retained_13day,dev_retained_14day,dev_retained_15day,dev_retained_30day," +
        "promotion_channel,medium_account,promotion_mode,head_people,parent_game_id,os,group_id)" +
        " values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)" +
        " on duplicate key update dev_retained_2day=?,dev_retained_3day=?,dev_retained_4day=?,dev_retained_5day=?,dev_retained_6day=?,dev_retained_7day=?,dev_retained_8day=?," +
        "dev_retained_9day=?,dev_retained_10day=?,dev_retained_11day=?,dev_retained_12day=?,dev_retained_13day=?,dev_retained_14day=?,dev_retained_15day=?,dev_retained_30day=?"
      val params = new ArrayBuffer[Array[Any]]()
      for (insertedRow <- rows) {
        if (insertedRow.get(2) != null) {
          val channelArray = StringUtils.getArrayChannel(insertedRow.get(2).toString)
          if (channelArray(0).length <= 10 && channelArray(1).length <= 10 && channelArray(2).length <= 15) {
            params.+=(Array[Any](insertedRow.get(0), insertedRow.get(1), channelArray(0), channelArray(1), channelArray(2),insertedRow.get(3),
              insertedRow.get(4), insertedRow.get(5), insertedRow.get(6), insertedRow.get(7), insertedRow.get(8), insertedRow.get(9),insertedRow.get(10),
              insertedRow.get(11),insertedRow.get(12),insertedRow.get(13),insertedRow.get(14),insertedRow.get(15),insertedRow.get(16),insertedRow.get(17),
              if(insertedRow(18)==null) "" else insertedRow(18),if(insertedRow(19)==null) "" else insertedRow(19),if(insertedRow(20)==null) "" else insertedRow(20),
              if(insertedRow(21)==null) "" else insertedRow(21),if(insertedRow(22)==null) "0" else insertedRow(22),if(insertedRow(23)==null) "1" else insertedRow(23),if(insertedRow(24)==null) "0" else insertedRow(24)
              , insertedRow.get(3),insertedRow.get(4), insertedRow.get(5), insertedRow.get(6), insertedRow.get(7), insertedRow.get(8), insertedRow.get(9),insertedRow.get(10),
              insertedRow.get(11),insertedRow.get(12),insertedRow.get(13),insertedRow.get(14),insertedRow.get(15),insertedRow.get(16),insertedRow.get(17)))
          } else {
            println("expand_channel is null: " + insertedRow.get(0) + " - " + insertedRow.get(2) + " - " + insertedRow.get(1))
          }
        }
        try {
          JdbcUtil.doBatch(sqlText, params, conn)
        } finally {
          statement.close()
          conn.close()
        }
      }
    })
  }

  private def accountRetainedForeachPartition(accountRetainedDf: DataFrame) = {
    accountRetainedDf.foreachPartition(rows => {
      val conn = JdbcUtil.getConn()


      val sqlText = " insert into bi_gamepublic_actions(publish_date,child_game_id,medium_channel,ad_site_channel,pkg_code" +
        ",acc_retained_2day,acc_retained_3day,acc_retained_4day,acc_retained_5day,acc_retained_6day,acc_retained_7day,acc_retained_8day,acc_retained_9day," +
        "acc_retained_10day,acc_retained_11day,acc_retained_12day,acc_retained_13day,acc_retained_14day,acc_retained_15day,acc_retained_30day," +
        "promotion_channel,medium_account,promotion_mode,head_people,parent_game_id,os,group_id)" +
        " values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)" +
        " on duplicate key update acc_retained_2day=?,acc_retained_3day=?,acc_retained_4day=?,acc_retained_5day=?,acc_retained_6day=?,acc_retained_7day=?,acc_retained_8day=?," +
        "acc_retained_9day=?,acc_retained_10day=?,acc_retained_11day=?,acc_retained_12day=?,acc_retained_13day=?,acc_retained_14day=?,acc_retained_15day=?,acc_retained_30day=?"
      val params = new ArrayBuffer[Array[Any]]()
      for (insertedRow <- rows) {
        if (insertedRow.get(2) != null) {
          val channelArray = StringUtils.getArrayChannel(insertedRow.get(2).toString)
          if (channelArray(0).length <= 10 && channelArray(1).length <= 10 && channelArray(2).length <= 15){
            params.+=(Array[Any](insertedRow.get(0), insertedRow.get(1), channelArray(0), channelArray(1), channelArray(2),insertedRow.get(3),
              insertedRow.get(4), insertedRow.get(5), insertedRow.get(6), insertedRow.get(7), insertedRow.get(8), insertedRow.get(9),insertedRow.get(10),
              insertedRow.get(11),insertedRow.get(12),insertedRow.get(13),insertedRow.get(14),insertedRow.get(15),insertedRow.get(16),insertedRow.get(17),
              if(insertedRow(18)==null) "" else insertedRow(18),if(insertedRow(19)==null) "" else insertedRow(19),if(insertedRow(20)==null) "" else insertedRow(20),
              if(insertedRow(21)==null) "" else insertedRow(21),if(insertedRow(22)==null) "0" else insertedRow(22),if(insertedRow(23)==null) "1" else insertedRow(23),if(insertedRow(24)==null) "0" else insertedRow(24)
              , insertedRow.get(3),insertedRow.get(4), insertedRow.get(5), insertedRow.get(6), insertedRow.get(7), insertedRow.get(8), insertedRow.get(9),insertedRow.get(10),
              insertedRow.get(11),insertedRow.get(12),insertedRow.get(13),insertedRow.get(14),insertedRow.get(15),insertedRow.get(16),insertedRow.get(17)
            ))
          }
        }else{
          println("expand_channel is null : " +insertedRow.get(0)+ insertedRow.get(2)+insertedRow.get(1) )
        }
        try {
          JdbcUtil.doBatch(sqlText, params, conn)
        }
        finally{
          conn.close()
        }
      }
    })
  }

}
