package cn.xiaopeng.bi.gamepublish

import cn.xiaopeng.bi.utils.{DimensionUtil, JdbcUtil}
import org.apache.spark.sql.{DataFrame, Row, SaveMode}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.hive.HiveContext

/**
  * Created by bigdata on 17-10-28.
  * 付费结构
  */
object GamePublish2Pay {


  def main(args: Array[String]): Unit = {
    val currentday = args(0)
    val sparkConf = new SparkConf().setAppName(this.getClass.getName.replace("$", ""))
    val sc = new SparkContext(sparkConf)
    val sqlContext = new HiveContext(sc)
    sparkConf.set("spark,storage.memoryFraction", "0.7").set("spark.sql.shuffle.partitions", "60")
    sqlContext.sql("use yyft")
    //发行订单tb_fxorder*
    val fxOrderSql = "select distinct\nod.game_id,\norder_no,\nto_date(order_time) order_date,\nlower(trim(game_account)) game_account,\nif(od.order_status = 4,(if(ori_price is null,0,ori_price)+if(total_amt is null,0,total_amt)),-(if(ori_price is null,0,ori_price)+if(total_amt is null,0,total_amt))) ori_price\nfrom ods_order od   join (select distinct game_id from ods_publish_game) pg on pg.game_id=od.game_id \nwhere od.order_time is not null and order_status in(4,8) and od.prod_type=6 and od.game_account is not null and to_date(order_time)='currentday'".replace("currentday", currentday)
    sqlContext.sql(fxOrderSql).registerTempTable("tb_fxorder")
    sqlContext.cacheTable("tb_fxorder")
    //发行帐号 tb_fxaccount*
    val fxAccountSql = "select \ndistinct \nrz.game_id,\nlower(trim(game_account)) game_account,\nto_date(reg_time) account_regi_date,\nexpand_channel   ,\nif((split(expand_channel,'_')[0] is null or split(expand_channel,'_')[0]=''),'21',split(expand_channel,'_')[0]) as parent_channel   ,\nif(split(expand_channel,'_')[1] is null,'',split(expand_channel,'_')[1]) as child_channel   ,\nif(split(expand_channel,'_')[2] is null,'',split(expand_channel,'_')[2]) as ad_label\nfrom ods_regi_rz rz join (select distinct game_id from ods_publish_game) pg on pg.game_id=rz.game_id where rz.game_id is not null and rz.reg_time is not null and rz.game_account is not null \nand to_date(reg_time)>='2017-06-01'"
    sqlContext.sql(fxAccountSql).registerTempTable("tb_fxaccount")
    sqlContext.cacheTable("tb_fxaccount")
    //今天订单数据tmp_order_td,合并*
    val tmpOrderTdSql = "select distinct od.game_id,od.order_no,String(order_date) as order_date,rz.game_account,String(rz.account_regi_date) as account_regi_date,od.ori_price, parent_channel, child_channel,ad_label\n   from tb_fxorder od join tb_fxaccount rz on rz.game_account=od.game_account\n   where to_date(order_date)='currentday'".replace("currentday", currentday)
    val tmpOrderTdDf = sqlContext.sql(tmpOrderTdSql)
    tmpOrderTdDf.coalesce(10).write.mode(SaveMode.Overwrite).save("/user/hive/warehouse/archive.db/fxorder.parquet/key=" + currentday)
    /*提取历史所有数据，并注册成临时表 tmp_order*/
    sqlContext.read.option("mergeSchema", "true").parquet("/user/hive/warehouse/archive.db/fxorder.parquet").registerTempTable("tmp_order")
    /*充值金额数据分布*/
    val fbSql = "    select  \n\t   publish_date,\n\t   child_game_id,\n\t   medium_channel,\n\t   ad_site_channel,\n\t   pkg_code,\n\t   parent_game_id,\n\t   group_id,\n\t   os,\n       sum(case when ori_price<1 then 1 else 0 end) as recharge_accounts_1,\n\t   sum(case when ori_price>=1 and ori_price<50 then 1 else 0 end) as recharge_accounts_1_50,\n\t   sum(case when ori_price>=50 and ori_price<100 then 1 else 0 end) as recharge_accounts_50_100,\n\t   sum(case when ori_price>=100 and ori_price<300 then 1 else 0 end) as recharge_accounts_100_300,\n\t   sum(case when ori_price>=300 and ori_price<500 then 1 else 0 end) as recharge_accounts_300_500,\n\t   sum(case when ori_price>=500 and ori_price<1000 then 1 else 0 end) as recharge_accounts_500_1000,\n\t   sum(case when ori_price>=1000 and ori_price<2000 then 1 else 0 end) as recharge_accounts_1000_2000,\n\t   sum(case when ori_price>=2000 and ori_price<3000 then 1 else 0 end) as recharge_accounts_2000_3000,\n\t   sum(case when ori_price>=3000 and ori_price<5000 then 1 else 0 end) as recharge_accounts_3000_5000,\n\t   sum(case when ori_price>=5000 and ori_price<10000 then 1 else 0 end) as recharge_accounts_5000_10000,\n\t   sum(case when ori_price>=10000 and ori_price<30000 then 1 else 0 end) as recharge_accounts_10000_30000,\n\t   sum(case when ori_price>=30000 then 1 else 0 end) as recharge_accounts_30000,\n       count(distinct game_account) recharge_accounts\t   \n\t   from \n\t   (\n\t\t   select \n\t\t\tto_date(od.account_regi_date) publish_date,\n\t\t\tod.game_id child_game_id,\n\t\t\tod.parent_channel medium_channel,\n\t\t\tod.child_channel ad_site_channel,\n\t\t\tod.ad_label pkg_code,\n\t\t\tif(sdk.game_id is null,0,sdk.game_id) as parent_game_id,\n\t\t\tif(sdk.group_id is null,0,sdk.group_id) group_id,\n\t\t\tif(sdk.system_type is null,0,sdk.system_type)  as os,\n\t\t\tgame_account,\n\t\t\tsum(ori_price) ori_price,\n\t\t\tcount(distinct to_date(order_date)) dts \n\t\t\tfrom tmp_order   od\n\t\t\tjoin game_sdk sdk on sdk.old_game_id=od.game_id \n\t\t\tgroup by od.game_id,parent_channel,child_channel,ad_label,to_date(account_regi_date),game_account,sdk.game_id,sdk.group_id,sdk.system_type\n\t\t) rs group by \t   \n\t   publish_date,\n\t   child_game_id,\n\t   medium_channel,\n\t   ad_site_channel,\n\t   pkg_code,\n\t   parent_game_id,\n\t   group_id,\n\t   os"
    val istFb = "insert into bi_gamepublic_actions(publish_date,child_game_id,medium_channel,ad_site_channel,pkg_code,parent_game_id,group_id,os\n\t   ,recharge_accounts_1\n\t   ,recharge_accounts_1_50\n\t   ,recharge_accounts_50_100\n\t   ,recharge_accounts_100_300\n\t   ,recharge_accounts_300_500\n\t   ,recharge_accounts_500_1000\n\t   ,recharge_accounts_1000_2000\n\t   ,recharge_accounts_2000_3000\n\t   ,recharge_accounts_3000_5000\n\t   ,recharge_accounts_5000_10000\n\t   ,recharge_accounts_10000_30000\n\t   ,recharge_accounts_30000\n\t    ,recharge_accounts)\n        values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) on duplicate key update \n\t    recharge_accounts_1=?,recharge_accounts_1_50=?,recharge_accounts_50_100=?,recharge_accounts_100_300=?,recharge_accounts_300_500=?,recharge_accounts_500_1000=?,recharge_accounts_1000_2000=?,\n\t\trecharge_accounts_2000_3000=?,recharge_accounts_3000_5000=?,recharge_accounts_5000_10000=?,recharge_accounts_10000_30000=?,recharge_accounts_30000=?,recharge_accounts=?"
    val fbDf = sqlContext.sql(fbSql)
    processDbFct(fbDf, istFb)
    /*推送维度表*/
    DimensionUtil.processDbDim(sqlContext, currentday)

    System.clearProperty("spark.driver.port")
    sc.stop()
    sc.clearCallSite()

  }


  def processDbFct(dataf: DataFrame, insertSql: String): Unit = {
    //全部转为小写
    val sql2Mysql = insertSql.replace("|", "").toLowerCase()
    //获取values()里面有多少个?参数，有利于后面的循环
    val startValuesIndex = sql2Mysql.indexOf("(?") + 1
    val endValuesIndex = sql2Mysql.indexOf("?)") + 1
    //values中的个数
    val valueArray: Array[String] = sql2Mysql.substring(startValuesIndex, endValuesIndex).split(",")
    //两个(??)中间的值
    //条件中的参数个数
    val wh: Array[String] = sql2Mysql.substring(sql2Mysql.indexOf("update") + 6).split(",")
    //找update后面的字符串再判断
    //查找需要insert的字段
    val cols_ref = sql2Mysql.substring(0, sql2Mysql.lastIndexOf("(?"))
    //获取(?特殊字符前的字符串，然后再找字段
    val cols: Array[String] = cols_ref.substring(cols_ref.lastIndexOf("(") + 1, cols_ref.lastIndexOf(")")).split(",")

    //数据库操作
    dataf.foreachPartition((rows: Iterator[Row]) => {
      val conn = JdbcUtil.getConn()
      val ps = conn.prepareStatement(sql2Mysql)
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
    })
  }
  //新注册帐号数
  def newRegi(currentday:String) = {
    val sql = " insert into bi_gamepublic_actions(publish_date,parent_game_id,child_game_id,medium_channel,ad_site_channel,pkg_code,medium_account,\n              promotion_channel,promotion_mode,head_people,group_id,os,acc_add_regi) \n              select publish_date,parent_game_id,child_game_id,medium_channel,ad_site_channel,pkg_code,medium_account,\n              promotion_channel,promotion_mode,head_people,group_id,os,regi_account_num  \n              from bi_gamepublic_base_day_kpi rs where publish_date='currentday' on DUPLICATE key update os=rs.os,child_game_id=rs.child_game_id,medium_channel=rs.medium_channel,\n                ad_site_channel=rs.ad_site_channel,medium_account=rs.medium_account,promotion_channel=rs.promotion_channel,promotion_mode=rs.promotion_mode,head_people=rs.head_people,ad_site_channel=rs.ad_site_channel,\n                acc_add_regi=rs.regi_account_num".replace("currentday", currentday)
    val sql1 = "update bi_gamepublic_actions set recharge_regi_arpu=if(acc_add_regi=0,0,recharge_price/acc_add_regi) where publish_date='currentday' ".replace("currentday", currentday)
    val conn = JdbcUtil.getConn()
    val sta = conn.createStatement()
    sta.executeUpdate(sql)
    sta.executeUpdate(sql1)
    sta.close()
    conn.close()
  }
}
