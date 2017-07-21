package cn.xiaopeng.bi.sometime

import cn.wanglei.bi.ConfigurationUtil
import cn.xiaopeng.bi.utils.{FileUtil, SparkUtils}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.{StringType, StructType}

/**
  * Created by bigdata on 7/19/17.
  */
object GamePublishImei {
  def main(args: Array[String]): Unit = {
    // 创建各种上下文
    val sparkConf = new SparkConf().setAppName(this.getClass.getName.replace("$", ""))
      .set("spark.sql.shuffle.partitions", ConfigurationUtil.getProperty("spark.sql.shuffle.partitions"))
    SparkUtils.setMaster(sparkConf);
    val sc = new SparkContext(sparkConf)
    val hiveContext = new HiveContext(sc)


    val rdd: RDD[String] = sc.textFile("/cpuid.txt")
    val df = rdd.map(t => {
      //zheli xuyao yi ge pan duan
      val arr = t.split("__")


      Row(arr(1))


    })

    val regiStruct = (new StructType).add("uid_cp", StringType);
    val regiDF = hiveContext.createDataFrame(df, regiStruct);
    regiDF.registerTempTable("cp_uid")

    hiveContext.sql("select distinct login.game_account game_account,login.login_time login_time,login.channel_expand channel_expand,login.ip ip\nfrom\n(\nselect  distinct lower(trim(game_account)) game_account,login_time,channel_expand,ip from yyft.ods_login\nunion all\nselect  distinct lower(trim(game_account)) game_account,login_time,channel_expand,ip from archive.ods_login_day\n) login ").registerTempTable("ods_login_rz")
    SparkUtils.readXiaopeng2Table("bgameaccount", hiveContext)

    hiveContext.sql("use yyft")
    val resultSql ="select  \nods_regi.game_account,--账户\nods_regi.reg_time,--注册时间\nods_order_rz.amount,--累计充值金额\nlogin3.channel_expand,--第一次登录 渠道\nlogin4.ip,-- 最后一次登录时间\nlogin4.login_time--最后一次登录ip\nfrom \n(select  lower(trim(game_account)) game_account, min(reg_time) reg_time, max(expand_channel) expand_channel from ods_regi_rz where game_id is not null group by lower(trim(game_account))) \nods_regi\njoin (select distinct accounts.account account from (select account,cp_uid from bgameaccount) accounts join (select distinct uid_cp from  cp_uid)cp_uid on  accounts.cp_uid= cp_uid.uid_cp ) cp_account on cp_account.account=ods_regi.game_account\nleft join (select  game_account,sum(pay.ori_price_all) amount from(select distinct order_no,order_time, lower(trim(game_account)) game_account,game_id,(if(ori_price is null,0,ori_price)+if(total_amt is null,0,total_amt)) as ori_price_all,payment_type,order_status from ods_order) pay group by pay.game_account) ods_order_rz on ods_regi.game_account=ods_order_rz.game_account\nleft join (select distinct login1.game_account,login1.channel_expand from (select  distinct game_account,login_time,channel_expand from  ods_login_rz) login1 join (select  game_account,min(login_time) login_time from ods_login_rz group by  game_account) login2 on  login1.game_account=login2.game_account and login1.login_time=login2.login_time) login3 on ods_regi.game_account=login3.game_account\nleft join (select distinct login1.game_account,login1.ip,login1.login_time from (select  distinct game_account,login_time,ip from  ods_login_rz) login1 join (select  game_account,max(login_time) login_time from ods_login_rz group by  game_account) login2 on  login1.game_account=login2.game_account and login1.login_time=login2.login_time) login4 on ods_regi.game_account=login4.game_account"
    val resultDf = hiveContext.sql(resultSql)

    val header = "账户,注册时间,累计充值金额,第一次登录渠道,最后一次登录时间,最后一次登录ip"
    FileUtil.appendToFile("/home/hduser/crontabFiles/ltv/cp_uid.csv", header)
//        FileUtil.apppendTofile("/home/bigdata/cp_uid.csv", header)
    resultDf.foreachPartition(rows => {
      for (insertedRow <- rows) {
        FileUtil.appendToFile("/home/hduser/crontabFiles/ltv/cp_uid.csv", insertedRow.get(0) + "," + insertedRow.get(1) + "," + insertedRow.get(2) + "," + insertedRow.get(3) + "," + insertedRow.get(4) + "," + insertedRow.get(5))
        //        FileUtil.apppendTofile("/home/bigdata/cp_uid.csv", insertedRow.get(0) + "," + insertedRow.get(1) + "," + insertedRow.get(2) + "," + insertedRow.get(3) + "," + insertedRow.get(4) + "," + insertedRow.get(5))
      }
    })
    sc.stop()
  }


}
