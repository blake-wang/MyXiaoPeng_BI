package cn.xiaopeng.bi.gamepublish

import cn.wanglei.bi.udf.ChannelUDF
import cn.xiaopeng.bi.utils.{JdbcUtil, StringUtils, SparkUtils}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.hive.HiveContext

import org.apache.spark.sql.types.DataTypes

import org.apache.spark.{SparkContext, SparkConf}

import scala.collection.mutable.ArrayBuffer

/**
 * Created by JSJSB-0071 on 2017/7/13.
 *
 * ltv:用户价值分析，注册和支付
 *
 * //这个版本已经废弃不用了
 */
object GamePublishLtv2 {
  val logger = Logger.getLogger(GamePublishLtv2.getClass)

  def main(args: Array[String]) {
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)
    val currentday = args(0)
    //创建上下文环境
    val sparkConf = new SparkConf().setAppName(this.getClass.getName.replace("$", ""))
      .set("spark.default.parallelism", "60")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.shuffle.consolidateFiles", "true")
      .set("spark.storage.memoryFraction", "0.4")
      .set("spark.streaming.stopGracefullyOnShutdown", "true")
    SparkUtils.setMaster(sparkConf)
    val sc = new SparkContext(sparkConf)
    val hiveContext = new HiveContext(sc)
    hiveContext.udf.register("getchannel", new ChannelUDF(), DataTypes.StringType)
    hiveContext.sql("use yyft")

    //1、过滤出符合条件的注册帐号：1、关联深度联运 2、时间范围当前日往前180天
    val filterRegiSql = ("select\n" +
      "distinct \n" +
      "to_date(reg.reg_time) reg_time,\n" +
      "reg.game_id game_id,\n" +
      "if(reg.expand_channel = '' or reg.expand_channel is null,'21',reg.expand_channel) expand_channel,\n" +
      "lower(trim(reg.game_account)) game_account\n" +
      "from \n" +
      "(select  lower(trim(game_account)) game_account, min(reg_time) reg_time,game_id,max(expand_channel) expand_channel from ods_regi_rz " +
      "where game_id is not null  group by lower(trim(game_account)),game_id) \n" +
      "reg join (select distinct game_id from " +
      "ods_publish_game) pg on reg.game_id = pg.game_id " +
      "where to_date(reg.reg_time)<='currentday' and to_date (reg.reg_time) >= date_add('currentday',-180)")
      .replace("currentday", currentday)
    val filterRegiDF = hiveContext.sql(filterRegiSql)
    filterRegiDF.cache()
    filterRegiDF.registerTempTable("filter_regi")

    //二、获取注册人数
    val accountCountSql = "select reg.reg_time,reg.game_id,reg.expand_channel,count(distinct reg.game_account) account_count from " +
      "filter_regi reg group by reg.reg_time,reg.game_id,reg.expand_channel"
    hiveContext.sql(accountCountSql).registerTempTable("account_count")

    //三、计算每个时间段内 充值信息
    val ltvDayArray = Array[Int](1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 30, 45, 60, 90, 180)
    for (ltvDay <- ltvDayArray) {
      val ltvDaySql =
        "select reg.reg_time,\n" +
          "reg.game_id,\n" +
          "reg.expand_channel,\n" +
          "sum(if(o.order_status = 4,o.ori_price_all,-o.ori_price_all)) amount\n" +
          "from \n" +
          "filter_regi reg \n" +
          "join \n" +
          "(select distinct order_time,lower(trim(game_account)) game_account,game_id," +
          "(if(ori_price is null,0,ori_price)+if(total_amt is null,0,total_amt)) as ori_price_all,order_status from ods_order " +
          "where order_status in(4,8)) o on reg.game_account = o.game_account and" +
          " reg.game_id=o.game_id and to_date(o.order_time)>=reg.reg_time and " +
          "to_date(o.order_time)<date_add(reg.reg_time,'" + ltvDay + "') group by reg.reg_time,reg.game_id,reg.expand_channel"
      hiveContext.sql(ltvDaySql).registerTempTable("ltv" + ltvDay + "_day")
    }
    //四、关联注册和充值信息 和一些基本维度信息
    val resultSql = "select\n" +
      "account.reg_time as publish_date,\n" +
      "game_sdk_cach.parent_game_id,\n" +
      "account.game_id,\n" +
      "account.expand_channel,\n" +
      "game_sdk_cach.os,\n" +
      "game_sdk_cach.group_id,\n" +
      "account.account_count as acc_add_regi,\n" +
      "if(ltv1day.amount is null,0,ltv1day.amount*100) ltv1_amount, \n" +
      "if(ltv2day.amount is null,0,ltv2day.amount*100) ltv2_amount, \n" +
      "if(ltv3day.amount is null,0,ltv3day.amount*100) ltv3_amount, \n" +
      "if(ltv4day.amount is null,0,ltv4day.amount*100) ltv4_amount, \n" +
      "if(ltv5day.amount is null,0,ltv5day.amount*100) ltv5_amount, \n" +
      "if(ltv6day.amount is null,0,ltv6day.amount*100) ltv6_amount, \n" +
      "if(ltv7day.amount is null,0,ltv7day.amount*100) ltv7_amount, \n" +
      "if(ltv8day.amount is null,0,ltv8day.amount*100) ltv8_amount, \n" +
      "if(ltv9day.amount is null,0,ltv9day.amount*100) ltv9_amount, \n" +
      "if(ltv10day.amount is null,0,ltv10day.amount*100) ltv10_amount, \n" +
      "if(ltv11day.amount is null,0,ltv11day.amount*100) ltv11_amount, \n" +
      "if(ltv12day.amount is null,0,ltv12day.amount*100) ltv12_amount, \n" +
      "if(ltv13day.amount is null,0,ltv13day.amount*100) ltv13_amount, \n" +
      "if(ltv14day.amount is null,0,ltv14day.amount*100) ltv14_amount, \n" +
      "if(ltv15day.amount is null,0,ltv15day.amount*100) ltv15_amount, \n" +
      "if(ltv30day.amount is null,0,ltv30day.amount*100) ltv30_amount, \n" +
      "if(ltv45day.amount is null,0,ltv45day.amount*100) ltv45_amount,  \n" +
      "if(ltv60day.amount is null,0,ltv60day.amount*100) ltv60_amount, \n" +
      "if(ltv90day.amount is null,0,ltv90day.amount*100) ltv90_amount,  \n" +
      "if(ltv180day.amount is null,0,ltv180day.amount*100) ltv180_amount \n" +
      "from account_count account \n" +
      "join  (select  distinct game_id as parent_game_id , old_game_id as child_game_id,system_type as os,group_id from game_sdk where game_id is not null) game_sdk_cach on account.game_id=game_sdk_cach.child_game_id\n" +
      "left join ltv1_day ltv1day on ltv1day.reg_time=account.reg_time and ltv1day.game_id=account.game_id and account.expand_channel=ltv1day.expand_channel \n" +
      "left join ltv2_day ltv2day on ltv2day.reg_time=account.reg_time and ltv2day.game_id=account.game_id and account.expand_channel=ltv2day.expand_channel \n" +
      "left join ltv3_day ltv3day on account.reg_time=ltv3day.reg_time and account.game_id=ltv3day.game_id and account.expand_channel=ltv3day.expand_channel  \n" +
      "left join ltv4_day ltv4day on ltv4day.reg_time=account.reg_time and ltv4day.game_id=account.game_id and account.expand_channel=ltv4day.expand_channel  \n" +
      "left join ltv5_day ltv5day on account.reg_time=ltv5day.reg_time and account.game_id=ltv5day.game_id and account.expand_channel=ltv5day.expand_channel  \n" +
      "left join ltv6_day ltv6day on ltv6day.reg_time=account.reg_time and ltv6day.game_id=account.game_id and account.expand_channel=ltv6day.expand_channel \n" +
      "left join ltv7_day ltv7day on account.reg_time=ltv7day.reg_time and account.game_id=ltv7day.game_id and account.expand_channel=ltv7day.expand_channel \n" +
      "left join ltv8_day ltv8day on ltv8day.reg_time=account.reg_time and ltv8day.game_id=account.game_id and account.expand_channel=ltv8day.expand_channel \n" +
      "left join ltv9_day ltv9day on ltv9day.reg_time=account.reg_time and ltv9day.game_id=account.game_id and account.expand_channel=ltv9day.expand_channel \n" +
      "left join ltv10_day ltv10day on account.reg_time=ltv10day.reg_time and account.game_id=ltv10day.game_id and account.expand_channel=ltv10day.expand_channel  \n" +
      "left join ltv11_day ltv11day on ltv11day.reg_time=account.reg_time and ltv11day.game_id=account.game_id and account.expand_channel=ltv11day.expand_channel  \n" +
      "left join ltv12_day ltv12day on account.reg_time=ltv12day.reg_time and account.game_id=ltv12day.game_id and account.expand_channel=ltv12day.expand_channel  \n" +
      "left join ltv13_day ltv13day on ltv13day.reg_time=account.reg_time and ltv13day.game_id=account.game_id and account.expand_channel=ltv13day.expand_channel \n" +
      "left join ltv14_day ltv14day on account.reg_time=ltv14day.reg_time and account.game_id=ltv14day.game_id and account.expand_channel=ltv14day.expand_channel  \n" +
      "left join ltv15_day ltv15day on ltv15day.reg_time=account.reg_time and ltv15day.game_id=account.game_id and account.expand_channel=ltv15day.expand_channel \n" +
      "left join ltv30_day ltv30day on account.reg_time=ltv30day.reg_time and account.game_id=ltv30day.game_id and account.expand_channel=ltv30day.expand_channel \n" +
      "left join ltv45_day ltv45day on account.reg_time=ltv45day.reg_time and account.game_id=ltv45day.game_id and account.expand_channel=ltv45day.expand_channel   \n" +
      "left join ltv60_day ltv60day on ltv60day.reg_time=account.reg_time and ltv60day.game_id=account.game_id and account.expand_channel=ltv60day.expand_channel \n" +
      "left join ltv90_day ltv90day on ltv90day.reg_time=account.reg_time and ltv90day.game_id=account.game_id and account.expand_channel=ltv90day.expand_channel \n" +
      "left join ltv180_day ltv180day on ltv180day.reg_time=account.reg_time and ltv180day.game_id=account.game_id and account.expand_channel=ltv180day.expand_channel"
    val resultDf = hiveContext.sql(resultSql)

    //五、把结果存入mysql
    resultDf.foreachPartition(rows => {
      val sqlText = "insert into bi_gamepublic_actions(publish_date,parent_game_id,child_game_id,medium_channel,ad_site_channel,pkg_code,os,group_id) " +
        "values(?,?,?,?,?,?,?,?) " +
        "on duplicate key update " +
        "acc_add_regi=?," +
        "recharge_lj_1=?," +
        "recharge_lj_2=?," +
        "recharge_lj_3=?," +
        "recharge_lj_4=?," +
        "recharge_lj_5=?," +
        "recharge_lj_6=?," +
        "recharge_lj_7=?," +
        "recharge_lj_8=?," +
        "recharge_lj_9=?," +
        "recharge_lj_10=?," +
        "recharge_lj_11=?," +
        "recharge_lj_12=?," +
        "recharge_lj_13=?," +
        "recharge_lj_14=?," +
        "recharge_lj_15=?," +
        "recharge_lj_30=?," +
        "recharge_lj_45=?," +
        "recharge_lj_60=?," +
        "recharge_lj_90=?," +
        "recharge_lj_180=?"
      val params = new ArrayBuffer[Array[Any]]()
      for (insertedRow <- rows) {
        val channelArray = StringUtils.getArrayChannel(insertedRow.get(3).toString)
        if (channelArray(0).length <= 10 && channelArray(1).length <= 10 && channelArray(2).length <= 12) {
          params.+=(Array[Any](insertedRow.get(0), insertedRow.get(1), insertedRow.get(2),
          channelArray(0), channelArray(1), channelArray(2), insertedRow.get(4),
          insertedRow.get(5), insertedRow.get(6), insertedRow.get(7), insertedRow.get(8),
          insertedRow.get(9), insertedRow.get(10), insertedRow.get(11), insertedRow.get(12),
          insertedRow.get(13), insertedRow.get(14), insertedRow.get(15), insertedRow.get(16),
          insertedRow.get(17), insertedRow.get(18), insertedRow.get(19), insertedRow.get(20),
          insertedRow.get(21), insertedRow.get(22), insertedRow.get(23), insertedRow.get(24),
          insertedRow.get(25), insertedRow.get(26)))
        } else {
          logger.error("expand_channel format error: " + insertedRow.get(3) + " - " + insertedRow.get(1))
        }
      }
      JdbcUtil.executeBatch(sqlText, params)
    })
    System.clearProperty("spark.driver.port")
    sc.stop

  }
}
