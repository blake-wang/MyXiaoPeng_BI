
import cn.wanglei.bi.ConfigurationUtil
import cn.xiaopeng.bi.utils.{StringUtils, JdbcUtil}
import org.apache.log4j.Logger
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{Logging, SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer

object GamePublishLtv extends Logging {
  val logger = Logger.getLogger(GamePublishLtv.getClass)


  /*
  * 对有关系发布数据报表统计 - 生命周期
  *
  * 次日LTV ： (1月1日新增用户在1月1日至1月2日的充值总金额)/1月1日新增用户数
  * 三日LTV ： (1月1日新增用户在1月1日至1月3日的充值总金额)/1月1日新增用户数
  *
  * */
  def main(args: Array[String]): Unit = {
    logger.error("start the ltv app")
    val currentday = args(0)
    val sparkConf = new SparkConf().setAppName(this.getClass.getName.replace("$", "")).set("spark.sql.shuffle.partitions", ConfigurationUtil.getProperty("spark.sql.shuffle.partitions"))
    val sc = new SparkContext(sparkConf)
    val sqlContext = new HiveContext(sc)
    sqlContext.sql("use yyft")


    //过滤出符合条件的有用帐号：1、关联深度联运，2、时间范围当前日往前60天
    val filterRegiSql = "select to_date(reg.reg_time) reg_time,reg.game_id game_id," +
      "if(reg.expand_channel = '','21',reg.expand_channel) expand_channel,reg.game_account game_account from ods_regi_rz reg " +
      "join (select distinct game_id from ods_publish_game) pg on reg.game_id = pg.game_id " +
      "where to_date(reg.reg_time)<='currentday' and to_date (reg.reg_time) >= date_add('currentday',-59)"
        .replace("currentday", currentday)
    val filterRegiDF = sqlContext.sql(filterRegiSql)
    filterRegiDF.cache()
    //注册成 filter_regi表
    filterRegiDF.registerTempTable("filter_regi")

    /**
     * 业务定义：
     * 次日LTV：（1月1日新增用户在1月1日至1月2日的充值总金额）/1月1日新增用户数
     * 三日LTV：（1月1日新增用户在1月1日至1月3日的充值总金额）/1月1日新增用户数
     */

    val ltvDayArray = Array[Int](2, 3, 4, 5, 6, 7, 15, 30, 60)
    for (ltvDay <- ltvDayArray) {
      //      val ltvDaySql = "select reg.reg_time,reg.g to_date(o.order_time)<date_add(reg.reg_time," + ltvDay + ") " +
      //        "group by reg.reg_time,reg.game_ame_id,reg.expand_channel,sum(if(o.order_status = 4,o.ori_price,-o.ori_price)) amount \" +\n        \"from filter_regi reg join ods_order o on reg.game_account = o.game_account and o.order_status in(4,8) \" +\n        \"where to_date(o.order_time)>=reg.reg_time andid,reg.expand_channel"

      val ltvDaySql =
        "select reg.reg_time,reg.game_id,reg.expand_channel,sum(if(o.order_status = 4,o.ori_price,-o.ori_price)) amount " +
          "from filter_regi reg join ods_order o on reg.game_account = o.game_account and o.order_status in(4,8) " +
          "where to_date(o.order_time)>=reg.reg_time and to_date(o.order_time)<date_add(reg.reg_time," + ltvDay + ") " +
          "group by reg.reg_time,reg.game_id,reg.expand_channel"
      //注册成 ltv2_day,ltv3_day,ltv4_day... 表  共9张临时表
      sqlContext.sql(ltvDaySql).registerTempTable("ltv" + ltvDay + "_day")
    }
    //获取游戏id，推广渠道，对应的注册账号数
    //注册 account_count表
    val accountCountSql = "select reg.reg_time,reg.game_id,reg.expand_channel,count(reg.game_account) account_count " +
      "from filter_regi reg group by reg.reg_time,reg.game_id,reg.expand_channel"
    sqlContext.sql(accountCountSql).registerTempTable("account_count")


    //把所有查询结果join起来，生成结果集
    val resultDf = sqlContext.sql("select account.reg_time,account.game_id,account.expand_channel,account.account_count," +
      "if(ltv2day.amount is null,0,ltv2day.amount) ltv2_amount,if(ltv3day.amount is null,0,ltv3day.amount) ltv3_amount," +
      "if(ltv4day.amount is null,0,ltv4day.amount) ltv4_amount,if(ltv5day.amount is null,0,ltv5day.amount) ltv5_amount" +
      ",if(ltv6day.amount is null,0,ltv6day.amount) ltv6_amount,if(ltv7day.amount is null,0,ltv7day.amount) ltv7_amount," +
      "if(ltv15day.amount is null,0,ltv15day.amount) ltv15_amount,if(ltv30day.amount is null,0,ltv30day.amount) ltv30_amount," +
      "if(ltv60day.amount is null,0,ltv60day.amount) ltv60_amount " +
      "from account_count account " +
      "left join ltv2_day ltv2day on ltv2day.reg_time=account.reg_time and ltv2day.game_id=account.game_id and ltv2day.expand_channel=account.expand_channel " +
      "left join ltv3_day ltv3day on account.reg_time=ltv3day.reg_time and account.game_id=ltv3day.game_id and account.expand_channel=ltv3day.expand_channel " +
      "left join ltv4_day ltv4day on ltv4day.reg_time=account.reg_time and ltv4day.game_id=account.game_id and ltv4day.expand_channel=account.expand_channel " +
      "left join ltv5_day ltv5day on account.reg_time=ltv5day.reg_time and account.game_id=ltv5day.game_id and account.expand_channel=ltv5day.expand_channel " +
      "left join ltv6_day ltv6day on ltv6day.reg_time=account.reg_time and ltv6day.game_id=account.game_id and ltv6day.expand_channel=account.expand_channel " +
      "left join ltv7_day ltv7day on account.reg_time=ltv7day.reg_time and account.game_id=ltv7day.game_id and account.expand_channel=ltv7day.expand_channel " +
      "left join ltv15_day ltv15day on ltv15day.reg_time=account.reg_time and ltv15day.game_id=account.game_id and ltv15day.expand_channel=account.expand_channel " +
      "left join ltv30_day ltv30day on account.reg_time=ltv30day.reg_time and account.game_id=ltv30day.game_id and account.expand_channel=ltv30day.expand_channel " +
      "left join ltv60_day ltv60day on ltv60day.reg_time=account.reg_time and ltv60day.game_id=account.game_id and ltv60day.expand_channel=account.expand_channel")

    //把结果存入mysql
    resultDf.foreachPartition(rows => {
      val conn = JdbcUtil.getConn()
      val statement = conn.createStatement

      val sqlText = " insert into bi_gamepublic_retainedltv(reg_time,game_id,parent_channel,child_channel,ad_label,add_user_num," +
        "ltv_1day,ltv_2day,ltv_3day,ltv_4day,ltv_5day,ltv_6day,ltv_14day,ltv_29day,ltv_59day)" +
        " values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)" +
        " on duplicate key update " +
        "add_user_num=?," +
        "ltv_1day=?," +
        "ltv_2day=?," +
        "ltv_3day=?," +
        "ltv_4day=?," +
        "ltv_5day=?," +
        "ltv_6day=?," +
        "ltv_14day=?," +
        "ltv_29day=?," +
        "ltv_59day=?"
      val params = new ArrayBuffer[Array[Any]]()
      for (insertedRow <- rows) {
        val channelArray = StringUtils.getArrayChannel(insertedRow(2).toString)
        if (channelArray(0).length <= 10 && channelArray(1).length <= 10 && channelArray(2).length <= 12) {
          params.+=(Array[Any](insertedRow.get(0), insertedRow.get(1),
          channelArray(0), channelArray(1), channelArray(2),
            insertedRow.get(3), insertedRow.get(4), insertedRow.get(5), insertedRow.get(6)
            , insertedRow.get(7), insertedRow.get(8), insertedRow.get(9), insertedRow.get(10), insertedRow.get(11), insertedRow.get(12),
            insertedRow.get(3), insertedRow.get(4), insertedRow.get(5), insertedRow.get(6)
            , insertedRow.get(7), insertedRow.get(8), insertedRow.get(9), insertedRow.get(10), insertedRow.get(11), insertedRow.get(12)
          ))
        } else {
          logger.error("expand_channel format error:" + insertedRow.get(2) + " - " + insertedRow.get(1))
        }
      }
      JdbcUtil.doBatch(sqlText, params, conn)
      statement.close()
      conn.close()
    })
    System.clearProperty("spark.driver.port")
    sc.stop()
  }
}