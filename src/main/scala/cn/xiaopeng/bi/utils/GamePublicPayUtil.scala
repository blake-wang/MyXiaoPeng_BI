package cn.xiaopeng.bi.utils

import java.sql.{Connection, Statement}
import java.text.SimpleDateFormat
import java.util

import org.apache.log4j.Logger
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.hive.HiveContext
import redis.clients.jedis.{Jedis, JedisPool}

import scala.collection.mutable.ArrayBuffer

/**
  * Created by bigdata on 18-2-9.
  */
object GamePublicPayUtil {
  val logger = Logger.getLogger(GamePublicPayUtil.getClass)
  var arg = "60"


  def loadPayInfo(rdd: RDD[String], hiveContext: HiveContext) = {
    StreamingUtils.convertOrderToDfTmpTable(rdd, hiveContext)

    //运营报表  充值设备数
    val payPeopleDf = hiveContext.sql("select distinct split(publish_time,' ')[0],game_id,imei,game_account from ods_order ")
    foreachPartitionPayDev(payPeopleDf)

    //投放报表  充值账户数，累计充值账户数
    val payAccountDf = hiveContext.sql("select distinct publish_time,game_id,game_account from ods_order")
    foreachPartitionPayAccount(payAccountDf)

    //投放报表  充值金额，累计充值金额，ltv
    val payMoneyDf = hiveContext.sql("select publish_time,game_id,game_account,ori_price,total_amt from ods_order")
    foreachPartitionPayMoney(payMoneyDf)
  }


  def insertPayMoney(rows: Iterator[Row], jedis: Jedis, conn: Connection, statement: Statement) = {
    val sqlDayText = "insert into bi_gamepublic_base_day_kpi(publish_date,child_game_id,medium_channel,ad_site_channel,pkg_code,pay_money,parent_game_id,os,medium_account,promotion_channel,promotion_mode,head_people,group_id) values(?,?,?,?,?,?,?,?,?,?,?,?,?) on duplicate key update pay_money=pay_money+?"
    val sqlHourText = "insert into bi_gamepublic_basekpi(publish_time,game_id,parent_channel,child_channel,ad_label,pay_money_new,parent_game_id,os,medium_account,promotion_channel,promotion_mode,head_people,group_id) values(?,?,?,?,?,?,?,?,?,?,?,?,?) on duplicate key update pay_money_new=pay_money_new+?"
    val sqlOperDayText = "insert into bi_gamepublic_base_opera_kpi(publish_date,child_game_id,recharge_price,ltv_1day_b,ltv_2day_b,ltv_3day_b,ltv_7day_b,ltv_30day_b) values(?,?,?,?,?,?,?,?) on duplicate key update \nrecharge_price=recharge_price+values(recharge_price),\nltv_1day_b=ltv_1day_b+values(ltv_1day_b),\nltv_2day_b=ltv_2day_b+values(ltv_2day_b),\nltv_3day_b=ltv_3day_b+values(ltv_3day_b),\nltv_7day_b=ltv_7day_b+values(ltv_7day_b),\nltv_30day_b=ltv_30day_b+values(ltv_30day_b)"

    val dayParams = new ArrayBuffer[Array[Any]]()
    val hourParams = new ArrayBuffer[Array[Any]]()
    val operDayParams = new ArrayBuffer[Array[Any]]()

    for (insertedRow <- rows) {
      val game_id = insertedRow.getInt(1)
      val order_hour = insertedRow.getString(0)
      val order_date = order_hour.split(" ")(0)
      val game_account = insertedRow.getString(2).toLowerCase().trim
      val order_money = insertedRow.getDouble(3)

      //注册相关信息
      val accountInfo = jedis.hgetAll(game_account)
      val regi_game_id = if (accountInfo.get("game_id") == null) 0 else accountInfo.get("game_id")
      val regi_date = if (accountInfo == null) "0000-00-00" else accountInfo.get("reg_time").split(" ")(0)
      var expandChannel = accountInfo.get("expand_channel")
      if (expandChannel == null || expandChannel.equals("no_acc")) {
        logger.error("the account is not into redis:" + insertedRow.getString(2))
        expandChannel = "no_acc"
      }
      val channelArray = StringUtils.getArrayChannel(expandChannel)
      val pkg_code = channelArray(2)

      //维度信息
      val redisValue = JedisUtil.getRedisValue(game_id, pkg_code, order_date, jedis)

      //ltv
      val ltv_day = getltvDays(accountInfo, order_date)
      val ltv_1day_b = if (ltv_day == 0) order_money else 0
      val ltv_2day_b = if (ltv_day <= 1) order_money else 0
      val ltv_3day_b = if (ltv_day <= 2) order_money else 0
      val ltv_7day_b = if (ltv_day <= 6) order_money else 0
      val ltv_30day_b = if (ltv_day <= 29) order_money else 0

      if (channelArray(0).length <= 10 && channelArray(1).length <= 10 && channelArray(2).length <= 15) {
        dayParams.+=(Array[Any](order_date, insertedRow.get(1), channelArray(0), channelArray(1), channelArray(2), insertedRow.getDouble(3) + insertedRow.getDouble(4),
          redisValue(0), redisValue(1), redisValue(2), redisValue(3), redisValue(4), redisValue(5), redisValue(6)
          , insertedRow.getDouble(3) + insertedRow.getDouble(4)))

        hourParams.+=(Array[Any](order_hour, insertedRow.get(1), channelArray(0), channelArray(1), channelArray(2)
          , insertedRow.getDouble(3) + insertedRow.getDouble(4),
          redisValue(0), redisValue(1), redisValue(2), redisValue(3), redisValue(4), redisValue(5), redisValue(6)
          , insertedRow.getDouble(3) + insertedRow.getDouble(4)))

        operDayParams.+=(Array[Any](regi_date, regi_game_id, insertedRow.getDouble(3), ltv_1day_b, ltv_2day_b, ltv_3day_b, ltv_7day_b, ltv_30day_b))

      }
    }

    try {
      JdbcUtil.doBatch(sqlHourText, hourParams, conn)
      JdbcUtil.doBatch(sqlDayText, dayParams, conn)
      JdbcUtil.doBatch(sqlOperDayText, operDayParams, conn)
    } catch {
      case e: Exception => {
        logger.error("=========================插入insertPayMoney表异常：" + e)
      }
    }


  }


  /**
    * 支付日期和注册日期相差天数
    *
    * @param accountInfo
    * @param publish_date
    */
  def getltvDays(accountInfo: util.Map[String, String], publish_date: String): Int = {
    var ltvDay = 100
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd")
    val regTime = accountInfo.get("reg_time")
    val regDate = dateFormat.parse(if (regTime != null && (!regTime.equals(""))) regTime.substring(0, 10) else "0000-00-00")
    val loginDate = dateFormat.parse(publish_date)
    ltvDay = ((loginDate.getTime - regDate.getTime) / (1000 * 3600 * 24)).toInt
    return ltvDay
  }

  def main(args: Array[String]): Unit = {
    val regTime = "2018-02-10 19:34:22"
    val payTime = "2018-02-09"

    val dateFormat = new SimpleDateFormat("yyyy-MM-dd")

    val a = dateFormat.parse(regTime)
    val b = dateFormat.parse(payTime)

    println("a : " + a)
    println("b : " + b)

    val res = b.getTime - a.getTime
    println(res)

    val res2 = ((b.getTime - a.getTime) / (1000 * 3600 * 24)).toInt
    println(res2)

    val res3 = b.compareTo(a)

    println(res3)


  }

  def foreachPartitionPayMoney(payMoneyDf: DataFrame) = {
    payMoneyDf.foreachPartition(rows => {
      if (!rows.isEmpty) {
        //创建jedis客户端
        val pool = JedisUtil.getJedisPool
        val jedis = pool.getResource

        val conn = JdbcUtil.getConn()
        val statement = conn.createStatement()

        insertPayMoney(rows, jedis, conn, statement)

        pool.returnResource(jedis)
        pool.destroy()
        statement.close()
        conn.close()
      }
    })

  }


  def foreachPartitionPayDev(payPeopleDf: DataFrame) = {
    payPeopleDf.foreachPartition(rows => {
      if (!rows.isEmpty) {
        //创建jedis客户端
        val pool = JedisUtil.getJedisPool
        val jedis = pool.getResource

        val conn = JdbcUtil.getConn()
        val statement = conn.createStatement()

        insertPayDevNum(rows, jedis, conn, statement)

        pool.returnResource(jedis)
        pool.destroy()
        statement.close()
        conn.close()

      }
    })
  }

  def insertPayDevNum(rows: Iterator[Row], jedis: Jedis, conn: Connection, statement: Statement) = {
    //更新天表
    val sqlDayText = "insert into bi_gamepublic_base_day_kpi(publish_date,child_game_id,medium_channel," +
      "ad_site_channel,pkg_code,pay_people_num," +
      "parent_game_id,os,medium_account,promotion_channel,promotion_mode,head_people,group_id)" +
      " values(?,?,?,?,?,?,?,?,?,?,?,?,?)" +
      " on duplicate key update pay_people_num=pay_people_num+?"
    val dayParams = new ArrayBuffer[Array[Any]]

    //更新明细表
    val sqlDetailText = "insert into bi_gamepublic_pay_device_detail(publish_date,game_id,medium_channel,ad_site_channel,pkg_code,imei,create_time) values(?,?,?,?,?,?,?)"
    val detailParams = new ArrayBuffer[Array[Any]]()

    //更新日报
    val sql_opera_kpi = "insert into bi_gamepublic_base_opera_kpi(publish_date,parent_game_id,child_game_id,os,group_id,pay_people_num) values(?,?,?,?,?,?) on duplicate key update pay_people_num=pay_people_num+VALUES(pay_people_num)"
    val Params_opera_kpi_params = new ArrayBuffer[Array[Any]]()

    for (row <- rows) {
      var expandChannel = jedis.hget(row.getString(3), "expand_channel")
      if (expandChannel == null || expandChannel.equals("no_acc")) {
        logger.error("the account is not into redis : " + row.getString(3))
        expandChannel = "no_acc"
        jedis.set("no_acc|" + row.getString(3), row.getString(3))
        jedis.expire("no_acc|" + row.getString(3), 3600 * 24 * 3)
      }

      val channelArray = StringUtils.getArrayChannel(expandChannel)
      val game_id = row.getInt(1)
      val pkg_code = channelArray(2)
      val order_date = row.getString(0)
      val redisValue = JedisUtil.getRedisValue(game_id, pkg_code, order_date, jedis)
      val rs = statement.executeQuery("select count(1) from bi_gamepublic_pay_device_detail where publish_date ='" + row.getString(0) + "' and game_id = " + row.getInt(1) + "" +
        " and imei='" + row.getString(2) + "' and medium_channel = '" + channelArray(0) + "' and ad_site_channel = '" + channelArray(1) + "' and pkg_code='" + channelArray(2) + "'")
      if (rs.next()) {
        if (rs.getInt(1) == 0) {
          if (channelArray(0).length <= 10 && channelArray(1).length <= 10 && channelArray(2).length <= 15) {
            dayParams.+=(Array[Any](row.get(0), row.get(1), channelArray(0), channelArray(1), channelArray(2), 1, redisValue(0), redisValue(1), redisValue(2), redisValue(3), redisValue(4), redisValue(5), redisValue(6), 1))
            detailParams.+=(Array[Any](row.get(0), row.get(1), channelArray(0), channelArray(1), channelArray(2), row.get(2), DateUtils.getTodayTime()))
          }
        }
      }
      val rs_oper = statement.executeQuery("select count(1) from bi_gamepublic_pay_device_detail where publish_date = '" + row.getString(0) + "' and game_id = '" + row.getInt(1) + "' and imei='" + row.getString(2) + "'")
      if (rs_oper.next) {
        if (rs_oper.getInt(1) == 0) {
          if (channelArray(0).length <= 10 && channelArray(1).length <= 10 && channelArray(2).length <= 15) {
            Params_opera_kpi_params.+=(Array[Any](row.get(0), redisValue(0), row.get(1), redisValue(1), redisValue(6), 1))
          }
        }
      }
    }
    try {
      JdbcUtil.doBatch(sqlDetailText, detailParams, conn)
      JdbcUtil.doBatch(sqlDayText, dayParams, conn)
      JdbcUtil.doBatch(sql_opera_kpi, Params_opera_kpi_params, conn)
    } catch {
      case e: Exception => {
        logger.error("=========================插入foreachPartitionPayPeople表异常：" + e)
      }
    }


  }

  def insertPayAccountNum(rows: Iterator[Row], jedis: Jedis, conn: Connection, statement: Statement) = {
    if (!rows.isEmpty) {
      val sqlDayText = "insert into bi_gamepublic_base_day_kpi(publish_date,child_game_id,medium_channel,ad_site_channel,pkg_code,pay_account_num,parent_game_id,os,medium_account,promotion_channel,promotion_mode,head_people,group_id) values(?,?,?,?,?,?,?,?,?,?,?,?,?) on duplicate key update pay_account_num=pay_account_num+?"
      val sqlHourText = "insert into bi_gamepublic_basekpi(publish_time,game_id,parent_channel,child_channel,ad_label,dau_pay_account_num,parent_game_id,os,medium_account,promotion_channel,promotion_mode,head_people,group_id) values(?,?,?,?,?,?,?,?,?,?,?,?,?) on duplicate key update dau_pay_account_num=dau_pay_account_num+?"
      val sqlDetailText = "insert into bi_gamepublic_pay_account_detail(publish_date,game_id,medium_channel,ad_site_channel,pkg_code,game_account,create_time) values(?,?,?,?,?,?,?)"
      val sqloperDayText = "insert into bi_gamepublic_base_opera_kpi(publish_date,child_game_id,recharge_accounts) values(?,?,?) on duplicate key update recharge_accounts=recharge_accounts+values(recharge_accounts)"

      val dayParams = new ArrayBuffer[Array[Any]]()
      val hourParams = new ArrayBuffer[Array[Any]]()
      val detailParams = new ArrayBuffer[Array[Any]]()
      val operDayParams = new ArrayBuffer[Array[Any]]()

      for (row <- rows) {
        val order_hour = row.getString(0)
        val order_date = order_hour.split(" ")(0)
        val game_id = row.getInt(1)
        val game_account = row.getString(2).toLowerCase().trim

        //注册相关信息
        val accountInfo = jedis.hgetAll(game_account)
        val regi_game_id = if (accountInfo.get("game_id") == null) 0 else accountInfo.get("game_id")
        val regi_date = if (accountInfo.get("reg_time") == null) "0000-00-00" else accountInfo.get("reg_time").split(" ")(0)
        var expandChannel = accountInfo.get("expand_channel")
        if (expandChannel == null || expandChannel.equals("no_acc")) {
          logger.error("the account is not into redis:" + row.getString(2))
          expandChannel = "no_acc"
        }
        val channelArray = StringUtils.getArrayChannel(expandChannel)
        val pkg_code = channelArray(2)

        //按小时去重
        val sql_hour = "select count(1) from bi_gamepublic_pay_account_detail where publish_date ='" + order_hour + "' and game_id = '" + game_id + "' and game_account='" + game_account + "' and medium_channel = '" + channelArray(0) + "' and ad_site_channel = '" + channelArray(1) + "' and pkg_code='" + channelArray(2) + "'"
        val rs_hour = statement.executeQuery(sql_hour)

        if (rs_hour.next()) {
          if (rs_hour.getInt(1) == 0) {
            //维度信息
            val redisValue = JedisUtil.getRedisValue(game_id, pkg_code, order_date, jedis)
            if (channelArray(0).length <= 10 && channelArray(1).length <= 10 && channelArray(2).length <= 15) {
              hourParams.+=(Array[Any](order_hour, game_id, channelArray(0), channelArray(1), channelArray(2), 1, redisValue(0), redisValue(1), redisValue(2), redisValue(3), redisValue(4), redisValue(5), redisValue(6), 1))
              detailParams.+=(Array[Any](order_hour, game_id, channelArray(0), channelArray(1), channelArray(2), game_account, DateUtils.getTodayTime()))
            }
          }
        }

        //按天去重
        val sql_day = "select count(1) from bi_gamepublic_pay_account_detail where date(publish_date) ='" + order_date + "' and game_id = '" + game_id + "' and game_account='" + game_account + "' and medium_channel = '" + channelArray(0) + "' and ad_site_channel = '" + channelArray(1) + "' and pkg_code='" + channelArray(2) + "'"
        val rs_day = statement.executeQuery(sql_day)

        if (rs_day.next()) {
          if (rs_day.getInt(1) == 0) {
            val redisValue = JedisUtil.getRedisValue(game_id, pkg_code, order_date, jedis)
            if (channelArray(0).length <= 10 && channelArray(1).length <= 10 && channelArray(2).length <= 15) {
              dayParams.+=(Array[Any](order_date, game_id, channelArray(0), channelArray(1), channelArray(2), 1, redisValue(0), redisValue(1), redisValue(2), redisValue(3), redisValue(4), redisValue(5), redisValue(6), 1))
            }
          }
        }


        //永久去重
        val sql_all = "select count(1) from bi_gamepublic_pay_account_detail where game_account='" + game_account + "'"
        val rs_all = statement.executeQuery(sql_all)
        if (rs_all.next()) {
          if (rs_all.getInt(1) == 0) {
            if (channelArray(0).length <= 10 && channelArray(1).length <= 10 && channelArray(2).length <= 15) {
              operDayParams.+=(Array[Any](regi_date, regi_game_id, 1))
            }
          }
        }
      }
      try {
        JdbcUtil.doBatch(sqlDetailText, detailParams, conn)
        JdbcUtil.doBatch(sqlHourText, hourParams, conn)
        JdbcUtil.doBatch(sqlDayText, dayParams, conn)
        JdbcUtil.doBatch(sqloperDayText, operDayParams, conn)
      } catch {
        case e: Exception => {
          logger.error("===================插入insertPayAccountNum表异常：" + e)
        }
      }

    }
  }

  def foreachPartitionPayAccount(payAccountDf: DataFrame) = {
    payAccountDf.foreachPartition(rows => {
      if (!rows.isEmpty) {
        //创建jedis客户端
        val pool: JedisPool = JedisUtil.getJedisPool;
        val jedis = pool.getResource

        val conn = JdbcUtil.getConn()
        val statement = conn.createStatement

        //插入充值帐号数到mysql
        insertPayAccountNum(rows, jedis, conn, statement)

        pool.returnResource(jedis)
        pool.destroy()
        statement.close()
        conn.close()
      }
    })
  }


}
