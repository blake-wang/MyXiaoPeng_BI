package cn.xiaopeng.bi.utils

import java.sql.{Connection, PreparedStatement, ResultSet}

import redis.clients.jedis.Jedis


/**
  * Created by bigdata on 17-9-8.
  */
object CommonsThirdData {

  //检测单击设备数，一天只计算一次
  def isClickDev(clickDate: String, pkgCode: String, imei: String, topic: String, jedis6: Jedis): Int = {
    var jg = 0
    if (jedis6.exists("click|" + topic + "|" + pkgCode + "|" + imei + "|" + clickDate)) {
      jg = 0
    } else {
      jg = 1
      jedis6.set("click|" + topic + "|" + pkgCode + "|" + imei + "|" + clickDate, topic)
      jedis6.expire("click|" + topic + "|" + pkgCode + "|" + imei + "|" + clickDate, 1000 * 3600 * 48)
    }
    return jg
  }


  //获取发行游戏媒介信息
  def getRedisValue(game_id: Int, pkg_code: String, order_date: String, jedis: Jedis, connFx: Connection) = {
    //取出parent_game_id
    var parent_game_id = jedis.hget(game_id.toString + "_publish_game", "mainid")
    //如果parent_game_id不存在，就默认为 0
    if (parent_game_id == null) parent_game_id = "0"

    var medium_account = jedis.hget(pkg_code + "_pkgcode", "medium_account")
    if (medium_account == null) {
      jedis.set("no_medium_account" + pkg_code, order_date)
      jedis.expire("no_medium_account" + pkg_code, 1000 * 3600 * 30)
      medium_account = ""
    }

    var promotion_channel = jedis.hget(pkg_code + "_pkgcode", "promotion_channel")
    if (promotion_channel == null) promotion_channel = ""

    var promotion_mode = jedis.hget(pkg_code + "_" + order_date + "_pkgcode", "promotion_mode")
    if (promotion_mode == null) promotion_mode = ""
    var head_people = jedis.hget(pkg_code + "_" + order_date + "_pkgcode", "head_people")
    if (head_people == null) {
      jedis.set("no_head_people" + pkg_code, order_date)
      jedis.expire("no_head_peole" + pkg_code, 1000 * 3600 * 28)
      head_people = ""
    }
    val os = getPubGameGroupIdAndOs(game_id, connFx)(1)
    val groupid = getPubGameGroupIdAndOs(game_id, connFx)(0)

    Array[String](parent_game_id, os, medium_account, promotion_channel, promotion_mode, head_people, groupid)
  }

  //发行组和平台
  def getPubGameGroupIdAndOs(game_id: Int, connFx: Connection): Array[String] = {
    var jg = Array[String]("0", "1")
    var stmt: PreparedStatement = null
    val sql: String = " select distinct system_type os,group_id from game_sdk  where old_game_id=? limit 1"
    stmt = connFx.prepareStatement(sql)
    stmt.setInt(1, game_id)
    val rs: ResultSet = stmt.executeQuery()
    while (rs.next()) {
      jg = Array[String](rs.getString("group_id"), rs.getString("os"))
    }
    stmt.close()
    return jg
  }


  //点击记录是否存在
  def isExistClick(pkgCode: String, imei: String, url: String, conn: Connection): Int = {
    var jg = 0
    val sql = "select callback from bi_ad_momo_click where pkg_id=? and imei=? and matched=0 limit 1"
    val stmt = conn.prepareStatement(sql)
    stmt.setString(1, pkgCode)
    stmt.setString(2, imei)
    val rs = stmt.executeQuery()
    while (rs.next()) {
      jg = 1
    }
    stmt.close()
    return jg
  }


  //有广告监控的游戏才统计
  def isNeedStaGameId(gameId: Int, conn: Connection): Boolean = {

    var jg = false
    var stmt: PreparedStatement = null
    val sql = "select 1 as flag from bi_ad_momo_click where game_id=? limit 1"
    stmt = conn.prepareStatement(sql)
    stmt.setInt(1, gameId)
    val rs: ResultSet = stmt.executeQuery()
    while (rs.next()) {
      if (rs.getString("flag").toInt == 1) {
        jg = true
      }
    }
    stmt.close()
    return jg
  }

}
