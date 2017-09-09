package cn.xiaopeng.bi.utils

import java.sql.{Connection, PreparedStatement, ResultSet}
import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import redis.clients.jedis.Jedis


/**
  * Created by bigdata on 17-9-8.
  */
object CommonsThirdData {
  //注册设备数，一天只能计算一次
  def isRegiDev(regiDate: String, pkgCode: String, imei: String, topic: String, jedis6: Jedis) = {
    //默认这个设备已经注册过
    var jg = 0
    if(jedis6.exists("regi|"+topic+"|"+pkgCode+"|"+imei+"|"+regiDate)){
      //如果redis中存在数据，则注册设备数 返回0
      jg=0
    }else{
      jg=1
      jedis6.set("regi|"+topic+"|"+pkgCode+"|"+imei+"|"+regiDate,topic)
      jedis6.expire("regi|"+topic+"|"+pkgCode+"|"+imei+"|"+regiDate,1000*3600*48)
    }
  }


  def regiMatchActive(pkgCode: String, imei: String, dt30before: String, regiTime: String, gameId: Int, os: Int, conn: Connection): Tuple5[Int, String, String, String, String] = {
    var tp5 = Tuple5(0, "", "", "", "")
    var adName = 0
    var ideaId = ""
    var firstLevel = ""
    var secondLevel = ""
    var pkgId = ""
    var stmt: PreparedStatement = null
    if (os == 2) {
      //苹果设备
      //如果是苹果设备，用imei和gameId 来匹配
      val sql: String = "select adv_name,pkg_id,idea_id,first_level,second_level from bi_ad_active_o_detail where imei=?  and active_time>? and active_time<=? and game_id=? limit 1"
      stmt = conn.prepareStatement(sql)
      stmt.setString(1, imei)
      stmt.setString(2, dt30before)
      stmt.setString(3, regiTime)
      stmt.setInt(4, gameId)
    } else {
      //安卓设备
      val sql: String = "select adv_name,pkg_id,idea_id,first_level,second_level from bi_ad_active_o_detail where imei=? and  pkg_id=? and active_time>? and active_time<=? and game_id=? limit 1"
      stmt = conn.prepareStatement(sql)
      stmt.setString(1, imei)
      stmt.setString(2, pkgCode)
      stmt.setString(3, dt30before)
      stmt.setString(4, regiTime)
      stmt.setInt(5, gameId)
    }
    val rs: ResultSet = stmt.executeQuery()
    while (rs.next()) {
      ideaId = rs.getString("idea_id")
      firstLevel = rs.getString("first_level")
      secondLevel = rs.getString("second_level")
      adName = rs.getString("adv_name").toInt
      pkgId = rs.getString("pkg_id")
    }
    stmt.close()
    tp5 = new Tuple5(adName, ideaId, firstLevel, secondLevel, pkgId)
    return tp5
  }


  //排除imei号为
  //00000000000000000000000000000000  32个0
  //000000000000000  15个0
  //momo  比较特殊，是加密的  5284047f4ffb4e04824a2fd1d1f0cd62
  def isVadDev(imei: String, os: Int, advName: Int): Boolean = {
    var jg = true
    if (imei.replace("-", "").equals("00000000000000000000000000000000") || imei.replace("-", "").equals("000000000000000") || imei.replace("-", "").equals("")) {
      jg = false
    }
    //如果是陌陌，匹配加密,  os=1  是android设备
    if (os == 1 && advName == 1 && imei.equals("5284047f4ffb4e04824a2fd1d1f0cd62")) {
      jg = false
    }
    return jg
  }


  //获取7天前的日期，从今天向前推7天
  def getDt7Before(pidt: String, i: Int): String = {
    val dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
    val cal: Calendar = Calendar.getInstance()
    val date: Date = dateFormat.parse(pidt)
    cal.setTime(date)
    cal.add(Calendar.DATE, i)
    val dt = dateFormat.format(cal.getTime)
    return dt
  }

  //获取原生idfa，带横杠的 -
  def getYSIdfa(idfa: String): String = {
    var jg = idfa
    if (jg.length == 32) {
      jg = jg.substring(0, 8) + jg.substring(8, 12) + jg.substring(12, 16) + jg.substring(16, 20) + jg.substring(20, 32)
    }
    return jg

  }


  //获取imei
  def getImei(imei: String) = {
    var jg = ""
    if (imei.contains("&")) {
      //安卓设备取&符号前面的字段
      jg = imei.split("&", -1)(0)
    } else {
      jg = imei
    }
    jg
  }


  //检测单击设备数，一天只计算一次，这里是按设备去重的
  def isClickDev(clickDate: String, pkgCode: String, imei: String, topic: String, jedis6: Jedis): Int = {
    var jg = 0
    if (jedis6.exists("click|" + topic + "|" + pkgCode + "|" + imei + "|" + clickDate)) {
      //redis中存在，就不加1,这里是按imei去重的
      jg = 0
    } else {
      //redis中不存在，就加1
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


  //这个原始日志，是点击就会被保存
  //点击记录是否存在
  def isExistClick(pkgCode: String, imei: String, url: String, conn: Connection): Int = {
    var jg = 0
    //通过 imei 和 pkg_id 以及matched来匹配一条记录
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
    jg
  }

}
