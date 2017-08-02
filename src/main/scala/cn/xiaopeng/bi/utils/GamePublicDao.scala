package cn.xiaopeng.bi.utils

import java.sql.{Connection, PreparedStatement}

import cn.wanglei.bi.checkdata.MissInfo2Redis
import redis.clients.jedis.Jedis

/**
  * Created by bigdata on 7/18/17.
  */
object GamePublicDao {

  /**
    * 从库中判断是否为新注册设备
    *
    * @param pkgCode
    * @return
    */
  def isNewRegiDevDay(pkgCode: String, imei: String, publishDate: String, medium_channel: String, ad_site_channel: String, gameId: Int, conn: Connection): Int = {
    var res = 0
    val sql2Mysql = "select left(regi_hour,10) as publish_date from bi_gamepublic_regi_detail " +
      "where ad_label=? and imei=? and parent_channel=? and child_channel=? and game_id=? order by regi_hour asc limit 1"
    val ps: PreparedStatement = conn.prepareStatement(sql2Mysql)
    ps.setString(1, pkgCode)
    ps.setString(2, imei)
    ps.setString(3, medium_channel)
    ps.setString(4, ad_site_channel)
    ps.setInt(5, gameId)
    val result = ps.executeQuery()
    while (result.next()) {
      if (result.getString("publish_date").equals(publishDate)) {
        res = 1
      } else res = 0
    }
    ps.close()
    return res
  }

  /**
    * 是否当日注册账号
    *
    * @param gameAccount
    * @return
    */
  def isNewRegiAccountDay(gameAccount: String, regiDate: String, isNewRegiDay: Int, pkgId: String, Imei: String, jedis: Jedis): Int = {
    var res = 0
    jedis.select(0)
    val arr = jedis.hgetAll(gameAccount)
    var reg_time = arr.get("reg_time")
    val expandChannel = arr.get("expand_channel")
    val imei = arr.get("imei")
    if (reg_time == null) {
      val s = MissInfo2Redis.checkAccount(gameAccount)

    }
    1
  }

  def isNewLgAccountDay(pkgCode: String, gameAccount: String, publishDate: String, gameId: Int, isNewRegiDevDay: Int, isNewRegiAccountDay: Int, jedis: Jedis): Unit = {
    var res = 1;
    //jedis.select(3)
    //第一次当日新增注册设备，帐号为当天注册，只取第一次登录(不再算多次登录)
    if (isNewRegiDevDay == 1 && isNewRegiAccountDay == 1 && (!jedis.exists("isLoginAccountDay|" + publishDate + "|" + gameAccount + "|" + pkgCode + "|" + gameId.toString))) {
      res = 1
    } else res = 0
    return res;
  }


  //判断是否为新增注册活跃设备
  def isNewActiveDevDay(pkgCode: String, imei: String, publicDate: String, gameId: Int, jedis: Jedis): Unit = {
    var res = 0
    //jedis.select(3)
    var jg = jedis.get("isNewLgDev|" + publicDate + "|" + imei + "|" + pkgCode + "|" + gameId.toString)

    if (jg != null) {
      if (jg.equals("1")) {
        res = 1
      } else {
        res = 0
      }
      jg = (jg.toInt + 1).toString
      jedis.set("isNewLgDev|" + publicDate + "|" + imei + "|" + pkgCode + "|" + gameId.toString, jg)
      jedis.expire("isNewLgDev|" + publicDate + "|" + imei + "|" + pkgCode + "|" + gameId.toString, 3600 * 24)
    }
    return res
  }

  //判断是否为今天已经登录过，若登录过则不再计算
  def isLoginDevDay(pkgCode: String, imei: String, publishDate: String, gameId: Int, jedis: Jedis): Unit = {
    var res = 1
    //jedis.select(3)
    if (jedis.exists("isLoginDevDay|" + publishDate + "|" + imei + "|" + pkgCode + "|" + gameId.toString)) {
      res = 0
    }
    return res
  }

  def isLoginAccountDay(pkgCode: String, gameAccount: String, publishDate: String, gameId: Int, jedis: Jedis): Unit = {
    var res = 0
    if (!jedis.exists("isLoginAccountDay|" + publishDate + "|" + gameAccount + "|" + pkgCode + "|" + gameId.toString)) {
      res = 1
    }
    return res
  }

  //判断是否为今天已经登录过，若登录过则不再计算
  def isLoginAccountHour(pkgCode: String, gameAccount: String, publishTime: String, gameId: Int, jedis: Jedis): Unit = {
    var res = 1
    //jedis.select(3)
    if (jedis.exists("isLoginAccountHour|" + publishTime + "|" + gameAccount + "|" + pkgCode + "|" + gameId.toString)) {
      res = 0
    }
    return res
  }

  //从库中判断是否为新登录设备
  def isNewLgDevDay(pkgCode: String, imei: String, publishDate: String, gameId: Int, isNewRegiDevDay: Int, isNewRegiAccountDay: Int, jedis: Jedis): Unit = {
    var res = 0
    //为当天新增注册设备并且今天第一次登录（今天第二次登录等不再统计）
    if(isNewRegiDevDay == 1 && isNewRegiAccountDay == 1 && (!jedis.exists("isNewLgDev|" + publishDate + "|" + imei + "|" + pkgCode + "|" + gameId.toString))){
      res =1
    }
    return res

  }

}
