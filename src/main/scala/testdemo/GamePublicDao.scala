package testdemo

import java.sql.{Connection, PreparedStatement}

import cn.wanglei.bi.utils.MissInfo2Redis
import cn.xiaopeng.bi.utils.JedisUtil

/**
  * Created by bigdata on 17-8-18.
  */
object GamePublicDao {
  //是否为当日新增注册设备
  def isNewRegiDevDay(pkgCode: String, imei: String, publishDate: String, medium_channel: String, ad_site_channel: String, gameId: Int, conn: Connection): Int = {
    var res = 0;
    val sql2Mysql = "select left(regi_hour,10) as publish_date from bi_gamepublic_regi_detail where ad_label=? and imei=? and parent_channel=? and child_channel=? and game_id=? order by regi_hour asc limit 1"
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

  //是否当日注册帐号
  def isNewRegiAccountDay(gameAccount: String, regiDate: String, isNewRegiDevDay: Int, pkgId: String, Imei: String): Int = {
    var jedis = JedisUtil.getJedisPool.getResource
    var res = 0
    jedis.select(0)
    val arr = jedis.hgetAll(gameAccount)
    var reg_time = arr.get("reg_time")
    val expandChannel = arr.get("expand_channel")
    val imei = arr.get("imei")
    if (reg_time == null) {
      val s = MissInfo2Redis.checkAccount(gameAccount)
      reg_time = jedis.hget(gameAccount, "reg_time")
    }

    if((reg_time.contains(regiDate)) && isNewRegiDevDay == 1 && imei != null && expandChannel.equals(pkgId) && imei.equals(imei)){
      res = 1
    }else {
      res = 0
    }
    return res
  }
}
