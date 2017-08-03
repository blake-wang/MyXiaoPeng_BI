package cn.xiaopeng.bi.utils

import java.sql.{Connection, PreparedStatement}

import cn.wanglei.bi.checkdata.MissInfo2Redis
import redis.clients.jedis.Jedis

/**
  * Created by bigdata on 17-8-3.
  */
object GamePublicDao2 {

  //判断是否为今天已经登录过，若登录过则不再计算
  def isLoginDevDay(imei: String, publishDate: String, gameId: Int, jedis: Jedis): Int = {
    var res = 1
    if (jedis.exists("isLoginDevDay2|" + publishDate + "|" + imei + "|" + gameId.toString)) {
      res = 0
    }
    return res
  }


  def isNewRegiDevDay(imei: String, publishDate: String, gameId: Int, conn: Connection): Int = {
    var res = 0
    val sql2Mysql = "select left(regi_hour,10) as publish_date from bi_gamepublic_regi_detail " +
      "where imei=? and  game_id=? order by regi_hour asc limit 1"
    val ps: PreparedStatement = conn.prepareStatement(sql2Mysql)
    ps.setString(1, imei)
    ps.setInt(2, gameId)
    val result = ps.executeQuery()
    while (result.next()) {
      if (result.getString("publish_date").equals(publishDate)) {
        res = 1
      } else res = 0
    }
    ps.close
    return res
  }


  //是否为当日注册帐号
  def isNewRegiAccountDay(gameAccount: String, regiDate: String, isNewRegiDevDay: Int, Imei: String, jedis: Jedis): Int = {
    var res = 0
    val arr = jedis.hgetAll(gameAccount)
    var reg_time = arr.get("reg_time")
    val imei = arr.get("imei")
    if (reg_time == null) {
      val s = MissInfo2Redis.checkAccount(gameAccount)
      reg_time = jedis.hget(gameAccount, "reg_time")
    }
    if ((reg_time.contains(regiDate)) && isNewRegiDevDay == 1 && imei != null && imei.equals(Imei)) {
      res = 1
    } else res = 0
    return res
  }

  //从库中判断是否为新登录设备
  def isNewLgDevDay(imei: String, publishDate: String, gameId: Int, isNewRegiDevDay: Int, isNewRegiAccountDay: Int, jedis: Jedis): Int = {
    var res = 0
    //为当天新增注册设备并且今天第一次登录（今天第二次登录不再统计）
    if (isNewRegiDevDay == 1 && isNewRegiDevDay == 1 && (!jedis.exists("isNewLgDev2|" + publishDate + "|" + imei + "|" + gameId.toString))) {
      res = 1
    }
    return res
  }

  //从库中判断是否为新帐号
  def isNewLgAccountDay(gameAccount: String, publishDate: String, gameId: Int, isNewRegiDevDay: Int, isNewRegiAccountDay: Int, jedis: Jedis): Int = {
    var res = 1
    //第一次当日新增注册设备，帐号为当天注册，只取第一次登录(不再算多次登录)
    if (isNewRegiDevDay == 1 && isNewRegiAccountDay == 1 && (!jedis.exists("isLoginAccountDay2|" + publishDate + "|" + gameAccount + "|" + gameId.toString))) {
      res = 1
    } else res = 0
    return res
  }

  //加载数据到xiaopeng2bi 主数据库，推送数据到bi_gamepublic_actions的登录相关字段
  def loginActionsByDayProcessDB(tp10: (String, String, Int, String, Int, Int, Int, Int, String, String), conn: Connection): Unit = {
    val sql2Mysql = "insert into bi_gamepublic_base_opera_kpi" +
      "(publish_date,parent_game_id,child_game_id,os,group_id,new_login_device_num,new_login_account_num,dau_device_num) " +
      "values(?,?,?,?,?,?,?,?) " +
      "on duplicate key update new_login_device_num=new_login_device_num+?,new_login_account_num=new_login_account_num+?,dau_device_num=dau_device_num+?";
    val ps:PreparedStatement = conn.prepareStatement(sql2Mysql)
    //insert
    ps.setString(1, tp10._1) //date
    ps.setString(2, tp10._2) //parent_game_id
    ps.setInt(3, tp10._3) //game_id
    ps.setString(4, tp10._4) //os
    ps.setInt(5, tp10._5) //gropuid
    ps.setInt(6, tp10._6) //isNewLgDev
    ps.setInt(7, tp10._7) //isNewLgAccount
    ps.setInt(8, tp10._8) //isLoginDevDay
    //update
    ps.setInt(9, tp10._6) //isNewLgDev
    ps.setInt(10, tp10._7) //isNewLgAccount
    ps.setInt(11, tp10._8) //isLoginDevDay
    ps.executeUpdate()
    ps.close()
  }

}
