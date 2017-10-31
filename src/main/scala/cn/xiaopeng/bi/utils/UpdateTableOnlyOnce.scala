package cn.xiaopeng.bi.utils

import cn.wanglei.bi.utils.MD5Util

import scala.collection.mutable.ArrayBuffer


/**
  * Created by bigdata on 17-9-16.
  * 用来更新bi_adv_momo_click 历史数据 执行一次
  */
object UpdateTableOnlyOnce {

  def main(args: Array[String]): Unit = {
    var currentday = "2017-09-18"
    if (args.length > 0) {
      currentday = args(0)
    }

//    update(currentday)
  }


  def update(currentday: String) = {
    val conn = JdbcUtil.getConn()
    val selectSql = "select pkg_id,game_id,imei,imei_md5_upper,os,ts,adv_name from bi_ad_momo_click where date(ts) <= '" + currentday + "'"
    val stat = conn.createStatement()
    val resultSet = stat.executeQuery(selectSql)
    var buffer = ArrayBuffer[Array[String]]()
    while (resultSet.next()) {
      val pkg_code = resultSet.getString("pkg_id")
      val game_id = resultSet.getString("game_id")
      val imei = resultSet.getString("imei")
      val imei_md5_upper = resultSet.getString("imei_md5_upper")
      val os = resultSet.getString("os")
      val ts = resultSet.getString("ts")
      val adv_name = resultSet.getString("adv_name")
      println("查询 ： " + pkg_code + " - " + game_id + " - " + imei + " - " + imei_md5_upper + " - " + os + " - " + ts + " - " + adv_name)
      buffer.+=(Array[String](pkg_code, game_id, imei, imei_md5_upper, os, ts, adv_name))
    }
    stat.close()

    for (arr <- buffer) {
      val pkg_code = arr(0)
      val game_id = arr(1)
      val imei = arr(2)
      var imei_md5_upper = arr(3)
      val os = arr(4)
      val ts = arr(5).toString.split("\\.", -1)(0)
      val adv_name = arr(6).toInt

      if (os.equals("ios")) {
        if (adv_name == 5 || adv_name == 6) {
          imei_md5_upper = imei.toUpperCase
        } else {
          imei_md5_upper = MD5Util.md5(imei.toString).toUpperCase
        }

      } else if (os.equals("android")) {
        imei_md5_upper = imei.toString.toUpperCase
      }
      println("更新 ： " + pkg_code + " - " + game_id + " - " + imei + " - " + imei_md5_upper + " - " + os + " - " + ts + " - " + adv_name)
      val updateSql = "update bi_ad_momo_click set imei_md5_upper=? where pkg_id=? and game_id=? and imei=? and os=? and ts=? and adv_name=?"
      val pstat = conn.prepareStatement(updateSql)
      pstat.setString(1, imei_md5_upper)
      pstat.setString(2, pkg_code)
      pstat.setString(3, game_id)
      pstat.setString(4, imei)
      pstat.setString(5, os)
      pstat.setString(6, ts)
      pstat.setInt(7, adv_name)
      val i = pstat.executeUpdate()
      println("更新了： " + i + " 行 ")
      pstat.close()
    }
    conn.close()
  }


}
