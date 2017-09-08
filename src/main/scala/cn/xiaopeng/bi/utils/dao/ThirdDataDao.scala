package cn.xiaopeng.bi.utils.dao

import java.sql.{Connection, PreparedStatement}

/**
  * Created by bigdata on 17-9-8.
  */
object ThirdDataDao {
  def insertClickStat(clickDate: String, gameId: Int, group_id: String, pkgCode: String, head_people: String, medium_account: String, medium: Int, idea_id: String, first_level: String, second_level: String, clicks: Int, clickDevs: Int, conn: Connection) = {
    
  }


  //插入点击数据
  def insertMomoClick(pkgCode: String, imei: String, ts: String, os: String, url: String, gameId: String, advName: Int, conn: Connection) = {
    val instSql = "insert into bi_ad_momo_click(pkg_id,imei,ts,os,callback,game_id,adv_name) values(?,?,?,?,?,?,?)"
    val ps: PreparedStatement = conn.prepareStatement(instSql)
    ps.setString(1, pkgCode)
    ps.setString(2, imei)
    ps.setString(3, ts)
    ps.setString(4, os)
    ps.setString(5, url)
    ps.setString(6, gameId)
    ps.setInt(7, advName)
    ps.executeUpdate()
    ps.close()
  }

  //更新陌陌单击时间ts
  def updateMomoClickTs(pkgCode: String, imei: String, ts: String, url: String, conn: Connection) = {
    val instSql = "update bi_ad_momo_click set ts=?,callback=? where pkg_id=? and imei=?"
    val ps: PreparedStatement = conn.prepareStatement(instSql)
    ps.setString(1, ts)
    ps.setString(2, url)
    ps.setString(3, pkgCode)
    ps.setString(4, imei)
    ps.executeUpdate()
    ps.close()
  }
}
