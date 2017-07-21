package cn.xiaopeng.bi.utils

import java.sql.{Connection, PreparedStatement}

/**
  * Created by bigdata on 7/19/17.
  */
object SubCenturionDao {

  /**
    *账号明细补数
    * @param gameAccount
    * @param gamiId
    * @param regiTime
    * @param regiDate
    * @param imei
    * @param ip
    * @param promoCode
    * @param userCode
    * @param pkgCode
    * @param memberId
    * @param userAccount
    * @param groupId
    * @param bindMember
    * @param memberRegiTime
    */
  def regiInfoProcessAccountBs(gameAccount: String, gamiId: String, regiTime: String, regiDate: String, imei: String,ip: String, promoCode: String,
                               userCode:String, pkgCode: String, memberId: String, userAccount: String,groupId:Int,bindMember:String,memberRegiTime:String,loginTime:String,conn:Connection)={
    val instSql = "insert into bi_sub_centurition_account" +
      "(game_account,game_id,regi_time,regi_date,imei,ip,promo_code,user_code,pkg_code,member_id,member_name,group_id,bind_phone,member_regi_time,last_login_time) " +
      "values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)" +
      " on duplicate key update group_id=?,bind_phone=?,last_login_time=?"
    val ps:PreparedStatement=conn.prepareStatement(instSql)
    ps.setString(1,gameAccount)
    ps.setString(2,gamiId)
    ps.setString(3,regiTime)
    ps.setString(4,regiDate)
    ps.setString(5,imei)
    ps.setString(6,ip)
    ps.setString(7, promoCode)
    ps.setString(8, userCode)
    ps.setString(9, pkgCode)
    ps.setString(10, memberId)
    ps.setString(11, userAccount)
    ps.setInt(12, groupId)
    ps.setString(13, bindMember)
    ps.setString(14, memberRegiTime)
    ps.setString(15, loginTime)
    //update
    ps.setInt(16, groupId)
    ps.setString(17, bindMember)
    ps.setString(18, loginTime)
    ps.executeUpdate()
    ps.close()
  }
  /**
    * 推送数据到统计表
    * @param regiDate
    * @param promoCode
    * @param userCode
    * @param pkgCode
    * @param memberId
    * @param userAccount
    * @param gamiId
    * @param groupId
    * @param memberRegiTime
    * @param regiAccounts
    * @param regiDevs
    * @param conn
    */
  def regiInfoProcessPkgStat(regiDate: String, promoCode: String, userCode: String, pkgCode: String, memberId: String, userAccount: String, gamiId: String, groupId: Int,
                             memberRegiTime: String, regiAccounts: Int, regiDevs: Int, conn: Connection) ={
    val instSql = "insert into bi_sub_centurition_pkgstats(statistics_date,pkg_code,promo_code,user_code,member_id,member_name,game_id,regi_accounts,regi_devs,group_id,member_regi_time)" +
      " values(?,?,?,?,?,?,?,?,?,?,?)" +
      " on duplicate key update regi_accounts=regi_accounts+?,regi_devs=regi_devs+?"
    val ps:PreparedStatement = conn.prepareStatement(instSql)
    ps.setString(1, regiDate)
    ps.setString(2, pkgCode)
    ps.setString(3, promoCode)
    ps.setString(4, userCode)
    ps.setString(5, memberId)
    ps.setString(6, userAccount)
    ps.setString(7, gamiId)
    ps.setInt(8, regiAccounts)
    ps.setInt(9, regiDevs)
    ps.setInt(10, groupId)
    ps.setString(11, memberRegiTime)
    //update
    ps.setInt(12, regiAccounts)
    ps.setInt(13, regiDevs)
    ps.executeUpdate()
    ps.close()
  }

}
