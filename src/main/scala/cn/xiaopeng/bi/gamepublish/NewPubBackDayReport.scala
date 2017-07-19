package cn.xiaopeng.bi.gamepublish

import java.sql.ResultSet
import java.text.SimpleDateFormat
import java.util.Date

import cn.xiaopeng.bi.utils.JdbcUtil

import scala.collection.mutable.ArrayBuffer

/**
  * Created by bigdata on 7/17/17.
  */
object NewPubBackDayReport {
  def main(args: Array[String]): Unit = {
    val df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    println("start time: "+df.format(new Date()))
    println("start day report,the start time and end time is: "+args(0) +" - "+args(1))
    val conn = JdbcUtil.getConn()
    val statement = conn.createStatement()
    val querySql = "select bdk.publish_date,bdk.parent_game_id,bdk.child_game_id,bdk.medium_channel,bdk.ad_site_channel,bdk.pkg_code," +
      "bdk.os,bdk.medium_account,bdk.promotion_channel,bdk.promotion_mode,bdk.head_people,bdk.group_id,bdk.adpage_click_uv,bdk.request_click_uv,bdk.download_uv," +
      "bdk.new_regi_device_num,bdk.new_login_device_num,bdk.new_active_device_num,bdk.new_regi_account_num,ga.dev_add_lg_times new_regi_device_login_num,bdk.active_num,bdk.pay_people_num order_num,bdk.pay_money order_amount" +
      ",ga.dev_retained_2day retained_1day,ga.dev_retained_3day retained_3day,ga.dev_retained_7day retained_7day,ga.recharge_lj_1 order_amount_1day,ga.recharge_lj_3 order_amount_3day,ga.recharge_lj_7 order_amount_7day " +
      "from bi_gamepublic_base_day_kpi bdk left join bi_gamepublic_actions ga on bdk.publish_date=ga.publish_date and bdk.child_game_id=ga.child_game_id and bdk.medium_channel=ga.medium_channel and bdk.ad_site_channel=ga.ad_site_channel and bdk.pkg_code=ga.pkg_code " +
      "where bdk.publish_date>='"+args(0)+"' and bdk.publish_date<='"+args(1)+"'"
    val resultSet = statement.executeQuery(querySql)
    val statement2 = conn.createStatement()
    val statement3 = conn.createStatement()
    val statement7 = conn.createStatement()

    val sqlText = "insert into bi_gamepublic_day(publish_date,parent_game_id,child_game_id,medium_channel,ad_site_channel," +
      "pkg_code,os,medium_account,promotion_channel,promotion_mode,head_people,group_id,adpage_click_uv,request_click_uv,download_uv," +
      "new_regi_device_num,new_login_device_num,new_active_device_num,new_regi_account_num,new_regi_device_login_num,active_num,order_num," +
      "order_amount,retained_1day,retained_3day,retained_7day,order_num_1day,order_num_3day,order_num_7day,order_amount_1day," +
      "order_amount_3day,order_amount_7day) " +
      "values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) " +
      "on duplicate key update parent_game_id=?,os=?,medium_account=?,promotion_channel=?,promotion_mode=?,head_people=?,group_id=?,adpage_click_uv=?,request_click_uv=?,download_uv=?," +
      "new_regi_device_num=?,new_login_device_num=?,new_active_device_num=?,new_regi_account_num=?,new_regi_device_login_num=?,active_num=?,order_num=?," +
      "order_amount=?,retained_1day=?,retained_3day=?,retained_7day=?,order_num_1day=?,order_num_3day=?,order_num_7day=?,order_amount_1day=?,order_amount_3day=?,order_amount_7day=?"
    val params = new ArrayBuffer[Array[Any]]()
    while(resultSet.next()){
      val pkgCode = resultSet.getString("pkg_code")
      if(!pkgCode.contains("\\")){
        val order_num_1day = 0 //指标废弃
        val order_num_3day = 0//指标废弃
        val order_num_7day = 0//指标废弃

        params.+=(Array[Any](resultSet.getDate("publish_date"),resultSet.getInt("parent_game_id"),resultSet.getInt("child_game_id"),resultSet.getString("medium_channel"),resultSet.getString("ad_site_channel"),
          resultSet.getString("pkg_code"),resultSet.getInt("os"),resultSet.getString("medium_account"),resultSet.getString("promotion_channel"),resultSet.getString("promotion_mode"),
          resultSet.getString("head_people"),resultSet.getInt("group_id"),resultSet.getInt("adpage_click_uv"),resultSet.getInt("request_click_uv"),resultSet.getInt("download_uv"),
          resultSet.getInt("new_regi_device_num"),resultSet.getInt("new_login_device_num"),resultSet.getInt("new_active_device_num"),resultSet.getInt("new_regi_account_num"),
          resultSet.getInt("new_regi_device_login_num"),resultSet.getInt("active_num"),resultSet.getInt("order_num"),resultSet.getInt("order_amount"),
          resultSet.getInt("retained_1day"),resultSet.getInt("retained_3day"),resultSet.getInt("retained_7day"),order_num_1day,order_num_3day,order_num_7day,
          resultSet.getInt("order_amount_1day"),resultSet.getInt("order_amount_3day"),resultSet.getInt("order_amount_7day")
          ,resultSet.getInt("parent_game_id"),resultSet.getInt("os"),resultSet.getString("medium_account"),resultSet.getString("promotion_channel"),resultSet.getString("promotion_mode"),
          resultSet.getString("head_people"),resultSet.getInt("group_id"),resultSet.getInt("adpage_click_uv"),resultSet.getInt("request_click_uv"),resultSet.getInt("download_uv"),
          resultSet.getInt("new_regi_device_num"),resultSet.getInt("new_login_device_num"),resultSet.getInt("new_active_device_num"),resultSet.getInt("new_regi_account_num"),
          resultSet.getInt("new_regi_device_login_num"),resultSet.getInt("active_num"),resultSet.getInt("order_num"),resultSet.getInt("order_amount"),
          resultSet.getInt("retained_1day"),resultSet.getInt("retained_3day"),resultSet.getInt("retained_7day"),order_num_1day,order_num_3day,order_num_7day,
          resultSet.getInt("order_amount_1day"),resultSet.getInt("order_amount_3day"),resultSet.getInt("order_amount_7day")
        ))
      }
    }

    try {
      println("sqlText: " + sqlText)
      JdbcUtil.doBatch(sqlText, params, conn)
    } finally{
      statement.close()
      statement2.close()
      statement3.close()
      statement7.close()
    }
    println("end day report");
    println("end time: " + df.format(new Date()))
  }

  def getPayPeopleNumResult(resultSet:ResultSet)={
    if(resultSet.next()){
      resultSet.getInt(1)
    }
  }

}
