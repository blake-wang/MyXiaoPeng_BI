package cn.xiaopeng.bi.crontal

import java.sql.{PreparedStatement, ResultSet}
import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import cn.xiaopeng.bi.utils.JdbcUtil

/**
  * Created by bigdata on 17-9-18.
  * 广告监测，自然量数据导入到广告统计表
  */
object ADNaturalData {

  def main(args: Array[String]): Unit = {
    var currentday = args(0)
    val dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
    val date: Date = dateFormat.parse(currentday)
    val cal: Calendar = Calendar.getInstance()
    cal.setTime(date)
    //获取昨天
    cal.add(Calendar.DATE, -1)
    val yesterday = dateFormat.format(cal.getTime)
    val d: Date = new Date()
    val hours = d.getHours
    //如果时间早于2点，不跑今天的数据，只跑昨天的
    if (hours <= 2) {
      currentday = yesterday
    }
    //早上  2-8  点 不知行程序
    if (hours > 2 && hours < 7) {
      Thread.sleep(1000 * 1200)
      println("wait")
    } else {
      //1、处理苹果设备的数据
      var tp2 = getMedPkgGameInfo(2)
      //如果游戏不存在，就不处理了
      if (!tp2.equals("0")) {
        doADADataPay(tp2, 2, currentday)
        doADADataActive(tp2, 2, currentday)
      }

      //2、处理android数据
      tp2 = getMedPkgGameInfo(1)
      //如果游戏不存在，就不处理了
      if (!tp2.equals("0")) {
        doADADataPay(tp2, 1, currentday)
        doADADataActive(tp2, 1, currentday)
      }
    }
  }

  //获取媒介投放包哪些游戏有监控链接
  def getMedPkgGameInfo(os: Int): String = {
    var jg = "0"
    val conn = JdbcUtil.getXiaopeng2FXConn()
    var stmt: PreparedStatement = null
    val sql: String = "select GROUP_CONCAT(DISTINCT game_id) gs  from medium_package where feedbackurl!='' and os=? group by os limit 1"
    stmt = conn.prepareStatement(sql)
    stmt.setInt(1, os)
    val rs: ResultSet = stmt.executeQuery()
    while (rs.next()) {
      jg = rs.getString("gs")
    }
    stmt.close()
    jg
  }

  //处理广告监测平台安卓自然量，导充值(投放基础表)
  def doADADataPay(games: String, os: Int, currentday: String) = {
    val sql = "select kpi.publish_date,kpi.game_id,group_id,\nkpi.regi_num-IFNULL(rs.regi_num,0) as regi_num,\nkpi.pay_price-IFNULL(rs.pay_price,0) as pay_price,\nkpi.pay_accounts-IFNULL(rs.pay_accounts,0) as pay_accounts\n from\n(\nselect publish_date,child_game_id game_id,group_id,\nsum(regi_account_num) regi_num,\nsum(pay_money)*100 pay_price,\nsum(pay_account_num) pay_accounts\nfrom bi_gamepublic_base_day_kpi kpi\nwhere kpi.publish_date='" + currentday + "' and child_game_id in(" + games + ") group by publish_date,child_game_id,group_id\n) kpi\nleft join \n(select publish_date,game_id,\nsum(regi_num) regi_num,\nsum(pay_price) pay_price,\nsum(pay_accounts) pay_accounts\n from bi_ad_channel_stats kpi where kpi.pkg_id!='' and kpi.publish_date='" + currentday + "' and game_id in(" + games + ")) rs on kpi.publish_date=rs.publish_date and kpi.game_id=rs.game_id"
    val sql2Mysql = "insert into bi_ad_channel_stats" +
      "(publish_date,game_id,pkg_id,medium,regi_num,pay_price,pay_accounts,group_id)" +
      " values(?,?,?,?,?,?,?,?) " +
      " on duplicate key update regi_num=?,pay_price=?,pay_accounts=?,group_id=?"
    val conn = JdbcUtil.getConn()
    val connHip = JdbcUtil.getBiHippoConn()
    val psHip = connHip.prepareStatement(sql)
    val ps = conn.prepareStatement(sql2Mysql)
    val rs = psHip.executeQuery()
    while (rs.next) {
      //insert
      ps.setString(1, rs.getString("publish_date"))
      ps.setString(2, rs.getString("game_id"))
      ps.setString(3, "")
      ps.setInt(4, 0)
      ps.setInt(5, rs.getString("regi_num").toInt)
      ps.setInt(6, rs.getString("pay_price").toInt)
      ps.setInt(7, rs.getString("pay_accounts").toInt)
      ps.setInt(8, rs.getString("group_id").toInt)
      //update
      ps.setInt(9, rs.getString("regi_num").toInt)
      ps.setInt(10, rs.getString("pay_price").toInt)
      ps.setInt(11, rs.getString("pay_accounts").toInt)
      ps.setInt(12, rs.getString("group_id").toInt)
      ps.executeUpdate()
    }
    ps.close()
    psHip.close()
    conn.close()
    connHip.close()
  }

  //从运营基础表导激活数据
  def doADADataActive(games: String, os: Int, currentday: String) = {
    val sql = "select kpi.publish_date,kpi.game_id,kpi.group_id,\\nkpi.active_num-IFNULL(rs.active_num,0) as active_num,\\nkpi.regi_dev_num-IFNULL(rs.regi_dev_num,0) as regi_dev_num,\\nkpi.new_pay_price-IFNULL(rs.new_pay_price,0) as new_pay_price,\\nkpi.new_pay_accounts-IFNULL(rs.new_pay_accounts,0) as new_pay_accounts\\n from\\n(\\nselect publish_date,child_game_id game_id,\\nkpi.group_id,\\nsum(active_num) active_num,\\nsum(regi_device_num) regi_dev_num,\\nsum(new_pay_money)*100 new_pay_price,\\nsum(new_pay_account_num) new_pay_accounts\\nfrom bi_gamepublic_base_opera_kpi kpi\\nwhere kpi.publish_date='\" + currentday + \"' and child_game_id in(\" + games + \")\\ngroup by publish_date,child_game_id,group_id\\n) kpi\\nleft join \\n(select publish_date,game_id,sum(active_num) active_num,\\nsum(regi_dev_num) regi_dev_num,\\nsum(new_pay_price) new_pay_price,\\nsum(new_pay_accounts) new_pay_accounts\\nfrom bi_ad_channel_stats kpi where pkg_id!='' and kpi.publish_date='\" + currentday + \"' and game_id in(\" + games + \")) rs on kpi.publish_date=rs.publish_date and kpi.game_id=rs.game_id"
    val sql2Mysql = "insert into bi_ad_channel_stats" +
      "(publish_date,game_id,pkg_id,medium,active_num,regi_dev_num,new_pay_price,new_pay_accounts,group_id)" +
      " values(?,?,?,?,?,?,?,?,?) " +
      " on duplicate key update active_num=?,regi_dev_num=?,new_pay_price=?,new_pay_accounts=?,group_id=?"
    val conn = JdbcUtil.getConn()
    val connHip = JdbcUtil.getBiHippoConn()
    //这个查的是bi从库
    val psHip = connHip.prepareStatement(sql)
    val ps = conn.prepareStatement(sql2Mysql)
    val rs = psHip.executeQuery()
    while (rs.next()) {
      //insert
      ps.setString(1, rs.getString("publish_date"))
      ps.setString(2, rs.getString("game_id"))
      ps.setString(3, "")
      ps.setInt(4, 0)
      ps.setInt(5, rs.getString("active_num").toInt)
      ps.setInt(6, rs.getString("regi_dev_num").toInt)
      ps.setInt(7, rs.getString("new_pay_price").toInt)
      ps.setInt(8, rs.getString("new_pay_accounts").toInt)
      ps.setInt(9, rs.getString("group_id").toInt)
      //update
      ps.setInt(10, rs.getString("active_num").toInt)
      ps.setInt(11, rs.getString("regi_dev_num").toInt)
      ps.setInt(12, rs.getString("new_pay_price").toInt)
      ps.setInt(13, rs.getString("new_pay_accounts").toInt)
      ps.setInt(14, rs.getString("group_id").toInt)
      ps.executeUpdate()
    }
    ps.close()
    psHip.close()
    conn.close()
    connHip.close()

  }
}
