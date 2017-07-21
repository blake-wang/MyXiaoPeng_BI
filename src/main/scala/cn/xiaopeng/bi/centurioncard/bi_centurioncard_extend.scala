package cn.xiaopeng.bi.centurioncard

import java.sql.PreparedStatement
import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import cn.xiaopeng.bi.utils.{Hadoop, JdbcUtil}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by bigdata on 17-7-21.
  */
object bi_centurioncard_extend {
  def main(args: Array[String]): Unit = {
    if (args.length < 1) {
      System.err.println("Usage: <currentday> ")
      System.exit(1)
    }
    if (args.length > 1) {
      System.err.println("参数个数传入太多，固定为1个 ：<currentday>")
      System.exit(1)
    }
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)
    //跑数日期
    val currentday = args(0)
    //昨天
    val dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
    val date: Date = dateFormat.parse(args(0))
    val cal: Calendar = Calendar.getInstance()
    cal.setTime(date)
    cal.add(Calendar.DATE, -1)
    val yesterday = dateFormat.format(cal.getTime)
    //月第一天
    val monthfirstday = args(0).substring(0, 7) + "-01"
    //把统计流水迁移到从明细表gameAcc来
    val hivesql = "select\ncase when rs.dt is null then '0000-00-00' else rs.dt end statistics_date,\ncase when pu.username is null then '' else pu.username end user_account,\ncase when mainid is null then 0 else mainid end game_id,\ncase when bg.maingname is null then '' else bg.maingname end game_name,\ncase when rs.reg_os_type is null then 'UNKNOW' else rs.reg_os_type end os,\ncase when sum(rs.totaluser) is null then 0 else sum(rs.totaluser) end game_accounts,\ncase when sum(rs.add_users) is null then 0 else sum(rs.add_users) end add_users,\ncase when sum(rs.regi_accouts) is null then 0 else sum(rs.regi_accouts) end add_regi_accounts\nfrom\n(\n--新增用户\nselect regi.owner_id,to_date(bind_uid_time) as dt,regi.game_id,regi.reg_os_type,0 as totaluser,count(regi.game_account) add_users,0 as ori_price,0 as deal_times,0 as regi_accouts from \n     ods_regi regi  \n    where to_date (bind_uid_time)='currentday'   ----取当天\n    group by regi.owner_id,regi.game_id,regi.reg_os_type,to_date(bind_uid_time)\n union all\n select regi.owner_id,to_date(relate_time) as dt,regi.game_id,regi.reg_os_type,0 as totaluser,0 add_users,0 as ori_price,0 as deal_times,count(regi.game_account) regi_accouts from \n ods_regi regi  where to_date (regi.relate_time)='currentday' \n    group by regi.owner_id,regi.game_id,regi.reg_os_type,to_date(relate_time)\n ) rs \n join \n   gameinfo  bg on bg.id=rs.game_id \n join promo_user pu on rs.owner_id=pu.member_id\n where pu.member_grade=1 and pu.status in(0,1) \n group by\n pu.username,\n rs.dt,\n mainid,\n bg.maingname,\n rs.reg_os_type"
    val mysqlsql = "insert into bi_centurioncard_extend(statistics_date,user_account,game_id,game_name,os,game_accounts,add_users,add_regi_accounts)\nvalues(?,?,?,?,?,?,?,?)\non duplicate key update game_name=?,game_accounts=?,add_users=?,add_reg_accounts=?"
    //全部转为小写，后面好判断
    val execSql = hivesql.replace("currentday", currentday).replace("yesterday", yesterday).replace("monthfirstday", monthfirstday)
    val sql2Mysql = mysqlsql.replace("|", " ").toLowerCase()
    //hadoop library
    //    Hadoop.hd

    //获取values()里面有多少个?参数,有利于后面的循环
    val startValuesIndex = sql2Mysql.indexOf("(?") + 1
    val endValuesIndex = sql2Mysql.indexOf("?)") + 1
    //values中的个数
    val valueArray: Array[String] = sql2Mysql.substring(startValuesIndex, endValuesIndex).split(",") //两个（？？）中间的值
    //条件中的参数个数
    val wh: Array[String] = sql2Mysql.substring(sql2Mysql.indexOf("update") + 6).split(",") //找update后面的字符串再判断
    //查找需要insert的字段
    val cols_ref = sql2Mysql.substring(0, sql2Mysql.lastIndexOf("(?")) //获取（?特殊字符前的字符串，然后再找字段
    val cols: Array[String] = cols_ref.substring(cols_ref.lastIndexOf("(") + 1, cols_ref.lastIndexOf(")")).split(",")


    //hive库的操作
    val sparkConf = new SparkConf().setAppName(this.getClass.getName.replace("$", ""))
    val sc = new SparkContext(sparkConf)
    val sqlContext = new HiveContext(sc)
    sqlContext.sql("use yyft")
    val dataf = sqlContext.sql(execSql) //执行hive sql

    //Mysql数据库操作

    dataf.foreachPartition(rows => {

      val conn = JdbcUtil.getConn();
      conn.setAutoCommit(false)
      val ps: PreparedStatement = conn.prepareStatement(sql2Mysql)
      ps.clearBatch()
      for (x <- rows) {
        for (rs <- 0 to valueArray.length - 1) {
          ps.setString(rs.toInt + 1, x.get(rs).toString)
        }
        for (i <- 0 to wh.length - 1) {
          val rs = wh(i).trim.substring(0, wh(i).trim.lastIndexOf("="))
          for (ii <- 0 to cols.length - 1) {
            if (cols(ii).trim.equals(rs)) {
              ps.setString(i.toInt + valueArray.length.toInt + 1, x.get(ii).toString)
            }
          }
        }
        ps.addBatch()
      }
      ps.executeBatch()
      conn.commit()
      conn.close()

    }
    )
    System.clearProperty("spark.driver.port")
    sc.stop()


  }

}
