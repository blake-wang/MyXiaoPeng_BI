package cn.xiaopeng.bi.backend

import java.sql.PreparedStatement

import cn.xiaopeng.bi.utils.{Hadoop, JdbcUtil}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by bigdata on 17-8-17.
  */
object LoadHisData {
  def main(args: Array[String]): Unit = {
    Hadoop.hd
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.WARN)
    val currentday = args(0)
    val sparkConf = new SparkConf().setAppName(this.getClass.getName.replace("$", ""))
      .set("spark.storage.memoryFraction", "0.7")
      .set("spark.sql.shuffle.partitions", "60")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new HiveContext(sc)

    //会员
    val member_sql = "select member_id,username,regtime addtime,regtype from views.v_memebr where to_date(regtime)<='currentday'"
    val istFb = "insert into bi_member_info(member_id,username,addtime,regtype) values (?,?,?,?) on duplicate key update username=?,addtime=?,regtype=?"
    val memberDf = sqlContext.sql(member_sql)
    processDbFct(memberDf, istFb)
    //账号数
    val zhsql = "select member_id,count(distinct account) accounts from views.v_bind_member where to_date(bind_time)<='currentday' ".replace("currentday", currentday)
    val zhist = "insert into bi_member_info(member_id,accounts)  values(?,?) on duplicate key update accounts=?"
    val zhdf: DataFrame = sqlContext.sql(zhsql)
    processDbFct(zhdf, zhist)
    /*充值*/
    val czsql = "select vo.userid,count(distinct vo.orderid) ordernum,sum(oriprice*productnum) oriprice,sum(payprice) payprice,max(erectime) last_pay_time\n from yyft.orders vo  where vo.state=4 and userid!=0 and to_date(erectime)<='currentday' group by userid ".replace("currentday", currentday)
    val czist = "insert into bi_member_info(member_id,ordernum,oriprice,payprice,last_pay_time)  values(?,?,?,?,?) on duplicate key update ordernum=?,oriprice=?,payprice=?,last_pay_time=?"
    val czdf: DataFrame = sqlContext.sql(czsql)
    processDbFct(czdf, czist)
    //delete
    val conn = JdbcUtil.getConn()
    val stmt = conn.createStatement()
    stmt.executeUpdate("delete from bi_member_info where username ='' and addtime = '0000-00-00 00:00:00'")
    stmt.close()
    conn.close()

    System.clearProperty("spark.driver.port")
    sc.stop
    sc.clearCallSite()
  }

  def processDbFct(dataf: DataFrame, insertSql: String) = {
    //全部转为小写，后面好判断
    val sql2Mysql = insertSql.replace("|", " ").toLowerCase
    //获取values（）里面有多少个?参数，有利于后面的循环
    val startValuesIndex = sql2Mysql.indexOf("(?") + 1
    val endValuesIndex = sql2Mysql.indexOf("?)") + 1
    //values中的个数
    val valueArray: Array[String] = sql2Mysql.substring(startValuesIndex, endValuesIndex).split(",") //两个（？？）中间的值
    //条件中的参数个数
    val wh: Array[String] = sql2Mysql.substring(sql2Mysql.indexOf("update") + 6).split(",") //找update后面的字符串再判断
    //查找需要insert的字段
    val cols_ref = sql2Mysql.substring(0, sql2Mysql.lastIndexOf("(?")) //获取（?特殊字符前的字符串，然后再找字段
    val cols: Array[String] = cols_ref.substring(cols_ref.lastIndexOf("(") + 1, cols_ref.lastIndexOf(")")).split(",")

    /** ******************数据库操作 *******************/
    dataf.foreachPartition((rows: Iterator[Row]) => {
      val conn = JdbcUtil.getConn()
      val ps: PreparedStatement = conn.prepareStatement(sql2Mysql)
      for (x <- rows) {
        //补充value值
        for (rs <- 0 to valueArray.length - 1) {
          ps.setString(rs.toInt + 1, x.get(rs).toString)
        }
        //补充条件
        for (i <- 0 to wh.length - 1) {
          val rs = wh(i).trim.substring(0, wh(i).trim.lastIndexOf("="))
          for (ii <- 0 to cols.length - 1) {
            if (cols(ii).trim.equals(rs)) {
              ps.setString(i.toInt + valueArray.length.toInt + 1, x.get(ii).toString)
            }
          }
        }
        ps.executeUpdate()
      }
      conn.close()
    }
    )
  }

}
