package cn.xiaopeng.bi.utils

import java.sql.{Connection, PreparedStatement, ResultSet}

import org.apache.spark.sql.DataFrame

/**
  * Created by bigdata on 7/18/17.
  */
object Commons {
  /**
    * Null转0
    *
    * @param ls
    * @return
    */

  def getNullTo0(ls: String): Float = {
    var rs = 0.0.toFloat
    if (!ls.equals("")) {
      rs = ls.toFloat
    }
    return rs
  }

  def getImei(imei: String): String = {
    return imei.replace("$", "|")
  }

  //推送data数据到数据库
  def processDbFct(dataf: DataFrame, insertSql: String): Unit = {
    //全部转为小写
    val sql2Mysql = insertSql.replace("|", " ").toLowerCase()
    //获取values（）里面有多少个?参数，有利于后面的循环
    val startValuesIndex = sql2Mysql.indexOf("(?") + 1
    val endValuesIndex = sql2Mysql.indexOf("?)") + 1
    //values中？的个数
    val valueArray: scala.Array[String] = sql2Mysql.substring(startValuesIndex, endValuesIndex).split(",") //两个（？？）中间的值
    //条件中的参数个数
    val wh: scala.Array[String] = sql2Mysql.substring(sql2Mysql.indexOf("update") + 6).split(",") //找update后面的字符串再判断
    //查找需要insert的字段
    val cols_ref = sql2Mysql.substring(0, sql2Mysql.lastIndexOf("(?")) //获取（?特殊字符前的字符串，然后再找字段
    val cols = cols_ref.substring(cols_ref.lastIndexOf("(") + 1, cols_ref.lastIndexOf(")")).split(",")

    //数据库操作
    dataf.foreachPartition(rows => {
      val conn = JdbcUtil.getConn()
      val ps = conn.prepareStatement(sql2Mysql)
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
      ps.close()
      conn.close()
    })


  }

  /**
    * 获取发行组&平台
    */
  def getPubGameGroupIdAndOs(gameId:Int,conn:Connection):Array[String]={
    var jg = Array[String]("1","0")
    var stmt:PreparedStatement = null
    val sql:String = "select distinct system_type os,group_id from game_sdk where old_game_id=? limit 1"
    stmt = conn.prepareStatement(sql)
    stmt.setInt(1,gameId)
    val rs:ResultSet = stmt.executeQuery()
    while(rs.next()){
      jg = Array[String](rs.getString("group_id"),rs.getString("os"))
    }
    stmt.close()
    return jg
  }
}
