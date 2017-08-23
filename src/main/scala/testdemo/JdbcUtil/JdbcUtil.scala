package testdemo.JdbcUtil

import java.sql.{Connection, DriverManager, PreparedStatement}

import cn.wanglei.bi.{ConfigurationUtil, Constants}

import scala.collection.mutable.ArrayBuffer

/**
  * Created by bigdata on 17-8-16.
  */
object JdbcUtil {
  //获取 xiaopeng2_bi主库链接，推送结果数据目标库
  def getConn(): Connection = {
    val url = ConfigurationUtil.getProperty(Constants.JDBC_URL)
    val driver = ConfigurationUtil.getProperty(Constants.JDBC_DRIVER)
    try {
      Class.forName(driver)
      return DriverManager.getConnection(url)
    } catch {
      case ex: Exception => {
        println("获取数据库链接错误：Exception = " + ex)
      }
        return null
    }
  }

  def doBatch(sqlText: String, params: ArrayBuffer[Array[Any]], conn: Connection) = {
    if (params.length > 0) {
      val pstmt = conn.prepareStatement(sqlText)
      pstmt.clearBatch()
      for (param <- params) {
        for (index <- 0 to param.length - 1) {
          pstmt.setObject(index + 1, param(index))
        }
        pstmt.addBatch()
      }
      pstmt.executeBatch
      pstmt.close()
      params.clear
    }
  }

  def executeBatch(sqlText: String, params: ArrayBuffer[Array[Any]]) = {
    if (params.length > 0) {
      val conn = getConn()
      val pstmt = conn.prepareStatement(sqlText)
      pstmt.clearBatch()
      for (param <- params) {
        for (index <- 0 to param.length - 1) {
          pstmt.setObject(index + 1, param(index))
        }
        pstmt.addBatch()
      }
      pstmt.executeBatch()
      pstmt.close()
      conn.close()
      params.clear
    }
  }

  def executeUpdate(pstmt: PreparedStatement, params: ArrayBuffer[Array[Any]], connection: Connection) = {
    if (params.length > 0) {
      for (param <- params) {
        for (index <- 0 to param.length - 1) {
          pstmt.setObject(index + 1, param(index))
        }
        pstmt.executeUpdate()
      }
      params.clear
    }
  }

}
