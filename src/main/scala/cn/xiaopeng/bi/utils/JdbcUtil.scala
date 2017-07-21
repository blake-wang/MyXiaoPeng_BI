package cn.xiaopeng.bi.utils

import java.sql.{PreparedStatement, Connection, DriverManager}

import cn.wanglei.bi.{ConfigurationUtil, Constants}

import scala.collection.mutable.ArrayBuffer


/**
 * Created by wanglei on 2017/7/13.
 */
object JdbcUtil {

  /**
    * 获取xiaopeng2库连接，利于补充缺失数据
    *
    * @return MysqlConnection
    */
  def getXiaopeng2Conn():Connection = {
    val url = ConfigurationUtil.getProperty(Constants.JDBC_XIAOPENG2_URL)
    val driver = ConfigurationUtil.getProperty(Constants.JDBC_DRIVER)
    try {
      Class.forName(driver)
      return DriverManager.getConnection(url)
    } catch {
      case ex:Exception =>{
        println("获取数据库连接错误：Exception=" + ex)
       return  null
      }
    }
  }


  //获取xiaopeng_bi主库连接，推送结果数据目标库
  def getConn(): Connection = {
    val url = ConfigurationUtil.getProperty(Constants.JDBC_URL)
    val driver = ConfigurationUtil.getProperty(Constants.JDBC_DRIVER)
    Class.forName(driver)
    try {
      return DriverManager.getConnection(url)
    } catch {
      case ex: Exception => {
        println("获取数据库连接错误：Exception=" + ex)
      }
        return null;
    }
  }

  def doBatch(sqlText: String, params: ArrayBuffer[Array[Any]], conn: Connection): Unit = {
    if (params.length > 0) {
      val pstat = conn.prepareStatement(sqlText)
      pstat.clearBatch()
      //params是一个ArrayBuffer，里面装的是Array，每个Array里面装的是每一条的记录，有15个字段
      for (param <- params) {
        for (index <- 0 to param.length - 1) {
          //给sql中的？设置值，这是sql注入的专门用法
          pstat.setObject(index + 1, param(index))
        }
        pstat.addBatch()
      }
      pstat.executeBatch()
      pstat.close()
      params.clear()
    }
  }

  def executeBatch(sqlText: String, params: ArrayBuffer[Array[Any]]): Unit = {
    if (params.length > 0) {
      var conn: Connection = getConn()
      var pstat: PreparedStatement = conn.prepareStatement(sqlText)
      pstat.clearBatch()
      for (param <- params) {
        for (index <- 0 to params.length - 1) {
          pstat.setObject(index + 1, param(index))
        }
        pstat.addBatch()
      }
      pstat.executeBatch()
      pstat.close()
      conn.close()
      params.clear
    }
  }
}
