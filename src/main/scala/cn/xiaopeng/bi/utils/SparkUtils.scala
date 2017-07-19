package cn.xiaopeng.bi.utils

import java.util.Properties

import cn.wanglei.bi.{Constants, ConfigurationUtil}
import org.apache.spark.SparkConf
import org.apache.spark.sql.hive.HiveContext
import org.datanucleus.properties.PropertyTypeInvalidException

/**
 * Created by JSJSB-0071 on 2017/7/13.
 */
object SparkUtils {
  def readXiaopeng2Table(tabName: String, hiveContext: HiveContext): Unit = {
    val properties = new Properties()
    properties.put("driver",ConfigurationUtil.getProperty(Constants.JDBC_DRIVER))
    properties.put("user",ConfigurationUtil.getProperty(Constants.JDBC_XIAOPENG2_USER))
    properties.put("password",ConfigurationUtil.getProperty(Constants.JDBC_PWD))
    val deptDF = hiveContext.read.jdbc(ConfigurationUtil.getProperty(Constants.JDBC_URL),tabName,properties)
    deptDF.registerTempTable(tabName)
  }

  def setMaster(conf: SparkConf): Unit = {
    val local = ConfigurationUtil.getBoolean(Constants.SPARK_LOCAL)
    if (local) conf.setMaster("local[4]")
  }

  def readBiTable(tabName: String, hiveContext: HiveContext) = {
    val properties = new Properties()
    properties.put("driver", ConfigurationUtil.getProperty(Constants.JDBC_DRIVER))
    properties.put("user", ConfigurationUtil.getProperty(Constants.JDBC_USER))
    properties.put("password", ConfigurationUtil.getProperty(Constants.JDBC_PWD))

    val deptDF = hiveContext.read.jdbc(ConfigurationUtil.getProperty(Constants.JDBC_URL), tabName, properties)
    deptDF.registerTempTable(tabName)
  }

}
