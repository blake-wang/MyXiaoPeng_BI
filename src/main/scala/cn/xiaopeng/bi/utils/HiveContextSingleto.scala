package cn.xiaopeng.bi.utils

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.hive.HiveContext

/**
  * Created by bigdata on 17-8-1.
  */
object HiveContextSingleton {
  @transient private var instance: HiveContext = _

  def getInstance(sparkContext: SparkContext): HiveContext = {
    if (instance == null) {
      instance = new HiveContext(sparkContext)
    }
    instance
  }


  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local").setAppName("tt")
    val sparkContext = new SparkContext(sparkConf)
    for (i <- 0 to 1000) {
      println("nihao :" + i)
      val hiveContext =  getInstance(sparkContext)

      println(hiveContext)
    }

  }

}
