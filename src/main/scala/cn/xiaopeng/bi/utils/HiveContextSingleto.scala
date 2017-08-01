package cn.xiaopeng.bi.utils

import org.apache.spark.SparkContext
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

}
