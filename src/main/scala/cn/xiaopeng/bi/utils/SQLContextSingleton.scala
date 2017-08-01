package cn.xiaopeng.bi.utils

import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

/**
  * Created by bigdata on 17-7-26.
  */
object SQLContextSingleton {

  @transient var instacne: SQLContext = _

  def getInstance(sparkContext: SparkContext): SQLContext = {
    if (instacne == null) {
      instacne = new SQLContext(sparkContext)
    }
    instacne
  }
}
