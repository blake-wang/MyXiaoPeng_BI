package cn.xiaopeng.bi.utils

import java.sql.{Connection, PreparedStatement, Statement}

import cn.wanglei.bi.bean.MoneyMasterBean
import cn.wanglei.bi.utils.AnalysisJsonUtil
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer

/**
  * Created by kequan on 8/31/17.
  * 钱大师广告监控
  */
object AdMoneyMasterUtil {


  def main(args: Array[String]): Unit = {
    //创建各种上下文
    val sparkConf = new SparkConf().setAppName(this.getClass.getName.replace("$", ""))
      .set("spark.default.parallelism", "60")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.shuffle.consolidateFiles", "true")
      .set("spark.sql.shuffle.partitions", "60")
    SparkUtils.setMaster(sparkConf);
    val sc = new SparkContext(sparkConf);
    val hiveContext: HiveContext = HiveContextSingleton.getInstance(sc)
    val rdd1 = sc.textFile("fill:///home/bigdata/IdeaProjects/MyXiaoPeng_BI/src/test/scala/com/xiaopeng/test/money")
    loadMoneyMasterInfo(rdd1, hiveContext)

  }

  def loadMoneyMasterInfo(rdd: RDD[String], hiveContext: HiveContext): Unit = {
    val clickRdd: RDD[MoneyMasterBean] = rdd.filter(line=>{
      (line.contains("bi_adv_money_click") || line.contains("bi_adv_money_active")) && (AnalysisJsonUtil.AnalysisMoneyMasterData(line.substring(line.indexOf("{"))) != null)
    })
  }


}
