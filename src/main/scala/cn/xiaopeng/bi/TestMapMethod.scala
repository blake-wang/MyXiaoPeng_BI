package cn.xiaopeng.bi

import cn.wanglei.bi.bean.MoneyMasterBean
import cn.wanglei.bi.utils.AnalysisJsonUtil
import cn.xiaopeng.bi.utils.{HiveContextSingleton, JdbcUtil, SparkUtils}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by bigdata on 17-9-6.
  */
object TestMapMethod {


  // 打印rdd中内容的 2种方式：
  //  1    rdd.collect().foreach{println}
  //  2    rdd.take(10).foreach{println}
  //  //take(10) 取前10个
  //  二、例子
  //  val logData = sparkcontext.textFile(logFile, 2).cache()
  //  logData.collect().foreach {println}
  //  logData.take(10).foreach { println }
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName(this.getClass.getName.replace("$", ""))
      .set("spark.default.parallelism", "60")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.shuffle.consolidateFiles", "true")
      .set("spark.sql.shuffle.partitions", "60")
    SparkUtils.setMaster(sparkConf);
    val sparkContext = new SparkContext(sparkConf)
    val jsonRdd = sparkContext.textFile("file:///home/bigdata/IdeaProjects/MyXiaoPeng_BI/src/test/scala/com/xiaopeng/test/money")
    //两种方式都可以打印rdd中内容
    jsonRdd.collect().foreach {
      println
    }
    jsonRdd.take(1).foreach {
      println
    }
    val hiveContext = HiveContextSingleton.getInstance(sparkContext)
    loadJsonRdd(jsonRdd, hiveContext)
  }


  def loadJsonRdd(jsonRdd: RDD[String], hiveContext: HiveContext): Unit = {
    val moneyRdd = jsonRdd.map(json => {
      val mmb = AnalysisJsonUtil.AnalysisMoneyMasterData(json.substring(json.indexOf("{")))


      val conn = JdbcUtil.getConn()
      val testSql = "select publish_date,parent_game_id from bi_gamepublic_opera_regi_pay where child_game_id = 3812 "
      val stat = conn.createStatement()
      val resultSet = stat.executeQuery(testSql)
      while (resultSet.next()) {
        //        val publis_date = resultSet.getString("publish_date")
        //        val parent_game_id = resultSet.getString("parent_game_id")
        val publis_date = resultSet.getString(1)
        val parent_game_id = resultSet.getString(2)

        println("publis_date : " + publis_date + "   ,   parent_game_id : " + parent_game_id)
      }

      mmb
    })
    foreachRDD(moneyRdd)
  }

  def foreachRDD(moneyRdd: RDD[MoneyMasterBean]): Unit = {
    moneyRdd.foreachPartition(iter => {
      iter.foreach(bean => {
        println(bean.toString)
      })
    })
  }


}

