package cn.xiaopeng.bi.utils

import cn.xiaopeng.bi.gamepublish.DayLoginKpi2.getArrayChannel
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.{StringType, StructType}

import scala.collection.mutable.ArrayBuffer

/**
  * Created by bigdata on 17-8-16.
  */
object GameDayLoginUtil {


  def loadNewLoginInfo(rdd: RDD[String], hiveContext: HiveContext): Unit = {
    val newLoginRdd = rdd.filter(line => {
      val fields = line.split("\\|", -1)
      fields(0).equals("bi_login") && fields.length >= 5
    }).map(line => {
      val fields = line.split("\\|", -1)
      //game_account,login_time,parent_channel,child_channel,ad_label,imei,game_id
      Row(fields(3), fields(4), getArrayChannel(fields(6))(0), getArrayChannel(fields(6))(1), getArrayChannel(fields(6))(2), fields(7), fields(9))
    })
    val loginStruct = new StructType()
      .add("login_time", StringType)
      .add("game_account", StringType)
      .add("parent_channel", StringType)
      .add("child_channel", StringType)
      .add("ad_label", StringType)
      .add("imei", StringType)
      .add("game_id", StringType)
    val newLoginDataFrame = hiveContext.createDataFrame(newLoginRdd, loginStruct)
    newLoginDataFrame.registerTempTable("ods_login")

    val new_login_account_num = "select distinct game_id,parent_channel,child_channel,ad_label,imei,substr(min(login_time),0,10) as login_time,count(distinct game_account) as count_acc from ods_login where game_account != '' group by game_id,parent_channel,child_channel,ad_label,imei,to_date(login_time)"
    val newLoginDF = hiveContext.sql(new_login_account_num)

    foreachNewLoginDF(newLoginDF)


  }

  def loadLoginInfo(rdd: RDD[String], hiveContext: HiveContext): Unit = {
    val loginRdd = rdd.filter(line => {
      val fields = line.split("\\|", -1)
      fields(0).contains("bi_login") && fields.length >= 5
    }).map(line => {
      println(line)
      val fields = line.split("\\|", -1)
      //game_account,login_time,expand_channel,imei,game_id
      //                        parent_channel,child_channel,ad_label
      Row(fields(3), fields(4), getArrayChannel(fields(6))(0), getArrayChannel(fields(6))(1), getArrayChannel(fields(6))(2), fields(7), fields(8))
    })

    if (!loginRdd.isEmpty()) {
      val loginStruct = new StructType()
        .add("game_account", StringType)
        .add("login_time", StringType)
        .add("parent_channel", StringType)
        .add("child_channel", StringType)
        .add("ad_label", StringType)
        .add("imei", StringType)
        .add("game_id", StringType)
      val loginRddDf = hiveContext.createDataFrame(loginRdd, loginStruct)
      loginRddDf.registerTempTable("ods_login") //原始数据
      loginRddDf.show()


      //去重之后的数据集
      val sql_day_login_num = "select distinct game_id,parent_channel,child_channel,ad_label,imei,substr(min(login_time),0,10) as login_time,count(distinct game_account) as count_acc from ods_login where game_account != '' group by game_id,parent_channel,child_channel,ad_label,imei,to_date(login_time)"
      val loginDataFrame = hiveContext.sql(sql_day_login_num)
      loginDataFrame.show()
      foreachLoginDataFrame(loginDataFrame)
    }
  }

  def foreachLoginDataFrame(loginDataFrame: DataFrame) = {
    loginDataFrame.foreachPartition(iter => {
      if (!iter.isEmpty) {
        val conn = JdbcUtil.getConn()
        val stmt = conn.createStatement()

        //更新游戏发布日活跃表  bi_gamepublic_basekpi
        val day_login_account_num = "insert into bi_gamepublic_basekpi (publish_time,game_id,parent_channel,child_channel,ad_label,login_accounts) values (?,?,?,?,?,?) on duplicate key update login_accounts = login_accounts + values(login_accounts)"
        val day_login_account_num_params = ArrayBuffer[Array[Any]]()
        val pstmt = conn.prepareStatement(day_login_account_num)

        iter.foreach(row => {
          //取出datarame中的数据
          val login_time = row.getAs[String]("login_time")
          val game_id = row.getAs[String]("game_id")
          val parent_channel = row.getAs[String]("parent_channel")
          val child_channel = row.getAs[String]("child_channel")
          val ad_label = row.getAs[String]("ad_label")
          val imei = row.getAs[String]("imei")
          val count_acc = row.getAs[Long]("count_acc")  //count函数计算出来的结果是lang类型

          //先判断这个game_account今天是否已经登录过
          val select_login_time = "select login_time from bi_gamepublish_login where login_time ='" + login_time + "' and game_id = '" + game_id + "' and parent_channel = '" + parent_channel + "' and child_channel = '" + child_channel + "' and ad_label = '" + ad_label + "'"
          val result_login_time = stmt.executeQuery(select_login_time)
          if (!result_login_time.next()) {
            //同一个帐号，每天登录多次也只计算一次，今天这个帐号没有登录过，再把数据插入到数据库
            //这个game_account今天没有登录过
            day_login_account_num_params.+=(Array(login_time, game_id, parent_channel, child_channel, ad_label, count_acc))
          }
          JdbcUtil.executeUpdate(pstmt, day_login_account_num_params, conn)
        })
        pstmt.close()
        conn.close()
      }
    })
  }

  //新增注册帐号数
  def foreachNewLoginDF(newLoginDF: DataFrame): Unit = {
    newLoginDF.foreachPartition(iter => {
      if (!iter.isEmpty) {
        val conn = JdbcUtil.getConn()
        val stmt = conn.createStatement()

        val day_new_login_account_num = "insert into bi_gamepublic_basekpi (publish_time,game_id,parent_channel,child_channel,ad_label,login_accounts) values (?,?,?,?,?,?) on duplicate key update login_accounts = login_accounts + values(login_accounts)"
        val day_new_login_account_num_params = ArrayBuffer[Array[Any]]()
        val pstmt = conn.prepareStatement(day_new_login_account_num)

        iter.foreach(row => {

        })


      }
    })


  }


}
