package cn.xiaopeng.bi.gamepublish

import java.text.SimpleDateFormat
import java.util.Date

import cn.wanglei.bi.ConfigurationUtil
import cn.xiaopeng.bi.utils.{JdbcUtil, SparkUtils}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by bigdata on 17-9-13.
  * 广告监测的 激活-注册-订单  离线数据修复
  */
object GamePublishThirdDataOffLine {
  //广告监测的点击是来自mysql的，hive中并没有点击对应的表
  //这个离线怎么写
  //click点击日志直接存在了mysql里面，
  //怎样把json格式的日志存入hive表 --这个方法现在好像行不通，原因有2：json前面待了头部，二，查询json的hive表代码怎么写

  //以后再出现线上bug，导致实时数据出现问题
  //1：把出事时间段的点击日志找出来，重新消费
  //2：离线主要处理激活，注册，支付的日志


  def main(args: Array[String]): Unit = {
//    val str = "2017-10-07 10:00:00"
    val str = "2017-10"
    val a = str.substring(0,10)
    println(a)


  }
}
