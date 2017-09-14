package testdemo

import java.text.SimpleDateFormat
import java.util.{Calendar, Date}


/**
  * Created by JSJSB-0071 on 2017/7/13.
  * 测试Simpleateotmat
  */
object SimpleDateFormatTest {

  def main(args: Array[String]) {


    longToString
  }

  def stringToDate = {
    val str = "2017-09-11 16:12:20"
    val str2 = "Wed Sep 13 16:12:20 CST 2017"
    val simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val date = simpleDateFormat.parse(str)
    val calendar = Calendar.getInstance()
    calendar.setTime(date)
    calendar.add(Calendar.DATE, -7)
    val strA = simpleDateFormat.format(calendar.getTime)
    println(date)
    println(strA)
  }

  //simpleDateFormat中的格式的定义，是解析后的格式定义，不一定非要和字符串日期时间匹配
  def stringToDate2 = {
    val str = "2017-09-13 16:12:20"
    val simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
    val date = simpleDateFormat.parse(str)
    val calendar = Calendar.getInstance()
    calendar.setTime(date)
    calendar.add(Calendar.DATE, -7)
    val strA = simpleDateFormat.format(calendar.getTime)
    println(date)
    println(strA)
  }

  //这个报错
  def stringToDate3 = {
    val str = "2017-09-13"
    val simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val date = simpleDateFormat.parse(str)
    val calendar = Calendar.getInstance()
    calendar.setTime(date)
    calendar.add(Calendar.DATE, -7)
    val strA = simpleDateFormat.format(calendar.getTime)
    println(date)
    println(strA)
  }

  //把毫秒时间值转换成日期时间字符串
  private def longToString = {
    val longAQY = 1505293001781L
    val longUC = 1505293655865L
    val longGDT = 1505359653L * 1000
    val date = new Date(longGDT)
    val simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    //format是把date对象转换成日期字符串
    //parse是把时间字符串，转换成date对象
    val dateStr = simpleDateFormat.format(date)
    println(date)
    println(dateStr)
  }
}
