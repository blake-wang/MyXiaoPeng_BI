package testdemo

import java.text.SimpleDateFormat
import java.util.{Calendar, Date}


/**
  * Created by JSJSB-0071 on 2017/7/13.
  * 测试Simpleateotmat
  */
object SimpleDateFormatTest {

  def main(args: Array[String]) {


    stringToDate
  }

  def stringToDate = {
    val str = "2017-09-11 16:12:20"
    val simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val date = simpleDateFormat.parse(str)
    val calendar = Calendar.getInstance()
    calendar.setTime(date)
    calendar.add(Calendar.DATE, -7)
    val strA = simpleDateFormat.format(calendar.getTime)
    println(date)
    println(strA)
  }

  private def longToString = {
    val long = 1505124784000L
    val date = new Date(long)
    val simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    //format是把date对象转换成日期字符串
    //parse是把时间字符串，转换成date对象
    val dateStr = simpleDateFormat.format(date)
    println(date)
    println(dateStr)
  }
}
