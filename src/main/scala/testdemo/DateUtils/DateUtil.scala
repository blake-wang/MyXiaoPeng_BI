package testdemo.DateUtils

import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

/**
  * Created by bigdata on 17-8-15.
  */
object DateUtil {
  val HOUR_FORMAT = new SimpleDateFormat("yyyy-MM-dd HH")
  val TIME_FORMAT = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
  val DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd")
  val DATEKEY_FORMAT = new SimpleDateFormat("yyyyMMdd")
  val mm_FORMAT = new SimpleDateFormat("mm")
  val HH_FORMAT = new SimpleDateFormat("HH")

  def main(args: Array[String]): Unit = {
    val request = "192.168.20.22 - - 29/Aug/2016:10:17:07 +0800 \"GET /h5/ssxy/index.html?p=gdt_lm_00024&g=1 HTTP/1.1\" 304 0 \"-\"\"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/46.0.2486.0 Safari/537.36 Edge/13.10586\" - tg.pyw.cn test4 - 0.000"
    //    val dateStr = request.split("\\[")(1).split("\\]")(0).split(":")(0).split("/")

    //    println(getNowFullDate("yyyy-MM-dd HH:mm:ss"))
    //    println(getDateForRequest(request))

    val date = "2017-09-30 12:36:42"
    val a = getDt7Before(date, -29)
    println("a : "+ a)


    val d1 = "2017-09-16"
    val d2 = addDay2(d1, 5)
    println(d2)

  }

  //转换时间，2017-09-01 12:36:42   ->  2017-09-08
  def getDt7Before(pidt: String, i: Int): String = {
//    val dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
    val dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val cal: Calendar = Calendar.getInstance
    val date: Date = dateFormat.parse(pidt)
    cal.setTime(date)
    cal.add(Calendar.DATE, i)
    val dt = dateFormat.format(cal.getTime())
    return dt
  }

  /**
    * 判断一个时间是否在另一个时间之前
    *
    * @param time1
    * @param time2
    * @return
    */
  def beforeTime(time1: String, time2: String): Boolean = {
    try {
      val dateTime1 = TIME_FORMAT.parse(time1)
      val dateTime2 = TIME_FORMAT.parse(time2)
      if (dateTime1.before(dateTime2)) return true
    } catch {
      case e: Exception => {
        e.printStackTrace()
        return false
      }
    }
    return false
  }

  /**
    * 判断一个时间是否在另一个时间之后
    *
    * @param time1 第一个时间
    * @param time2 第二个时间
    * @return 判断结果
    */
  def afterTime(time1: String, time2: String): Boolean = {
    try {
      val dateTime1 = TIME_FORMAT.parse(time1)
      val dateTime2 = TIME_FORMAT.parse(time2)
      if (dateTime1.after(dateTime2)) return true
    } catch {
      case e: Exception => {
        e.printStackTrace()
        return false
      }
    }
    return false
  }

  /**
    * 判断一个时间是否在另一个时间之前
    *
    * @param time1 第一个时间
    * @param time2 第二个时间
    * @return 判断结果
    */
  def beforeHour(time1: String, time2: String): Boolean = {
    try {
      val dateTime1 = HOUR_FORMAT.parse(time1)
      val dateTime2 = HOUR_FORMAT.parse(time2)
      if (dateTime1.before(dateTime2)) return true
    } catch {
      case e: Exception => {
        e.printStackTrace()
        return false
      }
    }
    return false
  }

  /**
    * 判断一个时间是否在另一个时间之后
    *
    * @param time1 第一个时间
    * @param time2 第二个时间
    * @return 判断结果
    */
  def afterHour(time1: String, time2: String): Boolean = {
    try {
      val dateTime1 = HOUR_FORMAT.parse(time1)
      val dateTime2 = HOUR_FORMAT.parse(time2)
      if (dateTime1.after(dateTime2)) return true
    } catch {
      case e: Exception => {
        e.printStackTrace()
        return false
      }
    }
    return false
  }

  /**
    * 判断一个时间是否在另一个时间之前
    *
    * @param time1 第一个时间
    * @param time2 第二个时间
    * @return 判断结果
    */
  def beforeDay(time1: String, time2: String): Boolean = {
    try {
      val dateTime1 = DATE_FORMAT.parse(time1)
      val dateTime2 = DATE_FORMAT.parse(time2)
      if (dateTime1.before(dateTime2)) return true
    } catch {
      case e: Exception => {
        e.printStackTrace()
        return false
      }
    }
    return false
  }

  /**
    * 判断一个时间是否在另一个时间之后
    *
    * @param time1 第一个时间
    * @param time2 第二个时间
    * @return 判断结果
    */
  def afterDay(time1: String, time2: String): Boolean = {
    try {
      val dateTime1 = DATE_FORMAT.parse(time1)
      val dateTime2 = DATE_FORMAT.parse(time2)
      if (dateTime1.after(dateTime2)) return true
    } catch {
      case e: Exception => {
        e.printStackTrace()
        return false
      }
    }
    return false
  }


  /**
    * 判断两个时间的天数差
    *
    * @param time1
    * @param time2
    * @return
    */
  def compareToDay(time1: String, time2: String): Int = {
    try {
      val dateTime1 = DATE_FORMAT.parse(time1)
      val dateTime2 = DATE_FORMAT.parse(time2)
      ((dateTime1.getTime - dateTime2.getTime) / (24 * 60 * 60 * 100) + "").toInt
    } catch {
      case e: Exception =>
        e.printStackTrace()
        0
    }
  }

  /**
    * 给某个时间添加几天，第一种写法
    * @param time1
    * @param day
    * @return
    */
  def addDay(time1: String, day: Int): String = {
    try {
      val dateTime1 = DATE_FORMAT.parse(time1)
      val calendar = Calendar.getInstance()
      calendar.setTime(dateTime1)
      calendar.add(Calendar.DATE, day)
      DATE_FORMAT.format(calendar.getTime)
    } catch {
      case e: Exception => {
        e.printStackTrace()
        null
      }
    }
  }

  /**
    * 给某个时间添加 几天 第二种写法
    * @param time1
    * @param day
    * @return
    */
  def addDay2(time1: String, day: Int): String = {
    try {
      val dateTime1 = DATE_FORMAT.parse(time1)
      DATE_FORMAT.format(new Date((dateTime1.getTime + day * (24 * 60 * 60 * 1000))))
    } catch {
      case e: Exception => {
        e.printStackTrace()
        null
      }
    }

  }


  def getNowFullDate(pattern: String): String = {
    val now = new Date()
    val dateFormat = new SimpleDateFormat(pattern)
    val stringDate = dateFormat.format(now)
    return stringDate
  }

  def getDateForRequest(requestDateStr: String): String = {
    try {
      val split = requestDateStr.split("\\[")(1).split("\\]")(0).split(":")
      val dateStr = split(0).split("/")
      val hour = split(1)
      dateStr(2) + "-" + changeEnglishMonthTo(dateStr(1)) + "-" + dateStr(0) + " " + hour
    } catch {
      case ex: Exception => {
        //这会出什么异常
        "0000-00-00 00"
      }
    }
  }


  def changeEnglishMonthTo(m: String): String = {
    if (m.equals("Jan")) {
      "01"
    } else if (m.equals("Feb")) {
      "02"
    } else if (m.equals("Mar")) {
      "03"
    } else if (m.equals("Apr")) {
      "04"
    } else if (m.equals("May")) {
      "05"
    } else if (m.equals("Jun")) {
      "06"
    } else if (m.equals("Jul")) {
      "07"
    } else if (m.equals("Aug")) {
      "08"
    } else if (m.equals("Sept")) {
      "09"
    } else if (m.equals("Sep")) {
      "09"
    } else if (m.equals("Oct")) {
      "10"
    } else if (m.equals("Nov")) {
      "11"
    } else if (m.equals("Dec")) {
      "12"
    } else {
      "00"
    }
  }


}
