package testdemo.DateUtils

import java.text.SimpleDateFormat
import java.util.Date

/**
  * Created by bigdata on 17-8-15.
  */
object DateUtil {
  def main(args: Array[String]): Unit = {
    val request = "192.168.20.22 - - 29/Aug/2016:10:17:07 +0800 \"GET /h5/ssxy/index.html?p=gdt_lm_00024&g=1 HTTP/1.1\" 304 0 \"-\"\"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/46.0.2486.0 Safari/537.36 Edge/13.10586\" - tg.pyw.cn test4 - 0.000"
//    val dateStr = request.split("\\[")(1).split("\\]")(0).split(":")(0).split("/")

    println(getNowFullDate("yyyy-MM-dd HH:mm:ss"))
    println(getDateForRequest(request))

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
      case  ex =>{
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
