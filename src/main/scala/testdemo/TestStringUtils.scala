package testdemo

import cn.xiaopeng.bi.utils.StringUtils

/**
  * Created by bigdata on 17-8-1.
  */
object TestStringUtils {
  def main(args: Array[String]): Unit = {
    val str = "2017-07-25 12:16:33"
    val res = StringUtils.isNumber(str)
    println(res)
  }

}
