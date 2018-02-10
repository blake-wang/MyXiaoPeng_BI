package cn.xiaopeng.bi.utils

import cn.wanglei.bi.ConfigurationUtil

import scala.collection.mutable.ArrayBuffer

/**
  * Created by sumenghu on 2016/9/1.
  */
object StringUtils {

  val mode = ConfigurationUtil.getProperty("web.url.mode");

  //  def isNumber(str: String): Boolean = {
  //    if (str.equals("")) {
  //      return false;
  //    }
  //    for (i <- 0.to(str.length - 1)) {
  //      if (!Character.isDigit(str.charAt(i))) {
  //        return false;
  //      }
  //    }
  //    return true;
  //  }

  //判断一个字符串是数字
  def isNumber(str: String): Boolean = {
    if (str.equals("")) {
      return false;
    }
    for (i <- 0.to(str.length - 1)) {
      if (!Character.isDigit(str.charAt(i))) {
        return false
      }
    }
    return true
  }

  def isTime(str: String): Boolean = {
    if (str.length != 19) {
      return false;
    }
    for (i <- 0.to(str.length - 1)) {
      if (!(Character.isDigit(str.charAt(i)) || str.charAt(i).equals(' ') || str.charAt(i).equals('-') || str.charAt(i).equals(':'))) {
        return false;
      }
    }
    return true;
  }


  def defaultEmptyTo21(str: String): String = {
    if ("".equals(str)) {
      "21"
    } else if ("\\N".equals(str)) {
      "pyw"
    } else {
      str
    }
  }

  def main(args: Array[String]): Unit = {
    //    val rs = isRequestLog("1",".*0800] \"GET /Ssxy/loadComplete[?]p=[\\d|_|a-z|A-Z]+&g=[\\d]+.*")
    val rs = isRequestLog("HTTP/1.0\" 404", ".*0800] \"GET /Ssxy/loadComplete[?]p=[\\d|_|a-z|A-Z]+&g=[\\d]+.*")
    println(rs)
  }

  //判断字符串是否符合正则表达式
  def isRequestLog(log: String, regx: String): Boolean = {
    val p1 = regx.r
    val p1Matches = log match {
      case p1() => true
      case _ => false
    }
    p1Matches
  }


  def getArrayChannel(channelId: String): Array[String] = {
    val splited = channelId.split("_")
    if (channelId == null || channelId.equals("no_acc")) {
      Array[String]("no_acc", "", "")
    } else if (channelId.equals("")) {
      Array[String]("21", "", "")
    } else if (splited.length == 1 || splited.length == 2) {
      Array[String](splited(0), "", "")
    } else {
      Array[String](splited(0), "", "")
    }

  }

  /**
    * 在分成比例变化的时候，把变化的 game_id 的数组转化为字符串，然后 http请求告诉php
    *
    * @param params_all game_id 的数组
    * @return http请求 参数
    */
  def changParamToString(params_all: ArrayBuffer[Array[Any]]): String = {
    var param: String = "";
    for (param2 <- params_all) {
      param = param + ""
      for (index <- 0 to param2.length - 1) {
        if (index == 0) {
          param = param + "{\"game_id\":" + param2(index) + ","
        } else if (index == 1) {
          param = param + "\"divide_date\":" + "\"" + param2(index) + "\",\"from\":\"" + mode + "\"},"
        }
      }
    }
    params_all.clear()
    if (param.length > 0) {
      param = "param=[" + param.substring(0, param.length - 1) + "]"
    }
    param
  }

}
