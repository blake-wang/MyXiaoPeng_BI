package cn.xiaopeng.bi.utils

import cn.wanglei.bi.ConfigurationUtil

import scala.collection.mutable.ArrayBuffer

/**
  * Created by sumenghu on 2016/9/1.
  */
object StringUtils {

  val  mode=  ConfigurationUtil.getProperty("web.url.mode");

  def isNumber(str: String): Boolean = {
    if (str.equals("")) {
      return false;
    }
    for (i <- 0.to(str.length - 1)) {
      if (!Character.isDigit(str.charAt(i))) {
        return false;
      }
    }
    return true;
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

  /**
    * 判断字符串是否符合正则表达式
    *
    * @param log
    * @param regx
    * @return
    */
  def isRequestLog(log: String, regx: String): Boolean = {
    val p1 = regx.r
    val p1Matches = log match {
      case p1() => true // no groups
      case _ => false
    }
    p1Matches
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

  def getArrayChannel(channelId: String): Array[String] = {
    val splited = channelId.split("_")
    if (channelId == null || channelId.equals("") || channelId.equals("0")) {
      Array[String]("21", "", "")
    } else if (splited.length < 3) {
      Array[String](channelId, "", "")
    } else {
      Array[String](splited(0), splited(1), splited(2))
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
          param = param + "\"divide_date\":" + "\"" + param2(index) + "\",\"from\":\""+mode+"\"},"
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
