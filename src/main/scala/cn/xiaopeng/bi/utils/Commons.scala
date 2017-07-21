package cn.xiaopeng.bi.utils

/**
  * Created by bigdata on 7/18/17.
  */
object Commons {
  /**
    * Null转0
    *
    * @param ls
    * @return
    */

  def getNullTo0(ls: String): Float = {
    var rs = 0.0.toFloat
    if (!ls.equals("")) {
      rs = ls.toFloat
    }
    return rs
  }

  def getImei(imei: String): String = {
    return imei.replace("$", "|")
  }


}
