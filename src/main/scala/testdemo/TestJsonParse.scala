package testdemo

import net.sf.json.JSONObject

/**
  * Created by bigdata on 17-9-6.
  */
object TestJsonParse {
  val jsonStr = "{\"pkg_id\":\"xxx\",\"os\":\"a\",\"imei\":\"aaaa\",\"mac\":\"kkkk\",\"idfa\":\"adfaf\",\"ua\":\"adfa;kdfj\",\"ts\":\"1123\",\"lbs\":\"adaffd\",\"callback\":\"7897879\",\"adv_name\":\"bi_adv_momo_click\"}"

  def main(args: Array[String]): Unit = {

    //json解析
    val bean = JSONObject.fromObject(jsonStr)
    println("pkg_id : " + bean.get("pkg_id"))
    println("os : " + bean.get("os"))
    println("imei : " + bean.get("imei"))
    println("mac : " + bean.get("mac"))
    println("idfa : " + bean.get("idfa"))
  }

}
