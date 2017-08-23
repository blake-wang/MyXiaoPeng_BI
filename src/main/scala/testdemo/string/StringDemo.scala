package testdemo.string

/**
  * Created by bigdata on 17-8-23.
  * 切割字符串，取出其中的   ?   的个数
  */
object StringDemo {
  def main(args: Array[String]): Unit = {
    val str = "insert into bi_gamepublic_actions(publish_date,child_game_id,medium_channel,ad_site_channel,pkg_code,parent_game_id,group_id,os,dev_add_lg_1,dev_add_lg_2,dev_add_lg_3,dev_add_lg_4,dev_add_lg_5,dev_add_lg_6_10,dev_add_lg_11)\n values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) on duplicate key update dev_add_lg_1=?,dev_add_lg_2=?,dev_add_lg_3=?,dev_add_lg_4=?,dev_add_lg_5=?,dev_add_lg_6_10=?,dev_add_lg_11=?"

    val sql2Mysql = str.replace("|", " ").toLowerCase
    //获取values（）里面有多少个?参数，有利于后面的循环
    val startValuesIndex = sql2Mysql.indexOf("(?") + 1
    val endValuesIndex = sql2Mysql.indexOf("?)") + 1
    //values中的个数
    val valueArray: Array[String] = sql2Mysql.substring(startValuesIndex, endValuesIndex).split(",") //两个（？？）中间的值
    //条件中的参数个数
    val wh: Array[String] = sql2Mysql.substring(sql2Mysql.indexOf("update") + 6).split(",") //找update后面的字符串再判断
    //查找需要insert的字段
    val cols_ref = sql2Mysql.substring(0, sql2Mysql.lastIndexOf("(?")) //获取（?特殊字符前的字符串，然后再找字段
    val cols: Array[String] = cols_ref.substring(cols_ref.lastIndexOf("(") + 1, cols_ref.lastIndexOf(")")).split(",")
    println("startValuesIndex  :  "+startValuesIndex)
    println("endValuesIndex  :  "+endValuesIndex)

    val splitedStr = str.substring(startValuesIndex,endValuesIndex)
    println(splitedStr)
  }

}
