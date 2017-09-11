package testdemo

/**
  * Created by bigdata on 17-9-11.
  */
object TestMatchMethod {
  def main(args: Array[String]): Unit = {

    //偏函数
    val advName = 0
    val topic = advName match {
      case 1 => "momo"
      case 2 => "baidu"
      case 3 => "jinritoutiao"
      case 5 => "uc"
      case 4 => "aiqiyi"
      case 6 => "guangdiantong"
      case 0 => "pyw"
    }

    println(topic)
  }

}
