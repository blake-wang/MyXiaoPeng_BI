package testdemo.string

/**
  * Created by bigdata on 17-8-2.
  */
object StringSplitDemo {

  def main(args: Array[String]): Unit = {
    val line = "2017-05-17 13:57:45,046 [INFO] bi: bi_regi|2ccf59ac-8c52-4e||tc272165712|3649|2017-05-17 13:57:45|6|17||17166660006|1|ANDROID|0|weixin_ios007_3649M97001|862005035210265&a505c6fd5506aef8&f0:43:47:1f:f8:eb"
    val arr1 = line.split("\\|");
    println(arr1.toBuffer)
    val arr2 = line.split("\\|",-1);
    println(arr2.toBuffer)
  }

}
