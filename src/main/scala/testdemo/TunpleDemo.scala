package testdemo

/**
  * Created by bigdata on 17-8-30.
  */
object TunpleDemo {
  def main(args: Array[String]): Unit = {
    val t = ("tc285924963","2017-08-28 18:05:05",3812,"shuju613_pcopen613_3812M114005","862005035210265&a505c6fd5506aef8&f0:43:47:1f:f8:eb")
    println(t._1+"------"+t._2)


    val arr = Array("1","2","3")
    val i =arr.apply(2)
    val x = arr(1)
    println(i)
    println(x)
  }
}
