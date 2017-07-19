package testdemo

import java.io.{BufferedWriter, File, FileWriter}

/**
  * Created by bigdata on 7/17/17.
  */
object FileCreateDemo {
  def main(args: Array[String]): Unit = {
    val file = new File("/home/bigdata/aaa.txt")
    if(!file.exists()){
      file.createNewFile()
    }
    //
    val writer = new BufferedWriter(new FileWriter(file,true))

    for (i <- 1 to 10){
      writer.append("wo jiu shi te shi yi xia--- "+i)
      // newLine is BufferedWriter method
      writer.newLine()
    }
    writer.flush()
    writer.close()


  }

}
