package testdemo

import java.io.File

/**
 * Created by JSJSB-0071 on 2017/7/14.
 */
object FileTest {

  def main(args: Array[String]) {
    val path1 = new File(".").getAbsolutePath
    val path2 = new File(".").getPath
    val path3 = new File(".").getCanonicalPath


    val path: String = new File(".").getCanonicalPath
    System.getProperties().put("hadoop.home.dir", path3)
    new File("./bin").mkdirs()
    new File("./bin/winutils.exe").createNewFile()

    println("path1:   "+path1)
    println("path2:   "+path2)
    println("path3:   "+path3)

  }
}
