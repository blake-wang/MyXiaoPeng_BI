package cn.xiaopeng.bi.utils

import java.io.File

/**
 * Created by JSJSB-0071 on 2017/7/13.
 */
object Hadoop {
    def hd={
      val path:String =new File(".").getCanonicalPath
      System.getProperties.put("hadoop.home.dir",path)
      new File("./bin").mkdirs()
      new File("./bin/winutils.exe").createNewFile()
    }
}
