package cn.xiaopeng.bi.gamepublish

import org.apache.log4j.{Level, Logger}

/**
  * Created by bigdata on 17-8-31.
  */
object GamePublishKpi {
  var arg ="60"
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)

  }

}
