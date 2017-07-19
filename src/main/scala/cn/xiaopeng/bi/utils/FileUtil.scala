package cn.xiaopeng.bi.utils

import java.io.{BufferedWriter, File, FileWriter}

import org.apache.spark.sql.DataFrame

/**
  * Created by bigdata on 7/17/17.
  */
object FileUtil {

  def appendToFile(fileName:String,s:String)={
    //将数据存入文件
    val file = new File(fileName)
    if(!file.exists()){
      file.createNewFile()
    }
    val writer = new BufferedWriter(new FileWriter(file,true))
    writer.append(s)
    writer.newLine()
    writer.flush()
    writer.close()

  }
  /**
    * 用于测试的一个方法
    * @param df
    * @param fileName
    */
  def appendDfToFile(df:DataFrame,fileName:String) = {
    df.foreachPartition(iter=>{
      iter.foreach(t=>{
        appendToFile(fileName,t.toString());
      })
    })
  }
}
