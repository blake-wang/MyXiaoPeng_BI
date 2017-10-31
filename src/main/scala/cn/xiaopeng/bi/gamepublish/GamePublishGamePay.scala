package cn.xiaopeng.bi.gamepublish

import java.io.File
import java.sql.PreparedStatement
import java.text.SimpleDateFormat
import java.util.{Calendar, Date}
import cn.xiaopeng.bi.utils.JdbcUtil
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.Row
import org.apache.spark.sql.hive.HiveContext

/**
  * Created by bigdata on 17-10-31.
  */
object GamePublishGamePay {
  def main(args: Array[String]): Unit = {
//    val currentday = args(0)
    val currentday = "2017-10-30"
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd")
    val date = dateFormat.parse(currentday)
    val cal = Calendar.getInstance()
    cal.setTime(date)
    cal.add(Calendar.DATE,-15)
    val day_15 = dateFormat.format(cal.getTime)

    val hivesql="select \ndistinct \n if(reg_date is null,'000-00-00',reg_date) pub_date\n,if(game_id is null,0,game_id) game_id\n,if(bg.name is null,'',bg.name) game_name\n,if((split(expand_channel,'_')[0] is null or split(expand_channel,'_')[0]=''),'21',split(expand_channel,'_')[0]) as par_channel\n,if(split(expand_channel,'_')[1] is null,'',split(expand_channel,'_')[1]) as sub_channel\n,if(split(expand_channel,'_')[2] is null,'',split(expand_channel,'_')[2]) as adver_flag\n,if(datediff(order_date,reg_date) is null,0,datediff(order_date,reg_date))+1 as date_diff\n,sum(case when rw=1 then 1 else 0 end) over(partition by game_id,expand_channel,reg_date order by order_date) as pays\n,sum(if(ori_price is null,0,ori_price)) over(partition by game_id,expand_channel,reg_date order by order_date) as pay_amount\n\nfrom \n(\n  \nselect\n      regi.game_id\n     ,if(regi.expand_channel is null or expand_channel='',21,regi.expand_channel) expand_channel\n     ,to_date(regi.reg_time) reg_date\n     ,to_date(dd.date_value) order_date\n     ,case when to_date(dd.date_value)=to_date(order_time) then ori_price else 0 end ori_price  --当日期有数据时写充值金额，没数据则写0\n     ,regi.game_account\n     ,row_number() over(partition by regi.game_id,regi.expand_channel,regi.game_account,to_date(regi.reg_time) order by date_value asc) as rw\n     from (select distinct game_account,reg_time,game_id,expand_channel from  yyft.ods_regi_rz rz where  game_id is not null) regi \njoin  (select distinct game_id,order_no,order_time,order_status,game_account,ori_price,prod_type from yyft.ods_order where to_date(order_time)>'day_15' \nand to_date(order_time)<='currentday' )  od on lower(trim(od.game_account))=lower(trim(regi.game_account)) \njoin (select distinct game_id,status from yyft.ods_publish_game) gm on gm.game_id=od.game_id and gm.status=1\n     join archive.dim_day dd \n     on to_date(order_time)<=to_date(date_value) and date_add(to_date(order_time),15)>=to_date(date_value)  --取账号注册时间起 15天内数据，当日要小于当前日期\n     where od.order_status in(4) and to_date(regi.reg_time)>'day_15' and to_date(regi.reg_time)<='currentday' and regi.game_id is not null\n) a join yyft.bgame bg on bg.id=a.game_id"
    val mysqlsql = "insert into bi_gamepublish_gamepay(pub_date,game_id,game_name,par_channel,sub_channel,adver_flag,date_diff,pays,pay_amount)" +
      " values(?,?,?,?,?,?,?,?,?)" +
      " on duplicate key update date_diff=?,pays=?,pay_amount=?,game_name=?"

    //全部转为小写，后面好判断
    val execSql = hivesql.replace("currentday", currentday).replace("day_15", day_15) //hive sql
    val sql2Mysql = mysqlsql.replace("|", " ").toLowerCase

    //Hadoop libariy
    val path: String = new File(".").getCanonicalPath
    System.getProperties().put("hadoop.home.dir", path)
    new File("./bin").mkdirs()
    new File("./bin/winutils.exe").createNewFile()

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

    /** ******************hive库操作 *******************/
    val sparkConf = new SparkConf().setAppName(this.getClass.getName.replace("$", ""))
    val sc = new SparkContext(sparkConf)
    val sqlContext = new HiveContext(sc)
    sqlContext.sql("use yyft")
    sqlContext.cacheTable("dim_day")  //缓存表
    sqlContext.sql("drop table archive.dim_day")
    sqlContext.sql("create table archive.dim_day as  select date_value from yyft.dim_day where to_date(date_value)<='currentday' and to_date(date_value)>='day_15'".replace("currentday", currentday).replace("day_15", day_15))
    val dataf = sqlContext.sql(execSql)//执行hive sql
    /********************数据库操作 *******************/
    dataf.foreachPartition((rows: Iterator[Row]) => {
      val conn = JdbcUtil.getConn()
      val ps: PreparedStatement = conn.prepareStatement(sql2Mysql)
      for (x <- rows) {
        //补充value值
        for (rs <- 0 to valueArray.length - 1) {
          ps.setString(rs.toInt + 1, x.get(rs).toString)
        }
        //补充条件
        for (i <- 0 to wh.length - 1) {
          val rs = wh(i).trim.substring(0, wh(i).trim.lastIndexOf("="))
          for (ii <- 0 to cols.length - 1) {
            if (cols(ii).trim.equals(rs)) {
              ps.setString(i.toInt + valueArray.length.toInt + 1, x.get(ii).toString)
            }
          }
        }
        ps.executeUpdate()
      }
      conn.close()
    }
    )
    System.clearProperty("spark.driver.port")
    sc.stop()
    sc.clearCallSite()


  }
}
