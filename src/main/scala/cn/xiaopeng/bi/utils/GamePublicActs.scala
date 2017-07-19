package cn.xiaopeng.bi.utils

import org.apache.spark.rdd.RDD

/**
 * Created by JSJSB-0071 on 2017/7/13.
 */
object GamePublicActs {
  /*
  *
  * 加载订单数据
  * 参数：游戏账号（5），订单号（2），订单日期（6），游戏id（7）,充值流水（10）+代金券，imei(24).sub
  * */
  def loadOrderInfo(dslogs: RDD[String]) = {
//    val rdd = dslogs.filter(x => {
//      val arr = x.split("\\|")
//
//    })
  }

}
