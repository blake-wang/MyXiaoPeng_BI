package cn.wanglei.bi.udf;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.spark.sql.api.java.UDF1;

/**
 * Created by bigdata on 17-10-24.
 * 实现这个UDF类，
 * UDF1的意思是，传入1个参数
 * <>中第一个是传入参数，第二个是返回值
 */
public class MD5UDF2 implements UDF1<String,String> {

    public String call(String imei) throws Exception {
        String newImei = "";
        String newLineImei = "";
        String imei_md5_upper = "";

        //激活日志中imei如果是带&的，取&前面的，并转成大写
        if(imei.contains("&")){
            newImei = imei.split("&")[0].toUpperCase();
        }else{
            newImei = imei.toUpperCase();
        }

        //如果是ios的imei，给大写的imei加横杠，长度等于15的是android设备，长度等于32的是ios设备
        if(newImei.length() ==32){
            newLineImei = newImei.substring(0, 8) + "-" + newImei.substring(8, 12) + "-" + newImei.substring(12, 16) + "-" + newImei.substring(16, 20) + "-" + newImei.substring(20, 32);
        }

        //如果是android的imei，md5加密后大写
        if(newImei.length() ==15){
            newLineImei = newImei;
        }

        imei_md5_upper = md5(newLineImei).toUpperCase();

        return imei_md5_upper;
    }

    public static String md5(String target) {
        return DigestUtils.md5Hex(target);
    }
}
