package cn.wanglei.bi.udf;

import org.apache.commons.codec.digest.DigestUtils;

/**
 * Created by bigdata on 17-9-29.
 */
public class MD5UDF {
    public static void main(String[] args) {
        String imei = "C8233FE018B64581B1E4DB188877DDE2";
//        String imei = "863858039318693&86ba658697831d3b&54:19:c8:aa:89:6d";


        String newImei = "";
        String newLineImei = "";
        String imei_md5_upper = "";

        //激活日志中imei如果是带&的，取&前面的，并转成大写
        if (imei.contains("&")) {
            newImei = imei.split("&")[0].toUpperCase();
        } else {
            newImei = imei.toUpperCase();
        }

        //如果是ios的imei，给大写的imei加横杠 ，长度等于15的是android设备，长度等于32的是ios设备
        if (newImei.length() == 32) {
            newLineImei = newImei.substring(0, 8) + "-" + newImei.substring(8, 12) + "-" + newImei.substring(12, 16) + "-" + newImei.substring(16, 20) + "-" + newImei.substring(20, 32);
        }
        //如果是android的imei，md5加密后大写
        if(newImei.length() == 15){
            newLineImei = newImei;
        }
        //ios返回的是 ： imei大写后，加横杠，再md5，再转大写
        //android返回的是 ： imei大写后，md5，再转大写
        imei_md5_upper = md5(newLineImei).toUpperCase();
        System.out.println(imei);
        System.out.println(newImei);
        System.out.println(newLineImei);
        System.out.println(imei_md5_upper);

    }

    public static String md5(String target) {
        return DigestUtils.md5Hex(target);
    }
}
