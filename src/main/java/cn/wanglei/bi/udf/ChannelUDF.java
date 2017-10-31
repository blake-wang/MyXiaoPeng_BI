package cn.wanglei.bi.udf;


import org.apache.spark.sql.api.java.UDF2;

public class ChannelUDF implements UDF2<String,String,String> {

    public String call(String channel, String s2) throws Exception {
        String medium_channel = "";
        String ad_site_channel = "";
        String pkg_code = "";
        if(channel == null || channel.equals("no_acc")){
            medium_channel = "no_acc";
            ad_site_channel="";
            pkg_code="";
        }else if(channel.equals("")){
            medium_channel = "";
            ad_site_channel="";
            pkg_code = "";
        }else if(channel.split("_").length==1 || channel.split("_").length ==2){
            medium_channel = channel.split("_")[0];
            ad_site_channel = "";
            pkg_code="";
        }else{
            medium_channel = channel.split("_")[0];
            ad_site_channel = channel.split("_")[1];
            pkg_code = channel.split("_")[2];
        }
        if(s2.equals("0")){
            return medium_channel;
        }else if (s2.equals("1")){
            return ad_site_channel;
        }else if(s2.equals("2")){
            return pkg_code;
        }else{
            return medium_channel;
        }

    }
}
