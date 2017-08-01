package cn.wanglei.bi.utils;

import jodd.util.PropertiesUtil;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * Created by bigdata on 17-7-31.
 * 根据key获取properties文件的value
 */
public class PropertiesUtils {
    public static String getRelativePathValue(String key) {
        Properties properties = new Properties();
        InputStream in = PropertiesUtils.class.getResourceAsStream("/resources/conf.properties");

        try {
            properties.load(in);
            in.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return (String) properties.get(key);
    }
    public static void main(String[] args){
        String brokers = getRelativePathValue("redis.host");
        System.out.println(brokers);
    }

}
