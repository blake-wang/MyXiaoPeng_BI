package cn.wanglei.bi.utils;

import org.apache.commons.codec.digest.DigestUtils;

import java.security.MessageDigest;

/**
 * Created by wanglei on 2017/9/9.
 */
public class MD5Util {

    //32位小写加密
    public static String md5(String target) {
        return DigestUtils.md5Hex(target);
    }

    //给字符串加密  32位小写加密
    public static String string2MD5(String string) {
        MessageDigest md5 = null;
        try {
            md5 = MessageDigest.getInstance("MD5");
        } catch (Exception e) {
            System.out.println(e.toString());
            e.printStackTrace();
            return "";
        }
        char[] charArray = string.toCharArray();
        byte[] byteArray = new byte[charArray.length];

        for (int i = 0; i < charArray.length; i++) { byteArray[i] = (byte) charArray[i]; }
        byte[]       md5Bytes = md5.digest(byteArray);
        StringBuffer hexValue = new StringBuffer();
        for (int i = 0; i < md5Bytes.length; i++) {
            int val = ((int) md5Bytes[i]) & 0xff;
            if (val < 16) { hexValue.append("0"); }
            hexValue.append(Integer.toHexString(val));
        }
        return hexValue.toString();
    }
}
