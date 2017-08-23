package cn.wanglei.bi.udf;


import org.apache.hadoop.hive.ql.exec.UDF;

import java.security.MessageDigest;

/**
 * Created by bigdata on 17-8-23.
 */
public class MD5Util extends UDF {
    public String evaluate(String s1, String s2) {
        if (s2.equals("1")) {
            return string2MD5(s1.split("&")[0]);
        } else {
            String ss1 = s1.substring(0, 8);
            String ss2 = s1.substring(8, 12);
            String ss3 = s1.substring(12, 16);
            String ss4 = s1.substring(16, s1.length());
            String ss = ss1 + "-" + ss2 + "-" + ss3 + "-" + ss4;
            return string2MD5(ss);
        }
    }

    public static String string2MD5(String inStr) {
        MessageDigest md5 = null;
        try {
            md5 = MessageDigest.getInstance("MD5");
        } catch (Exception e) {
            System.out.println(e.toString());
            e.printStackTrace();
            return "";
        }
        char[] charArray = inStr.toCharArray();
        byte[] byteArray = new byte[charArray.length];

        for (int i = 0; i < charArray.length; i++)
            byteArray[i] = (byte) charArray[i];
        byte[] md5Bytes = md5.digest(byteArray);
        StringBuffer hexValue = new StringBuffer();
        for (int i = 0; i < md5Bytes.length; i++) {
            int val = ((int) md5Bytes[i]) & 0xff;
            if (val < 16)
                hexValue.append("0");
            hexValue.append(Integer.toHexString(val));
        }
        return hexValue.toString();
    }
}
