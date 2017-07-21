package require;

import org.apache.commons.codec.digest.DigestUtils;

/**
 * Created by bigdata on 17-7-21.
 */
public class MD5Util {

    public static void main(String[] args) {

        String str = "ASFASFAa";
        String s = md5(str);

        System.out.println(s.toUpperCase());

    }

    //32位小写加密

    public static String md5(String target) {
        return DigestUtils.md5Hex(target);
    }
}
