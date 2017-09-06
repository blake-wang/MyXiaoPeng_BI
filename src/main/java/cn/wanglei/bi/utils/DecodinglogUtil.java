package cn.wanglei.bi.utils;

import java.io.IOException;

/**
 * Created by bigdata on 7/17/17.
 */
public class DecodinglogUtil {

    public static void main(String[] largs) {

    }

    public static String serviceDecode(String base64) throws IOException {
        byte[] data = base64Decode(base64);
        //数据打乱交换
        byte[] tmpBuf = new byte[data.length];
        for (int i = 0, j = tmpBuf.length -1, k = 0; ; i++,j--) {
            byte b = tmpBuf[i];

        }
    }

    /**
     * Base64解码，输入的String都会被转成UTF-8形式进行解码
     *
     * @param base64Str
     * @return base64解码后的数据
     */
    public static byte[] base64Decode(String base64Str) throws IOException {
        return new sun.misc.BASE64Decoder().decodeBuffer(base64Str);
    }

}
