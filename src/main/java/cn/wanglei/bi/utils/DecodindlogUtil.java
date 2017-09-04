package cn.wanglei.bi.utils;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.zip.DataFormatException;
import java.util.zip.Inflater;

/**
 * Created by panyangjun on 2017/3/4.
 */
public class DecodindlogUtil {

    /**
     * 缓存大小
     */
    public static final int BUFFER_SIZE = 1024;
    /**
     * Base64解码，输入的String都会被转成UTF-8形式进行解码
     *
     * @param base64Str
     * @return base64解码后的数据
     */
    public static byte[] base64Decode(String base64Str) throws IOException {
        return new sun.misc.BASE64Decoder().decodeBuffer(base64Str);
    }

    /***
     * 解码
     */
    public static String serviceDecode(String base64) throws DataFormatException, IOException {

        byte[] data = base64Decode(base64);

        //数据打乱交换
        byte[] tmpBuf = new byte[data.length];
        for (int i = 0, j = tmpBuf.length - 1, k = 0; ; i++, j--) {
            if (i == j) {
                tmpBuf[k] = data[i];
                break;
            } else if (i > j) {
                break;
            }

            tmpBuf[k++] = data[i];
            tmpBuf[k++] = data[j];
        }

        //数据压缩
        int read = -1;
        Inflater inf = new Inflater(true);
        inf.setInput(tmpBuf);
        ByteArrayOutputStream bout = new ByteArrayOutputStream();
        byte[] buf = new byte[BUFFER_SIZE];
        while (0 != (read = inf.inflate(buf))) {
            bout.write(buf, 0, read);
        }
        bout.flush();
        bout.close();
        inf.end();

        return new String(bout.toByteArray(),"UTF-8");

    }
}
