package require;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by bigdata on 17-7-21.
 */
public class readFile {
    public static void main( String[] args )
    {
        File readFile=new File("/home/hduser/0721in/11_andorid.txt");
        InputStream in=null;
        InputStreamReader ir=null;
        BufferedReader br=null;

        OutputStream out=null;
        OutputStreamWriter ow=null;
        BufferedWriter bw=null;
        try {
            //用流读取文件
            in=new BufferedInputStream(new FileInputStream(readFile));
            //如果你文件已utf-8编码的就按这个编码来读取，不然又中文会读取到乱码
            ir=new InputStreamReader(in,"utf-8");
            br= new BufferedReader(ir);
            String line="";
            String md5Str = "";
            //定义集合一行一行存放
            List<String> list=new ArrayList<String>();
            //一行一行读取
            while((line=br.readLine())!=null){
                System.out.println(line);
                String[] splitLine = line.split("    ");
                md5Str = MD5Util.md5(splitLine[0]);
                line = md5Str+" "+splitLine[1];
                list.add(line);
            }
            //将集合转换成数组
            String[] arr=list.toArray(new String[list.size()]);

            //写入新文件
            File newFile=new File("/home/hduser/0721in/11_andorid.txt");
            if(!newFile.exists()){
                newFile.createNewFile();
            }

            out=new BufferedOutputStream(new FileOutputStream(newFile));
            //这里也可以给定编码写入新文件
            ow=new OutputStreamWriter(out,"utf8");
            bw=new BufferedWriter(ow);
            //遍历数组把字符串写入新文件中
            for(int x=0;x<arr.length;x++){
                bw.write(arr[x]);
                if(x!=arr.length-1){
                    //换行
                    bw.newLine();
                }

            }
            //刷新该流的缓冲，这样才真正写入完整到新文件中
            bw.flush();
        } catch (Exception e) {

            e.printStackTrace();
        }finally{
            //一定要关闭流,倒序关闭
            try {
                if(bw!=null){
                    bw.close();
                }
                if(ow!=null){
                    ow.close();
                }
                if(out!=null){
                    out.close();
                }
                if(br!=null){
                    br.close();
                }
                if(ir!=null){
                    ir.close();
                }
                if(in!=null){
                    in.close();
                }
            } catch (Exception e2) {

            }

        }

    }


}
