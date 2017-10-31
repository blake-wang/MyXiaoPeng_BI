package TestDemo;

/**
 * Created by bigdata on 17-10-28.
 */
public class TestStringDemo {
    public static void main(String[] args) {
        String str = "haha (?,?,?,?,?) nihao";
        System.out.println(str.length());
        System.out.println(str.indexOf("(?"));
        System.out.println(str.indexOf("?)"));
        System.out.println(str.lastIndexOf("?"));
        System.out.println(str.getBytes());

        str = str.substring(str.indexOf("(?")+1,str.indexOf("?)")+1);
        System.out.println(str);

    }
}
