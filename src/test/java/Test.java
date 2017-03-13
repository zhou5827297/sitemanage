import java.util.WeakHashMap;

/**
 * Created by zhoukai on 2017/2/6.
 */
public class Test {
    public static  void main(String[] args){
        System.out.println("100000".getBytes().length);

        WeakHashMap<String ,String> map =new WeakHashMap<String, String>();
        map.put("1","1");
    }
}
