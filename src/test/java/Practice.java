import java.util.List;
import java.util.ArrayList;

/**
 * Created by hadoop on 16-12-18.
 */
public class Practice {
    public static void main(String [] args){
        List<String> list = new ArrayList<String>();
        for (int i=0; i<10; i++){
            String cont = "A"+i;
            System.out.println(cont);
            list.add("A"+i);
        }
        System.out.println(list.get(2));

        list.remove(2);
        System.out.println(list.get(2));
    }
}
