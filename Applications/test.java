package Applications;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.util.*;

public class test {
    public static void main(String[] argv) throws FileNotFoundException {
        class t{
            int x = 0;
        }
        Vector<t> tmp = new Vector<>();
        class b{
            Vector<t> x;
        }
        b bb = new b();
        bb.x = tmp;
        tmp.add(new t());
        System.out.println(bb.x.size());
    }
}
