package test;

/**
 * Created by Smart on 2016/12/30.
 */
public class TestArges {

    public static int add(int... list) {
        int sum = 0;
        for(int item : list) {
            sum += item;
        }
        return sum;
    }

    public static void main(String[] args) {
        System.out.println(add(1,2,3));
    }
}
