package maxA.util;

/**
 * Created by max2 on 7/1/2015.
 */
public class MaxLogger {

    public MaxLogger(){
        // nothing to do here
    }

    public static void debug(Class<?> where, String message){
        System.out.println("[debug] - " + where + " - " + message);
    }

    public static void info(Class<?> where, String message){
        System.out.println("[info]-" + where + " - " + message);
    }
    public static void error(Class<?> where, String message){
        System.out.println("[error]-" + where + " - " + message);
    }
}
