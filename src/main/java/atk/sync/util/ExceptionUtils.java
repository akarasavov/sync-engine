package atk.sync.util;

import java.util.concurrent.Callable;

public class ExceptionUtils {

    public static void wrapToRuntimeException(Runnable runnable) {
        try {
            runnable.run();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static <T> T wrapToRuntimeException(Callable<T> callable) {
        try {
            return callable.call();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @FunctionalInterface
    public static interface Runnable {
        void run() throws Exception;
    }
}
