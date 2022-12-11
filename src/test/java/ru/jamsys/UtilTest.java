package ru.jamsys;

import java.util.concurrent.TimeUnit;

public class UtilTest {

    public static void sleepSec(int seconds) {
        try {
            TimeUnit.SECONDS.sleep(seconds);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

}
