package ru.jamsys;

import ru.jamsys.thread.balancer.ThreadBalancerStatistic;

import java.util.concurrent.TimeUnit;

public class UtilTest {

    public static void sleepSec(int seconds) {
        try {
            TimeUnit.SECONDS.sleep(seconds);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    /*public static ThreadBalancerStatistic getThreadBalancerStatisticFromString(String log){
        ThreadBalancerStatistic t = new ThreadBalancerStatistic();
        //needThread: 1 => getNeedCountThread: needTransaction: 10; threadTps: null; ThreadBalancerStatistic(serviceName=Test, threadCount=1, queueSize=0, tpsInput=12, tpsOutput=0, tpsIdle=2, threadCountPark=1, sumTimeTpsMax=0, sumTimeTpsMin=0, sumTimeTpsAvg=0.0)
        log.split("ThreadBalancerStatistic(")
        return t;
    }*/

}
