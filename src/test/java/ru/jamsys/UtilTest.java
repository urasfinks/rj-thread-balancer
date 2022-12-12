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

    public static ThreadBalancerStatistic instanceConsumerTest(int tpsOutput, int threadCount, int queueSize, int tpsInput) {
        ThreadBalancerStatistic t = new ThreadBalancerStatistic();
        t.setTpsOutput(tpsOutput);
        t.setThreadCount(threadCount);
        t.setQueueSize(queueSize);
        t.setTpsInput(tpsInput);
        return t;
    }

    public static ThreadBalancerStatistic instanceSupplierTest(int sumTimeTpsAvg, int threadCount, int threadCountPark, int tpsInput) {
        ThreadBalancerStatistic t = new ThreadBalancerStatistic();
        t.setSumTimeTpsAvg(sumTimeTpsAvg);
        t.setThreadCount(threadCount);
        t.setThreadCountPark(threadCountPark);
        t.setTpsInput(tpsInput);
        return t;
    }

}
