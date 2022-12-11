package ru.jamsys.thread.balancer;

import com.fasterxml.jackson.annotation.JsonIgnore;
import lombok.Data;
import org.springframework.lang.Nullable;

import java.util.LongSummaryStatistics;
import java.util.concurrent.ConcurrentLinkedDeque;

@Data
public class ThreadBalancerStatistic implements Cloneable {

    @JsonIgnore
    String serviceName;

    int threadCount;
    int queueSize;
    int tpsInput;
    int tpsOutput;
    int tpsIdle;
    int threadCountPark;

    long sumTimeTpsMax;
    long sumTimeTpsMin;
    double sumTimeTpsAvg;

    public void setTimeTransaction(ConcurrentLinkedDeque<Long> queue) {
        LongSummaryStatistics avgTimeTps = queue.stream().mapToLong(Long::longValue).summaryStatistics();
        sumTimeTpsMax = avgTimeTps.getMax();
        sumTimeTpsMin = avgTimeTps.getMin();
        sumTimeTpsAvg = avgTimeTps.getMin();
    }

    @Nullable
    public ThreadBalancerStatistic clone() {
        try {
            return (ThreadBalancerStatistic) super.clone();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
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
