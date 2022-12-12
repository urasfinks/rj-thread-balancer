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
}
