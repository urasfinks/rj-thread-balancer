package ru.jamsys.thread.balancer;

import com.fasterxml.jackson.annotation.JsonIgnore;
import lombok.Data;
import org.springframework.lang.Nullable;

import java.util.*;
import java.util.concurrent.ConcurrentLinkedDeque;

@Data
public class ThreadBalancerStatisticData implements Cloneable {

    @JsonIgnore
    String threadBalancerName;

    int threadCount;
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
        sumTimeTpsAvg = avgTimeTps.getAverage();
    }

    @Nullable
    public ThreadBalancerStatisticData clone() {
        try {
            return (ThreadBalancerStatisticData) super.clone();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }
}
