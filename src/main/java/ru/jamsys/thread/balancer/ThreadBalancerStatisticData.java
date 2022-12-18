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

    int threadPark;
    int threadPool;
    int threadRuns;
    int tpsInput;
    int tpsOutput;
    int tpsIdle;
    int tpsPark;
    int tpsWakeUp;
    int zTpsThread; //z - что бы она в конце отсортировалась в агрегации отображения статистики

    //long sumTimeTpsMax;
    //long sumTimeTpsMin;
    int timeTpsAvg;

    public void setTimeTransaction(ConcurrentLinkedDeque<Long> queue) {
        LongSummaryStatistics avgTimeTps = queue.stream().mapToLong(Long::longValue).summaryStatistics();
        //sumTimeTpsMax = avgTimeTps.getMax();
        //sumTimeTpsMin = avgTimeTps.getMin();
        timeTpsAvg = (int) avgTimeTps.getAverage();
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
