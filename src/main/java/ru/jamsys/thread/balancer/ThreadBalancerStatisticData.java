package ru.jamsys.thread.balancer;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import lombok.Data;
import org.springframework.lang.Nullable;

import java.util.*;
import java.util.concurrent.ConcurrentLinkedDeque;

@Data
@JsonPropertyOrder({"pool", "run", "park", "parkIn", "idle", "wakeUp", "add", "remove", "input", "output", "oneThreadTps", "timeTpsAvg", "tpsCalc"})
public class ThreadBalancerStatisticData implements Cloneable {

    @JsonIgnore
    String threadBalancerName;

    @JsonProperty("park")
    int park; //Кол-во припаркованных патоков

    @JsonProperty("parkIn")
    int parkIn; //Кол-во потоков ушедших в паркинг

    @JsonProperty("pool")
    int pool; //Кол-во потоков в пуле

    @JsonProperty("run")
    int run; //Кол-во потоков, которые приняли участие в работе на текущей секунде

    @JsonProperty("idle")
    int idle; //Кол-во провёрнутых потоков, с вероятностью, что они были холостыми

    @JsonProperty("wakeUp")
    int wakeUp; //Кол-во пробуждений

    @JsonProperty("add")
    int add; //Сколько было добавлений потоков

    @JsonProperty("remove")
    int remove; //Сколько был

    @JsonProperty("input")
    int input; //Кол-во зарегистрированных чтений

    @JsonProperty("output")
    int output; //Кол-во зарегистрированных выдач

    @JsonProperty("oneThreadTps")
    int oneThreadTps; //Кол-во транзакций на поток

    @JsonProperty("tpsCalc")
    int tpsCalc; //Процент сопротивления

    //long sumTimeTpsMax;
    //long sumTimeTpsMin;
    @JsonProperty("timeTpsAvg")
    int timeTpsAvg;

    public void setTimeTransaction(ConcurrentLinkedDeque<Long> queue) {
        int resAvg = 0;
        try { //Ловим модификатор, пока ни разу не ловил, на всякий случай
            LongSummaryStatistics avgTimeTps = queue.stream().mapToLong(Long::longValue).summaryStatistics();
            resAvg = (int) avgTimeTps.getAverage();
        } catch (Exception e) {
            e.printStackTrace();
        }
        //sumTimeTpsMax = avgTimeTps.getMax();
        //sumTimeTpsMin = avgTimeTps.getMin();
        timeTpsAvg = resAvg;
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
