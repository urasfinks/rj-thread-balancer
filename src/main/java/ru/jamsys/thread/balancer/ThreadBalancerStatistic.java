package ru.jamsys.thread.balancer;

import lombok.Getter;
import lombok.Setter;
import org.springframework.lang.Nullable;
import ru.jamsys.Util;
import ru.jamsys.WrapJsonToObject;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.AtomicInteger;

public class ThreadBalancerStatistic {

    @Setter
    protected boolean debug = false;

    protected final int statisticListSize = 10; //Агрегация статистики кол-во секунд

    @Getter
    protected final AtomicInteger tpsMax = new AtomicInteger(1); //Максимальное кол-во выданных massage Supplier от всего пула потоков, это величина к которой будет стремиться пул, но из-за задежек Supplier может постоянно колебаться
    protected final ConcurrentLinkedDeque<WrapThread> threadParkQueue = new ConcurrentLinkedDeque<>(); //Очередь припаркованных потоков
    protected final AtomicInteger tpsIdle = new AtomicInteger(0); //Счётчик холостого оборота iteration не зависимо вернёт supplier сообщение или нет
    protected final AtomicInteger tpsInput = new AtomicInteger(0); //Счётчик вернувшик сообщение supplier
    protected final AtomicInteger tpsOutput = new AtomicInteger(0); //Счётчик отработанных сообщений Consumer
    protected final AtomicInteger tpsPark = new AtomicInteger(0); //Счётчик вышедших потоков на паркинг
    protected final AtomicInteger tpsThreadWakeUp = new AtomicInteger(0); //Счётчик принудительных просыпаний
    protected final ThreadBalancerStatisticData statLastSec = new ThreadBalancerStatisticData(); //Агрегированная статистика за прошлый период (сейчас 1 секунда)
    protected final List<ThreadBalancerStatisticData> statList = new ArrayList<>();
    protected final ConcurrentLinkedDeque<Long> timeTransactionQueue = new ConcurrentLinkedDeque<>(); // Статистика времени транзакций, для расчёта создания новых или пробуждения припаркованных потоков
    protected int threadCountMin; //Минимальное кол-во потоков, которое создаётся при старте и в процессе работы не сможет опустится ниже
    protected AtomicInteger threadCountMax; //Максимальное кол-во потоков, которое может создать балансировщик
    protected long threadKeepAlive; //Время жизни потока без работы

    @Nullable
    public ThreadBalancerStatisticData getStatisticAggregate() {
        return getAvgThreadBalancerStatisticData(new ArrayList<>(statList), debug);
    }

    public void setTpsMax(int max) {
        tpsMax.set(max);
    }

    public int getTpsPerThread() { //Получить сколько транзакций делает один поток
        try {
            if (statLastSec.getTimeTpsAvg() > 0) {
                BigDecimal threadTps = new BigDecimal(1000)
                        .divide(BigDecimal.valueOf(statLastSec.getTimeTpsAvg()), 2, RoundingMode.HALF_UP);
                return threadTps.intValue();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return 0;
    }

    public static ThreadBalancerStatisticData getAvgThreadBalancerStatisticData(List<ThreadBalancerStatisticData> list, boolean debug) {
        Map<String, List<Object>> agg = new HashMap<>();
        Map<String, Object> aggResult = new HashMap<>();
        for (ThreadBalancerStatisticData threadBalancerStatisticData : list) {
            WrapJsonToObject<Map> mapWrapJsonToObject = Util.jsonToObject(Util.jsonObjectToString(threadBalancerStatisticData), Map.class);
            Map<String, Object> x = (Map<String, Object>) mapWrapJsonToObject.getObject();
            for (String key : x.keySet()) {
                if (!agg.containsKey(key)) {
                    agg.put(key, new ArrayList<>());
                }
                agg.get(key).add(Util.padLeft(x.get(key).toString(), 3));
            }
        }
        if (debug) {
            Object[] objects = agg.keySet().stream().sorted().toArray();
            System.out.println("\n-------------------------------------------------------------------------------------------------------------------------------");
            for (Object o : objects) {
                System.out.println(Util.padLeft(o.toString(), 10) + ": " + Util.jsonObjectToStringPretty(agg.get(o)).replaceAll("\"", ""));
            }
            System.out.println("-------------------------------------------------------------------------------------------------------------------------------\n");
        }
        for (String key : agg.keySet()) {
            Double t = agg.get(key)
                    .stream()
                    .mapToDouble(a -> Double.parseDouble(a.toString().trim()))
                    .average()
                    .orElse(0.0);
            aggResult.put(key, t.intValue());
        }
        WrapJsonToObject<ThreadBalancerStatisticData> p = Util.jsonToObject(Util.jsonObjectToString(aggResult), ThreadBalancerStatisticData.class);
        return p.getObject();
    }

}
