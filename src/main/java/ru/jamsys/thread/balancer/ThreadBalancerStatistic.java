package ru.jamsys.thread.balancer;

import lombok.Getter;
import lombok.Setter;
import org.springframework.lang.Nullable;
import ru.jamsys.Util;
import ru.jamsys.WrapJsonToObject;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public abstract class ThreadBalancerStatistic implements ThreadBalancer {

    @Setter
    protected boolean debug = false;
    protected final int statisticListSize = 10; //Агрегация статистики кол-во секунд (по умолчанию 10 должно быть)
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
    protected final List<WrapThread> threadList = new CopyOnWriteArrayList<>(); //Список всех потоков
    protected final AtomicBoolean isActive = new AtomicBoolean(false); //Флаг активности текущего балансировщика
    protected final AtomicBoolean autoRestoreResistanceTps = new AtomicBoolean(true); //Автоматическое снижение выставленного сопротивления, на каждом тике будет уменьшаться (авто коррекция на прежний уровень)
    protected final AtomicInteger poolAdd = new AtomicInteger(0);
    protected final ConcurrentLinkedQueue<WrapThread> poolRemove = new ConcurrentLinkedQueue();

    @Getter
    private final AtomicInteger resistancePercent = new AtomicInteger(0); //Процент сопротивления, которое могут выставлять внешние компаненты системы (просьба сбавить обороты)

    @Nullable
    public ThreadBalancerStatisticData getStatisticAggregate() {
        return getAvgThreadBalancerStatisticData(new ArrayList<>(statList), debug);
    }

    @Override
    public void setTpsMax(int max) {
        tpsMax.set(max);
    }

    public int getTpsPerThread() { //Получить сколько транзакций делает один поток
        //Если время supplier и consumer будут очень быстрыми (равно 0, а микро секунды мы не считаем) то расчёт кол-ва потоков будет не верный, а именно равен -1
        try {
            if (statLastSec.getTimeTpsAvg() > 0) {
                BigDecimal threadTps = new BigDecimal(1000)
                        .divide(BigDecimal.valueOf(statLastSec.getTimeTpsAvg()), 2, RoundingMode.HALF_UP);
                return threadTps.intValue();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return -1; //Не смогли рассчитать, так как время нулевое (как минимум это может быть в быстроте операций, а мы MicroTime не считаем)
    }

    public static ThreadBalancerStatisticData getAvgThreadBalancerStatisticData(List<ThreadBalancerStatisticData> list, boolean debug) {
        Map<String, List<Object>> agg = new LinkedHashMap<>();
        Map<String, Object> aggResult = new HashMap<>();
        int maxKey = 0;
        for (ThreadBalancerStatisticData threadBalancerStatisticData : list) {
            WrapJsonToObject<Map> mapWrapJsonToObject = Util.jsonToObject(Util.jsonObjectToString(threadBalancerStatisticData), Map.class);
            Map<String, Object> x = (Map<String, Object>) mapWrapJsonToObject.getObject();
            int maxValue = 0;

            for (String key : x.keySet()) {
                if(maxValue < x.get(key).toString().length()){
                    maxValue = x.get(key).toString().length();
                }
            }
            for (String key : x.keySet()) {
                if (!agg.containsKey(key)) {
                    agg.put(key, new ArrayList<>());
                }
                if(maxKey < key.length()){
                    maxKey = key.length();
                }
                agg.get(key).add(Util.padLeft(x.get(key).toString(), maxValue));
            }
        }
        if (debug) {
            Object[] objects = agg.keySet().stream().toArray();
            System.out.println("\n-------------------------------------------------------------------------------------------------------------------------------");
            for (Object o : objects) {
                System.out.println(Util.padLeft(o.toString(), maxKey) + ": " + Util.jsonObjectToStringPretty(agg.get(o)).replaceAll("\"", ""));
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

    private int getActiveThreadStatistic() {
        int counter = 0;
        for (WrapThread wrapThread : threadList) {
            if (wrapThread.getFine()) {
                counter++;
            }
            wrapThread.setFine(false);
        }
        return counter;
    }

    @Override
    public ThreadBalancerStatisticData flushStatistic() { //Вызывается планировщиком StatisticThreadBalancer для агрегации статистики за секунду
        statLastSec.setThreadBalancerName(getName());
        statLastSec.setIdle(tpsIdle.getAndSet(0));
        statLastSec.setInput(tpsInput.getAndSet(0));
        statLastSec.setOutput(tpsOutput.getAndSet(0));
        statLastSec.setPool(threadList.size());
        statLastSec.setPark(threadParkQueue.size());
        statLastSec.setParkIn(tpsPark.getAndSet(0));
        statLastSec.setWakeUp(tpsThreadWakeUp.getAndSet(0));
        statLastSec.setRun(getActiveThreadStatistic());
        statLastSec.setOneThreadTps(getTpsPerThread());
        statLastSec.setAdd(poolAdd.getAndSet(0));

        statLastSec.setTimeTransaction(timeTransactionQueue);
        timeTransactionQueue.clear();

        statLastSec.setRemove(poolRemove.size());
        poolRemove.clear();

        statList.add(statLastSec.clone());
        if (statList.size() > statisticListSize) {
            statList.remove(0);
        }
        return statLastSec;
    }

    public boolean isIterationWrapThread(WrapThread wrapThread) {
        return wrapThread.getIsRun().get() && isIteration();
    }

    public boolean isIteration() {
        return isActive.get() && tpsInput.get() < tpsMax.get() && tpsOutput.get() < tpsMax.get();
    }

    @Override
    public void setTestAutoRestoreResistanceTps(boolean status) {
        autoRestoreResistanceTps.set(status);
    }

    @Override
    public int setResistance(int prc) { //Установить процент внешнего сопротивления
        return 0;
    }

}
