package ru.jamsys.thread.balancer;

import lombok.Setter;
import org.springframework.lang.Nullable;
import ru.jamsys.Util;
import ru.jamsys.UtilJson;
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
    protected long threadKeepAlive; //Время жизни потока без работы

    protected final AtomicInteger tpsMax = new AtomicInteger(1); //Максимальное кол-во выданных massage Supplier от всего пула потоков, это величина к которой будет стремиться пул, но из-за задежек Supplier может постоянно колебаться
    protected final ConcurrentLinkedDeque<WrapThread> threadParkQueue = new ConcurrentLinkedDeque<>(); //Очередь припаркованных потоков
    protected final AtomicInteger tpsIdle = new AtomicInteger(0); //Счётчик холостого оборота iteration не зависимо вернёт supplier сообщение или нет
    protected final AtomicInteger tpsInput = new AtomicInteger(0); //Счётчик вернувшик сообщение supplier
    protected final AtomicInteger tpsOutput = new AtomicInteger(0); //Счётчик отработанных сообщений Consumer
    protected final AtomicInteger tpsThreadParkIn = new AtomicInteger(0); //Счётчик вышедших потоков на паркинг
    protected final AtomicInteger tpsThreadWakeUp = new AtomicInteger(0); //Счётчик принудительных просыпаний
    protected final AtomicInteger tpsThreadAdd = new AtomicInteger(0); //Добавление потоков в балансировщик
    protected final ConcurrentLinkedQueue<WrapThread> tpsThreadRemove = new ConcurrentLinkedQueue<>(); //Удаление потоков из балансировщика
    protected final ThreadBalancerStatisticData statLastSec = new ThreadBalancerStatisticData(); //Агрегированная статистика за прошлый период (сейчас 1 секунда)
    protected final List<ThreadBalancerStatisticData> statList = new ArrayList<>();
    protected final ConcurrentLinkedDeque<Long> timeTransactionQueue = new ConcurrentLinkedDeque<>(); // Статистика времени транзакций, для расчёта создания новых или пробуждения припаркованных потоков
    protected final AtomicInteger threadCountMin = new AtomicInteger(1); //Минимальное кол-во потоков, которое создаётся при старте и в процессе работы не сможет опустится ниже
    protected final AtomicInteger threadCountMax = new AtomicInteger(1); //Максимальное кол-во потоков, которое может создать балансировщик
    protected final List<WrapThread> threadList = new CopyOnWriteArrayList<>(); //Список всех потоков
    protected final AtomicBoolean isActive = new AtomicBoolean(false); //Флаг активности текущего балансировщика
    protected final AtomicBoolean autoRestoreResistanceTps = new AtomicBoolean(true); //Автоматическое снижение сопротивления (авто коррекция на прежний уровень)

    protected int resistancePercent = 0; //Процент сопротивления, которое могут выставлять внешние компаненты системы (просьба сбавить обороты)
    protected final AtomicInteger tpsResistance = new AtomicInteger(0); //Tps сопротивления
    protected final ConcurrentLinkedQueue<Integer> listResistanceRequest = new ConcurrentLinkedQueue<>();

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

    @SuppressWarnings("all")
    public static ThreadBalancerStatisticData getAvgThreadBalancerStatisticData(List<ThreadBalancerStatisticData> list, boolean debug) {
        Map<String, List<Object>> agg = new LinkedHashMap<>();
        Map<String, Object> aggResult = new HashMap<>();
        int maxKey = 0;
        for (ThreadBalancerStatisticData threadBalancerStatisticData : list) {
            WrapJsonToObject<Map> mapWrapJsonToObject = UtilJson.toObject(UtilJson.toString(threadBalancerStatisticData, "{}"), Map.class);
            Map<String, Object> x = (Map<String, Object>) mapWrapJsonToObject.getObject();
            int maxValue = 0;

            for (String key : x.keySet()) {
                if (maxValue < x.get(key).toString().length()) {
                    maxValue = x.get(key).toString().length();
                }
            }
            for (String key : x.keySet()) {
                if (!agg.containsKey(key)) {
                    agg.put(key, new ArrayList<>());
                }
                if (maxKey < key.length()) {
                    maxKey = key.length();
                }
                agg.get(key).add(Util.padLeft(x.get(key).toString(), maxValue));
            }
        }
        if (debug) {
            Object[] objects = agg.keySet().toArray();
            System.out.println("\n-------------------------------------------------------------------------------------------------------------------------------");
            for (Object o : objects) {
                System.out.println(Util.padLeft(o.toString(), maxKey) + ": " + Objects.requireNonNull(UtilJson.toStringPretty(agg.get(o), "{}")).replaceAll("\"", ""));
            }
            System.out.println("-------------------------------------------------------------------------------------------------------------------------------\n");
        }
        for (String key : agg.keySet()) {
            double t = agg.get(key)
                    .stream()
                    .mapToDouble(a -> Double.parseDouble(a.toString().trim()))
                    .average()
                    .orElse(0.0);
            aggResult.put(key, (int) t);
        }
        WrapJsonToObject<ThreadBalancerStatisticData> p = UtilJson.toObject(UtilJson.toString(aggResult, "{}"), ThreadBalancerStatisticData.class);
        return p.getObject();
    }

    private int getActiveThreadStatistic() {
        int counter = 0;
        for (WrapThread wrapThread : threadList) {
            if (wrapThread.getActive()) {
                counter++;
            }
            wrapThread.setActive(false);
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
        statLastSec.setParkIn(tpsThreadParkIn.getAndSet(0));
        statLastSec.setWakeUp(tpsThreadWakeUp.getAndSet(0));
        statLastSec.setRun(getActiveThreadStatistic());
        statLastSec.setOneThreadTps(getTpsPerThread());
        statLastSec.setAdd(tpsThreadAdd.getAndSet(0));
        statLastSec.setResistance(tpsResistance.get());

        statLastSec.setTimeTransaction(timeTransactionQueue);
        timeTransactionQueue.clear();

        statLastSec.setRemove(tpsThreadRemove.size());
        tpsThreadRemove.clear();

        statList.add(statLastSec.clone());
        if (statList.size() > statisticListSize) {
            statList.remove(0);
        }
        return statLastSec;
    }

    public boolean isIterationWrapThread(WrapThread wrapThread) {
        return wrapThread.getIsAlive().get() && isIteration();
    }

    public boolean isIteration() {
        return isActive.get() && tpsInput.get() < tpsResistance.get() && tpsOutput.get() < tpsResistance.get();
    }

    @Override
    public void setTestAutoRestoreResistanceTps(boolean status) {
        autoRestoreResistanceTps.set(status);
    }

    @Override
    public void setResistancePrc(int prc) { //Установить процент внешнего сопротивления
        listResistanceRequest.add(prc);
    }

}
