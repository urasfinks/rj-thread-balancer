package ru.jamsys.thread.balancer;

import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import org.springframework.lang.Nullable;
import ru.jamsys.Util;
import ru.jamsys.WrapJsonToObject;
import ru.jamsys.message.Message;
import ru.jamsys.scheduler.SchedulerTick;
import ru.jamsys.scheduler.SchedulerTickImpl;
import ru.jamsys.thread.balancer.exception.ShutdownException;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Stream;

public abstract class AbstractThreadBalancer implements ThreadBalancer, SchedulerTick {

    @Setter
    protected boolean debug = false;

    @Setter
    @NonNull
    protected volatile Supplier<Message> supplier = () -> null; //Поставщик входящих сообщений в балансировщик

    @Setter
    @NonNull
    protected volatile Consumer<Message> consumer = (msg) -> {
    }; //Потребитель сообщений представленных поставщиком

    @Getter
    protected String name; //Имя балансировщика - будет отображаться в пуле jmx

    private int statisticListSize = 10; //Агрегация статистики кол-во секунд
    private int threadCountMin; //Минимальное кол-во потоков, которое создаётся при старте и в процессе работы не сможет опустится ниже
    private AtomicInteger threadCountMax; //Максимальное кол-во потоков, которое может создать балансировщик
    private long threadKeepAlive; //Время жизни потока без работы

    @Getter
    protected final AtomicInteger tpsInputMax = new AtomicInteger(1); //Максимальное кол-во выданных massage Supplier от всего пула потоков, это величина к которой будет стремиться пул, но из-за задежек Supplier может постоянно колебаться

    @Getter
    private final AtomicInteger resistancePercent = new AtomicInteger(0); //Процент сопротивления, которое могут выставлять внешние компаненты системы (просьба сбавить обороты)

    private final AtomicInteger threadNameCounter = new AtomicInteger(0); //Индекс создаваемого потока, что бы всё красиво было в jmx

    private final AtomicBoolean isActive = new AtomicBoolean(false); //Флаг активности текущего балансировщика
    private final List<WrapThread> threadList = new CopyOnWriteArrayList<>(); //Список всех потоков
    protected final ConcurrentLinkedDeque<WrapThread> threadParkQueue = new ConcurrentLinkedDeque<>(); //Очередь припаркованных потоков

    private final AtomicInteger tpsIdle = new AtomicInteger(0); //Счётчик холостого оборота iteration не зависимо вернёт supplier сообщение или нет
    protected final AtomicInteger tpsInput = new AtomicInteger(0); //Счётчик вернувшик сообщение supplier
    protected final AtomicInteger tpsOutput = new AtomicInteger(0); //Счётчик отработанных сообщений Consumer
    protected final AtomicInteger tpsPark = new AtomicInteger(0); //Счётчик вышедших потоков на паркинг
    protected final AtomicInteger tpsThreadWakeUp = new AtomicInteger(0); //Счётчик принудительных просыпаний

    protected final ThreadBalancerStatisticData statLastSec = new ThreadBalancerStatisticData(); //Агрегированная статистика за прошлый период (сейчас 1 секунда)
    protected List<ThreadBalancerStatisticData> statList = new ArrayList<>();

    protected final ConcurrentLinkedDeque<Long> timeTransactionQueue = new ConcurrentLinkedDeque<>(); // Статистика времени транзакций, для расчёта создания новых или пробуждения припаркованных потоков

    protected AtomicBoolean autoRestoreResistanceTps = new AtomicBoolean(true); //Автоматическое снижение выставленного сопротивления, на каждом тике будет уменьшаться (авто коррекция на прежний уровень)

    @Setter
    protected Function<Integer, Integer> formulaAddCountThread = (need) -> need; //При расчёте необходимого кол-ва потоков, происходит прогон через эту формулу (в дальнейшем для корректировки плавного старта)

    @Setter
    protected Function<Integer, Integer> formulaRemoveCountThread = (need) -> need;

    private SchedulerTickImpl scheduler; //Планировщик тиков

    public int getThreadSize() {
        return threadList.size();
    }

    @Override
    @Nullable
    public ThreadBalancerStatisticData getStatisticAggregate() {
        return getAvgThreadBalancerStatisticData(new ArrayList<>(statList), debug);
    }

    @Override
    public void threadStabilizer() { //Вызывается планировщиком StabilizerThread каждую секунду
        //Когда мы ничего не знаем о внешней среде, можно полагаться, только на работу текущих потоков
        Util.logConsole(Thread.currentThread(), "threadStabilizer()");
        if (isActive()) {
            try {
                if (isAddThreadCondition()) {
                    int addThreadCount = overclocking(formulaAddCountThread.apply(getThreadSize()));
                    if (debug) {
                        Util.logConsole(Thread.currentThread(), "AddThread: " + addThreadCount);
                    }
                } else if (isThreadRemove()) { //Кол-во потоков больше минимума
                    checkKeepAliveAndRemoveThread();
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
            if (autoRestoreResistanceTps.get() && getResistancePercent().get() > 0) {
                getResistancePercent().decrementAndGet();
            }
        } else {
            Util.logConsole(Thread.currentThread(), "threadStabilizer() ThreadBalancer not active");
        }
    }

    public boolean isIteration(WrapThread wrapThread) {
        return isActive() && wrapThread.getIsRun().get() && tpsInput.get() < tpsInputMax.get();
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

    @Override
    public void setTpsInputMax(int max) {
        tpsInputMax.set(max);
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

    @Nullable
    public String getStatisticMomentum() {
        ThreadBalancerStatisticData curStat = new ThreadBalancerStatisticData();
        curStat.setThreadBalancerName(getName());
        curStat.setTpsIdle(tpsIdle.get());
        curStat.setTpsInput(tpsInput.get());
        curStat.setTpsOutput(tpsOutput.get());
        curStat.setThreadPool(threadList.size());
        curStat.setThreadPark(threadParkQueue.size());
        curStat.setTpsPark(tpsPark.get());
        curStat.setTpsWakeUp(tpsThreadWakeUp.get());
        curStat.setThreadRuns(statLastSec.getThreadRuns());
        curStat.setZTpsThread(statLastSec.getZTpsThread());
        //Сумарная статистика дожна браться за более долгое время, поэтому просто копируем
        //curStat.setSumTimeTpsAvg(statLast.getSumTimeTpsAvg());
        return Util.jsonObjectToString(curStat);
    }

    @Override
    public ThreadBalancerStatisticData flushStatistic() { //Вызывается планировщиком StatisticThreadBalancer для агрегации статистики за секунду
        //Предлагаю ничего не навешивать на статистику, каждый должен заниматься своим делом и выполнять всё как надо
        statLastSec.setThreadBalancerName(getName());
        statLastSec.setTpsIdle(tpsIdle.getAndSet(0));
        statLastSec.setTpsInput(tpsInput.getAndSet(0));
        statLastSec.setTpsOutput(tpsOutput.getAndSet(0));
        statLastSec.setThreadPool(threadList.size());
        statLastSec.setThreadPark(threadParkQueue.size());
        statLastSec.setTpsPark(tpsPark.getAndSet(0));
        statLastSec.setTpsWakeUp(tpsThreadWakeUp.getAndSet(0));
        statLastSec.setTimeTransaction(timeTransactionQueue);
        statLastSec.setThreadRuns(getActiveThreadStatistic());
        statLastSec.setZTpsThread(getTpsPerThread());
        timeTransactionQueue.clear();
        statList.add(statLastSec.clone());
        if (statList.size() > statisticListSize) {
            statList.remove(0);
        }
        //loadPool(getThreadSize() / 4);
        return statLastSec;
    }

    public void loadPool(int maxCount) { //Это стартер, будем половину оживлять только
        //Так как потокам надо время, что бы выйти в режим
        int count = 0;
        while (isActive() && tpsInput.get() < tpsInputMax.get()) {
            if (tpsPark.get() > 0) {
                if (debug) {
                    Util.logConsole(Thread.currentThread(), "LoadPool -> STOP by parking [" + count + "]. Stat: " + getStatisticMomentum());
                }
                break;
            }
            boolean status = wakeUpOnceThreadLast();
            if (status == false) {
                if (debug) {
                    Util.logConsole(Thread.currentThread(), "LoadPool -> STOP by wakeUp [" + count + "]. Stat: " + getStatisticMomentum());
                }
                System.out.println("STOP by wakeUp [" + count + "]. Stat: " + getStatisticMomentum());
                break;
            }
            count++;
            if (count >= maxCount) {
                if (debug) {
                    Util.logConsole(Thread.currentThread(), "LoadPool -> STOP by maxCount [" + count + "]. Stat: " + getStatisticMomentum());
                }
                break;
            }
        }
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

    @Override
    public int setResistance(int prc) { //Установить процент внешнего сопротивления
        return 0;
    }

    @Override
    public void setTestAutoRestoreResistanceTps(boolean status) {
        autoRestoreResistanceTps.set(status);
    }

    @Override
    public void shutdown() throws ShutdownException { //Остановка пула потоков
        Util.logConsole(Thread.currentThread(), "TO SHUTDOWN");
        if (isActive.compareAndSet(true, false)) { //Только один поток будет останавливать
            scheduler.shutdown();
            long startShutdown = System.currentTimeMillis();
            while (threadList.size() > 0) {
                try {
                    WrapThread wrapThread = threadParkQueue.getFirst(); //Замысел такой, что бы выцеплять только заверенные процессы
                    forceRemoveThread(wrapThread);
                } catch (NoSuchElementException e) { //Если нет в отстойнике - подождём немного
                    try {
                        TimeUnit.MILLISECONDS.sleep(100);
                    } catch (InterruptedException e2) {
                        e2.printStackTrace();
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
                if (System.currentTimeMillis() > startShutdown + 30000) { //Пошла жара
                    //Util.logConsole(Thread.currentThread(), "ERROR SHUTDOWN 30 sec -> throw INTERRUPT");
                    new Exception("ERROR SHUTDOWN 30 sec -> throw INTERRUPT").printStackTrace();
                    try {
                        Stream.of(threadList.toArray(new WrapThread[0])).forEach(threadWrap -> { //Боремся за атамарность конкурентных изменений
                            try {
                                threadWrap.getThread().interrupt();
                            } catch (Exception e) {
                                e.printStackTrace();
                            }
                        });
                    } catch (Exception e) { //Край, вилы
                        e.printStackTrace();
                    } finally {
                        threadList.clear();
                    }
                    break;
                }
            }
        } else {
            Util.logConsole(Thread.currentThread(), "Already shutdown in other Thread");
        }
        if (threadList.size() > 0) {
            throw new ShutdownException("ThreadPoolSize: " + threadList.size());
        }
    }

    protected boolean isThreadRemove() {
        return threadList.size() > threadCountMin;
    }

    protected boolean isThreadParkAll() {
        return threadParkQueue.size() > 0 && threadParkQueue.size() == threadList.size();
    }

    protected long schedulerSleepMillis = 333;

    public void configure(String name, int threadCountMin, int threadCountMax, int tpsInputMax, long threadKeepAliveMillis, long schedulerSleepMillis) {
        setTpsInputMax(tpsInputMax);
        if (isActive.compareAndSet(false, true)) {
            this.name = name;
            this.threadCountMin = threadCountMin;
            this.threadCountMax = new AtomicInteger(threadCountMax);
            this.threadKeepAlive = threadKeepAliveMillis;
            this.schedulerSleepMillis = schedulerSleepMillis;
            scheduler = new SchedulerTickImpl(name + "-Scheduler", schedulerSleepMillis);
            scheduler.run(this);
            overclocking(threadCountMin);
        }
    }

    protected boolean isActive() {
        return isActive.get();
    }

    protected boolean wakeUpOnceThreadLast() {
        while (isActive()) { //Хотел тут добавить проверку, что бы последний на паркинге не забирать, но так низя - иначе ThreadBalancer увеличит потоки, когда в паркинге никого не будет => надо правильно рассчитывать то кол-во, которое реально надо разбудить
            WrapThread wrapThread = threadParkQueue.pollLast(); //Всегда забираем с конца, в начале тушаться потоки под нож
            if (wrapThread != null) {
                //Так как последующая операция перед вставкой в очередь - блокировка
                //Надо проверить, что поток припаркован (возможна гонка)
                if (wrapThread.getThread().getState().equals(Thread.State.WAITING)) {
                    try {
                        tpsThreadWakeUp.incrementAndGet();
                        wrapThread.setLastWakeUp(System.currentTimeMillis());
                        LockSupport.unpark(wrapThread.getThread());
                        return true;
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                } else { // Ещё статус не переключился, просто отбрасываем в начала очереди, к тем, кто ждёт ножа
                    threadParkQueue.addFirst(wrapThread);
                }
            } else { //Null - элементы закончились, хватит
                break;
            }
        }
        return false;
    }

    protected int overclocking(int count) {
        if (!isActive() || threadList.size() >= threadCountMax.get()) {
            return 0;
        }
        int countRealAdd = 0;
        if (count > 0) {
            for (int i = 0; i < count; i++) {
                if (addThread()) {
                    countRealAdd++;
                } else {
                    break;
                }
            }
        }
        return countRealAdd;
    }

    protected boolean addThread() {
        if (isActive() && threadList.size() < threadCountMax.get()) {
            final ThreadBalancer self = this;
            final WrapThread wrapThread = new WrapThread();
            wrapThread.setThread(new Thread(() -> {
                while (isActive() && wrapThread.getIsRun().get()) {
                    try {
                        wrapThread.incCountIteration(); // Это для отслеживания, что поток вообще работает
                        tpsIdle.incrementAndGet();
                        iteration(wrapThread, self);
                        //В методе wakeUpOnceThread решена проблема гонки за предварительный старт
                        threadParkQueue.add(wrapThread);
                        tpsPark.incrementAndGet();
                        LockSupport.park();
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
                try {
                    forceRemoveThread(wrapThread);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }));
            wrapThread.getThread().setName(getName() + "-" + threadNameCounter.getAndIncrement());
            threadList.add(wrapThread);
            wrapThread.getThread().start();
            return true;
        }
        return false;
    }

    protected void removeThread(WrapThread wrapThread) {
        //Кол-во неприкасаемых потоков, которые должно быть
        if (threadList.size() > threadCountMin) {
            forceRemoveThread(wrapThread);
        }
    }

    synchronized private void forceRemoveThread(WrapThread wrapThread) { //Этот метод может загасит сервис до конца, используйте обычный removeThread
        WrapThread curWrapThread = wrapThread != null ? wrapThread : threadList.get(0);
        if (curWrapThread != null) {
            int count = 0;
            while (true) {
                count++;
                if (count > 3) {
                    break;
                }
                try {
                    curWrapThread.getIsRun().set(false);
                    LockSupport.unpark(curWrapThread.getThread()); //Мы его оживляем, что бы он закончился
                    threadList.remove(curWrapThread);
                    threadParkQueue.remove(curWrapThread); // На всякий случай
                    if (debug) {
                        Util.logConsole(Thread.currentThread(), "removeThread: " + curWrapThread);
                    }
                    break;
                } catch (Exception e) {
                    e.printStackTrace();
                }
                try {
                    TimeUnit.MILLISECONDS.sleep(333);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    protected void checkKeepAliveAndRemoveThread() { //Проверка ждунов, что они давно не вызывались и у них кол-во итераций равно 0 -> нож
        try {
            final long now = System.currentTimeMillis();
            final AtomicInteger maxCounterRemove = new AtomicInteger(formulaRemoveCountThread.apply(1));
            Util.forEach(WrapThread.toArrayWrapThread(threadList), (wrapThread) -> {
                long future = wrapThread.getLastWakeUp() + threadKeepAlive;
                //Время последнего оживления превысило keepAlive + поток реально не работал
                if (now > future && wrapThread.getCountIteration().get() == 0 && maxCounterRemove.getAndDecrement() > 0) {
                    removeThread(wrapThread);
                } else {
                    wrapThread.getCountIteration().set(0);
                }
                return null;
            });
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
