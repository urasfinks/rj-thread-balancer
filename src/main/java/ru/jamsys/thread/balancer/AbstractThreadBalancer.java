package ru.jamsys.thread.balancer;

import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import org.springframework.lang.Nullable;
import ru.jamsys.Util;
import ru.jamsys.message.Message;
import ru.jamsys.message.MessageHandle;
import ru.jamsys.scheduler.SchedulerTick;
import ru.jamsys.scheduler.TickScheduler;
import ru.jamsys.thread.balancer.exception.ShutdownException;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.List;
import java.util.NoSuchElementException;
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
    protected Function<Integer, Integer> formulaAddCountThread = (need) -> need;

    @Setter
    @NonNull
    protected Supplier<Message> supplier = () -> null;

    @Setter
    @NonNull
    protected Consumer<Message> consumer = (msg) -> {
    };

    @Getter
    protected String name;

    private int threadCountMin;
    private AtomicInteger threadCountMax; //Я подумал, что неплохо в рантайме управлять
    private long threadKeepAlive;

    @Getter
    private AtomicInteger tpsInputMax = new AtomicInteger(1);

    @Setter
    @Getter
    private final AtomicInteger resistancePercent = new AtomicInteger(0);

    private final AtomicInteger threadNameCounter = new AtomicInteger(0);

    private final AtomicBoolean isActive = new AtomicBoolean(false);
    private final List<WrapThread> threadList = new CopyOnWriteArrayList<>();
    private final ConcurrentLinkedDeque<WrapThread> threadParkQueue = new ConcurrentLinkedDeque<>();

    private final AtomicInteger tpsIdle = new AtomicInteger(0);
    private final AtomicInteger tpsInput = new AtomicInteger(0);
    private final AtomicInteger tpsOutput = new AtomicInteger(0);

    private volatile ThreadBalancerStatistic statLast = new ThreadBalancerStatistic();

    private final ConcurrentLinkedDeque<Long> timeTransactionQueue = new ConcurrentLinkedDeque<>();

    private TickScheduler scheduler;

    @Override
    @Nullable
    public ThreadBalancerStatistic getStatisticLastClone() {
        return statLast.clone();
    }

    @Override
    public void iteration(WrapThread wrapThread, ThreadBalancer service) {
        if (supplier != null) {
            while (isActive() && wrapThread.getIsRun().get() && !isLimitTpsInputOverflow()) {
                long startTime = System.currentTimeMillis();
                Message message = supplier.get();
                if (message != null) {
                    tpsInput.incrementAndGet();
                    message.onHandle(MessageHandle.CREATE, this);
                    consumer.accept(message);
                    timeTransactionQueue.add(System.currentTimeMillis() - startTime);
                    tpsOutput.incrementAndGet();
                } else {
                    break;
                }
            }
        } else {
            Util.logConsole(Thread.currentThread(), "Supplier is null");
        }
    }

    @Override
    public void threadStabilizer() {
        Util.logConsole(Thread.currentThread(), "threadStabilizer()");
        try {
            ThreadBalancerStatistic stat = getStatisticMomentum();
            if (stat != null) {
                if (getThreadParkQueueSize() == 0) {//В очереди нет ждунов, значит все трудятся, накинем ещё
                    int needCountThread = formulaAddCountThread.apply(getNeedCountThreadRelease(stat, true));
                    int addThreadCount = overclocking(needCountThread);
                    if (debug) {
                        Util.logConsole(Thread.currentThread(), "AddThread: " + addThreadCount);
                    }
                } else if (isThreadRemove(stat)) { //Кол-во потоков больше минимума
                    checkKeepAliveAndRemoveThread();
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        if (autoRestoreResistanceTps.get() && getResistancePercent().get() > 0) {
            getResistancePercent().decrementAndGet();
        }
    }

    @Override
    public void tick() { //Сколько надо пробудить потоков
        if (isActive()) {
            ThreadBalancerStatistic stat = getStatisticMomentum();
            int needThreadCount = Math.min(getNeedCountThreadRelease(stat, false), stat.getThreadCountPark());
            if (needThreadCount > 0) {
                for (int i = 0; i < needThreadCount; i++) {
                    wakeUpOnceThread();
                }
            } else if (isThreadParkAll()) {
                wakeUpOnceThread();
            }
        }
    }

    @Nullable
    public ThreadBalancerStatistic getStatisticMomentum() {
        ThreadBalancerStatistic curStat = new ThreadBalancerStatistic();
        curStat.setServiceName(getName());
        curStat.setTpsIdle(tpsIdle.get());
        curStat.setTpsInput(tpsInput.get());
        curStat.setTpsOutput(tpsOutput.get());
        curStat.setThreadCount(threadList.size());
        curStat.setThreadCountPark(threadParkQueue.size());
        //Сумарная статистика дожна браться за более долгое время, поэтому просто копируем
        curStat.setSumTimeTpsAvg(statLast.getSumTimeTpsAvg());
        return curStat;
    }

    @Override
    public void setTpsInputMax(int max) {
        tpsInputMax.set(max);
    }

    @Override
    public ThreadBalancerStatistic flushStatistic() {
        statLast.setServiceName(getName());
        statLast.setTpsIdle(tpsIdle.getAndSet(0));
        statLast.setTpsInput(tpsInput.getAndSet(0));
        statLast.setTpsOutput(tpsOutput.getAndSet(0));
        statLast.setThreadCount(threadList.size());
        statLast.setThreadCountPark(threadParkQueue.size());
        statLast.setTimeTransaction(timeTransactionQueue);
        timeTransactionQueue.clear();
        return statLast;
    }

    @Override
    public int setResistance(int prc) { //Отноcится только к Supplier
        //Для того, что бы не надо было реализовывать в Consumer
        return 0;
    }

    protected AtomicBoolean autoRestoreResistanceTps = new AtomicBoolean(true); //Отночиться только к Supplier

    @Override
    public void setTestAutoRestoreResistanceTps(boolean status) { //Отночиться только к Supplier
        autoRestoreResistanceTps.set(status);
    }

    @Override
    public void shutdown() throws ShutdownException {
        Util.logConsole(Thread.currentThread(), "TO SHUTDOWN");
        scheduler.shutdown();
        if (isActive.compareAndSet(true, false)) { //Только один поток будет останавливать
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
                        Stream.of(threadList.toArray(new WrapThread[0])).forEach(threadWrap -> { //Боремся за атамарность конкурентны изменений
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

    protected boolean isThreadRemove(@NonNull ThreadBalancerStatistic stat) {
        return stat.getThreadCount() > threadCountMin;
    }

    protected boolean isThreadParkAll() {
        return threadParkQueue.size() > 0 && threadParkQueue.size() == threadList.size();
    }

    protected int getThreadParkQueueSize() {
        return threadParkQueue.size();
    }

    public void configure(String name, int threadCountMin, int threadCountMax, int tpsInputMax, long threadKeepAliveMillis, long schedulerSleepMillis) {
        setTpsInputMax(tpsInputMax);
        if (isActive.compareAndSet(false, true)) {
            this.name = name;
            this.threadCountMin = threadCountMin;
            this.threadCountMax = new AtomicInteger(threadCountMax);
            this.threadKeepAlive = threadKeepAliveMillis;

            scheduler = new TickScheduler(name + "-Scheduler", schedulerSleepMillis);
            scheduler.run(this);
            overclocking(threadCountMin);
        }
    }

    protected boolean isLimitTpsInputOverflow() {
        return tpsInput.get() >= tpsInputMax.get();
    }

    protected boolean isActive() {
        return isActive.get();
    }

    protected void wakeUpOnceThread() {
        while (true) {
            WrapThread wrapThread = threadParkQueue.pollLast(); //Всегда забираем с конца, в начале тушаться потоки под нож
            if (wrapThread != null) {
                //Так как последующая операция перед вставкой в очередь - блокировка
                //Надо проверить, что поток припаркован (возможна гонка)
                if (wrapThread.getThread().getState().equals(Thread.State.WAITING)) {
                    try {
                        wrapThread.setLastWakeUp(System.currentTimeMillis());
                        LockSupport.unpark(wrapThread.getThread());
                        break;
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
            wrapThread.getThread().start();
            threadList.add(wrapThread);
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
            final AtomicInteger c = new AtomicInteger(1);
            Util.forEach(WrapThread.toArrayWrapThread(threadList), (wrapThread) -> {
                long future = wrapThread.getLastWakeUp() + threadKeepAlive;
                //Время последнего оживления превысило keepAlive + поток реально не работал
                if (now > future && wrapThread.getCountIteration().get() == 0 && c.getAndDecrement() > 0) {
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

    public static int getNeedCountThreadByTransaction(@NonNull ThreadBalancerStatistic stat, int needTransaction, boolean debug, boolean create) {
        if (needTransaction <= 0) {
            return 0;
        }
        int needThread = create ? needTransaction : stat.getThreadCountPark();
        BigDecimal threadTps = null;
        if (needTransaction > 0) {
            // Может возникнуть такая ситуация, когда за 1 секунду не будет собрана статистика
            if (stat.getSumTimeTpsAvg() > 0) {
                try {
                    threadTps = new BigDecimal(1000)
                            .divide(BigDecimal.valueOf(stat.getSumTimeTpsAvg()), 2, RoundingMode.HALF_UP);

                    if (threadTps.doubleValue() > 0.0) {
                        needThread = new BigDecimal(needTransaction)
                                .divide(threadTps, 2, RoundingMode.HALF_UP)
                                .setScale(0, RoundingMode.CEILING)
                                .intValue();
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
        if (debug) {
            Util.logConsole(Thread.currentThread(), (create ? "CREATE" : "TICK") + ": " + needThread + " => needTransaction: " + needTransaction + "; threadTps: " + threadTps + "; Statistic: " + Util.jsonObjectToString(stat));
        }
        if (create) {
            if (stat.getThreadCountPark() >= needThread) { //Если припаркованных больше, чем требуется, просто вернём 0
                return 0;
            } else { //Если необходимо больше, чем припаркованных, вернём разницу, которая необходима
                return needThread - stat.getThreadCountPark();
            }
        } else {
            return needThread;
        }
    }

}
