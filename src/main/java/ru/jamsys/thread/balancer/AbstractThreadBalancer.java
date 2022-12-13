package ru.jamsys.thread.balancer;

import lombok.NonNull;
import lombok.Setter;
import ru.jamsys.Util;
import ru.jamsys.message.Message;
import ru.jamsys.message.MessageHandle;
import ru.jamsys.scheduler.SchedulerTick;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

public abstract class AbstractThreadBalancer extends AbstractThreadBalancerShare implements SchedulerTick {

    public void configure(String name, int threadCountMin, int threadCountMax, int tpsInputMax, long threadKeepAliveMillis, long schedulerSleepMillis) {
        setTpsInputMax(tpsInputMax);
        super.configure(name, threadCountMin, threadCountMax, threadKeepAliveMillis, schedulerSleepMillis);
    }

    @Setter
    protected Supplier<Message> supplier = null;

    @Setter
    protected Consumer<Message> consumer = null;

    @Override
    public void tick() { //Сколько надо пробудить потоков
        if (isActive()) {
            ThreadBalancerStatistic stat = getStatisticMomentum();
            //int needTransaction = input ? (getTpsInputMax().get() - stat.getTpsInput()) : (queueTask.size());
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

    @Setter
    protected Function<Integer, Integer> formulaAddCountThread = (need) -> need;

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
    public void iteration(WrapThread wrapThread, ThreadBalancer service) {
        if (supplier != null) {
            while (isActive() && wrapThread.getIsRun().get() && !isLimitTpsInputOverflow()) {
                incTpsInput();
                long startTime = System.currentTimeMillis();
                Message message = supplier.get();
                if (message != null) {
                    incTpsOutput(System.currentTimeMillis() - startTime);
                    message.onHandle(MessageHandle.CREATE, this);
                    if (consumer != null) {
                        consumer.accept(message);
                    }
                } else {
                    break;
                }
            }
        } else {
            Util.logConsole(Thread.currentThread(), "Supplier is null");
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
