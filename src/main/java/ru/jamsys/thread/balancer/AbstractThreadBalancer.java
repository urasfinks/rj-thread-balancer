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
            int needThreadCount = Math.min(getNeedCountThreadRelease(stat), stat.getThreadCountPark());
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
        try {
            ThreadBalancerStatistic stat = getStatisticLastClone();
            if (stat != null) {
                if (getThreadParkQueueSize() == 0) {//В очереди нет ждунов, значит все трудятся, накинем ещё
                    int needCountThread = formulaAddCountThread.apply(getNeedCountThreadRelease(stat));
                    if (debug) {
                        Util.logConsole(Thread.currentThread(), "AddThread: " + needCountThread);
                    }
                    overclocking(needCountThread);
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

    public static int getNeedCountThread(@NonNull ThreadBalancerStatistic stat, int needTransaction, boolean debug) {
        //int needTransaction = tpsInputMax - stat.getTpsInput();
        int needThread = 0;
        BigDecimal threadTps = null;
        if (needTransaction > 0) {
            // Может возникнуть такая ситуация, когда за 1 секунду не будет собрана статистика
            if (stat.getSumTimeTpsAvg() > 0) {
                try {
                    threadTps = new BigDecimal(1000)
                            .divide(BigDecimal.valueOf(stat.getSumTimeTpsAvg()), 2, RoundingMode.HALF_UP);

                    if (threadTps.doubleValue() == 0.0) {
                        //Может случится такое, что потоки встанут на длительную работу или это просто начало
                        //И средняя по транзакция будет равна 0
                        //Пока думаю, освежу всех в отстойние)
                        needThread = stat.getThreadCountPark();
                    } else {
                        needThread = new BigDecimal(needTransaction)
                                .divide(threadTps, 2, RoundingMode.HALF_UP)
                                .setScale(0, RoundingMode.CEILING)
                                .intValue();
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            } else {
                // getSumTimeTpsAvg = 0 => / zero, если нет статистики значит, все потоки встали на одну транзакцию
                // Но могут быть запаркованные с предыдущей операции
                needThread = stat.getThreadCountPark();
            }
        }
        if (debug) {
            Util.logConsole(Thread.currentThread(), "getNeedCountThread: needTransaction: " + needTransaction + "; threadTps: " + threadTps + "; needThread: " + needThread + "; " + stat);
        }
        return needThread;
    }

}
