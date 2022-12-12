package ru.jamsys.thread.balancer;

import lombok.NonNull;
import lombok.Setter;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;
import ru.jamsys.Util;
import ru.jamsys.message.Message;
import ru.jamsys.message.MessageHandle;
import ru.jamsys.scheduler.SchedulerTick;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

@Component
@Scope("prototype")
public class ThreadBalancerSupplier extends AbstractThreadBalancer implements SchedulerTick {

    private Supplier<Message> supplier;
    private Consumer<Message> consumer;

    private ConcurrentLinkedQueue<Integer> queueResistance = new ConcurrentLinkedQueue();

    public void configure(String name, int threadCountMin, int threadCountMax, long threadKeepAliveMillis, long schedulerSleepMillis, Supplier<Message> supplier, Consumer<Message> consumer) {
        this.supplier = supplier;
        this.consumer = consumer;
        super.configure(name, threadCountMin, threadCountMax, threadKeepAliveMillis, schedulerSleepMillis);
    }

    @Setter
    Function<Integer, Integer> formulaAddCountThread = (y) -> y;

    @Override
    public void threadStabilizer() {
        try {
            ThreadBalancerStatistic stat = getStatisticLastClone();
            if (stat != null) {
                if (getThreadParkQueueSize() == 0) {//В очереди нет ждунов, значит все трудятся, накинем ещё
                    int needCountThread = formulaAddCountThread.apply(getThreadListSize());
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
    public void tick() {
        //Чистим очередь сопротивления, что бы проще было считать максимальное показание
        //Очередь была сделана, так как несколько потоков могут паралельно закидывать информацию с просьбой уменьшить tps
        try {
            queueResistance.clear();
        } catch (Exception e) {
            e.printStackTrace();
        }
        //При маленькой нагрузке дёргаем всегда последний тред, что бы не было простоев
        //Далее раскрутку оставляем на откуп стабилизатору
        if (isActive()) {
            ThreadBalancerStatistic stat = getStatisticMomentum();
            int diffTpsInput = getNeedCountThread(stat, getTpsInputMax().get(), debug);
            if (isThreadParkAll()) {
                wakeUpOnceThread();
            } else if (diffTpsInput > 0) {
                for (int i = 0; i < diffTpsInput; i++) {
                    wakeUpOnceThread();
                }
            }
        }
    }

    @Override
    public void iteration(WrapThread wrapThread, ThreadBalancer threadBalancer) {
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
    }

    public static int getNeedCountThread(@NonNull ThreadBalancerStatistic stat, int tpsInputMax, boolean debug) {
        int needTransaction = tpsInputMax - stat.getTpsInput();
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
                        needThread = Math.min(needThread, stat.getThreadCountPark()); //Если необходимое число транзакций больше чем кол-во припаркованныех потоков
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
            Util.logConsole(Thread.currentThread(), "getNeedCountThreadSupplier: needTransaction: " + needTransaction + "; threadTps: " + threadTps + "; needThread: " + needThread + "; " + stat);
        }
        return needThread;
    }

    @Override
    public int setResistance(int prc) { //Закидывается процент торможения, изначально он 0
        /*
         * Немного предистории:
         * В целом всю систему надо рассчитывать как посредника, есть INPUT нагрузка и OUTPUT разгрузка
         * Всё что мы взяли на себя - надо выполнить
         * INPUT и OUTPUT работают в своих собственных сбалансированных пулах, может случиться, что OUTPUT начнёт умирать
         * Поэтому OUTPUT будет трубить всем свом INPUT, что он не успевает, что бы они уменьшили свой tps
         * Если OUTPUT моментально восстанавливаться, предусмотрено служебное восстановление (вычитание 1% за tick)
         * */
        AtomicInteger resistancePercent = getResistancePercent();

        queueResistance.add(prc);
        try {
            resistancePercent.set(queueResistance.stream().reduce(Integer::max).orElse(0));
        } catch (Exception e) {
            e.printStackTrace();
        }
        return resistancePercent.get();
    }
}
