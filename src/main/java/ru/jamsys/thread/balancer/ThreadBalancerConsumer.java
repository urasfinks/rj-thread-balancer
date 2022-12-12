package ru.jamsys.thread.balancer;

import lombok.NonNull;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;
import ru.jamsys.Util;
import ru.jamsys.message.Message;
import ru.jamsys.message.MessageHandle;
import ru.jamsys.scheduler.SchedulerTick;
import ru.jamsys.thread.balancer.exception.ShutdownException;
import ru.jamsys.thread.balancer.exception.TpsOverflowException;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.function.Consumer;

@Component
@Scope("prototype")
public class ThreadBalancerConsumer extends AbstractThreadBalancerShare implements SchedulerTick {

    private Consumer<Message> consumer;
    private final ConcurrentLinkedDeque<Message> queueTask = new ConcurrentLinkedDeque<>();

    public void configure(String name, int threadCountMin, int threadCountMax, long threadKeepAliveMillis, long schedulerSleepMillis, Consumer<Message> consumer) {
        this.consumer = consumer;
        super.configure(name, threadCountMin, threadCountMax, threadKeepAliveMillis, schedulerSleepMillis);
    }

    @Override
    public void tick() {
        //Сколько задач в очереди, столько и будем будить потоков
        if (isActive()) {
            for (int i = 0; i < queueTask.size(); i++) {
                wakeUpOnceThread();
            }
        }
    }

    public void accept(Message message) throws ShutdownException, TpsOverflowException {
        if (!isActive()) {
            throw new ShutdownException("Consumer shutdown");
        }
        if (isLimitTpsInputOverflow()) {
            throw new TpsOverflowException("Max tps: " + getTpsInputMax().get());
        }
        queueTask.add(message);
        incTpsInput();
        message.onHandle(MessageHandle.PUT, this);
    }

    @Override
    public void iteration(WrapThread wrapThread, ThreadBalancer threadBalancer) {
        while (isActive() && !queueTask.isEmpty() && wrapThread.getIsRun().get()) { //Всегда проверяем, что поток не выводят из эксплуатации
            Message message = queueTask.pollLast();
            if (message != null) {
                long startTime = System.currentTimeMillis();
                message.onHandle(MessageHandle.EXECUTE, threadBalancer);
                try {
                    incTpsOutput(System.currentTimeMillis() - startTime);
                    consumer.accept(message);
                    message.onHandle(MessageHandle.COMPLETE, threadBalancer);
                } catch (Exception e) {
                    message.setError(e);
                }
            }
        }
    }

    @Override
    public int getNeedCountThreadRelease(ThreadBalancerStatistic stat) {
        return 0;
    }

    @Override
    public ThreadBalancerStatistic flushStatistic() {
        getStatisticLast().setQueueSize(queueTask.size());
        return super.flushStatistic();
    }

    @Override
    public void threadStabilizer() {
        try {
            ThreadBalancerStatistic stat = getStatisticLastClone();
            if (stat != null) {
                if (debug) {
                    Util.logConsole(Thread.currentThread(), "QueueSize: " + stat.getQueueSize() + "; CountThread: " + stat.getThreadCount());
                }
                if (stat.getQueueSize() > 0) { //Если очередь наполнена
                    //Расчет необходимого кол-ва потоков, что бы обработать всю очередь
                    int needCountThread = getNeedCountThread(stat, debug);
                    if (needCountThread > 0 && isThreadAdd()) {
                        if (debug) {
                            Util.logConsole(Thread.currentThread(), "addThread: " + needCountThread);
                        }
                        overclocking(needCountThread);
                    }
                } else if (isThreadRemove(stat)) { //нет необходимости удалять, когда потоков заявленный минимум
                    checkKeepAliveAndRemoveThread();
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static int getNeedCountThread(@NonNull ThreadBalancerStatistic stat, boolean debug) {
        int needThread = 0;
        BigDecimal threadTps = null;
        try {
            threadTps = new BigDecimal(Math.max(stat.getTpsOutput(), 1)) //Если все потоки встали и не один не отдал ни одного tps схватим / by zero
                    .divide(new BigDecimal(Math.max(stat.getThreadCount(), 1)), 2, RoundingMode.HALF_UP);
            //Может случится такое, что потоки встанут на длительную работу или перестанут работать из-за отсутсвия задач
            //И средняя по транзакция будет равна 0

            if (threadTps.doubleValue() == 0.0) {
                if (stat.getQueueSize() > 0) { //Задачи есть, вернём сколько потоков, столько и задач
                    needThread = stat.getQueueSize();
                }
            } else {
                needThread = new BigDecimal(stat.getQueueSize())
                        .divide(threadTps, 2, RoundingMode.HALF_UP)
                        .setScale(0, RoundingMode.CEILING)
                        .intValue();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        if (debug) {
            Util.logConsole(Thread.currentThread(), "getNeedCountThreadConsumer: queueSize: " + stat.getQueueSize() + "; threadTps: " + threadTps + "; needThread: " + needThread + "; " + stat);
        }
        return needThread;
    }

}
