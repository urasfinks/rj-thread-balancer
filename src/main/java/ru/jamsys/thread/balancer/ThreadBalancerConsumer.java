package ru.jamsys.thread.balancer;

import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;
import ru.jamsys.message.Message;
import ru.jamsys.message.MessageHandle;
import ru.jamsys.thread.balancer.exception.ShutdownException;
import ru.jamsys.thread.balancer.exception.TpsOverflowException;

import java.util.concurrent.ConcurrentLinkedDeque;

@Component
@Scope("prototype")
public class ThreadBalancerConsumer extends AbstractThreadBalancer {

    private final ConcurrentLinkedDeque<Message> queueTask = new ConcurrentLinkedDeque<>();

    @Override
    public void configure(String name, int threadCountMin, int threadCountMax, int tpsInputMax, long threadKeepAliveMillis, long schedulerSleepMillis) {
        setSupplier(queueTask::pollLast);
        super.configure(name, threadCountMin, threadCountMax, tpsInputMax, threadKeepAliveMillis, schedulerSleepMillis);
    }

    public void accept(Message message) throws ShutdownException, TpsOverflowException {
        if (!isActive()) {
            throw new ShutdownException("Consumer shutdown");
        }
        queueTask.add(message);
        message.onHandle(MessageHandle.PUT, this);
    }

    @Override
    public void tick() { //Вызывается планировщиком этого балансировщика для пробуждения припаркованнных потоков
        if (isActive()) {
            int needThreadCount = Math.min(queueTask.size(), threadParkQueue.size());
            if (needThreadCount > 0) {
                for (int i = 0; i < needThreadCount; i++) {
                    wakeUpOnceThreadLast();
                }
            } else if (isThreadParkAll()) {
                wakeUpOnceThreadLast();
            }
        }
    }

    @Override
    public void iteration(WrapThread wrapThread, ThreadBalancer threadBalancer) { //Это то, что выполняется в каждом потоке пула балансировки
        while (isIteration(wrapThread)) {
            wrapThread.incCountIteration();
            long startTime = System.currentTimeMillis();
            Message message = supplier.get();
            if (message != null) {
                tpsInput.incrementAndGet();
                consumer.accept(message);
                timeTransactionQueue.add(System.currentTimeMillis() - startTime);
                tpsOutput.incrementAndGet();
            } else {
                break;
            }
        }
    }

    @Override
    public boolean isAddThreadCondition() {
        return queueTask.size() > 0;
    }

}
