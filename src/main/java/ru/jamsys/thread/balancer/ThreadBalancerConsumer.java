package ru.jamsys.thread.balancer;

import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;
import ru.jamsys.message.Message;
import ru.jamsys.message.MessageHandle;
import ru.jamsys.thread.balancer.exception.ShutdownException;
import ru.jamsys.thread.balancer.exception.TpsOverflowException;

import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.AtomicInteger;

@Component
@Scope("prototype")
public class ThreadBalancerConsumer extends AbstractThreadBalancer {

    private final ConcurrentLinkedDeque<Message> queueTask = new ConcurrentLinkedDeque<>();
    AtomicInteger consumerInputTps = new AtomicInteger(0);

    @Override
    public void configure(String name, int threadCountMin, int threadCountMax, int tpsInputMax, long threadKeepAliveMillis, long schedulerSleepMillis) {
        setSupplier(() -> {
            Message message = queueTask.pollLast();
            return message;
        });
        super.configure(name, threadCountMin, threadCountMax, tpsInputMax, threadKeepAliveMillis, schedulerSleepMillis);
    }

    @Override
    public int getNeedCountThreadRelease(ThreadBalancerStatistic stat, boolean create) {
        return getNeedCountThreadByTransaction(stat, queueTask.size(), debug, create);
    }

    public void accept(Message message) throws ShutdownException, TpsOverflowException {
        if (!isActive()) {
            throw new ShutdownException("Consumer shutdown");
        }
        if (consumerInputTps.get() > getTpsInputMax().get()) {
            throw new TpsOverflowException("Max tps: " + getTpsInputMax().get());
        }
        consumerInputTps.incrementAndGet();
        queueTask.add(message);
        message.onHandle(MessageHandle.PUT, this);
    }
}
