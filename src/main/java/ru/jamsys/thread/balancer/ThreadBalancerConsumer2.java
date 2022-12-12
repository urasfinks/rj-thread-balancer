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
public class ThreadBalancerConsumer2 extends AbstractThreadBalancer {

    private final ConcurrentLinkedDeque<Message> queueTask = new ConcurrentLinkedDeque<>();

    @Override
    public void configure(String name, int threadCountMin, int threadCountMax, int tpsInputMax, long threadKeepAliveMillis, long schedulerSleepMillis) {
        setSupplier(() -> {
            Message message = queueTask.pollLast();
            return message;
        });
        super.configure(name, threadCountMin, threadCountMax, tpsInputMax, threadKeepAliveMillis, schedulerSleepMillis);
    }

    @Override
    public int getNeedCountThreadRelease(ThreadBalancerStatistic stat) {
        return getNeedCountThread(stat, queueTask.size(), debug);
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
}
