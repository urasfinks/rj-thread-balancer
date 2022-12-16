package ru.jamsys.thread.balancer;

import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;
import ru.jamsys.message.Message;
import ru.jamsys.message.MessageHandle;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.concurrent.atomic.AtomicBoolean;

@Component
@Scope("prototype")
public class ThreadBalancerSupplier extends AbstractThreadBalancer {

    AtomicBoolean onceTick = new AtomicBoolean(true);

    @Override
    public void threadStabilizer() {
        onceTick.set(true);
        super.threadStabilizer();
    }

    @Override
    public void tick() {
        //Были добавлены потоки, потому что балансировщик потоков в какое-то время не справлялся
        //А потом эти потоки могут переходить в паркинг, потому что не очень то и нужны были
        //Наша задача делать это плавно, допустим перешли на паркинг 200 потоков, 180 на текущей итарации снова попробуем запустить, а 20 путь отдыхают и ждут ножа
        if (onceTick.compareAndSet(true, false)) {
            if (statLastSec.getTpsPark() > 0) {
                int needThread = new BigDecimal(statLastSec.getTpsPark())
                        .divide(new BigDecimal(1.1), 2, RoundingMode.HALF_UP)
                        .setScale(0, RoundingMode.CEILING)
                        .intValue();
                for (int i = 0; i < needThread; i++) {
                    if (!wakeUpOnceThreadLast()) {
                        break;
                    }
                }
                return;
            }
            wakeUpOnceThreadLast();
        }

    }

    @Override
    public void iteration(WrapThread wrapThread, ThreadBalancer threadBalancer) { //Это то, что выполняется в каждом потоке пула балансировки
        while (isIteration(wrapThread)) {
            wrapThread.incCountIteration();
            long startTime = System.currentTimeMillis();
            tpsInput.incrementAndGet(); //Короче оно должно быть тут для supplier точно
            Message message = supplier.get();
            if (message != null) {
                message.onHandle(MessageHandle.CREATE, this);
                consumer.accept(message);
                timeTransactionQueue.add(System.currentTimeMillis() - startTime);
                tpsOutput.incrementAndGet();
            } else {
                break;
            }
        }
    }

    @Override
    public boolean isAddThreadCondition() { //В очереди нет ждунов, значит все трудятся, накинем ещё
        return threadParkQueue.size() == 0;
    }
}
