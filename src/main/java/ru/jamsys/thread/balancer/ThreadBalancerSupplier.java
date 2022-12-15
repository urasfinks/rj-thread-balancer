package ru.jamsys.thread.balancer;

import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;
import ru.jamsys.Util;
import ru.jamsys.message.Message;
import ru.jamsys.message.MessageHandle;

import java.math.BigDecimal;

@Component
@Scope("prototype")
public class ThreadBalancerSupplier extends AbstractThreadBalancer {

    @Override
    public void tick() {
        //System.out.println("TICK");
        //Мысль такая, пока есть слоты по inputTps начинать сгружать припаркованные thread до тех пор, пока очередь паркинга не истощится либо не станет расти
        int max = getThreadSize() / (1000 / (int) schedulerSleepMillis);

        int count = 0;
        while (tpsInput.get() < tpsInputMax.get()) {
            int parkSize = threadParkQueue.size();

            if(!wakeUpOnceThreadLast() || threadParkQueue.size() >= parkSize){
                break;
            }
            count++;
            if(count > max){
                break;
            }
        }
        //System.out.println("TICK: " + count);
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
    public boolean isAddThreadCondition() {
        return threadParkQueue.size() == 0;
    }
}
