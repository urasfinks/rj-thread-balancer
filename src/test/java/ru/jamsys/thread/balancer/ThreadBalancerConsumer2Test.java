package ru.jamsys.thread.balancer;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.boot.SpringApplication;
import org.springframework.context.ConfigurableApplicationContext;
import ru.jamsys.App;
import ru.jamsys.Util;
import ru.jamsys.UtilTest;
import ru.jamsys.component.ThreadBalancerFactory;
import ru.jamsys.message.Message;
import ru.jamsys.message.MessageImpl;
import ru.jamsys.thread.balancer.exception.ShutdownException;
import ru.jamsys.thread.balancer.exception.TpsOverflowException;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.*;

class ThreadBalancerConsumer2Test {
    static ConfigurableApplicationContext context;

    @BeforeAll
    static void beforeAll() {
        String[] args = new String[]{};
        context = SpringApplication.run(App.class, args);
        App.initContext(context, true);
    }

    @Test
    void firstStart() { //Проверяем разгон потоков под рост задач
        run(1, 5, 60000L, 2, 10, 5, 500, clone ->
                Assertions.assertEquals(5, clone.getThreadCount(), "Кол-во потоков должно быть 5")
        );
    }

    void run(int countThreadMin, int countThreadMax, long keepAlive, int countIteration, int countMessage, int sleep, int tpsInputMax, Consumer<ThreadBalancerStatistic> fnExpected) {
        Util.logConsole(Thread.currentThread(), "Start test");
        AtomicInteger serviceHandleCounter = new AtomicInteger(0);

        ThreadBalancerConsumer2 test = context.getBean(ThreadBalancerFactory.class).createConsumer2("Test", countThreadMin, countThreadMax, tpsInputMax, keepAlive, 333);
        test.setConsumer((msg) -> {
            //Util.logConsole(Thread.currentThread(), "Halomka");
            try {
                Thread.sleep(1000);
            } catch (Exception e) {
                e.printStackTrace();
            }
            serviceHandleCounter.incrementAndGet();
            //Util.logConsole("[" + c.incrementAndGet() + "] " + msg.getCorrelation());
        });

        test.setDebug(true);
        test.setTpsInputMax(tpsInputMax);
        Util.logConsole(Thread.currentThread(), "Init Bean");

        AtomicInteger realInsert = new AtomicInteger(0);

        Thread t1 = new Thread(() -> {
            int count = 0;
            Util.logConsole(Thread.currentThread(), "Run task thread");
            while (true) {
                count++;
                if (count == countIteration + 1) {
                    break;
                }
                List<Long> avgTime = new ArrayList<>();
                for (int i = 0; i < countMessage; i++) {
                    Message message = new MessageImpl();
                    long startTime = System.currentTimeMillis();
                    try {
                        test.accept(message);
                        realInsert.incrementAndGet();
                    } catch (ShutdownException | TpsOverflowException e) {
                        Util.logConsole(Thread.currentThread(), e.toString());
                    }
                    avgTime.add(System.currentTimeMillis() - startTime);
                }
                Util.logConsole(Thread.currentThread(), "Task insert: " + avgTime.stream().mapToLong(Long::longValue).summaryStatistics().toString());
                try {
                    TimeUnit.MILLISECONDS.sleep(5000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });
        t1.start();
        Util.logConsole(Thread.currentThread(), "Init task thread");
        UtilTest.sleepSec(sleep);
        Assertions.assertEquals(realInsert.get(), serviceHandleCounter.get(), "Не все задачи были обработаны");
        ThreadBalancerStatistic clone = test.getStatisticLastClone();
        Util.logConsole(Thread.currentThread(), "LAST STAT: " + clone);
        if (clone != null) {
            fnExpected.accept(clone);
        }
        context.getBean(ThreadBalancerFactory.class).shutdown("Test");
    }
}