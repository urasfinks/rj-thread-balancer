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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

class ThreadBalancerConsumerTest {

    /*static ConfigurableApplicationContext context;

    @BeforeAll
    static void beforeAll() {
        String[] args = new String[]{};
        context = SpringApplication.run(App.class, args);
        App.initContext(context, true);
    }

    @Test
    void overclocking() { //Проверяем разгон потоков под рост задач
        run(1, 5, 60000L, 1, 10, 5, 500, clone ->
                Assertions.assertEquals(5, clone.getThreadPool(), "Кол-во потоков должно быть 5")
        );
    }

    @Test
    void damping() { //Проверяем удаление потоков после ненадобности
        run(5, 10, 2000L, 2, 15, 15, 500, clone ->
                Assertions.assertEquals(5, clone.getThreadPool(), "Должен остаться только 5 потоков")
        );
    }

    @Test
    void timeout() { //Проверяем время жизни потоков, после теста они должны все статься
        run(1, 5, 18000L, 1, 5, 5, 500, clone ->
                Assertions.assertTrue(clone.getThreadPool() == 5, "Кол-во потокв дожно быть больше одного")
        );
    }

    @Test
    void summaryCount() { //Проверяем, что сообщения все обработаны при большом кол-ве потоков
        run(1, 1000, 16000L, 1, 5000, 13, 1000, clone ->
                Assertions.assertEquals(1000, clone.getThreadPool(), "Кол-во потокв дожно быть 1000")
        );
    }

    void run(int countThreadMin, int countThreadMax, long keepAlive, int countIteration, int countMessage, int timeTestSec, int tpsInputMax, Consumer<ThreadBalancerStatisticData> fnExpected) {
        Util.logConsole(Thread.currentThread(), "Start test");
        AtomicInteger serviceHandleCounter = new AtomicInteger(0);

        ThreadBalancerConsumer test = context.getBean(ThreadBalancerFactory.class).createConsumer("Test", countThreadMin, countThreadMax, tpsInputMax, keepAlive, 333);
        test.setConsumer((msg) -> {
            try {
                Thread.sleep(1000);
            } catch (Exception e) {
                e.printStackTrace();
            }
            serviceHandleCounter.incrementAndGet();
            //Util.logConsole("[" + c.incrementAndGet() + "] " + msg.getCorrelation());
        });

        test.setDebug(true);
        Util.logConsole(Thread.currentThread(), "Init Bean");

        final AtomicInteger realInsert = new AtomicInteger(0);

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
                    break;
                }
            }
        });
        t1.start();
        Util.logConsole(Thread.currentThread(), "Init task thread");
        UtilTest.sleepSec(timeTestSec);
        Assertions.assertEquals(realInsert.get(), serviceHandleCounter.get(), "Не все задачи были обработаны");
        ThreadBalancerStatisticData clone = test.getStatisticAggregate();
        Util.logConsole(Thread.currentThread(), "LAST STAT: " + clone);
        if (clone != null && fnExpected != null) {
            fnExpected.accept(clone);
        }
        t1.interrupt();
        context.getBean(ThreadBalancerFactory.class).shutdown("Test");
    }*/

}