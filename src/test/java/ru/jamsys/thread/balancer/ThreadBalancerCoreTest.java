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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

class ThreadBalancerCoreTest {

    static ConfigurableApplicationContext context;

    @BeforeAll
    static void beforeAll() {
        String[] args = new String[]{};
        context = SpringApplication.run(App.class, args);
        App.initContext(context, true);
    }

    @Test
    void overclockingSupplier() {
        runSupplier(1, 10, 2000L, 15, 5, clone -> {
            Assertions.assertTrue(clone.getTpsInput() >= 5 && clone.getTpsInput() < 8, "Должен выдавать минимум 5 tpsInput");
            Assertions.assertTrue(clone.getThreadPool() >= 3, "Должен был разогнаться минимум в 3 потока"); // 1 поток может выдавать 2tps нам надо 5 => 3 потока минимум
        });
    }

    @Test
    void overTpsSupplier() {
        runSupplier(1, 20, 6000L, 15, 5, clone ->
                Assertions.assertTrue(clone.getTpsOutput() >= 5, "Выходящих тпс должно быть больше либо равно 5"));
    }

    @Test
    void testThreadParkSupplier() {
        runSupplier(1, 250, 3000L, 30, 250, clone -> {
            Assertions.assertTrue(clone.getTpsInput() > 240, "getTpsInput Должно быть более 240 тпс");
            Assertions.assertTrue(clone.getTpsInput() < 260, "getTpsInput Должно быть меньше 260 тпс");
            Assertions.assertTrue(clone.getTpsOutput() > 240, "getTpsOutput Должно быть более 240 тпс");
            Assertions.assertTrue(clone.getTpsOutput() < 260, "getTpsOutput Должно быть меньше 260 тпс");
            /*
            Не применимо, потому что под нагрузкой есть вероятность, что припаркованные потоки, снова возьмут в работу, считать такое не целесообразно
            Assertions.assertTrue(clone.getThreadCountPark() > 1 && clone.getThreadCountPark() <  5, "На парковке должно быть от 1 до 5 потоков");*/
        });
    }

    void runSupplier(int countThreadMin, int countThreadMax, long keepAlive, int timeTestSec, int maxTps, Consumer<ThreadBalancerStatisticData> fnExpected) {
        Util.logConsole(Thread.currentThread(), "Start test");
        ThreadBalancerCore supplierTest = context.getBean(ThreadBalancerFactory.class).create("SupplierTest", countThreadMin, countThreadMax, maxTps, keepAlive, true);
        supplierTest.setSupplier(() -> {
            Util.sleepMillis(500);
            return new MessageImpl();
        });
        supplierTest.setDebug(true);

        UtilTest.sleepSec(timeTestSec);
        ThreadBalancerStatisticData clone = supplierTest.getStatisticAggregate();
        Util.logConsole(Thread.currentThread(), "LAST STAT: " + clone);
        if (clone != null) {
            fnExpected.accept(clone);
        }
        context.getBean(ThreadBalancerFactory.class).shutdown();
    }

    @Test
    void runConsumer() {

        int countIteration = 1;
        int countMessage = 10;
        int timeTestSec = 5;
        int countThreadMin = 1;
        int countThreadMax = 1;
        int tpsMax = 5;
        long keepAliveMillis = 6000L;

        Util.logConsole(Thread.currentThread(), "Start test");
        ConcurrentLinkedDeque<Message> queue = new ConcurrentLinkedDeque();
        AtomicInteger serviceHandleCounter = new AtomicInteger(0);

        ThreadBalancerCore consumerTest = context.getBean(ThreadBalancerFactory.class).create("ConsumerTest", countThreadMin, countThreadMax, tpsMax, keepAliveMillis, false);
        consumerTest.setCorrectTimeLag(false);
        consumerTest.setSupplier(() -> {
            Message message = queue.pollLast();
            if (message != null) {
                serviceHandleCounter.incrementAndGet();
            }
            return message;
        });
        consumerTest.setConsumer(System.out::println);
        consumerTest.setDebug(true);


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
                    queue.add(message);
                    realInsert.incrementAndGet();
                    avgTime.add(System.currentTimeMillis() - startTime);
                }
                consumerTest.wakeUpOnceThreadLast();
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
        ThreadBalancerStatisticData clone = consumerTest.getStatisticAggregate();
        Util.logConsole(Thread.currentThread(), "LAST STAT: " + clone);

        t1.interrupt();
        context.getBean(ThreadBalancerFactory.class).shutdown();
    }

}