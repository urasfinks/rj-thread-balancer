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

class ThreadBalancerImplTest {

    static ConfigurableApplicationContext context;

    @BeforeAll
    static void beforeAll() {
        String[] args = new String[]{};
        context = SpringApplication.run(App.class, args);
        App.initContext(context, true);
    }

    @Test
    void resistanceSupplier() {
        runSupplier(1, 250, 30000L, 15, 500, 50, clone -> {
            Assertions.assertTrue(clone.resistance > 290 && clone.resistance < 300, "Должен выдавать 295 tps");
        });
    }

    @Test
    void overclockingSupplier() {
        runSupplier(1, 10, 3000L, 15, 5, 0, clone -> {
            Assertions.assertTrue(clone.getInput() >= 4 && clone.getInput() <= 5, "Должен выдавать от 4 до 5 tps");
            Assertions.assertTrue(clone.getPool() >= 3, "Должен был разогнаться минимум в 3 потока"); // 1 поток может выдавать 2tps нам надо 5 => 3 потока минимум
        });
    }

    @Test
    void overTpsSupplier() {
        runSupplier(1, 20, 6000L, 15, 5, 0, clone ->
                Assertions.assertTrue(clone.getOutput() >= 5, "Выходящих тпс должно быть больше либо равно 5"));
    }

    @Test
    void testThreadParkSupplier() {
        runSupplier(1, 250, 3000L, 30, 250, 0, clone -> {
            Assertions.assertTrue(clone.getInput() > 240, "getTpsInput Должно быть более 240 тпс");
            Assertions.assertTrue(clone.getInput() < 260, "getTpsInput Должно быть меньше 260 тпс");
            Assertions.assertTrue(clone.getOutput() > 240, "getTpsOutput Должно быть более 240 тпс");
            Assertions.assertTrue(clone.getOutput() < 260, "getTpsOutput Должно быть меньше 260 тпс");
            /*
            Не применимо, потому что под нагрузкой есть вероятность, что припаркованные потоки, снова возьмут в работу, считать такое не целесообразно
            Assertions.assertTrue(clone.getThreadCountPark() > 1 && clone.getThreadCountPark() <  5, "На парковке должно быть от 1 до 5 потоков");*/
        });
    }

    void runSupplier(int countThreadMin, int countThreadMax, long keepAlive, int timeTestSec, int maxTps, int resistancePrc, Consumer<ThreadBalancerStatisticData> fnExpected) {
        Util.logConsole(Thread.currentThread(), "Start test");
        ThreadBalancerImpl supplierTest = context.getBean(ThreadBalancerFactory.class).create("SupplierTest", countThreadMin, countThreadMax, maxTps, keepAlive, true);
        supplierTest.setSupplier(() -> {
            Util.sleepMillis(500);
            return new MessageImpl();
        });
        supplierTest.setDebug(true);
        supplierTest.setResistancePrc(resistancePrc);

        UtilTest.sleepSec(timeTestSec);
        ThreadBalancerStatisticData clone = supplierTest.getStatisticAggregate();
        Util.logConsole(Thread.currentThread(), "LAST STAT: " + clone);
        if (clone != null) {
            fnExpected.accept(clone);
        }
        context.getBean(ThreadBalancerFactory.class).shutdown();
    }

    @Test
    void overclockingConsumer(){
        runConsumer(1, 5, 60000L, 1, 10, 15, 500, clone ->
                Assertions.assertEquals(5, clone.getPool(), "Кол-во потоков должно быть 5")
        );
    }

    @Test
    void dampingConsumer() { //Проверяем удаление потоков после ненадобности
        runConsumer(5, 10, 2000L, 1, 15, 25, 500, clone ->
                Assertions.assertEquals(5, clone.getPool(), "Должен остаться только 5 потоков")
        );
    }

    @Test
    void timeoutConsumer() { //Проверяем время жизни потоков, после теста они должны все статься
        runConsumer(1, 5, 18000L, 1, 5, 15, 500, clone ->
                Assertions.assertTrue(clone.getPool() == 5, "Кол-во потокв дожно быть равно 5")
        );
    }

    @Test
    void summaryCountConsumer() { //Проверяем, что сообщения все обработаны при большом кол-ве потоков
        runConsumer(1, 1000, 16000L, 1, 5000, 25, 1000, clone ->
                Assertions.assertEquals(1000, clone.getPool(), "Кол-во потокв дожно быть 1000")
        );
    }

    void runConsumer(int countThreadMin, int countThreadMax, long keepAliveMillis, int countIteration, int countMessage, int timeTestSec, int tpsMax, Consumer<ThreadBalancerStatisticData> fnExpected) {

        Util.logConsole(Thread.currentThread(), "Start test");
        ConcurrentLinkedDeque<Message> queue = new ConcurrentLinkedDeque();
        AtomicInteger serviceHandleCounter = new AtomicInteger(0);

        ThreadBalancerImpl consumerTest = context.getBean(ThreadBalancerFactory.class).create("ConsumerTest", countThreadMin, countThreadMax, tpsMax, keepAliveMillis, false);
        consumerTest.setCorrectTimeLag(false);
        consumerTest.setSupplier(() -> {
            Util.sleepMillis(1000);
            Message message = queue.pollLast();
            if (message != null) {
                serviceHandleCounter.incrementAndGet();
            }
            return message;
        });
        //consumerTest.setConsumer(System.out::println);
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

        if (clone != null && fnExpected != null) {
            fnExpected.accept(clone);
        }

        t1.interrupt();
        context.getBean(ThreadBalancerFactory.class).shutdown();
    }

}