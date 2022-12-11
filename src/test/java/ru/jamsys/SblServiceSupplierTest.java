package ru.jamsys;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.boot.SpringApplication;
import org.springframework.context.ConfigurableApplicationContext;
import ru.jamsys.component.ThreadBalancerFactory;
import ru.jamsys.message.MessageImpl;
import ru.jamsys.thread.balancer.ThreadBalancer;
import ru.jamsys.thread.balancer.ThreadBalancerStatistic;
import ru.jamsys.thread.balancer.ThreadBalancerSupplier;

import java.util.function.Consumer;

class SblServiceSupplierTest {

    static ConfigurableApplicationContext context;

    @BeforeAll
    static void beforeAll() {
        String[] args = new String[]{};
        context = SpringApplication.run(App.class, args);
        App.initContext(context, true);
    }

    @Test
    void overclocking() {
        run(1, 5, 6000L, 10, 5, clone -> {
            Assertions.assertTrue(clone.getTpsInput() >= 5 && clone.getTpsInput() < 8, "Должен выдавать минимум 5 tpsInput");
            Assertions.assertTrue(clone.getThreadCount() >= 4, "Должен был разогнаться минимум в 4 потока");
        });
    }

    @Test
    void overTps() {
        run(1, 20, 6000L, 15, 5, clone ->
                Assertions.assertTrue(clone.getTpsOutput() >= 5, "Выходящих тпс должно быть больше либо равно 5"));
    }

    @Test
    void testThreadPark() {
        run(1, 250, 3000L, 20, 250, clone -> {
            Assertions.assertTrue(clone.getTpsInput() > 240, "getTpsInput Должно быть более 240 тпс");
            Assertions.assertTrue(clone.getTpsInput() < 260, "getTpsInput Должно быть меньше 260 тпс");
            Assertions.assertTrue(clone.getTpsOutput() > 240, "getTpsOutput Должно быть более 240 тпс");
            Assertions.assertTrue(clone.getTpsOutput() < 260, "getTpsOutput Должно быть меньше 260 тпс");
            Assertions.assertTrue(clone.getThreadCountPark() > 1 && clone.getThreadCountPark() < 5, "На парковке должно быть от 1 до 5 потоков");
        });
    }

    void run(int countThreadMin, int countThreadMax, long keepAlive, int sleep, int maxTps, Consumer<ThreadBalancerStatistic> fnExpected) {
        Util.logConsole(Thread.currentThread(), "Start test");
        ThreadBalancer test = context.getBean(ThreadBalancerFactory.class).createSupplier("Test", countThreadMin, countThreadMax, keepAlive, 333, () -> {
            Util.sleepMillis(500);
            return new MessageImpl();
        }, message -> {
        });
        test.setDebug(false);
        test.setTpsInputMax(maxTps);
        UtilTest.sleepSec(sleep);
        ThreadBalancerStatistic clone = test.getStatistic();
        Util.logConsole(Thread.currentThread(), "LAST STAT: " + clone);
        if (clone != null) {
            fnExpected.accept(clone);
        }
        context.getBean(ThreadBalancerFactory.class).shutdown("Test");
    }

    @Test
    void getNeedCountThread() {
        Assertions.assertEquals(125, ThreadBalancerSupplier.getNeedCountThread(ThreadBalancerStatistic.instanceSupplierTest(500, 100, 150, 1), 250, true), "#1");
        Assertions.assertEquals(63, ThreadBalancerSupplier.getNeedCountThread(ThreadBalancerStatistic.instanceSupplierTest(500, 100, 150, 125), 250, true), "#2");
        Assertions.assertEquals(0, ThreadBalancerSupplier.getNeedCountThread(ThreadBalancerStatistic.instanceSupplierTest(500, 100, 0, 1), 250, true), "#3");
        Assertions.assertEquals(10, ThreadBalancerSupplier.getNeedCountThread(ThreadBalancerStatistic.instanceSupplierTest(500, 50, 10, 1), 250, true), "#4");
        Assertions.assertEquals(10, ThreadBalancerSupplier.getNeedCountThread(ThreadBalancerStatistic.instanceSupplierTest(0, 50, 10, 1), 250, true), "#5");
    }

}