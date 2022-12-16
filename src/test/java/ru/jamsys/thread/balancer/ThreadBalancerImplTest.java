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
import ru.jamsys.message.MessageImpl;

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
    void overclocking() {
        run(1, 10, 2000L, 15, 5, clone -> {
            Assertions.assertTrue(clone.getTpsInput() >= 5 && clone.getTpsInput() < 8, "Должен выдавать минимум 5 tpsInput");
            Assertions.assertTrue(clone.getThreadPool() >= 3, "Должен был разогнаться минимум в 3 потока"); // 1 поток может выдавать 2tps нам надо 5 => 3 потока минимум
        });
    }

    @Test
    void overTps() {
        run(1, 20, 6000L, 15, 5, clone ->
                Assertions.assertTrue(clone.getTpsOutput() >= 5, "Выходящих тпс должно быть больше либо равно 5"));
    }

    @Test
    void testThreadPark() {
        run(1, 250, 3000L, 30, 250, clone -> {
            Assertions.assertTrue(clone.getTpsInput() > 240, "getTpsInput Должно быть более 240 тпс");
            Assertions.assertTrue(clone.getTpsInput() < 260, "getTpsInput Должно быть меньше 260 тпс");
            Assertions.assertTrue(clone.getTpsOutput() > 240, "getTpsOutput Должно быть более 240 тпс");
            Assertions.assertTrue(clone.getTpsOutput() < 260, "getTpsOutput Должно быть меньше 260 тпс");
            /*
            Не применимо, потому что под нагрузкой есть вероятность, что припаркованные потоки, снова возьмут в работу, считать такое не целесообразно
            Assertions.assertTrue(clone.getThreadCountPark() > 1 && clone.getThreadCountPark() <  5, "На парковке должно быть от 1 до 5 потоков");*/
        });
    }

    void run(int countThreadMin, int countThreadMax, long keepAlive, int timeTestSec, int maxTps, Consumer<ThreadBalancerStatisticData> fnExpected) {
        Util.logConsole(Thread.currentThread(), "Start test");
        ThreadBalancerImpl test = context.getBean(ThreadBalancerFactory.class).create("Test", countThreadMin, countThreadMax, maxTps, keepAlive);
        test.setSupplier(() -> {
            Util.sleepMillis(500);
            return new MessageImpl();
        });
        test.setDebug(true);

        UtilTest.sleepSec(timeTestSec);
        ThreadBalancerStatisticData clone = test.getStatisticAggregate();
        Util.logConsole(Thread.currentThread(), "LAST STAT: " + clone);
        if (clone != null) {
            fnExpected.accept(clone);
        }
        context.getBean(ThreadBalancerFactory.class).shutdown("Test");
    }

}