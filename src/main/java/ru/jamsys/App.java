package ru.jamsys;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;
import ru.jamsys.component.SchedulerThreadBalancerStabilizer;
import ru.jamsys.component.SchedulerThreadBalancerStatistic;
import ru.jamsys.component.SchedulerThreadBalancerTimeLag;

@SpringBootApplication
public class App {

    public static ConfigurableApplicationContext context;

    public static void main(String[] args) {
        context = SpringApplication.run(App.class, args);
        System.out.println("Hello World!");
    }

    public static void initContext(ConfigurableApplicationContext context, boolean debug) {
        SchedulerThreadBalancerStabilizer schedulerThreadBalancerStabilizer = context.getBean(SchedulerThreadBalancerStabilizer.class);
        schedulerThreadBalancerStabilizer.setDebug(debug);

        SchedulerThreadBalancerStatistic schedulerThreadBalancerStatistic = context.getBean(SchedulerThreadBalancerStatistic.class);
        schedulerThreadBalancerStatistic.setDebug(debug);

        SchedulerThreadBalancerTimeLag schedulerThreadBalancerTimeLag = context.getBean(SchedulerThreadBalancerTimeLag.class);
        schedulerThreadBalancerTimeLag.setDebug(debug);
    }

}
