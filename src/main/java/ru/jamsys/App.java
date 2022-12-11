package ru.jamsys;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;
import ru.jamsys.component.StabilizerThread;
import ru.jamsys.component.StatisticAggregator;

@SpringBootApplication
public class App {

    public static ConfigurableApplicationContext context;

    public static void main(String[] args) {
        context = SpringApplication.run(App.class, args);
        System.out.println("Hello World!");
    }

    public static void initContext(ConfigurableApplicationContext context, boolean debug) {
        StabilizerThread cmpStabilizerThread = context.getBean(StabilizerThread.class);
        cmpStabilizerThread.setDebug(debug);
        cmpStabilizerThread.run();

        StatisticAggregator statisticAggregator = context.getBean(StatisticAggregator.class);
        statisticAggregator.setDebug(debug);
        statisticAggregator.run();
    }
}
