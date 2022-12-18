package ru.jamsys.component;

import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Component;
import ru.jamsys.App;
import ru.jamsys.scheduler.SchedulerThreadBalancerStabilizer;
import ru.jamsys.scheduler.SchedulerThreadBalancerStatistic;
import ru.jamsys.scheduler.SchedulerThreadBalancerTimeLag;
import ru.jamsys.thread.balancer.ThreadBalancer;
import ru.jamsys.thread.balancer.ThreadBalancerCore;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

@Component
public class ThreadBalancerFactory {

    private final ApplicationContext context;

    public ThreadBalancerFactory(ApplicationContext context, StatisticAggregator statisticAggregator) {
        this.context = context;
        try {
            SchedulerThreadBalancerTimeLag schedulerThreadBalancerTimeLag = new SchedulerThreadBalancerTimeLag(this);
            schedulerThreadBalancerTimeLag.setDebug(App.debug);

            SchedulerThreadBalancerStabilizer schedulerThreadBalancerStabilizer = new SchedulerThreadBalancerStabilizer(this);
            schedulerThreadBalancerStabilizer.setDebug(App.debug);

            SchedulerThreadBalancerStatistic schedulerThreadBalancerStatistic = new SchedulerThreadBalancerStatistic(this, statisticAggregator);
            schedulerThreadBalancerStatistic.setDebug(App.debug);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    Map<String, ThreadBalancer> listThreadBalancer = new ConcurrentHashMap<>();

    public List<ThreadBalancer> getListThreadBalancer() {
        return new ArrayList<>(listThreadBalancer.values());
    }

    public ThreadBalancerCore create(String name, int countThreadMin, int countThreadMax, int tpsMax, long keepAliveMillis, boolean supplierIdleInputTps) {
        ThreadBalancerCore bean = context.getBean(ThreadBalancerCore.class);
        bean.configure(name, countThreadMin, countThreadMax, tpsMax, keepAliveMillis, supplierIdleInputTps);
        listThreadBalancer.put(name, bean);
        return bean;
    }

    public void shutdown() {
        Set<String> strings = listThreadBalancer.keySet();
        for (String name : strings) {
            shutdown(name);
        }
    }

    public void shutdown(String name) {
        ThreadBalancer cs = listThreadBalancer.get(name);
        if (cs != null) {
            while (true) {
                try {// Так как shutdown публичный метод, его может вызвать кто-то другой, поэтому будем ждать пока сервис остановится
                    cs.shutdown();
                    break;
                } catch (Exception e) {
                    e.printStackTrace();
                }
                try {
                    TimeUnit.MILLISECONDS.sleep(500);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            listThreadBalancer.remove(name);
        }
    }

}
