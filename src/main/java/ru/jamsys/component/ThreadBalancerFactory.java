package ru.jamsys.component;

import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Component;
import ru.jamsys.AbstractCoreComponent;
import ru.jamsys.scheduler.SchedulerGlobal;
import ru.jamsys.thread.balancer.ThreadBalancer;
import ru.jamsys.thread.balancer.ThreadBalancerAggregateStatisticData;
import ru.jamsys.thread.balancer.ThreadBalancerImpl;
import ru.jamsys.thread.balancer.ThreadBalancerStatisticData;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

@Component
@Lazy
public class ThreadBalancerFactory extends AbstractCoreComponent {

    private final ApplicationContext context;
    private final Scheduler scheduler;
    private final StatisticAggregator statisticAggregator;
    private final String nameSchedulerStabilizer = "SchedulerThreadBalancerStabilizer";
    private final String nameSchedulerTimeLag = "SchedulerThreadBalancerTimeLag";
    private final Map<String, ThreadBalancer> listThreadBalancer = new ConcurrentHashMap<>();

    public ThreadBalancerFactory(ApplicationContext context, StatisticAggregator statisticAggregator, Scheduler scheduler) {

        this.context = context;
        this.scheduler = scheduler;
        this.statisticAggregator = statisticAggregator;

        scheduler.add(SchedulerGlobal.SCHEDULER_GLOBAL_STATISTIC_WRITE, this::flushStatistic);
        scheduler.add(nameSchedulerStabilizer, this::threadStabilizer, 1000);
        scheduler.add(nameSchedulerTimeLag, this::timeLag, 333);

    }

    @SuppressWarnings("unused")
    public List<ThreadBalancer> getListThreadBalancer() {
        return new ArrayList<>(listThreadBalancer.values());
    }

    @SuppressWarnings("unused")
    public ThreadBalancer getThreadBalancer(String name) {
        return listThreadBalancer.get(name);
    }

    public ThreadBalancerImpl create(String name, int countThreadMin, int countThreadMax, int tpsMax, long keepAliveMillis, boolean supplierIdleInputTps) {
        ThreadBalancerImpl bean = context.getBean(ThreadBalancerImpl.class);
        bean.configure(name, countThreadMin, countThreadMax, tpsMax, keepAliveMillis, supplierIdleInputTps);
        listThreadBalancer.put(name, bean);
        return bean;
    }

    @Override
    public void shutdown() {
        super.shutdown();

        scheduler.remove(SchedulerGlobal.SCHEDULER_GLOBAL_STATISTIC_WRITE, this::flushStatistic);
        scheduler.remove(nameSchedulerStabilizer, this::threadStabilizer);
        scheduler.remove(nameSchedulerTimeLag, this::timeLag);

        String[] strings = listThreadBalancer.keySet().toArray(new String[0]);
        for (String name : strings) {
            shutdown(name);
        }
    }

    private void threadStabilizer() {
        String[] strings = listThreadBalancer.keySet().toArray(new String[0]);
        for (String name : strings) {
            ThreadBalancer threadBalancer = listThreadBalancer.get(name);
            if (threadBalancer != null) {
                threadBalancer.threadStabilizer();
            }
        }
    }

    private void timeLag() {
        String[] strings = listThreadBalancer.keySet().toArray(new String[0]);
        for (String name : strings) {
            ThreadBalancer threadBalancer = listThreadBalancer.get(name);
            if (threadBalancer != null) {
                threadBalancer.timeLag();
            }
        }
    }

    @Override
    public void flushStatistic() {
        String[] strings = listThreadBalancer.keySet().toArray(new String[0]);
        ThreadBalancerAggregateStatisticData aggStat = new ThreadBalancerAggregateStatisticData();
        for (String name : strings) {
            ThreadBalancer threadBalancer = listThreadBalancer.get(name);
            if (threadBalancer != null) {
                ThreadBalancerStatisticData thStat = threadBalancer.flushStatistic();
                if (thStat != null) {
                    aggStat.getMap().put(name, thStat.clone());
                }
            }
        }
        statisticAggregator.add(aggStat);
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
