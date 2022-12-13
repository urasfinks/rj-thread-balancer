package ru.jamsys.component;

import org.springframework.stereotype.Component;
import ru.jamsys.StatisticData;
import ru.jamsys.Util;
import ru.jamsys.scheduler.AbstractThreadBalancerScheduler;
import ru.jamsys.thread.balancer.ThreadBalancer;
import ru.jamsys.thread.balancer.ThreadBalancerStatistic;

import javax.annotation.PreDestroy;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Function;

@Component
public class StatisticAggregator extends AbstractThreadBalancerScheduler {

    final private ThreadBalancerFactory threadBalancerFactory;

    final private StatisticCpu statisticCpu;

    public StatisticAggregator(ThreadBalancerFactory threadBalancerFactory, StatisticCpu statisticCpu) {
        super("Statistic", 1000);
        this.threadBalancerFactory = threadBalancerFactory;
        this.statisticCpu = statisticCpu;
    }

    private final Map<String, AtomicInteger> shareStat = new ConcurrentHashMap<>();

    public void shareStatistic(String name, Long count) {
        if (!shareStat.containsKey(name)) {
            shareStat.put(name, new AtomicInteger(count.intValue()));
        } else {
            shareStat.get(name).set(count.intValue());
        }
    }

    public void shareStatistic(String name, Integer count) {
        if (!shareStat.containsKey(name)) {
            shareStat.put(name, new AtomicInteger(count));
        } else {
            shareStat.get(name).set(count);
        }
    }

    public void incShareStatistic(String name) {
        if (!shareStat.containsKey(name)) {
            shareStat.put(name, new AtomicInteger(1));
        } else {
            shareStat.get(name).incrementAndGet();
        }
    }

    @Override
    protected ThreadBalancerFactory getThreadBalancerFactory() {
        return threadBalancerFactory;
    }

    @Override
    protected Function<ThreadBalancer, Object> getHandler() {
        return t -> {
            ThreadBalancerStatistic r = t.flushStatistic();
            return r == null ? null : r.clone();
        };
    }

    @SuppressWarnings("unchecked")
    @Override
    protected Consumer<Object> getResultHandler() {
        return result -> {
            StatisticData statisticData = new StatisticData();
            statisticData.setCpu(statisticCpu.getCpuUsage());
            Map<String, ThreadBalancerStatistic> map = new HashMap<>();
            for (ThreadBalancerStatistic item : (List<ThreadBalancerStatistic>) result) {
                map.put(item.getServiceName(), item);
            }
            statisticData.setService(map);

            Map<String, Integer> map2 = new HashMap<>();
            String[] list = shareStat.keySet().toArray(new String[0]);
            for (String item : list) {
                int value = shareStat.get(item).getAndSet(0);
                map2.put(item, value);
            }
            statisticData.setShare(map2);
            //Util.logConsole(Thread.currentThread(), Util.jsonObjectToString(statistic));
            /*
            try {
                greetingClient.nettyRequestPost(
                        Util.getApplicationProperties("elk.url"),
                        "/statistic/_doc",
                        Util.jsonObjectToString(statistic),
                        5).block();
            } catch (Exception e) {
                e.printStackTrace();
            }*/
        };
    }

    @Override
    public <T> Consumer<T> getConsumer() {
        if (debug) {
            Util.logConsole(Thread.currentThread(), "flushStatistic");
        }
        return super.getConsumer();
    }

    @PreDestroy
    public void destroy() {
        super.shutdown();
    }

}
