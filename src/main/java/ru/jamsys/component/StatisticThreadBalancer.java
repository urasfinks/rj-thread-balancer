package ru.jamsys.component;

import org.springframework.stereotype.Component;
import ru.jamsys.Util;
import ru.jamsys.scheduler.AbstractThreadBalancerScheduler;
import ru.jamsys.thread.balancer.ThreadBalancer;
import ru.jamsys.thread.balancer.ThreadBalancerStatisticData;

import javax.annotation.PreDestroy;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Function;

@Component
public class StatisticThreadBalancer extends AbstractThreadBalancerScheduler {

    final private ThreadBalancerFactory threadBalancerFactory;
    final private StatisticAggregator statisticAggregator;

    public StatisticThreadBalancer(ThreadBalancerFactory threadBalancerFactory, StatisticAggregator statisticAggregator) {
        super("StatisticThreadBalancer", 1000);
        this.threadBalancerFactory = threadBalancerFactory;
        this.statisticAggregator = statisticAggregator;
        run();
    }

    @Override
    protected ThreadBalancerFactory getThreadBalancerFactory() {
        return threadBalancerFactory;
    }

    @Override
    protected Function<ThreadBalancer, Object> getHandler() {
        return t -> {
            ThreadBalancerStatisticData r = t.flushStatistic();
            return r == null ? null : r.clone();
        };
    }

    @SuppressWarnings("unchecked")
    @Override
    protected Consumer<Object> getResultHandler() {
        return result -> {
            Map<String, ThreadBalancerStatisticData> map = new HashMap<>();
            for (ThreadBalancerStatisticData item : (List<ThreadBalancerStatisticData>) result) {
                map.put(item.getThreadBalancerName(), item);
            }
            statisticAggregator.get().setObject("ThreadBalancer", map);
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