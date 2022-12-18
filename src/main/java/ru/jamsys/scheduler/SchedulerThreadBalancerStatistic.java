package ru.jamsys.scheduler;

import ru.jamsys.Util;
import ru.jamsys.component.StatisticAggregator;
import ru.jamsys.component.ThreadBalancerFactory;
import ru.jamsys.thread.balancer.ThreadBalancer;
import ru.jamsys.thread.balancer.ThreadBalancerStatisticData;

import javax.annotation.PreDestroy;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Function;

public class SchedulerThreadBalancerStatistic extends AbstractSchedulerThreadBalancer {

    final private StatisticAggregator statisticAggregator;

    public SchedulerThreadBalancerStatistic(ThreadBalancerFactory threadBalancerFactory, StatisticAggregator statisticAggregator) {
        super("SchedulerThreadBalancerStatistic", 1000, threadBalancerFactory);
        this.statisticAggregator = statisticAggregator;
        run();
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
    protected Consumer<Object> getResultHandlerList() {
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
            Util.logConsole(Thread.currentThread(), "flushStatistic: " + Util.jsonObjectToString(statisticAggregator.flush()));
        }
        return super.getConsumer();
    }

    @PreDestroy
    public void destroy() {
        super.shutdown();
    }

}
