package ru.jamsys.scheduler;

import ru.jamsys.Util;
import ru.jamsys.component.SchedulerGlobal;
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

public class SchedulerThreadBalancerStatistic {

    final private StatisticAggregator statisticAggregator;
    final private ThreadBalancerFactory threadBalancerFactory;
    final private SchedulerGlobal schedulerGlobal;

    public SchedulerThreadBalancerStatistic(ThreadBalancerFactory threadBalancerFactory, StatisticAggregator statisticAggregator, SchedulerGlobal schedulerGlobal) {
        this.statisticAggregator = statisticAggregator;
        this.threadBalancerFactory = threadBalancerFactory;
        this.schedulerGlobal = schedulerGlobal;
        schedulerGlobal.add(this::run);
    }

    private void run() {
        if (threadBalancerFactory != null) {
            try {
                List<Object> objects = Util.forEach(ThreadBalancer.toArrayThreadBalancer(threadBalancerFactory.getListThreadBalancer()), getHandler());
                getResultHandlerList().accept(objects);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        //Util.logConsole(Thread.currentThread(), "flushStatistic: " + Util.jsonObjectToString(statisticAggregator.flush()));
    }


    protected Function<ThreadBalancer, Object> getHandler() {
        return t -> {
            ThreadBalancerStatisticData r = t.flushStatistic();
            return r == null ? null : r.clone();
        };
    }

    @SuppressWarnings("unchecked")
    protected Consumer<Object> getResultHandlerList() {
        return result -> {
            Map<String, ThreadBalancerStatisticData> map = new HashMap<>();
            for (ThreadBalancerStatisticData item : (List<ThreadBalancerStatisticData>) result) {
                map.put(item.getThreadBalancerName(), item);
            }
            statisticAggregator.get().setObject("ThreadBalancer", map);
        };
    }

    @PreDestroy
    public void destroy() {
        schedulerGlobal.remove(this::run);
    }

}
