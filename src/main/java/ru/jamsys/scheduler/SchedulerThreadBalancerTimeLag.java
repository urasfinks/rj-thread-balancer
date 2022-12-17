package ru.jamsys.scheduler;

import ru.jamsys.component.ThreadBalancerFactory;
import ru.jamsys.thread.balancer.ThreadBalancer;

import javax.annotation.PreDestroy;
import java.util.function.Function;

public class SchedulerThreadBalancerTimeLag extends AbstractThreadBalancerScheduler {

    final private ThreadBalancerFactory threadBalancerFactory;

    public SchedulerThreadBalancerTimeLag(ThreadBalancerFactory threadBalancerFactory) {
        super("SchedulerThreadBalancerTimeLag", 333);
        this.threadBalancerFactory = threadBalancerFactory;
        run();
    }

    @Override
    protected ThreadBalancerFactory getThreadBalancerFactory() {
        return threadBalancerFactory;
    }

    @Override
    protected Function<ThreadBalancer, Object> getHandler() {
        return consumer -> {
            consumer.timeLag();
            return null;
        };
    }

    @PreDestroy
    public void destroy() {
        super.shutdown();
    }
}
