package ru.jamsys.scheduler;

import ru.jamsys.component.ThreadBalancerFactory;
import ru.jamsys.thread.balancer.ThreadBalancer;

import javax.annotation.PreDestroy;
import java.util.function.Function;

public class SchedulerThreadBalancerStabilizer extends AbstractThreadBalancerScheduler {

    final private ThreadBalancerFactory threadBalancerFactory;

    public SchedulerThreadBalancerStabilizer(ThreadBalancerFactory threadBalancerFactory) {
        super("SchedulerThreadBalancerStabilizer", 1000);
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
            consumer.threadStabilizer();
            return null;
        };
    }

    @PreDestroy
    public void destroy() {
        super.shutdown();
    }

}
