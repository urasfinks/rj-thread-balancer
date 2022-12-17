package ru.jamsys.scheduler;

import ru.jamsys.component.ThreadBalancerFactory;
import ru.jamsys.thread.balancer.ThreadBalancer;

import javax.annotation.PreDestroy;
import java.util.function.Function;

public class SchedulerThreadBalancerStabilizer extends AbstractThreadBalancerScheduler {

    public SchedulerThreadBalancerStabilizer(ThreadBalancerFactory threadBalancerFactory) {
        super("SchedulerThreadBalancerStabilizer", 1000, threadBalancerFactory);
        run();
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
