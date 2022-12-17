package ru.jamsys.scheduler;

import ru.jamsys.component.ThreadBalancerFactory;
import ru.jamsys.thread.balancer.ThreadBalancer;

import javax.annotation.PreDestroy;
import java.util.function.Function;

public class SchedulerThreadBalancerTimeLag extends AbstractThreadBalancerScheduler {

    public SchedulerThreadBalancerTimeLag(ThreadBalancerFactory threadBalancerFactory) {
        super("SchedulerThreadBalancerTimeLag", 333, threadBalancerFactory);
        run();
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
