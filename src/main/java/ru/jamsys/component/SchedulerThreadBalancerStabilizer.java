package ru.jamsys.component;

import org.springframework.stereotype.Component;
import ru.jamsys.thread.balancer.ThreadBalancer;
import ru.jamsys.scheduler.AbstractThreadBalancerScheduler;

import javax.annotation.PreDestroy;
import java.util.function.Function;

@Component
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
