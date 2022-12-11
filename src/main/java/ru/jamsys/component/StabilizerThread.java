package ru.jamsys.component;

import org.springframework.stereotype.Component;
import ru.jamsys.thread.balancer.ThreadBalancer;
import ru.jamsys.scheduler.AbstractThreadBalancerScheduler;

import javax.annotation.PreDestroy;
import java.util.function.Function;

@Component
public class StabilizerThread extends AbstractThreadBalancerScheduler {

    final private ThreadBalancerFactory threadBalancerFactory;

    public StabilizerThread(ThreadBalancerFactory cmpService) {
        super("ServiceStabilizer", 2000);
        this.threadBalancerFactory = cmpService;
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

    @Override
    public void tick() {

    }
}
