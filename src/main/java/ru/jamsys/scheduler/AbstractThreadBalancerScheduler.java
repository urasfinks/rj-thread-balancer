package ru.jamsys.scheduler;

import org.springframework.lang.Nullable;
import ru.jamsys.Util;
import ru.jamsys.component.ThreadBalancerFactory;
import ru.jamsys.thread.balancer.ThreadBalancer;

import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;

public abstract class AbstractThreadBalancerScheduler extends AbstractScheduler implements SchedulerTick {

    public AbstractThreadBalancerScheduler(String name, int periodMillis) {
        super(name, periodMillis);
    }

    @Override
    public <T> Consumer<T> getConsumer() {
        return (t) -> {
            try {
                ThreadBalancerFactory threadBalancerFactory = getThreadBalancerFactory();
                if (threadBalancerFactory != null) {
                    List<Object> objects = Util.forEach(ThreadBalancer.toArrayThreadBalancer(threadBalancerFactory.getListService()), getHandler());
                    Consumer<Object> handler = getResultHandler();
                    if (handler != null) {
                        handler.accept(objects);
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
            try {
                tick();
            } catch (Exception e) {
                e.printStackTrace();
            }
        };
    }

    @Nullable
    protected ThreadBalancerFactory getThreadBalancerFactory() {
        return null;
    }

    @Nullable
    protected Function<ThreadBalancer, Object> getHandler() {
        return null;
    }

    @Nullable
    protected Consumer<Object> getResultHandler() {
        return null;
    }

    @Override
    public void tick() {

    }

}
