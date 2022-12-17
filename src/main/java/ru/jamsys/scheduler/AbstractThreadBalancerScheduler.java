package ru.jamsys.scheduler;

import org.springframework.lang.Nullable;
import ru.jamsys.Util;
import ru.jamsys.component.ThreadBalancerFactory;
import ru.jamsys.thread.balancer.ThreadBalancer;

import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;

public abstract class AbstractThreadBalancerScheduler extends AbstractScheduler {

    final private ThreadBalancerFactory threadBalancerFactory;

    public AbstractThreadBalancerScheduler(String name, long periodMillis, ThreadBalancerFactory threadBalancerFactory) {
        super(name, periodMillis);
        this.threadBalancerFactory = threadBalancerFactory;
    }

    @Override
    public <T> Consumer<T> getConsumer() {
        return (t) -> {
            try {
                if (threadBalancerFactory != null) {
                    List<Object> objects = Util.forEach(ThreadBalancer.toArrayThreadBalancer(threadBalancerFactory.getListThreadBalancer()), getHandler());
                    Consumer<Object> handler = getResultHandlerList();
                    if (handler != null) {
                        handler.accept(objects);
                    }
                }
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
    protected Consumer<Object> getResultHandlerList() {
        return null;
    }

}
