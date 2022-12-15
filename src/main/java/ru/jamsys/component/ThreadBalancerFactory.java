package ru.jamsys.component;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import ru.jamsys.thread.balancer.ThreadBalancer;
import ru.jamsys.thread.balancer.ThreadBalancerConsumer;
import ru.jamsys.thread.balancer.ThreadBalancerSupplier;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

public class ThreadBalancerFactory {

    private ApplicationContext context;

    @Autowired
    public void setContext(ApplicationContext context) {
        this.context = context;
    }

    Map<String, ThreadBalancer> listThreadBalancer = new ConcurrentHashMap<>();

    public List<ThreadBalancer> getListThreadBalancer() {
        return new ArrayList<>(listThreadBalancer.values());
    }

    public ThreadBalancerConsumer createConsumer(String name, int countThreadMin, int countThreadMax, int tpsInputMax, long keepAliveMillis, int schedulerSleepMillis) {
        ThreadBalancerConsumer bean = context.getBean(ThreadBalancerConsumer.class);
        bean.configure(name, countThreadMin, countThreadMax, tpsInputMax, keepAliveMillis, schedulerSleepMillis);
        listThreadBalancer.put(name, bean);
        return bean;
    }

    public ThreadBalancerSupplier createSupplier(String name, int countThreadMin, int countThreadMax, int tpsInputMax, long keepAliveMillis, int schedulerSleepMillis) {
        ThreadBalancerSupplier bean = context.getBean(ThreadBalancerSupplier.class);
        bean.configure(name, countThreadMin, countThreadMax, tpsInputMax, keepAliveMillis, schedulerSleepMillis);
        listThreadBalancer.put(name, bean);
        return bean;
    }

    public void shutdown(String name) {
        ThreadBalancer cs = listThreadBalancer.get(name);
        while (true) {
            try {// Так как shutdown публичный метод, его может вызвать кто-то другой, поэтому будем ждать пока сервис остановится
                cs.shutdown();
                break;
            } catch (Exception e) {
                e.printStackTrace();
            }
            try {
                TimeUnit.MILLISECONDS.sleep(500);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        listThreadBalancer.remove(name);
    }

}
