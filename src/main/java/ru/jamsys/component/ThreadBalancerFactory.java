package ru.jamsys.component;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import ru.jamsys.thread.balancer.ThreadBalancer;
import ru.jamsys.thread.balancer.ThreadBalancerConsumer;

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

    Map<String, ThreadBalancer> listService = new ConcurrentHashMap<>();

    public List<ThreadBalancer> getListService() {
        return new ArrayList<>(listService.values());
    }

    public ThreadBalancerConsumer createConsumer(String name, int countThreadMin, int countThreadMax, int tpsInputMax, long keepAliveMillis, long schedulerSleepMillis) {
        ThreadBalancerConsumer bean = context.getBean(ThreadBalancerConsumer.class);
        bean.configure(name, countThreadMin, countThreadMax, tpsInputMax, keepAliveMillis, schedulerSleepMillis);
        listService.put(name, bean);
        return bean;
    }

    public void shutdown(String name) {
        ThreadBalancer cs = listService.get(name);
        while (true) {
            try {// Так как shutdown публичный метод, его может вызвать кто-то другой, поэтому будем ждать пока сервис остановится
                cs.shutdown();
                break;
            } catch (Exception e) {
                e.printStackTrace();
            }
            try {
                TimeUnit.MILLISECONDS.sleep(100);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        listService.remove(name);
    }

}
