package ru.jamsys.component;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import ru.jamsys.message.Message;
import ru.jamsys.thread.balancer.ThreadBalancer;
import ru.jamsys.thread.balancer.ThreadBalancerConsumer;
import ru.jamsys.thread.balancer.ThreadBalancerConsumer2;
import ru.jamsys.thread.balancer.ThreadBalancerSupplier;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Supplier;

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

    public ThreadBalancerConsumer2 createConsumer2(String name, int countThreadMin, int countThreadMax, int tpsInputMax, long keepAliveMillis, long schedulerSleepMillis) {
        ThreadBalancerConsumer2 bean = context.getBean(ThreadBalancerConsumer2.class);
        bean.configure(name, countThreadMin, countThreadMax, tpsInputMax, keepAliveMillis, schedulerSleepMillis);
        listService.put(name, bean);
        return bean;
    }

    public ThreadBalancer createConsumer(String name, int countThreadMin, int countThreadMax, long keepAliveMillis, long schedulerSleepMillis, Consumer<Message> consumer) {
        ThreadBalancerConsumer sblServiceConsumer = context.getBean(ThreadBalancerConsumer.class);
        sblServiceConsumer.configure(name, countThreadMin, countThreadMax, keepAliveMillis, schedulerSleepMillis, consumer);
        listService.put(name, sblServiceConsumer);
        return sblServiceConsumer;
    }

    public ThreadBalancer createSupplier(String name, int countThreadMin, int countThreadMax, long keepAliveMillis, long schedulerSleepMillis, Supplier<Message> supplier, Consumer<Message> consumer) {
        ThreadBalancerSupplier sblServiceSupplier = context.getBean(ThreadBalancerSupplier.class);
        sblServiceSupplier.configure(name, countThreadMin, countThreadMax, keepAliveMillis, schedulerSleepMillis, supplier, consumer);
        listService.put(name, sblServiceSupplier);
        return sblServiceSupplier;
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
