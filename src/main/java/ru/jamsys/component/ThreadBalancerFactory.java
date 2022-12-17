package ru.jamsys.component;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Component;
import ru.jamsys.App;
import ru.jamsys.scheduler.SchedulerThreadBalancerStabilizer;
import ru.jamsys.scheduler.SchedulerThreadBalancerTimeLag;
import ru.jamsys.thread.balancer.ThreadBalancer;
import ru.jamsys.thread.balancer.ThreadBalancerCore;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

@Component
public class ThreadBalancerFactory {

    private ApplicationContext context;

    @Autowired
    public void setContext(ApplicationContext context) {
        this.context = context;
    }

    public ThreadBalancerFactory() {
        SchedulerThreadBalancerTimeLag schedulerThreadBalancerTimeLag = new SchedulerThreadBalancerTimeLag(this);
        schedulerThreadBalancerTimeLag.setDebug(App.debug);

        SchedulerThreadBalancerStabilizer schedulerThreadBalancerStabilizer = new SchedulerThreadBalancerStabilizer(this);
        schedulerThreadBalancerStabilizer.setDebug(App.debug);
    }

    Map<String, ThreadBalancer> listThreadBalancer = new ConcurrentHashMap<>();

    public List<ThreadBalancer> getListThreadBalancer() {
        return new ArrayList<>(listThreadBalancer.values());
    }

    public ThreadBalancerCore create(String name, int countThreadMin, int countThreadMax, int tpsInputMax, long keepAliveMillis) {
        ThreadBalancerCore bean = context.getBean(ThreadBalancerCore.class);
        bean.configure(name, countThreadMin, countThreadMax, tpsInputMax, keepAliveMillis);
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
