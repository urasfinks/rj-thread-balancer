package ru.jamsys.thread.balancer;

import lombok.Data;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

@Data
public class WrapThread {

    private Thread thread;
    private AtomicBoolean isRun = new AtomicBoolean(true);
    private long lastWakeUp = System.currentTimeMillis();
    private AtomicInteger countIteration = new AtomicInteger(0);

    private boolean fine = false;


    public void incCountIteration() {
        fine = true;
        countIteration.incrementAndGet();
    }

    @SuppressWarnings("all")
    public static WrapThread[] toArrayWrapThread(List<WrapThread> l) throws Exception {
        return l.toArray(new WrapThread[0]);
    }

    public boolean getFine() {
        return fine;
    }
}
