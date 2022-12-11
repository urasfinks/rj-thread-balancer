package ru.jamsys.thread.balancer;

import java.util.concurrent.ThreadFactory;

public class CustomThreadFactory implements ThreadFactory {

    private int threadsNum;
    private final String namePattern;

    public CustomThreadFactory(String baseName) {
        namePattern = baseName + "-%d";
    }

    @Override
    public Thread newThread(Runnable runnable) {
        threadsNum++;
        return new Thread(runnable, String.format(namePattern, threadsNum));
    }
    
}
