package ru.jamsys.thread.balancer;


import java.util.List;

public interface ThreadBalancer {

    ThreadBalancerStatistic statistic();

    void threadStabilizer();

    String getName();

    void iteration(WrapThread wrapThread, ThreadBalancer service);

    void shutdown();

    void setDebug(boolean b);

    ThreadBalancerStatistic getStatClone();

    @SuppressWarnings("unused")
    void incThreadMax();

    @SuppressWarnings("unused")
    void decThreadMax();

    void setTpsInputMax(int maxTps);

    @SuppressWarnings("all")
    public static ThreadBalancer[] toArraySblService(List<ThreadBalancer> l) throws Exception {
        return l.toArray(new ThreadBalancer[0]);
    }

}
