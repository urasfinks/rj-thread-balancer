package ru.jamsys.thread.balancer;

public class ThreadBalancerSupplier2 extends AbstractThreadBalancer {
    @Override
    public int getNeedCountThreadRelease(ThreadBalancerStatistic stat) {
        return getNeedCountThread(stat, getTpsInputMax().get() - stat.getTpsInput(), debug);
    }
}
