package ru.jamsys.thread.balancer;

public class ThreadBalancerSupplier extends AbstractThreadBalancer {
    @Override
    public int getNeedCountThreadRelease(ThreadBalancerStatisticData stat, boolean create) {
        return getNeedCountThreadByTransaction(stat, getTpsInputMax().get() - stat.getTpsInput(), debug, create);
    }
}