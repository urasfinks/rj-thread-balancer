package ru.jamsys.thread.balancer;

import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;
import ru.jamsys.Util;
import ru.jamsys.message.Message;
import ru.jamsys.message.MessageHandle;
import ru.jamsys.thread.balancer.exception.ShutdownException;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Stream;

@Component
@Scope("prototype")
public class ThreadBalancerCore extends ThreadBalancerStatistic implements ThreadBalancer {

    @Setter
    @NonNull
    private volatile Supplier<Message> supplier = () -> null; //Поставщик входящих сообщений в балансировщик

    @Setter
    @NonNull
    private volatile Consumer<Message> consumer = (msg) -> {
    }; //Потребитель сообщений представленных поставщиком

    @Getter
    private String name; //Имя балансировщика - будет отображаться в пуле jmx

    private final AtomicInteger threadNameCounter = new AtomicInteger(0); //Индекс создаваемого потока, что бы всё красиво было в jmx

    @Setter
    private Function<Integer, Integer> formulaAddCountThread = (need) -> need; //При расчёте необходимого кол-ва потоков, происходит прогон через эту формулу (в дальнейшем для корректировки плавного старта)

    @Setter
    private Function<Integer, Integer> formulaRemoveCountThread = (need) -> need;

    public void configure(String name, int threadCountMin, int threadCountMax, int tpsInputMax, long threadKeepAliveMillis) {
        this.setTpsMax(tpsInputMax);
        if (isActive.compareAndSet(false, true)) {
            this.name = name;
            this.threadCountMin = threadCountMin;
            this.threadCountMax = new AtomicInteger(threadCountMax);
            this.threadKeepAlive = threadKeepAliveMillis;
            overclocking(threadCountMin);
        }
    }


    @Override
    public void threadStabilizer() { //Вызывается планировщиком StabilizerThread каждую секунду
        //Когда мы ничего не знаем о внешней среде, можно полагаться, только на работу текущих потоков
        Util.logConsole(Thread.currentThread(), "threadStabilizer()");
        if (isActive.get()) {
            try {
                if (threadParkQueue.size() == 0) {
                    int addThreadCount = overclocking(formulaAddCountThread.apply(threadList.size()));
                    if (debug) {
                        Util.logConsole(Thread.currentThread(), "AddThread: " + addThreadCount);
                    }
                } else if (threadList.size() > threadCountMin) { //Кол-во потоков больше минимума
                    checkKeepAliveAndRemoveThread();
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
            if (autoRestoreResistanceTps.get() && getResistancePercent().get() > 0) {
                getResistancePercent().decrementAndGet();
            }
        } else {
            Util.logConsole(Thread.currentThread(), "threadStabilizer() ThreadBalancer not active");
        }
    }

    @Override
    public void timeLag() { //Убирание секундного лага
        if (threadParkQueue.size() == threadList.size()) {
            System.out.println("TimeLag");
            wakeUpOnceThreadLast();
        }
    }

    @Override
    public ThreadBalancerStatisticData flushStatistic() { //Вызывается планировщиком StatisticThreadBalancer для агрегации статистики за секунду
        super.flushStatistic();
        wakeUp();
        return statLastSec;
    }

    @Override
    public void shutdown() throws ShutdownException { //Остановка пула потоков
        Util.logConsole(Thread.currentThread(), "TO SHUTDOWN");
        if (isActive.compareAndSet(true, false)) { //Только один поток будет останавливать
            long startShutdown = System.currentTimeMillis();
            while (threadList.size() > 0) {
                try {
                    WrapThread wrapThread = threadParkQueue.getFirst(); //Замысел такой, что бы выцеплять только заверенные процессы
                    forceRemoveThread(wrapThread);
                } catch (NoSuchElementException e) { //Если нет в отстойнике - подождём немного
                    try {
                        TimeUnit.MILLISECONDS.sleep(100);
                    } catch (InterruptedException e2) {
                        e2.printStackTrace();
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
                if (System.currentTimeMillis() > startShutdown + 30000) { //Пошла жара
                    new Exception("ERROR SHUTDOWN 30 sec -> throw INTERRUPT").printStackTrace();
                    try {
                        Stream.of(threadList.toArray(new WrapThread[0])).forEach(threadWrap -> { //Боремся за атамарность конкурентных изменений
                            try {
                                threadWrap.getThread().interrupt();
                            } catch (Exception e) {
                                e.printStackTrace();
                            }
                        });
                    } catch (Exception e) { //Край, вилы
                        e.printStackTrace();
                    } finally {
                        threadList.clear();
                    }
                    break;
                }
            }
        } else {
            Util.logConsole(Thread.currentThread(), "Already shutdown in other Thread");
        }
        if (threadList.size() > 0) {
            throw new ShutdownException("ThreadPoolSize: " + threadList.size());
        }
    }

    private boolean wakeUpOnceThreadLast() {
        while (isActive.get()) { //Хотел тут добавить проверку, что бы последний на паркинге не забирать, но так низя - иначе ThreadBalancer увеличит потоки, когда в паркинге никого не будет => надо правильно рассчитывать то кол-во, которое реально надо разбудить
            WrapThread wrapThread = threadParkQueue.pollLast(); //Всегда забираем с конца, в начале тушаться потоки под нож
            if (wrapThread != null) {
                //Так как последующая операция перед вставкой в очередь - блокировка
                //Надо проверить, что поток припаркован (возможна гонка)
                if (wrapThread.getThread().getState().equals(Thread.State.WAITING)) {
                    try {
                        tpsThreadWakeUp.incrementAndGet();
                        wrapThread.setLastWakeUp(System.currentTimeMillis());
                        LockSupport.unpark(wrapThread.getThread());
                        return true;
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                } else { // Ещё статус не переключился, просто отбрасываем в начала очереди, к тем, кто ждёт ножа
                    threadParkQueue.addFirst(wrapThread);
                }
            } else { //Null - элементы закончились, хватит
                break;
            }
        }
        return false;
    }

    private int overclocking(int count) {
        if (!isActive.get() || threadList.size() >= threadCountMax.get()) {
            return 0;
        }
        int countRealAdd = 0;
        if (count > 0) {
            for (int i = 0; i < count; i++) {
                if (addThread()) {
                    countRealAdd++;
                } else {
                    break;
                }
            }
        }
        return countRealAdd;
    }

    private boolean addThread() {
        if (isActive.get() && threadList.size() < threadCountMax.get()) {
            final ThreadBalancer self = this;
            final WrapThread wrapThread = new WrapThread();
            wrapThread.setThread(new Thread(() -> {
                while (isActive.get() && wrapThread.getIsRun().get()) {
                    try {
                        wrapThread.incCountIteration(); // Это для отслеживания, что поток вообще работает
                        tpsIdle.incrementAndGet();
                        iteration(wrapThread, self);
                        //В методе wakeUpOnceThread решена проблема гонки за предварительный старт
                        threadParkQueue.add(wrapThread);
                        tpsPark.incrementAndGet();
                        LockSupport.park();
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
                try {
                    forceRemoveThread(wrapThread);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }));
            wrapThread.getThread().setName(getName() + "-" + threadNameCounter.getAndIncrement());
            threadList.add(wrapThread);
            wrapThread.getThread().start();
            return true;
        }
        return false;
    }

    private void removeThread(WrapThread wrapThread) {
        //Кол-во неприкасаемых потоков, которые должно быть
        if (threadList.size() > threadCountMin) {
            forceRemoveThread(wrapThread);
        }
    }

    synchronized private void forceRemoveThread(WrapThread wrapThread) { //Этот метод может загасит пул балансировщика до конца, используйте обычный removeThread
        WrapThread curWrapThread = wrapThread != null ? wrapThread : threadList.get(0);
        if (curWrapThread != null) {
            int count = 0;
            while (true) {
                count++;
                if (count > 3) {
                    break;
                }
                try {
                    curWrapThread.getIsRun().set(false);
                    LockSupport.unpark(curWrapThread.getThread()); //Мы его оживляем, что бы он закончился
                    threadList.remove(curWrapThread);
                    threadParkQueue.remove(curWrapThread); // На всякий случай
                    if (debug) {
                        Util.logConsole(Thread.currentThread(), "removeThread: " + curWrapThread);
                    }
                    break;
                } catch (Exception e) {
                    e.printStackTrace();
                }
                try {
                    TimeUnit.MILLISECONDS.sleep(333);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    private void checkKeepAliveAndRemoveThread() { //Проверка ждунов, что они давно не вызывались и у них кол-во итераций равно 0 -> нож
        try {
            final long now = System.currentTimeMillis();
            final AtomicInteger maxCounterRemove = new AtomicInteger(formulaRemoveCountThread.apply(1));
            Util.forEach(WrapThread.toArrayWrapThread(threadList), (wrapThread) -> {
                long future = wrapThread.getLastWakeUp() + threadKeepAlive;
                //Время последнего оживления превысило keepAlive + поток реально не работал
                if (now > future && wrapThread.getCountIteration().get() == 0 && maxCounterRemove.getAndDecrement() > 0) {
                    removeThread(wrapThread);
                } else {
                    wrapThread.getCountIteration().set(0);
                }
                return null;
            });
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void wakeUp() {
        //Были добавлены потоки, потому что балансировщик потоков в какое-то время не справлялся
        //А потом эти потоки могут переходить в паркинг, потому что не очень то и нужны были
        //Наша задача делать это плавно, допустим перешли на паркинг 200 потоков, 180 на текущей итарации снова попробуем запустить, а 20 путь отдыхают и ждут ножа
        if (statLastSec.getTpsPark() > 0) {
            int needThread = new BigDecimal(statLastSec.getTpsPark())
                    .divide(new BigDecimal("1.1"), 2, RoundingMode.HALF_UP)
                    .setScale(0, RoundingMode.CEILING)
                    .intValue();
            for (int i = 0; i < needThread; i++) {
                if (!wakeUpOnceThreadLast()) {
                    break;
                }
            }
        }
    }

    @Override
    public void iteration(WrapThread wrapThread, ThreadBalancer threadBalancer) { //Это то, что выполняется в каждом потоке пула балансировки
        while (isIteration(wrapThread)) {
            wrapThread.incCountIteration();
            long startTime = System.currentTimeMillis();
            tpsInput.incrementAndGet(); //Короче оно должно быть тут для supplier точно
            Message message = supplier.get();
            if (message != null) {
                message.onHandle(MessageHandle.CREATE, this);
                consumer.accept(message);
                timeTransactionQueue.add(System.currentTimeMillis() - startTime);
                tpsOutput.incrementAndGet();
            } else {
                break;
            }
        }
    }

}
