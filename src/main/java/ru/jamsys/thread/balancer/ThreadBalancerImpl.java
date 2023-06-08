package ru.jamsys.thread.balancer;

import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
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

public class ThreadBalancerImpl extends ThreadBalancerStatistic implements ThreadBalancer {

    @Setter
    @NonNull
    private volatile Supplier<Message> supplier = () -> null; //Поставщик входящих сообщений в балансировщик

    @Setter
    @NonNull
    private volatile Consumer<Message> consumer = (msg) -> {
    }; //Потребитель сообщений представленных поставщиком

    @Getter
    private String name; //Имя балансировщика - будет отображаться в пуле jmx

    private boolean idleInputTps = true; //Считать холостую отработку поставщика как успешную входящий TPS

    private final AtomicInteger threadNameCounter = new AtomicInteger(0); //Индекс создаваемого потока, что бы всё красиво было в jmx

    @Setter
    private Function<Integer, Integer> formulaAddCountThread = (need) -> need; //При расчёте необходимого кол-ва потоков, происходит прогон через эту формулу (в дальнейшем для корректировки плавного старта)

    @Setter
    private Function<Integer, Integer> formulaRemoveCountThread = (need) -> need;

    @Setter
    private Function<Integer, Integer> formulaRemoveResistancePrc = (need) -> need;

    @Setter
    private volatile boolean correctTimeLag = true;

    public void configure(String name, int threadCountMin, int threadCountMax, int tpsMax, long threadKeepAliveMillis, boolean supplierIdleInputTps) {
        this.idleInputTps = supplierIdleInputTps;
        this.setTpsMax(tpsMax);
        if (isActive.compareAndSet(false, true)) {
            this.name = name;
            this.threadCountMin.set(threadCountMin);
            this.threadCountMax.set(threadCountMax);
            this.threadKeepAlive = threadKeepAliveMillis;
            overclocking(threadCountMin);
        }
    }

    @Override
    public void threadStabilizer() { //Вызывается планировщиком StabilizerThread каждую секунду
        //Когда мы ничего не знаем о внешней среде, можно полагаться, только на работу текущих потоков
        //Util.logConsole(Thread.currentThread(), "threadStabilizer()");
        if (isActive.get()) {
            try {
                if (threadParkQueue.isEmpty()) {
                    overclocking(formulaAddCountThread.apply(threadList.size()));
                } else if (threadList.size() > threadCountMin.get()) { //Кол-во потоков больше минимума
                    checkKeepAliveAndRemoveThread();
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
            if (!listResistanceRequest.isEmpty()) { //Если были запросы на сопротивление
                int resAvg = 0;
                try { //Ловим модификатор, пока ни разу не ловил, на всякий случай
                    double avg = listResistanceRequest.stream().mapToLong(Integer::intValue).summaryStatistics().getAverage();
                    resAvg = (int) avg;
                } catch (Exception e) {
                    e.printStackTrace();
                }
                resistancePercent = resAvg;
                listResistanceRequest.clear();
            } else if (autoRestoreResistanceTps.get() && resistancePercent > 0) {
                resistancePercent -= formulaRemoveResistancePrc.apply(1);
            }
            if (resistancePercent > 0) {
                tpsResistance.set((100 - resistancePercent) * tpsMax.get() / 100);
            } else {
                tpsResistance.set(tpsMax.get());
            }
        } else {
            Util.logConsole(Thread.currentThread(), "threadStabilizer() ThreadBalancer not active");
        }
    }

    @Override
    public void timeLag() { //Убирание секундного лага
        //if (correctTimeLag && threadParkQueue.size() == threadList.size()) {
        if (correctTimeLag && isIteration()) {
            wakeUpOnceThreadLast();
        }
    }

    @Override
    public ThreadBalancerStatisticData flushStatistic() { //Вызывается планировщиком StatisticThreadBalancer для агрегации статистики за секунду
        super.flushStatistic();
        runThreadPark();
        return statLastSec;
    }

    @Override
    public void shutdown() throws ShutdownException { //Остановка пула потоков
        Util.logConsole(Thread.currentThread(), "TO SHUTDOWN");
        if (isActive.compareAndSet(true, false)) { //Только один поток будет останавливать
            long startShutdown = System.currentTimeMillis();
            while (!threadList.isEmpty()) {
                try {
                    WrapThread wrapThread = threadParkQueue.getFirst(); //Замысел такой, что бы выцеплять только заверенные процессы
                    removeThread(wrapThread, false);
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
        if (!threadList.isEmpty()) {
            throw new ShutdownException("ThreadPoolSize: " + threadList.size());
        }
    }

    public boolean wakeUpOnceThreadLast() {
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

    private void overclocking(int count) {
        if (!isActive.get() || threadList.size() >= threadCountMax.get()) {
            return;
        }
        if (count > 0) {
            for (int i = 0; i < count; i++) {
                if (!addThread()) {
                    break;
                }
            }
        }
    }

    private boolean addThread() {
        if (isActive.get() && threadList.size() < threadCountMax.get()) {
            final ThreadBalancer self = this;
            final WrapThread wrapThread = new WrapThread();
            wrapThread.setThread(new Thread(() -> {
                while (!wrapThread.getThread().isInterrupted() && isActive.get() && wrapThread.getIsRun().get()) {
                    try {
                        wrapThread.incCountIteration(); // Это для отслеживания, что поток вообще работает
                        tpsIdle.incrementAndGet();
                        iteration(wrapThread, self);
                        //В методе wakeUpOnceThread решена проблема гонки за предварительный старт
                        threadParkQueue.add(wrapThread);
                        tpsThreadParkIn.incrementAndGet();
                        LockSupport.park();
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
                removeThread(wrapThread, true);
            }));
            wrapThread.getThread().setName(getName() + "-" + threadNameCounter.getAndIncrement());
            tpsThreadAdd.incrementAndGet();
            threadList.add(wrapThread);
            wrapThread.getThread().start();
            return true;
        }
        return false;
    }

    private void safeRemoveThread(@NonNull WrapThread wrapThread) {
        //Кол-во неприкасаемых потоков, которые должно быть
        if (threadList.size() > threadCountMin.get()) {
            removeThread(wrapThread, false);
        }
    }

    synchronized private void removeThread(@NonNull WrapThread wrapThread, boolean fromThread) { //Этот метод может загасит пул балансировщика до конца, используйте обычный removeThread
        wrapThread.getIsRun().set(false);
        if (!fromThread) { //Если этот метод вызывается из самого потока, не надо его дополнительно выводить из парковки
            try {
                LockSupport.unpark(wrapThread.getThread()); //Мы его оживляем, что бы он закончился
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        threadList.remove(wrapThread);
        threadParkQueue.remove(wrapThread); // На всякий случай
        if (!tpsThreadRemove.contains(wrapThread)) {
            tpsThreadRemove.add(wrapThread);
        }
    }

    private void checkKeepAliveAndRemoveThread() { //Проверка ждунов, что они давно не вызывались и у них кол-во итераций равно 0 -> нож
        try {
            final long curTimeMillis = System.currentTimeMillis();
            final AtomicInteger maxCounterRemove = new AtomicInteger(formulaRemoveCountThread.apply(1));
            Util.forEach(WrapThread.toArrayWrapThread(threadList), (wrapThread) -> {
                long future = wrapThread.getLastWakeUp() + threadKeepAlive;
                //Время последнего оживления превысило keepAlive + поток реально не работал
                if (curTimeMillis > future && wrapThread.getCountIteration().get() == 0 && maxCounterRemove.getAndDecrement() > 0) {
                    safeRemoveThread(wrapThread);
                } else {
                    wrapThread.getCountIteration().set(0);
                }
                return null;
            });
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void runThreadPark() {
        //Когда потоки уезжат в паркинг их надо каждую секунду восстанавливать, они ведь были не просто так созданны
        //Наша задача делать плавное восстановление, так как и в паркинг они ушли не просто так
        //Допустим в паркинг упало 200 потоков, 180 на текущей итарации снова попробуем запустить, а 20 путь отдыхают и ждут ножа
        if (statLastSec.getParkIn() > 0) {
            int needThread = new BigDecimal(statLastSec.getParkIn())
                    .divide(new BigDecimal("1.1"), 2, RoundingMode.HALF_UP)
                    .setScale(0, RoundingMode.CEILING)
                    .intValue();
            //На больших числах округление нормально работает, но на меленьких - нет
            //Пример 5 / 1.1 = 4.5 ceil = 5, но вроде как надо сокращать кол-во, так как в паркинг они пошли от безделия
            //Но если в паркинг ушёл 1 поток, то сокращать ничего не надо, начнём
            if (needThread == statLastSec.getParkIn() && needThread > 1) {
                needThread--;
            }
            for (int i = 0; i < needThread; i++) {
                if (!wakeUpOnceThreadLast()) {
                    break;
                }
            }
        } else { //Стабильность надо поддерживать
            wakeUpOnceThreadLast();
        }
    }

    @Override
    public void iteration(WrapThread wrapThread, ThreadBalancer threadBalancer) { //Это то, что выполняется в каждом потоке пула балансировки
        while (isIteration(wrapThread)) {
            wrapThread.incCountIteration();
            long startTime = System.currentTimeMillis();
            if (idleInputTps) {
                tpsInput.incrementAndGet();
            }
            Message message = supplier.get();
            if (message != null) {
                if (idleInputTps) {
                    message.onHandle(MessageHandle.CREATE, this);
                } else {
                    tpsInput.incrementAndGet();
                }
                consumer.accept(message);
                timeTransactionQueue.add(System.currentTimeMillis() - startTime);
                tpsOutput.incrementAndGet();
            } else {
                break;
            }
        }
    }

}
