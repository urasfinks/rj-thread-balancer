package ru.jamsys.thread.balancer;

public interface ThreadBalancer {

    ThreadBalancerStatisticData flushStatistic(); //Дожно использоваться только Планировщиком статистики, который каждую секунду сбрасывает и агрегирует информацию

    ThreadBalancerStatisticData getStatisticAggregate(); //Получить срез статистики на текущий момент без сброса, можно использовать всем, кому это надо

    void threadStabilizer(); //Вызывается только Планировщиком стабилизации потоков (каждую секунду)

    void timeLag(); //Вызывается только планировщиком SchedulerThreadBalancerTimeLag, который дёргает этот метод 3 раза в секунду

    String getName(); //Имя пула балансировки

    void iteration(WrapThread wrapThread, ThreadBalancer threadBalancer); //Вызывается созданными потоками, для непосредственного вызова ваших функциональных блоков. Для вас это бесполезный метод

    void shutdown(); // Потушить сервис, вызывается на завершении программы, надеюсь вам никогда не прийдётся его использовать, однако будте вкурсе - он потоко безопасный, вы можете получить исключения

    void setDebug(boolean b); //Логировние в консоль отладочной информации, применялось только мной, ничего инетерсного там нет, врятли вам пригодится

    void setTpsMax(int maxTps); //Установить максимальный предел вызываемых блоков iteration (Я так же вам не советую этого делать)

    void setResistancePrc(int prc); //Используется для поддержки сопротивления на избыточную нагрузку (просьба сбавить нагрузку на n процентов)

    @SuppressWarnings("unused")
    void setTestAutoRestoreResistanceTps(boolean status); //По умолчанию восстановление tps будет работать, метод только для тестов

    void wakeUpIfEveryoneIsSleeping(); //Пробудить одного спящего если вме сяп

}
