package ru.jamsys.thread.balancer;


import java.util.List;

public interface ThreadBalancer {

    ThreadBalancerStatistic flushStatistic(); //Дожно использоваться только Планировщиком статистики, который каждую секунду сбрасывает и агрегирует информацию

    ThreadBalancerStatistic getStatistic(); //Получить срез статистики на текущий момент без сброса, можно использовать всем, кому это надо

    void threadStabilizer(); //Вызывается только Планировщиком стабилизации потоков (каждые 2 секунды)

    String getName(); //Имя пула балансировки

    void iteration(WrapThread wrapThread, ThreadBalancer service); //Вызывается созданными потоками, для непосредственного вызова ваших функциональных блоков. Для вас это бесполезный метод

    void shutdown(); // Потушить сервис, вызывается на завершении программы, надеюсь вам никогда не прийдётся его использовать, однако будте вкурсе - он потоко безопасный, вы можете получить исключения

    void setDebug(boolean b); //Логировние в консоль отладочной информации, применялось только мной, ничего инетерсного там нет, врятли вам пригодится

    @SuppressWarnings("unused")
    void incThreadMax(); //Увеличть в рантайме кол-во потоков (Крайне не советую вам такое делать)

    @SuppressWarnings("unused")
    void decThreadMax(); //Уменьшить в рантайме кол-во потоков (Крайне не советую вам такое делать)

    void setTpsInputMax(int maxTps); //Установить максимальный предел вызываемых блоков iteration (Я так же вам не советую этого делать)

    void setResistance(int prc); //Используется только для Supplier для поддержки сопротивления на избыточную нагрузку, для полного понимания - читать описание в реализации

    @SuppressWarnings("all")
    public static ThreadBalancer[] toArrayThreadBalancer(List<ThreadBalancer> l) throws Exception { // Маленька защита от конкуретных итераторов с измененеием (НЕ ПАНАЦЕЯ)
        return l.toArray(new ThreadBalancer[0]);
    }

}
