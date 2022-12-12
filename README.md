# rj-thread-balancer
Балансировщик потоков

Есть 2 типа балансировщиков:

1) Supplier - входные данные в систему (INPUT)
2) Consumer - выходные данные из системы (OUTPUT)

Разница между ними в том, что используются разные алгоритмы балансировки. 

Supplier это входящая нагрузка, расчёт кол-ва потоков зависит от максимальных TPS (потому что это внешние данные, о кол-ве которых нам не известно), при условии наличия данных. Если есть данные, и потоки все заняты - увеличиваем потоки и наоборот.

Consumer балансирует кол-во потоков только по длине своей очереди (очередь локальная, мы знаем как нам надо масштабироваться), задачи есть - растём, задач нет - потухаем.

У каждого балансировщика есть свой планировщик, в пуле потоков его можно найти, по имени балансировщика + "-Scheduler".
Балансировщик по заявленной частоте вызывает метод tick (тиканье). Метод проверяет нагрузку балансировщика и в случаи необходимости переводит потоки из паркинга в рабочий режим. 
За это отвечает аргумент schedulerSleepMillis, не смотря на его легковестность, надо подходить к значению сознательно.
Что бы определить сколько надо выставлять, можно ответить на вопрос: Критично ли будет, если в течении секунды, никто не обработает данные, которые приготовлены для обработки?
То есть параметр отвечает за время повторного запроса, в том случаи если все потоки обработали очередь и ушли спать, а в этот момент прилетела кучка запросов.
Вот это время, эти запросы будут нуходится необработанными. Если у вас поток неприрывный, то таких проблем вообще не будет, потоки в балансировщике будут рботать непрерывно.

Буду дополнять информацию постепенно
