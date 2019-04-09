# Система снятия телеметрии с Postgres

## Структура

Имеется список с базами данных. Для каждой базы:

- имя базы
- порт
- путь к конфигу

Имеется список с запросами в базу или внешним программам. Для каждой базы перебираются все элементы списка. Каждый элемент инициирует инстанс сборщика метрик. Каждый сборщик метрик собирает только одно конкретное значение.

~~Все сборщики метрик наследуются от базового класса (*[Шаблонный метод](https://refactoring.guru/ru/design-patterns/template-method)*).~~

Шаги сборщиков метрик:

 	1. Получение данных из таблицы или внешней программы (~~*[Стратегия](https://refactoring.guru/ru/design-patterns/strategy)*~~)
 	2. Обогащение мета-информацией (лучше снаружи)

Мета-информация:

- имя сервера / ip-адрес
- версия postgres
- hash конфига postgres

Данные поступают в очередь. По завершении операции для каждой базы очередь отправляется в хранилище (ClickHouse).

## Типы снимаемых метрик

Снимаются через определенные промежутки времени

1. С одного поля или агрегат
2. ~~С целой таблицы~~

Промежутки времени свои для каждой метрики

## Хранилище

В качестве хранилища используется *ClickHouse*.

Вероятно стоит хранить сырые данные, а расчетные реализовывать в виде [представлений](https://clickhouse.yandex/docs/ru/query_language/create/#create-view). Возможно стоит иметь одно большое представление, либо несколько поменьше. По идее можно на лету переключаться между ними, т.к. представления не хранят данные.

В качестве движка есть смысл использовать *ReplacingMergeTree*.

==NOTES:==

```sql
CREATE TABLE timeseries_example
(
    dt Date,
    ts DateTime,
    path String,
    value Float32 CODEC(Delta, ZSTD)
)
ENGINE = MergeTree
PARTITION BY dt
ORDER BY (path, ts)
```



## Снимаемые метрики

Обязательно включить *pg_stat_statements*.[^y1]

### Хэш настроек

```sql
SELECT
    md5(
        CAST(array_agg(
	        CAST(f.setting as text) order by f.name
	    ) as text)
    )
FROM
    pg_settings f
WHERE
    name != 'application_name';
```



### Время отклика

В таблице *pg_stat_statements*

$sum(total\_time) / sum(calls)​$

### Количество ошибок

В таблице *pg_stat_database*

$sum(xact\_rollback)$

### Текущая производительность

Метрики показывающие текущую нагрузку на систему:

#### TPS (количество транзакций в секунду)

В таблицах *pg_stat_database* и *pg_stat_statements*

$sum(xact\_commit + xact\_rollback)​$

#### QPS (количество запросов в секунду)

В таблицах *pg_stat_database* и *pg_stat_statements*

$sum(calls)$

### Uptime

Считается только непосредственно со старта сервера.[^y1]

$now() - pg\_postmaster\_start\_time()$

### Количество вакуумов

```sql
count(*) ... WHERE query ~*'^autovacuum'
```

Типы: user, auto, wraparound.

### Длительность вакуумов

```sql
count(*) ... WHERE now() - xact_start AND query ~'...'
```

Долгие вакуумы – низкая их эффективность, рост размера таблиц без изменения кол-ва строк[^y1]

### Самая длинная транзакция/запрос/вакуум

Долгие *idle* мешают вакууму. Запросы дольше 10-20 мин – потенциальная проблема, не типично для OLTP.[^y1]

```sql
date_trunc('seconds', max(now() - xact_start)), '00:00:00')
```

Если приложение упало, то оно могло не закрыть транзакции – они будут продолжать висеть.[^y1]

### Длительность транзакций и запросов

```sql
now() - xact_start, now() - query_start
```

==TODO:== SELECT latency, DML (INSERT/UPDATE/DELETE) latency, SELECT q/s, DML q/s, count slow queries, read/write iops, lock tables, disk io utilization, disk latency

### Количество коннектов

В таблице *pg_stat_activity*

Состояния клиентов:

```sql
count(*) ... WHERE state = '...'
```

### Объем операций в базе

Семейство таблиц *pg_stat_user_tables*, *pg_stat_system_tables*, *pg_stat_all_tables*: поля с кол-вом туплов (строк) по операциям – *n_tup_ins*, *n_tup_del*, *n_tup_upd*

Можно строить топы по таблицам.

### Размеры таблиц

```
pg_relation_size(), pg_total_relation_size()
```



### Throughput, Locks, Resource utilization, Checkpoints, Replication

Частые чекпоинты снижают дисковую производительность.[^y1]

==TODO:==

- scans: seq vs index
- heap-only updates
- top function calls
- deadlocks per database
- dead rows
- tables with most disk usages
- most frequently scanned indexes
- least frequently scanned indexes
- max replication delay
- scheduled and requested checkpoints
- pg_stat_bgwriter - checkpoint_req, checkpoint_timed
- pg_stat_replication[^y1]
- disk usage: latency, utilization
- network: errors

## Внешние модули

- pg_stat_statements



## Дополнительная документация

[^y1]: [Алексей Лесовский. Основы мониторинга PostgreSQL. Иркутск: HighLoad++ Siberia, 2018](https://youtu.be/Hbi2AFhd4nY)
[^sql_dataegret]: http://bit.do/dataegret_sql
[^sql_lesovsky]: http://bit.do/lesovsky_sql
[^sc]: [The Statistics Collector](http://bit.do/stats_collector)



