---
title: "Observability"
---

# Обзор

На этой странице описываются метрики, которые собирает и отдает приложение.

# Метрики

Какие метрики приложение имеет:

Пользовательские запросы:

- `taskflux_requests_accepted_total` - Принято запросов всего
- `taskflux_requests_processed_total` - Обработано запросов всего
- `taskflux_clients_connected_total` - Общее количество подключенных пользователей
- `taskflux_clients_disconnected_total` - Общее количество отключенных пользователей
- `taskflux_commands_processed_total` - Общее количество обработанных команд
- `taskflux_command_queue_length` - Размер очереди запросов
- `taskflux_commands_in_process` - Количество команд, находящихся в обработке в текущий момент

Рафт:

- `taskflux_rpc_duration` - Скорость отправки RPC запросов
- `taskflux_rpc_sent_bytes_total` - Общее количество отправленных байтов
- `taskflux_rpc_received_bytes_total` - Общее количество принятых байтов
- `taskflux_raft_uncommitted_entries_count` - Количество незакоммиченных записей
- `taskflux_raft_log_segments_count` - Количество файлов сегментов
- `taskflux_raft_term_total` - текущий терм узла
- `taskflux_raft_snapshot_index` - индекс записи в снапшоте
- `taskflux_raft_log_last_written_index` - индекс последней команды в логе
- `taskflux_raft_log_commit_index` - индекс закоммиченной команды
- `taskflux_fsync_duration` - Время выполнения `fsync`/`FileStream.Flush(true)`

Информация о приложении/узле:

- `taskflux_cluster_size` - размер кластера
- `taskflux_node_id` - ID узла в кластере
- `taskflux_app_version` - версия приложения

Память:

- `taskflux_gc_collections_count` - Количество запусков GC
- `taskflux_gc_objects_size` - Занимаемая объектами память в куче
- `taskflux_gc_allocations_size` - Общее число выделенной памяти с начала запуска приложения
- `taskflux_gc_committed_memory_size` - Размер закоммиченной памяти в байтах
- `taskflux_gc_duration` - Время затраченное на GC
- `taskflux_gc_available_size` - Количество байт доступных для выделения
- TODO: добавить размер каждого поколения

Рантайм:

- `taskflux_jit_il_compiled_size` - Количество байтов IL, которые были скомплированы в нативный код
- `taskflux_jit_compilation_time` - Время затраченное на JIT компиляцию
- `taskflux_monitor_lock_contention_count` - Количество случаев конфликта получения блокировки (попытка взять уже
  занятую блокировку)
- `taskflux_thread_pool_threads_count` - Размер пула потоков
- `taskflux_thread_pool_queue_length` - Количество ожидающих обработки элементов в очереди пула потоков
- `taskflux_threads_count` - Количество потоков, запущенных в приложении
- `taskflux_timer_count` - Количество запущенных таймеров
- `taskflux_exceptions_count` - Количество возникших исключений (через AppDomain сделать)

- Количество очередей существующих
- Скорость вставки в очередь (указывать какую конкретно очередь ??? high cardinality)

Идей из других приложений

## RabbitMQ

- Отслеживается общее кол-во открытых и закрытых соединений - текущее кол-во соединений = октрыто - закрыто
- Данные о приложении - версия, сборка и т.д.
- префикс приложения `taskflux_`
- Для рафта тоже отслеживаем метрики:
    - `raft_term_total`
    - `raft_log_snapshot_index`
    - `raft_log_last_applied_index`
    - `raft_log_last_written_index`
    - `raft_log_commit_index`
    - `raft_entry_commit_latency` - время, которое нужно для коммита записи
- Время GC
- Сколько работает и сколько GC
  собрал (https://github.com/open-telemetry/opentelemetry-dotnet-contrib/blob/main/src/OpenTelemetry.Instrumentation.Runtime/RuntimeMetrics.cs)

## Redis

...ХЗ

## etcd

Логирование:

- Начинаю выборы
- Перехожу в новую роль + терм
- Получил XXX запрос от узла YYY в терме ZZZ
- Избран лидер в терме (все это отправляют)

Метрики:

- Общее полученное число полученных байт от других узлов
- Имеет ли лидера
- Количество обнаруженных изменений лидера
- Количество потоков
- Открытые файловые дескрипторы
- Время работы (с начала запуска)

TODO: найти как etcd обнаруживает что лидер новый выбран

## Apache Kafka

не буду

## PostgreSQL

Нет

P.S. что-то с файлами сегментов и снапшота сделать (как их отображать), м.б. размер занимаемого хранилища.

# Получение метрик

Собираемые метрики можно получить в формате `Prometheus` по HTTP эндпоинту `/metrics`.
Используется HTTP порт.

# Проверка состояния

тут `/health` эндпоинт нужен будет

