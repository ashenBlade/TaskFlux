# Команды

Запросы для выполнения приложением отправляются в [CommandRequest](client-network-protocol.md#command-request) пакете.

Результаты выполнения отправляются в [CommandResponse](client-network-protocol.md#command-response) пакете,
либо [Ok](client-network-protocol.md#ok) пакете в зависимости от логики работы.
Какой именно ответ отправляется прописано для каждого запроса отдельно.

> Для всех запросов дополнительно возможны ответы [Error](#error) или [PolicyViolation](#policy-violation).
> Они сигнализируют о нарушениях бизнес-логики.

# Типы данных

Все используемые типы данных указаны в [сетевом протоколе](network-protocol.md).

# Запросы

## Enqueue

Запрос на добавления записи в требуемую очередь.

Формат:

| Маркер    | Очередь   | Ключ  | Данные |
|-----------|-----------|-------|--------|
| Byte('E') | QueueName | Int64 | Buffer |

Ответ: [Ok](client-network-protocol.md#ok)

## Dequeue

Запрос на получение записи из очереди.

Формат:

| Маркер    | Очередь   | Таймаут |
|-----------|-----------|---------|
| Byte('D') | QueueName | UInt32  |

`Таймаут` - время ожидания записи в мс (миллисекундах):
- 0 - означает без ожидания, получить ответ сразу
- другое значение - в случае, если в очереди не было записей, то приложение будет ожидать новых записей указанное
количество миллисекунд

Ответ: [Dequeue Response](#dequeue-1)

## Count

Получить количество записей в очереди 

Формат:

| Маркер    | Очередь   |
|-----------|-----------|
| Byte('C') | QueueName |

Ответ: [Count Response](#count-1)

## Create Queue

Запрос на создание новой очереди задач.

Поле `Название очереди` должно представляться допустимым названием очереди (описано
в [спецификации](func-spec.md#название-очереди))

Формат:

| Маркер    | Название очереди | Реализация | Максимальный размер очереди | Максимальный размер тела сообщения | Диапазон ключей                |
|-----------|------------------|------------|-----------------------------|------------------------------------|--------------------------------|
| Byte('Q') | QueueName        | Int32      | Int32                       | Int32                              | Nullable\<Pair\<Int64, Int64>> |

Поля `Максимальный размер очереди` и `Максимальный размер тела сообщения` содержат в себе `Int32` - знаковое число.
Если в нем содержится `-1` (все биты установлены в 1), то это означает отсутствие значения.

Ответ: [Create Queue Response](client-network-protocol.md#ok)

### Реализации очереди

В этой таблице определены коды, используемые в поле `Реализация`.
Это поле определяет тип, который будет использоваться для нижележащей структуры хранения.

| Код | Код структуры                          | 
|-----|----------------------------------------|
| 1   | [`H4`](../queue.md#основанная-на-куче) |
| 2   | [`QA`](../queue.md#список-очередей)    |

> При указании `QA` необходимо задать политику ограничения диапазона ключей

Также отдельно выделяется код `0`. Это код по умолчанию. Если указан он, то выбирается структура по умолчанию.
На данный момент, это [`H4`](../queue.md#основанная-на-куче).

## Delete Queue

Запрос на удаление существующей очереди задач.

Поле "Название очереди" не должно быть пустой строкой, исходя из того, что это название очереди по умолчанию, а ее удалить нельзя.

Формат:

| Маркер    | Название очереди |
|-----------|------------------|
| Byte('R') | QueueName        | 

Ответ: [Ok](client-network-protocol.md#ok)

## List queues

Запрос на получение информации обо всех очередях, хранящихся в системе на момент запроса.

Формат:

| Маркер    |
|-----------|
| Byte('L') |

Ответ: [List Queues Response](#list-queues-1).

# Ответы

В этой секции описаны пакеты, отправляемые в ответ на запросы.

## Dequeue

Ответ на [Dequeue запрос](#dequeue)

Если из очереди получилось получить данные, то в поле Success будет выставлено 1, в противном случае 0.
В зависимости от того, получилось ли извлечь данные (поле Success) будут выставлены остальные поля.
Если значение было получено, то остальные поля представлены, иначе дальше не будет данных

Формат:

| Маркер    | Успех | Ключ  | Данные |
|-----------|-------|-------|--------|
| Byte('d') | Bool  | Int64 | Buffer |

Если данные присутствуют:

| Маркер    | Успех      | Ключ  | Данные |
|-----------|------------|-------|--------|
| Byte('d') | Bool(true) | Int64 | Buffer |

Если данные отсутствуют:

| Маркер    | Успех       |
|-----------|-------------|
| Byte('d') | Bool(false) | 

## Count

Ответ на Count запрос. Отправляется в ответ на [Count запрос](#count)

Поле `Количество` содержит количество элементов, находящихся в очереди.

Формат:

| Маркер    | Количество |
|-----------|------------|
| Byte('c') | Int32      |

## Error

Ответ посылаемый в случае возникновения ошибки во время исполнения команды.
Эти типы ошибок относятся к бизнес-логике, а не самому приложению (больше про исполняемый файл, инфраструктуру и т.д.).

Примеры ошибок:
- Неправильное название очереди
- Очереди с указанным названием не существует
- Нарушено ограничение очереди (максимальный размер, диапазон ключей и т.д.)

Поля:

- `Код ошибки` - числовой код возникшей ошибки
- `Детали` - детали ошибки (может быть пустым)

Формат:

| Маркер    | Код ошибки | Детали |
|-----------|------------|--------|
| Byte('x') | Int32      | String |

Типы ошибок:

| Маркер | Описание                                                    |
|--------|-------------------------------------------------------------|
| 0      | Неизвестная ошибка бизнес-логики                            |
| 1      | Указанное название очереди представляет невалидное значение |
| 2      | Очереди с указанным названием не существует                 |
| 3      | Очередь с указанным названием уже существует                |
| 4      | Превышен диапазон допустимых значений ключа                 |
| 5      | Указан неверный диапазон ключей                             |
| 6      | Неверное значение максимального размера очереди             |
| 7      | Неверное значение максимального размера сообщения           |
| 8      | Диапазон приоритетов не указан                              |
| 9      | Неизвестный код очереди                                     |

## List queues

Отправляется в ответ на запрос [List queues](#list-queues).

Поле "Данные" содержит список информации о каждой очереди. 
Ключами являются названия очереди, а значениями словарь метаданных.
Пример метаданных, максимальный размер очереди

Формат:

| Маркер    | Количество очередей | Название очереди | Размер очереди | Описание политик      |
|-----------|---------------------|------------------|----------------|-----------------------|
| Byte('l') | Int32               | QueueName        | Int32          | Dict\<String, String> |

> Поля `Название очереди`, `Размер очереди` и `Описание политик` формируют единую запись для одной очереди.
> Количество этих записей указано в поле `Количество очередей`.
> Можно трактовать это поле как размер массива из структур описания для каждой очереди.

Возможные метаданные очереди:

| Название         | Описание                          | Значение                                   |
|------------------|-----------------------------------|--------------------------------------------|
| max-queue-size   | Максимальный размер очереди       | Int32                                      |
| max-payload-size | Максимальная длина тела сообщения | Int32                                      |
| priority-range   | Разрешенный диапазон ключей       | Два Int64 разделенных пустой строкой (' ') | 

> Примечание: все значения передаются в виде строк (для гибкости).
> В столбце "Значение" таблицы указывается тип, который был сериализован в строку.

> Примечание: поле `Название` регистрозависимое

> Примечание: если поле необязательно, то должно отсутствовать: пустая строка тоже является валидным значением.
> Нельзя вставлять пустую строку для значения, которое отсутствует.

## Policy Violation

Этот ответ сигнализирует об ошибке, вызванной нарушением политик, установленных на очередь.

> Строго говоря, нарушение политики не является ошибкой (как например, отсутствие нужной очереди), поэтому выделил в
> отдельный пакет.

Формат:

| Маркер    | Код политики | Дополнительные данные |
|-----------|--------------|-----------------------|
| Byte('p') | Int32        | ...                   |

Поле `Код политики` содержит число, характеризующее, какая политика была нарушена.

`Дополнительные данные` содержит поля, характерные для конкретной политики - для каждого типа свои.

### Общая политика

Код: `0`

Эта политика - общая. Как бы запасная на случай, если не найдется нужного кода.
Внутри себя она содержит только строку - сообщение-описание ошибки.

Формат:

| Маркер    | Код политики | Сообщение |
|-----------|--------------|-----------|
| Byte('p') | Int32(0)     | String    |

### Максимальный размер очереди

Код: `1`

Эта политика описывает ограничение на размер очереди.
Описана в секции [максимальный размер очереди](../queue-policies.md#максимальный-размер-очереди).

Внутри себя содержит только целое число — максимальный размер очереди (поле `Максимальный размер`).

Формат:

| Маркер    | Код политики | Максимальный размер |
|-----------|--------------|---------------------|
| Byte('p') | Int32(1)     | Int32               |

### Максимальный размер тела сообщения

Код: `2`

Эта политика описывает ограничение на размер тела сообщения.
Описана в секции [максимальный размер тела сообщения](../queue-policies.md#максимальный-размер-тела-сообщения).

Внутри себя содержит только целое число - максимальный размер тела сообщения (поле `Максимальный размер`).

| Маркер    | Код политики | Максимальный размер |
|-----------|--------------|---------------------|
| Byte('p') | Int32(2)     | Int32               |

### Диапазон ключей

Код: `3`

Эта политика определяет возможный диапазон ключей, который можно указывать для записей в очереди.
Описана в секции [диапазон ключей](../queue-policies.md#диапазон-ключей).

Внутри себя содержит 2 числа - минимальное (поле `Минимальное значение`) и максимальное (поле `Максимальное значение`)
значение ключей.

| Маркер    | Код политики | Минимальное значение | Максимальное значение |
|-----------|--------------|----------------------|-----------------------|
| Byte('p') | Int32(3)     | Int64                | Int64                 |
