# Восстановление состояния

На этой странице описан алгоритм восстановления состояния приложения на старте.

## Обзор

Состояние приложения хранится в 2 местах:

- Снапшот
- Лог команд

Приложение работает в кластере с использованием алгоритма Raft.
В отличие от самого Raft, где приложение "запущено всегда", здесь:

- Состояние восстанавливается только когда узел становится лидером и, когда этот статус теряется (последователем
  становится), состояние очищается.
- При получении очередной записи из она только записывается, но не применяется.

Это решает проблемы:

- Длительной обработки пришедшей команды
- Возможные утечки памяти при длительной работе приложения

Но главный недостаток - большое время перехода в состояние лидера.
В будущем планирую это исправить каким-либо образом.

## Восстановление состояния

Состояние восстанавливается, когда узел становится лидером.

Алгоритм восстановления зависит от наличия снапшота:

1. Если снапшот есть:
    1. Восстанавливаем состояние из снапшота
    2. Читаем и применяем все записи из лога, начиная с LSN сохраненного в снапшоте
2. Иначе снапшота нет
    1. Инициализируем изначальное состояние (единственная очередь по умолчанию)
    2. Читаем и применяем все записи из лога

## Команды добавления и удаления записей лога

В логе имеются команды добавления и удаления записей.
Все Id записей назначаются последовательно, поэтому в командах вставки имеется только приоритет и сообщение - без Id
записи, так как мы можем сами ее восстановить:

- Берем последний Id очереди
- Увеличиваем его на 1
- Полученный Id назначаем записи и сохраняем в очереди

Например, если последний Id очереди был 90 и была добавлена новая запись, то:

- Последний Id очереди - 91
- Id записи - 91

Мы можем вычислить Id записи самостоятельно, поэтому нет необходимости хранить его в команде.

Для удаления используется уже ранее назначенный Id.

Пример - изначально очередь пуста и последний Id = 0:

| Команда         | Записи в очереди (их Id) | Последний Id очереди |
|-----------------|--------------------------|----------------------|
| AddRecord       | 1                        | 1                    |
| AddRecord       | 1, 2                     | 2                    |                  
| RemoveRecord(1) | 2                        | 2                    |                 
| AddRecord       | 2, 3                     | 3                    |
| RemoveRecord(3) | 2                        | 3                    |
| AddRecord       | 2, 4                     | 4                    |

Таким образом:

- Id для всех записей начинаются с 1
- 0 - означает, что очередь была только создана и никаких записей в ней не было
