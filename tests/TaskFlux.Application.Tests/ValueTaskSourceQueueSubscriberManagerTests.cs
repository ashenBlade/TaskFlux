using System.Collections.Concurrent;
using System.Diagnostics.CodeAnalysis;
using System.Text;
using TaskFlux.Application.RecordAwaiter;
using TaskFlux.Core.Queue;

namespace TaskFlux.Application.Tests;

[Trait("Category", "BusinessLogic")]
[SuppressMessage("ReSharper", "AccessToDisposedClosure")]
public class ValueTaskSourceQueueSubscriberManagerTests
{
    private static QueueRecord Record(long priority, string payload) => new(priority, Encoding.UTF8.GetBytes(payload));
    /*
     * Какие тесты:
     * - Уведомляю одного менеджера - запись уходит его ждуну, а ждуну другого менеджера ничего не приходит, крч, разные менеджеры не зависят от своих
     * - TryNotifyRecord возвращает false если никого нет
     * - TryNotifyRecord возвращает true когда есть ждуны
     */

    [Fact(Timeout = 100)]
    public async Task WaitRecord__КогдаЗаписьДобавляетсяДоВызова__ДолженВернутьДобавленнуюЗапись()
    {
        var factory = new ValueTaskSourceQueueSubscriberManagerFactory(10);
        var manager = factory.CreateQueueSubscriberManager();
        var expected = Record(123, "hello, world");
        using var subscriber = manager.GetSubscriber();
        manager.TryNotifyRecord(expected);

        var actual = await subscriber.WaitRecordAsync();

        Assert.Equal(expected, actual);
    }

    [Fact(Timeout = 100)]
    public async Task WaitRecord__КогдаЗаписьДобавляетсяПослеВызова__ДолженВернутьДобавленнуюЗапись()
    {
        var factory = new ValueTaskSourceQueueSubscriberManagerFactory(10);
        var manager = factory.CreateQueueSubscriberManager();
        using var subscriber = manager.GetSubscriber();
        var expected = Record(234634, "asdfasdf");

        // Используется для синхронизации, чтобы запись не была добавлена быстрее, чем начало чтения
        var readyToInsertSource = new TaskCompletionSource();

        // Есть 2 потока - один читает, другой добавляет запись
        await Task.WhenAll(Task.Run(async () =>
        {
            // Этот поток читает запись
            readyToInsertSource.SetResult();
            var actual = await subscriber.WaitRecordAsync();
            Assert.Equal(expected, actual);
        }), Task.Run(async () =>
        {
            // Этот добавляет запись, но перед этим необходимо 
            await readyToInsertSource.Task;
            await Task.Delay(50);
            manager.TryNotifyRecord(expected);
        }));
    }

    [Fact(Timeout = 1000)]
    public async Task WaitRecord__КогдаОжидающийОдинИОперацийМного__ДолженКорректноЧитатьВсеЗаписи()
    {
        var factory = new ValueTaskSourceQueueSubscriberManagerFactory(10);
        var manager = factory.CreateQueueSubscriberManager();
        var records = Enumerable.Range(0, 100)
                                .Select(i => Record(i, i.ToString()))
                                .ToList();

        await Task.WhenAll(Task.Run(async () =>
        {
            // Запись
            foreach (var record in records)
            {
                while (!manager.TryNotifyRecord(record))
                {
                }
            }
        }), Task.Run(async () =>
        {
            // Чтение
            var actual = new List<QueueRecord>();
            for (int i = 0; i < records.Count; i++)
            {
                using var subscriber = manager.GetSubscriber();
                var record = await subscriber.WaitRecordAsync();
                actual.Add(record);
            }

            Assert.Equal(records, actual);
        }));
    }

    [Theory(Timeout = 500)]
    [InlineData(2)]
    [InlineData(10)]
    [InlineData(50)]
    [InlineData(100)]
    public async Task WaitRecord__КогдаЕстьНесколькоЖдуновИЗаписьТолько1__ПрочитатьЗаписьДолженТолько1(int readersCount)
    {
        var factory = new ValueTaskSourceQueueSubscriberManagerFactory(10);
        var manager = factory.CreateQueueSubscriberManager();
        var expected = Record(123, "hello, world");

        // Синхронизируемся - запись должна быть добавлена тогда, когда все ждуны начали чтение
        using var reset = new CountdownEvent(readersCount);
        _ = Task.Run(() =>
        {
            reset.Wait();
            manager.TryNotifyRecord(expected);
        });
        var dequeued = 0;
        var tasks = Enumerable.Range(0, readersCount)
                              .Select(_ => Task.Run(async () =>
                               {
                                   using var awaiter = manager.GetSubscriber();
                                   reset.Signal();
                                   var record = await awaiter.WaitRecordAsync();
                                   Assert.Equal(expected, record);
                                   var old = Interlocked.Exchange(ref dequeued, 1);
                                   if (old != 0)
                                   {
                                       throw new NeverThrownException("Прочитано больше 1 записи");
                                   }

                                   // Подождем на всякий случай
                                   await Task.Delay(100);
                               }))
                              .ToArray();
        await Task.WhenAny(tasks);

        foreach (var task in tasks)
        {
            if (task.IsCompleted)
            {
                var exception = Xunit.Record.Exception(() => task.GetAwaiter().GetResult());
                Assert.Null(exception);
            }
        }
    }

    [Fact(Timeout = 100)]
    public async Task TryNotifyRecord__КогдаЗаписьДобавляетсяДоВызова__ДолженВернутьTrue()
    {
        var factory = new ValueTaskSourceQueueSubscriberManagerFactory(10);
        var manager = factory.CreateQueueSubscriberManager();
        var expected = Record(123, "hello, world");
        using var subscriber = manager.GetSubscriber();

        var success = manager.TryNotifyRecord(expected);

        Assert.True(success);
    }

    [Fact(Timeout = 100)]
    public async Task TryNotifyRecord__КогдаНетОжидающих__ДолженВернутьFalse()
    {
        var factory = new ValueTaskSourceQueueSubscriberManagerFactory(10);
        var manager = factory.CreateQueueSubscriberManager();
        var expected = Record(123, "hello, world");

        var success = manager.TryNotifyRecord(expected);

        Assert.False(success);
    }

    [Fact(Timeout = 100)]
    public async Task WaitRecord__КогдаВремяОжиданияПревышается__ДолженВыкинутьOperationCancelledException()
    {
        var factory = new ValueTaskSourceQueueSubscriberManagerFactory(10);
        var manager = factory.CreateQueueSubscriberManager();
        using var subscriber = manager.GetSubscriber();
        using var cts = new CancellationTokenSource(TimeSpan.FromMilliseconds(50));

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => subscriber.WaitRecordAsync(cts.Token).AsTask());
    }

    [Theory(Timeout = 1000)]
    [InlineData(2)]
    [InlineData(10)]
    [InlineData(100)]
    [InlineData(200)]
    public async Task
        TryNotifyRecord__КогдаНесколькоУведомленийИЕдинственныйПодписчик__ЗаписиДолжныБытьПрочитаныКорректно(
        int writersCount)
    {
        var factory = new ValueTaskSourceQueueSubscriberManagerFactory(10);
        var manager = factory.CreateQueueSubscriberManager();
        var records = Enumerable.Range(0, 1000)
                                .Select(i => Record(i, i.ToString()))
                                .ToArray();
        var queue = new ConcurrentQueue<QueueRecord>(records);
        var readerReady = new ManualResetEvent(false);
        var writerThreads = Enumerable.Range(0, writersCount)
                                      .Select(_ => new Thread(() =>
                                       {
                                           readerReady.WaitOne();
                                           while (queue.TryDequeue(out var record))
                                           {
                                               while (!manager.TryNotifyRecord(record))
                                               {
                                               }
                                           }
                                       }))
                                      .ToArray();
        Array.ForEach(writerThreads, t => t.Start());

        var actual = await Task.Run(async () =>
        {
            var read = new List<QueueRecord>(records.Length);
            readerReady.Set();
            for (int i = 0; i < records.Length; i++)
            {
                using var subscriber = manager.GetSubscriber();
                var record = await subscriber.WaitRecordAsync();
                read.Add(record);
            }

            return read;
        });

        Assert.Equal(records.ToHashSet(), actual.ToHashSet());
        Array.ForEach(writerThreads, t => t.Join());
    }

    [Theory(Timeout = 200)]
    [InlineData(2)]
    [InlineData(5)]
    [InlineData(10)]
    [InlineData(100)]
    public async Task
        WaitRecord__КогдаЕстьНесколькоПодписчиковПринадлежащихРазнымМенеджерам__ЗаписьДолжнаПрийтиПодписчикуКоторыйПринадлежитУведомленныйМенеджер(
        int managersCount)
    {
        var factory = new ValueTaskSourceQueueSubscriberManagerFactory(10);
        var mainManager = factory.CreateQueueSubscriberManager();
        using var mainSubscriber = mainManager.GetSubscriber();
        var record = Record(123, "hello, world");
        var signal = new CountdownEvent(managersCount);
        var consumeResultTcs = new TaskCompletionSource<QueueRecord>();
        _ = Enumerable.Range(0, managersCount)
                      .Select(_ => Task.Run(async () =>
                       {
                           var manager = factory.CreateQueueSubscriberManager();
                           using var additionalSubscriber = manager.GetSubscriber();
                           signal.Signal();
                           await additionalSubscriber.WaitRecordAsync();
                           consumeResultTcs.SetException(
                               new NeverThrownException("Подписчик другого менеджера получил запись"));
                       }))
                      .ToArray();
        _ = Task.Run(async () =>
        {
            signal.Wait();
            var consumed = await mainSubscriber.WaitRecordAsync();
            consumeResultTcs.SetResult(consumed);
        });

        mainManager.TryNotifyRecord(record);

        var actual = await consumeResultTcs.Task;
        Assert.Equal(record, actual);
    }

    [Fact(Timeout = 200)]
    public async Task WaitRecord__КогдаПодписчикПолученИзПулаОтСтарогоМенеджера__ДолженПолучатьЗаписиОтНовогоМенеджера()
    {
        var factory = new ValueTaskSourceQueueSubscriberManagerFactory(1);
        var oldManager = factory.CreateQueueSubscriberManager();
        var newManager = factory.CreateQueueSubscriberManager();
        var oldRecord = Record(-12354, "asdfasdfasdf");
        var newRecord = Record(4565, "new record");

        // Работа старого менеджера
        {
            using var oldSubscriber = oldManager.GetSubscriber();
            oldManager.TryNotifyRecord(oldRecord);
            var oldActual = await oldSubscriber.WaitRecordAsync();
            Assert.Equal(oldRecord, oldActual);
        }

        // Работа нового менеджера
        {
            using var newSubscriber = newManager.GetSubscriber();
            newManager.TryNotifyRecord(newRecord);
            var newActual = await newSubscriber.WaitRecordAsync();
            Assert.Equal(newRecord, newActual);
        }
    }

    /// <summary>
    /// Маркерный класс исключения для логики проверки в тестах
    /// </summary>
    private class NeverThrownException(string message, Exception? inner = null) : Exception(message, inner);
}