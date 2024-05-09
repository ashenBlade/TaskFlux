using System.Diagnostics;
using System.Net;
using Serilog;
using TaskFlux.Core;
using TaskFlux.Transport.Tcp.Client;
using TaskFlux.Transport.Tcp.Client.Exceptions;

// ReSharper disable once AccessToDisposedClosure

using var totalCts = new CancellationTokenSource();
Log.Logger = new LoggerConfiguration()
    .WriteTo.Console()
    .CreateLogger();

Console.CancelKeyPress += (_, e) =>
{
    Console.WriteLine($"Получен Ctrl-C. Заканчиваю работу");
    totalCts.Cancel();
    e.Cancel = true;
};

Log.Debug("Подключась к кластеру");
var clientFactory = new TaskFluxClientFactory(new EndPoint[]
{
    new DnsEndPoint("localhost", 8080), new DnsEndPoint("localhost", 8081), new DnsEndPoint("localhost", 8082),
});

Log.Debug("Создаю клиентов");
await using var consumer = await clientFactory.ConnectAsync(totalCts.Token);
await using var producer = await clientFactory.ConnectAsync(totalCts.Token);


using var operationsCts = CancellationTokenSource.CreateLinkedTokenSource(totalCts.Token);
var workTime = TimeSpan.FromMinutes(1);
operationsCts.CancelAfter(workTime);

Log.Debug("Запускаю процессы. Время работы: {WorkTime}. Нажми Ctrl-C для преждевременного окончания", workTime);

var watch = Stopwatch.StartNew();
var results = await Task.WhenAll(RunConsumerAsync(consumer, operationsCts.Token),
    RunProducerAsync(producer, operationsCts.Token));
watch.Stop();

var consumerOperations = results[0];
var producerOperations = results[1];
var totalOperations = consumerOperations + producerOperations;

Console.WriteLine($"Всего операций: {totalOperations}");
Console.WriteLine($"  - Потребитель: {consumerOperations}");
Console.WriteLine($"  - Производитель: {producerOperations}");
Console.WriteLine($"Затраченное время, мс: {Math.Round(watch.Elapsed.TotalMilliseconds, 2)}");
var operationsPerSecond = totalOperations / watch.Elapsed.TotalSeconds;
Console.WriteLine($"Операций в секунду: {Math.Round(operationsPerSecond, 2)}");

return;

static async Task<int> RunConsumerAsync(ITaskFluxClient consumer, CancellationToken token)
{
    var operations = 0;
    while (!token.IsCancellationRequested)
    {
        try
        {
            await consumer.DequeueAsync(QueueName.Default, token);
        }
        catch (QueueEmptyException)
        {
        }
        catch (OperationCanceledException)
        {
            break;
        }

        operations++;
    }

    return operations;
}

static async Task<int> RunProducerAsync(ITaskFluxClient producer, CancellationToken token)
{
    var message = "Hello, world! This is sample message"u8.ToArray();
    var priority = 123L;

    var operations = 0;
    while (!token.IsCancellationRequested)
    {
        try
        {
            await producer.EnqueueAsync(QueueName.Default, priority, message, token);
        }
        catch (OperationCanceledException)
        {
            break;
        }

        operations++;
    }

    return operations;
}