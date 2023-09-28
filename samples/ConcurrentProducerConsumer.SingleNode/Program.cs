using System.Diagnostics;
using System.Net;
using JobQueue.Core;
using Serilog;
using TaskFlux.Commands.Dequeue;
using TaskFlux.Commands.Enqueue;
using TaskFlux.Network.Client;

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

var clientFactory = new TaskFluxClientFactory(new EndPoint[] {new DnsEndPoint("localhost", 8080)});


Log.Debug("Создаю клиентов");
using var consumer = await clientFactory.CreateClientAsync(totalCts.Token);
using var producer = await clientFactory.CreateClientAsync(totalCts.Token);


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
    var dequeueCommand = new DequeueCommand(QueueName.Default);
    var operations = 0;
    while (!token.IsCancellationRequested)
    {
        try
        {
            await consumer.SendAsync(dequeueCommand, token);
            operations++;
        }
        catch (OperationCanceledException)
        {
            break;
        }
    }

    return operations;
}

static async Task<int> RunProducerAsync(ITaskFluxClient producer, CancellationToken token)
{
    var dequeueCommand = new EnqueueCommand(123, "Hello, world! This is sample message"u8.ToArray(), QueueName.Default);
    var operations = 0;
    while (!token.IsCancellationRequested)
    {
        try
        {
            await producer.SendAsync(dequeueCommand, token);
            operations++;
        }
        catch (OperationCanceledException)
        {
            break;
        }
    }

    return operations;
}