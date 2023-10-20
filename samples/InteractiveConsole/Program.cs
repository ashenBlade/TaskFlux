using System.Net;
using InteractiveConsole;
using Serilog;
using TaskFlux.Commands;
using TaskFlux.Network.Client;
using TaskFlux.Network.Client.Exceptions;

// ReSharper disable AccessToDisposedClosure

/*
 * Пример интерактивного клиента.
 * Взаимодействие осуществляется через консоль.
 *
 * В данном примере используется кластер из одного узла.
 * Запусить узел можно из docker-compose.yaml, расположенного в директории проекта.
 */

using var cts = new CancellationTokenSource();

Log.Logger = new LoggerConfiguration()
            .WriteTo.Console()
            .CreateLogger();

Console.CancelKeyPress += (_, eventArgs) =>
{
    Log.Logger.Information($"Нажат Ctrl-C. Закрываю приложение");
    cts.Cancel();
    eventArgs.Cancel = true;
};

var clientFactory = new TaskFluxClientFactory(new EndPoint[] {new DnsEndPoint("localhost", 8080),});

Log.Logger.Debug($"Создаю клиента");
using var client = await CreateClientAsync(clientFactory, cts.Token);
Log.Logger.Debug("Клиент создан");

var resultPrinterVisitor = new OperationResultPrinterVisitor();

while (!cts.IsCancellationRequested)
{
    // 1. Читаю команду
    string commandString;
    try
    {
        var input = await ReadInputAsync(cts.Token);
        if (input is null)
        {
            break;
        }

        commandString = input;
    }
    catch (OperationCanceledException)
    {
        break;
    }


    if (commandString.StartsWith("help", StringComparison.InvariantCultureIgnoreCase))
    {
        PrintHelp();
        continue;
    }

    if (StringCommandParser.TryParseCommand(commandString, out var command))
    {
        Result result;
        try
        {
            result = await client.SendAsync(command, cts.Token);
        }
        catch (TaskFluxException tfe)
        {
            Log.Fatal(tfe, "Необработанное исключение во время обработки команды");
            break;
        }
        catch (OperationCanceledException)
        {
            break;
        }

        result.Accept(resultPrinterVisitor);
    }
    else
    {
        Console.WriteLine($"Ошибка парсинга команды");
    }
}


return;

static async Task<ITaskFluxClient> CreateClientAsync(ITaskFluxClientFactory factory, CancellationToken token)
{
    try
    {
        return await factory.CreateClientAsync(token);
    }
    catch (TaskFluxException e)
    {
        Log.Logger.Fatal(e, "Ошибка при создани клиента");
        throw;
    }
}

static void PrintHelp()
{
    Console.WriteLine($"Использование: COMMAND [ARGS...]");
    Console.WriteLine($"Основные команды:");
    foreach (var commandDescription in GetCommandDescriptions())
    {
        Console.WriteLine(commandDescription);
    }
}

static IEnumerable<string> GetCommandDescriptions()
{
    yield return """
                  - enqueue [QUEUE_NAME] KEY VALUES... - Вставить элемент в очередь
                 QUEUE_NAME - название очереди. Пропустить, если использовать стандартную
                 KEY - ключ для вставляемого значения.
                 VALUES... - разделенные пробелом слова, которые будут добавлены в нагрузку
                 """;
    yield return """
                 - dequeue [QUEUE_NAME] - получить элемент из очереди
                 QUEUE_NAME - название очереди. Пропустить, если использовать стандартную
                 """;
    yield return """
                  - create QUEUE_NAME - создать новую очередь с указанным названием
                 QUEUE_NAME - название очереди
                 """;
    yield return """
                  - delete QUEUE_NAME - удалить очередь с указанным названием
                 QUEUE_NAME - название очереди
                 """;
    yield return """
                  - count [QUEUE_NAME] - получить размер очереди
                 QUEUE_NAME - название очереди. Пропустить, если использовать очередь по умолчанию
                 """;
    yield return """
                  - list - получить список всех очередей и их данных
                 """;
    yield return """
                  - help - вывести это сообщение
                 """;
}

async Task<string?> ReadInputAsync(CancellationToken token)
{
    // Консольные ReadAsync всегда блокирующие, нужны отдельные таски
    var readTask = Task.Run(Console.ReadLine);
    var waitTask = Task.Delay(Timeout.Infinite, token);
    await Task.WhenAny(readTask, waitTask);
    return token.IsCancellationRequested
               ? null
               : readTask.Result;
}