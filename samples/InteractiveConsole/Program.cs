using System.Net;
using InteractiveConsole;
using Serilog;
using TaskFlux.Client;

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

var clientFactory = new TaskFluxClientFactory(new EndPoint[]
{
    new DnsEndPoint("localhost", 8080), new DnsEndPoint("localhost", 8081), new DnsEndPoint("localhost", 8082),
});

Log.Logger.Debug($"Создаю клиента");
await using var client = await clientFactory.ConnectAsync(cts.Token);
Log.Logger.Debug("Клиент создан");

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

    try
    {
        var command = StringCommandParser.ParseCommand(commandString);
        try
        {
            await command.Execute(client, cts.Token);
        }
        catch (OperationCanceledException)
        {
            break;
        }
    }
    catch (Exception e)
    {
        Console.WriteLine($"Ошибка выполнения команды: {e.Message}");
    }
}


return;

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
                  - create QUEUE_NAME [WITHMAXSIZE max_size] [WITHMAXPAYLOAD max_payload] [WITHPRIORITYRANGE min max] [TYPE code] - создать новую очередь с указанным названием
                        QUEUE_NAME - название очереди
                        WITHMAXSIZE max_size - выставить ограничение на максимальный размер очереди в max_size
                        WITHMAXPAYLOAD max_payload - выставить ограничение на максимальный размер сообщения в max_payload байтов
                        WITHPRIORITYRANGE min max - ограничить допустимый диапазон выставляемых ключей с min до max включительно
                        TYPE code - использовать указанную реализацию структуры для хранения. code - код структуры
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