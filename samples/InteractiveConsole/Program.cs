﻿using System.Net;
using InteractiveConsole;
using Serilog;
using TaskFlux.Transport.Tcp.Client;
using TaskFlux.Utils.Network;

// ReSharper disable AccessToDisposedClosure

/*
 * Пример интерактивного клиента.
 * Взаимодействие осуществляется через консоль.
 *
 * В данном примере используется кластер из одного узла.
 * Запустить узел можно из docker-compose.yaml, расположенного в директории проекта.
 */

using var cts = new CancellationTokenSource();

Console.CancelKeyPress += (_, eventArgs) =>
{
    cts.Cancel();
    eventArgs.Cancel = true;
};

if (!TryGetEndpoints(out var endpoints))
{
    return 1;
}

var clientFactory = new TaskFluxClientFactory(endpoints);

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

    if (commandString.Equals("exit", StringComparison.InvariantCultureIgnoreCase))
    {
        break;
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

return 0;

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
    Console.Write("$> ");
    var readTask = Task.Run(Console.ReadLine);
    var waitTask = Task.Delay(Timeout.Infinite, token);
    await Task.WhenAny(readTask, waitTask);
    return token.IsCancellationRequested
        ? null
        : readTask.Result;
}

bool TryGetEndpoints(out EndPoint[] endpoints)
{
    if (args.Length == 0)
    {
        Console.WriteLine(
            $"Необходимо передать адреса узлов кластера: {AppDomain.CurrentDomain.FriendlyName} [АДРЕС ...]");
        endpoints = default!;
        return false;
    }

    endpoints = new EndPoint[args.Length];
    for (var i = 0; i < endpoints.Length; i++)
    {
        try
        {
            endpoints[i] = EndPointHelpers.ParseEndPoint(args[i]);
        }
        catch (ArgumentException ae)
        {
            Console.WriteLine($"Ошибка парсинга адреса {args[i]}");
            return false;
        }
    }

    return true;
}