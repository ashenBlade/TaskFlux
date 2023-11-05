using System.Text;
using Serilog;
using TaskFlux.Commands.Count;
using TaskFlux.Commands.Dequeue;
using TaskFlux.Commands.Enqueue;
using TaskFlux.Commands.Error;
using TaskFlux.Commands.ListQueues;
using TaskFlux.Commands.Ok;
using TaskFlux.Commands.Visitors;

namespace InteractiveConsole;

public class OperationResponsePrinterVisitor : IResponseVisitor
{
    public void Visit(EnqueueResponse response)
    {
        if (response.Success)
        {
            Console.WriteLine($"Запись успешно добавлена");
        }
        else
        {
            Console.WriteLine($"Запись не была добавлена в очередь");
        }
    }

    public void Visit(DequeueResponse response)
    {
        if (response.Success)
        {
            try
            {
                var str = Encoding.UTF8.GetString(response.Payload);
                Console.WriteLine($"Получена запись: Ключ = {response.Key}; Данные = {str}");
                return;
            }
            catch (DecoderFallbackException)
            {
            }

            Log.Warning("Ошибка при декодированни нагрузки в строку");
            Console.WriteLine(
                $"Получена запись: Ключ = {response.Key}; Данные = {BitConverter.ToString(response.Payload).Replace("-", "")}");
        }
        else
        {
            Console.WriteLine($"Очередь пуста");
        }
    }

    public void Visit(CountResponse response)
    {
        Console.WriteLine($"Размер очереди: {response.Count}");
    }

    public void Visit(ErrorResponse response)
    {
        Console.WriteLine($"Ошибка: {response.Message}");
    }

    public void Visit(OkResponse response)
    {
        Console.WriteLine($"Команда выполнена");
    }

    public void Visit(ListQueuesResponse response)
    {
        Console.WriteLine($"Хранящиеся очереди:");
        foreach (var metadata in response.Metadata)
        {
            Console.WriteLine($" - Название: {metadata.QueueName.Name}");
            Console.WriteLine($"   Размер: {metadata.Count}");
            Console.WriteLine(metadata.HasMaxSize
                                  ? $"   Максимальный размер: {metadata.MaxSize}"
                                  : $"   Максимальный размер: -");
            Console.WriteLine();
        }
    }
}