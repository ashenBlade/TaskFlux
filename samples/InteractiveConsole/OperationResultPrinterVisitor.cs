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

public class OperationResultPrinterVisitor : IResultVisitor
{
    public void Visit(EnqueueResult result)
    {
        if (result.Success)
        {
            Console.WriteLine($"Запись успешно добавлена");
        }
        else
        {
            Console.WriteLine($"Запись не была добавлена в очередь");
        }
    }

    public void Visit(DequeueResult result)
    {
        if (result.Success)
        {
            try
            {
                var str = Encoding.UTF8.GetString(result.Payload);
                Console.WriteLine($"Получена запись: Ключ = {result.Key}; Данные = {str}");
                return;
            }
            catch (DecoderFallbackException)
            {
            }

            Log.Warning("Ошибка при декодированни нагрузки в строку");
            Console.WriteLine(
                $"Получена запись: Ключ = {result.Key}; Данные = {BitConverter.ToString(result.Payload).Replace("-", "")}");
        }
        else
        {
            Console.WriteLine($"Очередь пуста");
        }
    }

    public void Visit(CountResult result)
    {
        Console.WriteLine($"Размер очереди: {result.Count}");
    }

    public void Visit(ErrorResult result)
    {
        Console.WriteLine($"Ошибка: {result.Message}");
    }

    public void Visit(OkResult result)
    {
        Console.WriteLine($"Команда выполнена");
    }

    public void Visit(ListQueuesResult result)
    {
        Console.WriteLine($"Хранящиеся очереди:");
        foreach (var metadata in result.Metadata)
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