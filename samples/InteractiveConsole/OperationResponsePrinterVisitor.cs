using System.Text;
using Serilog;
using TaskFlux.Commands.Count;
using TaskFlux.Commands.Dequeue;
using TaskFlux.Commands.Error;
using TaskFlux.Commands.ListQueues;
using TaskFlux.Commands.Ok;
using TaskFlux.Commands.PolicyViolation;
using TaskFlux.Commands.Visitors;
using TaskFlux.Core.Policies;

namespace InteractiveConsole;

public class OperationResponsePrinterVisitor : IResponseVisitor
{
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
                                  ? $"   Максимальный размер: {metadata.MaxQueueSize}"
                                  : $"   Максимальный размер: -");
            Console.WriteLine();
        }
    }

    public void Visit(PolicyViolationResponse response)
    {
        var message = response.ViolatedPolicy.Accept(MessageFormatterQueuePolicyVisitor.Instance);
        Console.WriteLine($"Нарушено ограничение очереди: {message}");
    }

    private class MessageFormatterQueuePolicyVisitor : IQueuePolicyVisitor<string>
    {
        public static readonly MessageFormatterQueuePolicyVisitor Instance = new();

        public string Visit(PriorityRangeQueuePolicy policy)
        {
            return $"Превышен допустимый диапазон ключей. Разрешенный диапазон: {policy.Min} : {policy.Max}";
        }

        public string Visit(MaxQueueSizeQueuePolicy policy)
        {
            return $"Превышен максимальный размер очереди. Максимальный размер: {policy.MaxQueueSize}";
        }

        public string Visit(MaxPayloadSizeQueuePolicy policy)
        {
            return $"Превышен максимальный размер сообщения. Максимальный размер: {policy.MaxPayloadSize}";
        }
    }
}