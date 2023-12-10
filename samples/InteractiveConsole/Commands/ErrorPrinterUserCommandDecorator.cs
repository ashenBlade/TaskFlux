using TaskFlux.Client;
using TaskFlux.Client.Exceptions;
using TaskFlux.Network.Responses.Policies;

namespace InteractiveConsole.Commands;

public class ErrorPrinterUserCommandDecorator : UserCommand
{
    private readonly UserCommand _command;

    public ErrorPrinterUserCommandDecorator(UserCommand command)
    {
        _command = command;
    }

    public override async Task Execute(ITaskFluxClient client, CancellationToken token)
    {
        try
        {
            await _command.Execute(client, token);
        }
        catch (QueueAlreadyExistsException)
        {
            Console.WriteLine($"Указанная очередь уже существует");
        }
        catch (QueueEmptyException)
        {
            Console.WriteLine($"Очередь пуста");
        }
        catch (QueueNotExistException)
        {
            Console.WriteLine($"Очередь не существует");
        }
        catch (NotLeaderException)
        {
            Console.WriteLine($"Узел перестал быть лидером");
        }
        catch (ErrorResponseException ere)
        {
            Console.WriteLine($"Ошибка во время выполнения команды: {ere.ErrorMessage}");
        }
        catch (UnexpectedPacketException upe)
        {
            Console.WriteLine($"От узла получен неожиданный пакет: {upe.Actual}. Ожидался: {upe.Expected}");
        }
        catch (UnexpectedResponseException ure)
        {
            Console.WriteLine($"От узла получен неожиданный ответ: {ure.Actual}. Ожидался: {ure.Expected}");
        }
        catch (PolicyViolationException pve)
        {
            string policyMessage;
            switch (pve.Policy.Code)
            {
                case NetworkPolicyCode.Generic:
                    var genericPolicy = ( GenericNetworkQueuePolicy ) pve.Policy;
                    policyMessage = $"{genericPolicy.Message}";
                    break;
                case NetworkPolicyCode.MaxQueueSize:
                    var maxQueueSizePolicy = ( MaxQueueSizeNetworkQueuePolicy ) pve.Policy;
                    policyMessage = $"превышен максимальный размер очереди: ({maxQueueSizePolicy.MaxQueueSize})";
                    break;
                case NetworkPolicyCode.PriorityRange:
                    var priorityRangePolicy = ( PriorityRangeNetworkQueuePolicy ) pve.Policy;
                    policyMessage =
                        $"превышен допустимый диапазон ключей: {priorityRangePolicy.Min} - {priorityRangePolicy.Max}";
                    break;
                case NetworkPolicyCode.MaxMessageSize:
                    var maxMessageSizePolicy = ( MaxMessageSizeNetworkQueuePolicy ) pve.Policy;
                    policyMessage =
                        $"превышен максимальный размер сообщения: {maxMessageSizePolicy.MaxMessageSize} байт";
                    break;
                default:
                    policyMessage = "неизвестная политика";
                    break;
            }

            Console.WriteLine($"При выполнении команды была нарушена политика -  {policyMessage}");
        }
    }
}