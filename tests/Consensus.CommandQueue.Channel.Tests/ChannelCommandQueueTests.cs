using Moq;

// ReSharper disable AccessToDisposedClosure
namespace Consensus.CommandQueue.Channel.Tests;

public class ChannelCommandQueueTests
{
    private static ChannelCommandQueue StartQueue()
    {
        var mock = new Mock<ChannelCommandQueue>(MockBehavior.Default);
        var cts = new CancellationTokenSource();

        mock.Setup(x => x.Dispose()).Callback(() =>
        {
            cts.Cancel();
            cts.Dispose();
        });
        var queue = mock.Object;
        _ = queue.RunAsync(CancellationToken.None);
        return queue;
    }
    
    [Fact]
    public async Task КогдаТокенОтменяется__ДолженЗакрытьсяБезВыкидыванияИсключений()
    {
        using var channel = new ChannelCommandQueue();
        using var cts = new CancellationTokenSource();
        var task = channel.RunAsync(cts.Token);
        var command = new Mock<ICommand>();
        command.Setup(x => x.Execute()).Verifiable();
        channel.Enqueue(command.Object);
        
        command.Verify(x => x.Execute(), Times.Once());
        cts.Cancel();
        try
        {
            await task;
        }
        catch (Exception e)
        {
            Assert.True(false, $"Было выкинуто исключение: {e}");
        }
    }
    [Fact]
    public void КогдаЕдинственнаяКомандаДобавлена__ОнаДолжнаБытьВыполнена()
    {
        using var queue = StartQueue();
        var command = new Mock<ICommand>();
        command.Setup(x => x.Execute()).Verifiable();
        queue.Enqueue(command.Object);
        command.Verify(x => x.Execute(), Times.Once());
    }

    [Theory]
    [InlineData(2)]
    [InlineData(3)]
    [InlineData(4)]
    [InlineData(5)]
    public void КогдаНесколькоКомандВыполняетсяПоследовательно__ВсеДолжныБытьВыполнены(int commandsCount)
    {
        using var queue = StartQueue();
        var commands = Enumerable.Range(0, commandsCount)
                                 .Select(_ =>
                                  {
                                      var mock = new Mock<ICommand>();
                                      mock.Setup(c => c.Execute()).Verifiable();
                                      return mock;
                                  })
                                 .ToArray();
        foreach (var command in commands)
        {
            queue.Enqueue(command.Object);
        }
        foreach (var command in commands)
        {
            command.Verify(x => x.Execute(), Times.Once());
        }
    }

    [Theory]
    [InlineData(2)]
    [InlineData(3)]
    [InlineData(5)]
    [InlineData(10)]
    [InlineData(20)]
    [InlineData(100)]
    [InlineData(500)]
    [InlineData(1000)]
    [InlineData(1500)]
    [InlineData(2000)]
    public void КогдаНесколькоКомандВыполняетсяПараллельно__ВсеДолжныБытьВыполнены(int commandsCount)
    {
        using var queue = StartQueue();
        var commands = Enumerable.Range(0, commandsCount)
                                 .Select(_ =>
                                  {
                                      var mock = new Mock<ICommand>();
                                      mock.Setup(c => c.Execute()).Verifiable();
                                      return mock;
                                  })
                                 .ToArray();

        Parallel.ForEachAsync(commands, async (mock, _) =>
        {
            if (Random.Shared.Next(0, 2) == 0)
            {
                await Task.Yield();
            }

            queue.Enqueue(mock.Object);
        });
        
        // Не все успеют выполниться
        Task.Delay(50).Wait();
        
        foreach (var command in commands)
        {
            command.Verify(x => x.Execute(), Times.Once());
        }
    }

    [Theory]
    [InlineData(5)]
    [InlineData(10)]
    [InlineData(15)]
    [InlineData(50)]
    [InlineData(100)]
    [InlineData(200)]
    [InlineData(300)]
    [InlineData(500)]
    [InlineData(1000)]
    [InlineData(1500)]
    [InlineData(2000)]
    public void КогдаНесколькоКомандВыполняетсяПараллельно__КаждаяКомандаДолжнаВыполнятьсяПоОднойЗаРаз(int commandsCount)
    {
        using var queue = StartQueue();
        var counter = 0;
        var source = new LambdaCommandSource(() => counter++);
        var commands = Enumerable.Range(0, commandsCount)
                                 .Select(_ => source.CreateCommand())
                                 .ToArray();
        
        Parallel.ForEachAsync(commands, async (c, _) =>
        {
            if (Random.Shared.Next(0, 2) == 0)
            {
                await Task.Yield();
            }
            queue.Enqueue(c);
        });
        
        Task.Delay(100).GetAwaiter().GetResult();
        
        Assert.Equal(commandsCount, counter);
    }
}