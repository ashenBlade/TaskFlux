using System.Text;

namespace JobQueue.Core.Tests;

[Trait("Category", "BusinessLogic")]
public class QueueNameTests
{
    private static void AssertBase(string name)
    {
        var q = QueueName.Parse(name);
        Assert.Equal(q.Name, name);
    }

    private static void AssertErrorBase(string name)
    {
        var ex = Record.Exception(() => QueueName.Parse(name));
        Assert.NotNull(ex);
    }

    [Fact]
    public void Parse__КогдаСтрокаПуста__ДолженСпарсить()
    {
        AssertBase("");
    }

    [Fact]
    public void Parse__КогдаНазваниеИз1РазрешенногоСимвола__ДолженСпарситьКорректно()
    {
        foreach (var ch in QueueName.AllowedCharacters)
        {
            AssertBase(ch.ToString());
        }
    }

    [Theory]
    [InlineData("queue")]
    [InlineData("messages:main:1")]
    [InlineData("messages.main.1")]
    [InlineData("GRAPH.EVENTS>0")]
    [InlineData("GRAPH.EVENTS>5.30")]
    [InlineData("_")]
    [InlineData("-")]
    [InlineData("---hello---world---!")]
    public void Parse__КогдаСтрокаДлиныНеБольше255СДопустимымиСимволами__ДолженСпарситьКорректно(string name)
    {
        AssertBase(name);   
    }

    [Fact]
    public void Parse__КогдаСтрокаДлиной255__ДолженСпарситьКорректно()
    {
        AssertBase("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA");
    }

    [Fact]
    public void Parse__КогдаСтрокаИз1НедопустимогоСимвола__ДолженСпарситьСОшибкой()
    {
        var i = (char) 0;
        while (i < (char) 33)
        {
            AssertErrorBase(i.ToString());
            i++;
        }

        i = ( char ) 127;
        while (i != (char) 0)
        {
            AssertErrorBase(i.ToString());
            i++;
        }
    }

    [Theory]
    [InlineData(256)]
    [InlineData(257)]
    [InlineData(258)]
    [InlineData(300)]
    public void Parse__КогдаДлинаСтрокиБольше255__ДолженСпарситьСОшибкой(int length)
    {
        var str = string.Create(length, 'a', (span, c) => span.Fill(c));
        AssertErrorBase(str);
    }
}