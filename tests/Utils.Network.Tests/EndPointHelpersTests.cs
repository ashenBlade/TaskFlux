using System.Net;

namespace Utils.Network.Tests;

[Trait("Category", "Infrastructure")]
public class EndPointHelpersTests
{
    [Theory]
    [InlineData("123.123.123.123:8080", "123.123.123.123", 8080)]
    [InlineData("127.0.0.1:2622", "127.0.0.1", 2622)]
    [InlineData("192.168.120.22:9091", "192.168.120.22", 9091)]
    [InlineData("1.1.1.1:9000", "1.1.1.1", 9000)]
    public void ParseEndpoint__КогдаПереданIpАдрес__ДолженСпарситьIPEndPoint(
        string original,
        string ipAddress,
        int port)
    {
        var expected = new IPEndPoint(IPAddress.Parse(ipAddress), port);
        var actual = EndPointHelpers.ParseEndPoint(original);
        Assert.Equal(expected, actual);
    }

    [Theory]
    [InlineData("localhost:1233", "localhost", 1233)]
    [InlineData("docker.host.internal:2323", "docker.host.internal", 2323)]
    [InlineData("rfg43ty3gwew.yandex.cloud.ru:5678", "rfg43ty3gwew.yandex.cloud.ru", 5678)]
    public void ParseEndPoint__КогдаПереданDnsАдрес__ДолженСпарситьDnsEndPoint(string original, string host, int port)
    {
        var expected = new DnsEndPoint(host, port);
        var actual = EndPointHelpers.ParseEndPoint(original);
        Assert.Equal(expected, actual);
    }

    [Theory]
    [InlineData("host/x:123")]
    [InlineData("host :123")]
    [InlineData("hello@here:80")]
    [InlineData("hello.com:1234124")]
    public void ParseEndPoint__КогдаПереданНеверныйАдрес__ДолженВыкинутьArgumentException(string invalid)
    {
        var ex = Record.Exception(() => EndPointHelpers.ParseEndPoint(invalid));
        Assert.NotNull(ex);
    }

    [Theory]
    [InlineData("123.123.123.128", 1111)]
    [InlineData("localhost", 9000)]
    [InlineData("what.is.happening.com", 2602)]
    [InlineData("146.2.45.99", 7777)]
    public void ParseEndPoint__КогдаНеУказанПорт__ДолженВыставитьПортПоУмолчанию(string hostnameOrIp, int defaultPort)
    {
        var actual = EndPointHelpers.ParseEndPoint(hostnameOrIp, defaultPort: defaultPort);
        if (actual is IPEndPoint ipEndPoint)
        {
            Assert.Equal(defaultPort, ipEndPoint.Port);
        }
        else
        {
            var dnsEndpoint = ( DnsEndPoint ) actual;
            Assert.Equal(defaultPort, dnsEndpoint.Port);
        }
    }
}