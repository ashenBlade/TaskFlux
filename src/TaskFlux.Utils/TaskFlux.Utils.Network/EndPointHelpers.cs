using System.Net;

namespace TaskFlux.Utils.Network;

public static class EndPointHelpers
{
    /// <summary>
    /// Спарсить строку в соответствующий <see cref="EndPoint"/>
    /// </summary>
    /// <param name="address">Строка с исходным адресом</param>
    /// <param name="defaultPort">Порт, который нужно задавать по умолчанию, если не был указан в адресе</param>
    /// <returns><see cref="EndPoint"/> представляемый этой строкой</returns>
    /// <exception cref="ArgumentNullException"><paramref name="address"/> - <c>null</c></exception>
    /// <exception cref="ArgumentException"><paramref name="address"/> представляет неверный адрес</exception>
    public static EndPoint ParseEndPoint(string address, int defaultPort = 2602)
    {
        ArgumentNullException.ThrowIfNull(address);
        if (IPEndPoint.TryParse(address, out var ipEndPoint))
        {
            if (ipEndPoint.Port == 0)
            {
                ipEndPoint.Port = defaultPort;
            }

            return ipEndPoint;
        }

        var semicolonIndex = address.IndexOf(':');
        int port;
        string host;
        if (semicolonIndex == -1)
        {
            port = defaultPort;
            host = address;
        }
        else if (int.TryParse(address.AsSpan(semicolonIndex + 1), out port))
        {
            host = address[..semicolonIndex];
        }
        else
        {
            throw new ArgumentException($"Не удалось спарсить адрес порта в адресе {address}");
        }

        if (Uri.CheckHostName(host) != UriHostNameType.Dns)
        {
            throw new ArgumentException(
                $"В переданной строке адреса указан невалидный DNS хост. Полный адрес: {address}. Хост: {host}");
        }

        return new DnsEndPoint(host, port);
    }
}