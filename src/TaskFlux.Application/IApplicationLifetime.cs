namespace TaskFlux.Application;

public interface IApplicationLifetime
{
    public void Stop();
    public void StopAbnormal();
}