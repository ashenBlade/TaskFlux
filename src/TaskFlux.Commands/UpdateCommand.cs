namespace TaskFlux.Commands;

public abstract class UpdateCommand : Command
{
    public override bool IsReadOnly => false;
}