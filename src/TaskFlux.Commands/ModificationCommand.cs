namespace TaskFlux.Commands;

public abstract class ModificationCommand : Command
{
    public override bool UseFastPath => false;
}