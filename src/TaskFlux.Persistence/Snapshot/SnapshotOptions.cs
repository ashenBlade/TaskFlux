namespace TaskFlux.Persistence.Snapshot;

public class SnapshotOptions
{
    public const int DefaultSegmentsBeforeSnapshot = 10;
    public static SnapshotOptions Default => new(DefaultSegmentsBeforeSnapshot);

    public SnapshotOptions(int segmentsBeforeSnapshot)
    {
        if (segmentsBeforeSnapshot < 0)
        {
            throw new ArgumentOutOfRangeException(nameof(segmentsBeforeSnapshot), segmentsBeforeSnapshot,
                "Количество сегментов между снапшотом и закоммиченным не может быть отрицательным");
        }

        SegmentsBeforeSnapshot = segmentsBeforeSnapshot;
    }


    /// <summary>
    /// Количество сегментов между тем, что содержит индекс снапшота и тем, что содержит индекс коммита,
    /// после преодоления которого необходимо создать новый снапшот.  
    /// </summary>
    public int SegmentsBeforeSnapshot { get; }
}