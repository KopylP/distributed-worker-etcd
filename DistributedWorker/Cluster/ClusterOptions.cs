namespace DistributedWorker.Cluster;

public class ClusterOptions
{
    public int LeaseTtlSeconds { get; set; } = 15;
    public int LeaseKeepAliveIntervalSeconds { get; set; } = 5;
    public int LeaseRetryDelaySeconds { get; set; } = 3;
    public int ElectionLoopDelaySeconds { get; set; } = 2;
    public int WorkerProcessingIntervalSeconds { get; set; } = 10;
    public int TotalSegments { get; set; } = 10;
}