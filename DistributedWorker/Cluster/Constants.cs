namespace DistributedWorker.Cluster;

public static class Constants
{
    public const string ElectionPrefix = "election/cluster";

    public const string ClusterPrefix = "cluster/";
    public const string AssignmentsKey = "cluster/segments";
    public const string ClusterLeasesKey = "cluster/lease";
}