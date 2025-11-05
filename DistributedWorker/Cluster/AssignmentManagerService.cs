using System.Text.Json;
using DistributedWorker.Services;
using dotnet_etcd.interfaces;
using Etcdserverpb;
using Google.Protobuf;
using Microsoft.Extensions.Options;

namespace DistributedWorker.Cluster;

public class AssignmentManagerService(
    IEtcdClient etcd,
    ClusterState state,
    ILogger<AssignmentManagerService> log,
    IOptions<ClusterOptions> options)
    : BackgroundService
{
    private readonly ClusterOptions _options = options.Value;

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        log.LogInformation("AssignmentManager starting for node {node}", state.NodeId);

        await foreach (var _ in state.ClusterChanged.ReadAllAsync(stoppingToken))
        {
            if (!state.IsLeader()) continue;
            
            log.LogInformation("Node {node} is leader, starting assignment management", state.NodeId);
            await RefreshAssignmentsAsync(stoppingToken);
        }
    }

    private async Task RefreshAssignmentsAsync(CancellationToken token)
    {
        try
        {
            var nodes = state.GetKnownNodes();

            if (nodes.Count == 0)
            {
                log.LogWarning("No active nodes found, skipping assignment");
                return;
            }

            var assignments = SegmentDistributor
                .DistributeEvenly(nodes, _options.TotalSegments);
            var json = JsonSerializer.Serialize(assignments);

            var putRequest = new PutRequest
            {
                Key = ByteString.CopyFromUtf8(Constants.AssignmentsKey),
                Value = ByteString.CopyFromUtf8(json),
                Lease = state.GetLeaseId()
            };
            await etcd.PutAsync(putRequest, cancellationToken: token);

            log.LogInformation("Leader {node} updated assignments: {count} nodes, {segments} segments",
                state.NodeId, nodes.Count, _options.TotalSegments);
        }
        catch (Exception ex)
        {
            log.LogError(ex, "Failed to refresh assignments");
        }
    }
}