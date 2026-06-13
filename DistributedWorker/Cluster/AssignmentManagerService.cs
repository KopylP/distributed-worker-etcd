using System.Text.Json;
using System.Threading.Channels;
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

        // Merge membership and leadership channels since both require re-assignment
        var clusterChanged = MergeChannels(
            state.MembershipChanged, state.LeadershipChanged, stoppingToken);

        await foreach (var _ in clusterChanged.ReadAllAsync(stoppingToken))
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
            var nodes = state.GetKnownNodes().OrderBy(n => n).ToList();

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

    /// <summary>
    /// Merges two ChannelReaders into one, forwarding values from both into a single output channel.
    /// </summary>
    private static ChannelReader<bool> MergeChannels(
        ChannelReader<bool> channel1,
        ChannelReader<bool> channel2,
        CancellationToken token)
    {
        var merged = Channel.CreateUnbounded<bool>();

        _ = ForwardChannel(channel1, merged.Writer, token);
        _ = ForwardChannel(channel2, merged.Writer, token);

        return merged.Reader;

        static async Task ForwardChannel(
            ChannelReader<bool> source,
            ChannelWriter<bool> target,
            CancellationToken ct)
        {
            try
            {
                await foreach (var item in source.ReadAllAsync(ct))
                {
                    target.TryWrite(item);
                }
            }
            catch (OperationCanceledException)
            {
                // Expected on shutdown
            }
        }
    }
}