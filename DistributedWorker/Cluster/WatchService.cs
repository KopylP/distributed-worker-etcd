using System.Text.Json;
using dotnet_etcd.interfaces;
using Etcdserverpb;
using Google.Protobuf;
using Mvccpb;

namespace DistributedWorker.Cluster;

public class WatchService(
    IEtcdClient etcd,
    ClusterState state,
    ILogger<WatchService> log)
    : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        log.LogInformation("Node {node} starting WatchService on {prefix}",
            state.NodeId, Constants.ClusterPrefix);

        // Phase 3: Load initial state before watching to avoid missing existing nodes.
        // We capture the revision so we can start the watch from this exact point,
        // eliminating any race conditions between the Get and Watch calls.
        long startRevision = await LoadInitialStateAsync(stoppingToken);

        await StartUnifiedWatchAsync(startRevision, stoppingToken);
    }

    public override Task StopAsync(CancellationToken cancellationToken)
    {
        log.LogInformation("Stopping WatchService for node {node}", state.NodeId);
        return base.StopAsync(cancellationToken);
    }

    /// <summary>
    /// Loads existing lease keys and assignments from etcd on startup,
    /// so we don't miss nodes that were registered before this watch started.
    /// Returns the etcd revision at the time of the read.
    /// </summary>
    private async Task<long> LoadInitialStateAsync(CancellationToken token)
    {
        while (!token.IsCancellationRequested)
        {
            try
            {
                var rangeEnd = GetPrefixRangeEnd(Constants.ClusterPrefix);
                var request = new RangeRequest
                {
                    Key = ByteString.CopyFromUtf8(Constants.ClusterPrefix),
                    RangeEnd = ByteString.CopyFromUtf8(rangeEnd)
                };

                var response = await etcd.GetAsync(request, cancellationToken: token);

                foreach (var kv in response.Kvs)
                {
                    var key = kv.Key.ToStringUtf8();

                    if (key.StartsWith(Constants.ClusterLeasesKey + "/"))
                    {
                        var nodeId = ExtractNodeIdFromKey(key);
                        state.AddKnownNode(nodeId);
                        log.LogInformation("Initial state: discovered node {nodeId}", nodeId);
                    }
                    else if (key == Constants.AssignmentsKey)
                    {
                        ProcessAssignments(kv.Value.ToStringUtf8());
                        log.LogInformation("Initial state: loaded assignments");
                    }
                }

                log.LogInformation("Initial state loaded: {count} keys processed at revision {rev}", 
                    response.Kvs.Count, response.Header.Revision);
                
                return response.Header.Revision;
            }
            catch (Exception ex)
            {
                log.LogWarning(ex, "Failed to load initial state — retrying in 5 seconds...");
                try
                {
                    await Task.Delay(5000, token);
                }
                catch (OperationCanceledException)
                {
                    break;
                }
            }
        }
        return 0;
    }

    private async Task StartUnifiedWatchAsync(long startRevision, CancellationToken token)
    {
        try
        {
            var request = new WatchRequest
            {
                CreateRequest = new WatchCreateRequest
                {
                    Key = ByteString.CopyFromUtf8(Constants.ClusterPrefix),
                    RangeEnd = ByteString.CopyFromUtf8(GetPrefixRangeEnd(Constants.ClusterPrefix)),
                    StartRevision = startRevision + 1 // Watch for events strictly AFTER the initial state
                }
            };

            // WatchAsync handles the gRPC stream and blocks until cancelled
            await etcd.WatchAsync(
                request,
                (WatchResponse response) => HandleClusterEvent(response),
                cancellationToken: token);
        }
        catch (Exception ex)
        {
            log.LogError(ex, "Failed to start unified watch");
            throw;
        }
    }

    private void HandleClusterEvent(WatchResponse response)
    {
        if (response.Events.Count == 0)
            return;

        foreach (var evt in response.Events)
        {
            HandleSingleEvent(evt);
        }
    }

    private void HandleSingleEvent(Event evt)
    {
        var key = evt.Kv.Key.ToStringUtf8();

        switch (key)
        {
            case var _ when key.StartsWith(Constants.ClusterLeasesKey + "/"):
                HandleLeaseEvent(evt);
                break;

            case Constants.AssignmentsKey:
                HandleAssignmentsEvent(evt);
                break;

            default:
                log.LogDebug("Unhandled etcd key event: {key}", key);
                break;
        }
    }

    private void HandleLeaseEvent(Event evt)
    {
        // Fix Issue #7: Use prefix-based extraction instead of fragile Split("/")[2]
        var nodeId = ExtractNodeIdFromKey(evt.Kv.Key.ToStringUtf8());

        switch (evt.Type)
        {
            case Event.Types.EventType.Put:
                OnNodeJoined(nodeId);
                break;
            case Event.Types.EventType.Delete:
                OnNodeLeft(nodeId);
                break;
        }
    }

    private void OnNodeJoined(string nodeId)
    {
        state.AddKnownNode(nodeId);
        log.LogInformation("Detected node joined: {nodeId}", nodeId);
    }

    private void OnNodeLeft(string nodeId)
    {
        state.RemoveKnownNode(nodeId);
        log.LogInformation("Detected node left: {nodeId}", nodeId);
    }

    private void HandleAssignmentsEvent(Event evt)
    {
        if (evt.Type == Event.Types.EventType.Delete)
        {
            log.LogWarning("Assignments key deleted");
            state.ResetMySegments();
            return;
        }

        ProcessAssignments(evt.Kv.Value.ToStringUtf8());
    }

    private void ProcessAssignments(string json)
    {
        if (string.IsNullOrWhiteSpace(json))
        {
            log.LogWarning("Received empty assignments payload");
            return;
        }

        var assignments = JsonSerializer.Deserialize<Dictionary<string, List<int>>>(json);

        if (assignments is null)
            return;

        var mySegments = ExtractMySegments(assignments);
        state.UpdateSegments(mySegments);

        log.LogInformation("Node {node} received new assignments: {count} segments [{list}]",
            state.NodeId, mySegments.Count, string.Join(", ", mySegments));
    }

    private List<int> ExtractMySegments(Dictionary<string, List<int>> assignments)
    {
        var hasAssignments = assignments.TryGetValue(state.NodeId, out var myAssignments);
        return hasAssignments ? myAssignments! : [];
    }

    /// <summary>
    /// Extracts the node ID from a full key like "cluster/lease/node123".
    /// Uses prefix-based substring instead of fragile Split/index.
    /// </summary>
    private static string ExtractNodeIdFromKey(string fullKey)
    {
        var prefix = Constants.ClusterLeasesKey + "/";
        return fullKey[prefix.Length..];
    }

    /// <summary>
    /// Computes the range end for a prefix scan (increment the last byte).
    /// </summary>
    private static string GetPrefixRangeEnd(string prefix)
    {
        var bytes = System.Text.Encoding.UTF8.GetBytes(prefix);
        bytes[^1]++;
        return System.Text.Encoding.UTF8.GetString(bytes);
    }
}
