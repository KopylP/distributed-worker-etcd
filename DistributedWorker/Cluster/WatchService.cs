using System.Text.Json;
using dotnet_etcd.interfaces;
using Etcdserverpb;
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
        log.LogInformation("Node {node} starting unified watch on {prefix}", 
            state.NodeId, Constants.ClusterPrefix);

        await StartUnifiedWatchAsync(stoppingToken);
    }

    public override Task StopAsync(CancellationToken cancellationToken)
    {
        log.LogInformation("Stopping WatchService for node {node}", state.NodeId);
        return base.StopAsync(cancellationToken);
    }

    private async Task StartUnifiedWatchAsync(CancellationToken token)
    {
        try
        {
            await etcd.WatchRangeAsync(
                Constants.ClusterPrefix,
                HandleClusterEvent,
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
            case var _ when key.StartsWith(Constants.ClusterLeasesKey):
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
        var key = evt.Kv.Key.ToStringUtf8().Split("/")[2];

        switch (evt.Type)
        {
            case Event.Types.EventType.Put:
                OnNodeJoined(key);
                break;
            case Event.Types.EventType.Delete:
                OnNodeLeft(key);
                break;
        }
    }

    private void OnNodeJoined(string key)
    {
        state.AddKnownNode(key);
        log.LogInformation("Detected node joined: {key}", key);
    }

    private void OnNodeLeft(string key)
    {
        state.RemoveKnownNode(key);
        log.LogInformation("Detected node left: {key}", key);
    }

    private void HandleAssignmentsEvent(Event evt)
    {
        if (!state.HasLease())
        {
            log.LogDebug("No lease, ignoring assignment update");
            return;
        }

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
        var hasAssigments = assignments.TryGetValue(state.NodeId, out var myAssigments);
        return hasAssigments ? myAssigments! : [];
    }
}
