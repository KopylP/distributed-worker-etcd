using dotnet_etcd.interfaces;
using Etcdserverpb;
using Google.Protobuf;
using Microsoft.Extensions.Options;

namespace DistributedWorker.Cluster;

public class LeaseManagerService(
    IEtcdClient etcd,
    ClusterState state,
    ILogger<LeaseManagerService> log,
    IOptions<ClusterOptions> options)
    : BackgroundService
{
    private readonly ClusterOptions _options = options.Value;

    /// <summary>
    /// Number of consecutive keep-alive failures before fully resetting the lease.
    /// A single transient network error shouldn't throw away a still-valid lease.
    /// </summary>
    private const int MaxKeepAliveFailures = 3;

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        log.LogInformation("LeaseManager starting for node {node}", state.NodeId);

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                if (!state.HasLease())
                {
                    await AcquireLeaseAsync(stoppingToken);
                    if (!state.HasLease())
                    {
                        await Task.Delay(TimeSpan.FromSeconds(_options.LeaseRetryDelaySeconds), stoppingToken);
                        continue;
                    }
                }

                await KeepAlive(stoppingToken);
                await Task.Delay(
                    TimeSpan.FromSeconds(_options.LeaseKeepAliveIntervalSeconds), stoppingToken);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                break;
            }
        }
    }

    private int _consecutiveKeepAliveFailures;

    private async Task KeepAlive(CancellationToken stoppingToken)
    {
        try
        {
            await etcd.LeaseKeepAlive(state.GetLeaseId(), cancellationToken: stoppingToken);

            _consecutiveKeepAliveFailures = 0;
            log.LogTrace("Lease {lease} refreshed", state.GetLeaseId());
        }
        catch (Grpc.Core.RpcException rpcEx) when (rpcEx.StatusCode == Grpc.Core.StatusCode.NotFound)
        {
            // Lease confirmed expired/revoked on server — reset immediately
            log.LogWarning("Lease {lease} confirmed revoked (NOT_FOUND), resetting immediately",
                state.GetLeaseId());
            _consecutiveKeepAliveFailures = 0;
            state.ResetLease();
        }
        catch (Exception e)
        {
            _consecutiveKeepAliveFailures++;

            if (_consecutiveKeepAliveFailures >= MaxKeepAliveFailures)
            {
                log.LogWarning(e,
                    "Keep-alive failed {count} consecutive times for lease {lease} — resetting lease",
                    _consecutiveKeepAliveFailures, state.GetLeaseId());
                _consecutiveKeepAliveFailures = 0;
                state.ResetLease();

                await Task.Delay(TimeSpan.FromSeconds(_options.LeaseRetryDelaySeconds), stoppingToken);
            }
            else
            {
                log.LogWarning(e,
                    "Keep-alive attempt {count}/{max} failed for lease {lease} — will retry",
                    _consecutiveKeepAliveFailures, MaxKeepAliveFailures, state.GetLeaseId());
            }
        }
    }

    private async Task AcquireLeaseAsync(CancellationToken token)
    {
        try
        {
            log.LogInformation("Node {node} acquiring lease...", state.NodeId);

            var leaseGrantRequest = new LeaseGrantRequest
            {
                TTL = _options.LeaseTtlSeconds
            };
            var leaseResponse = await etcd.LeaseGrantAsync(leaseGrantRequest, cancellationToken: token);
            var leaseId = leaseResponse.ID;

            var putRequest = new PutRequest()
            {
                Key = ByteString.CopyFromUtf8($"{Constants.ClusterLeasesKey}/{state.NodeId}"),
                Value = ByteString.CopyFromUtf8("active"),
                Lease = leaseId
            };
            await etcd.PutAsync(putRequest, cancellationToken: token);

            state.SetupLease(leaseId);

            // Ensure this node is in the known-nodes set immediately.
            // The WatchService may miss the PUT event for our own lease key
            // if the watch wasn't fully established yet (race condition).
            state.AddKnownNode(state.NodeId);

            log.LogInformation("Node {node} acquired lease {lease}", state.NodeId, leaseId);
        }
        catch (Exception ex)
        {
            log.LogError(ex, "Failed to acquire lease for node {node}", state.NodeId);
            state.ResetLease();
        }
    }

    public override async Task StopAsync(CancellationToken cancellationToken)
    {
        log.LogInformation("Stopping LeaseManager for node {node}", state.NodeId);

        if (state.GetLeaseId() != 0)
        {
            try
            {
                var leaseRevokeRequest = new LeaseRevokeRequest { ID = state.GetLeaseId() };
                await etcd.LeaseRevokeAsync(leaseRevokeRequest, cancellationToken: cancellationToken);
            }
            catch (Exception ex)
            {
                log.LogWarning(ex, "Failed to revoke lease on shutdown");
            }
        }

        await base.StopAsync(cancellationToken);
    }
}