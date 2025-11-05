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

    private async Task KeepAlive(CancellationToken stoppingToken)
    {
        try
        {
            await etcd.LeaseKeepAlive(state.GetLeaseId(), cancellationToken: stoppingToken);
                    
            log.LogTrace("Lease {lease} refreshed", state.GetLeaseId());
        }
        catch (Exception e)
        {
            log.LogWarning(e, "Keep-alive request failed for lease {lease}", state.GetLeaseId());
            state.ResetLease();
            
            await Task.Delay(TimeSpan.FromSeconds(_options.LeaseRetryDelaySeconds), stoppingToken);
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