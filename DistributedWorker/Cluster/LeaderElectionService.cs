using dotnet_etcd;
using dotnet_etcd.interfaces;
using Google.Protobuf;
using Microsoft.Extensions.Options;
using V3Electionpb;

namespace DistributedWorker.Cluster;

public class LeaderElectionService(
    IEtcdClient etcd,
    ClusterState state,
    ILogger<LeaderElectionService> log,
    IOptions<ClusterOptions> options)
    : BackgroundService
{
    private readonly ClusterOptions _options = options.Value;

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        log.LogInformation("LeaderElection starting for node {node}", state.NodeId);
        
        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                if (!state.HasLease())
                {
                    if (state.IsLeader())
                    {
                        log.LogWarning("Node {node} lost lease while being leader", state.NodeId);
                        state.LoseLeadership();
                    }
                    
                    await Task.Delay(_options.ElectionLoopDelaySeconds, stoppingToken);
                    continue;
                }

                await ParticipateInElectionAsync(stoppingToken);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex)
            {
                log.LogError(ex, "Unexpected error during election");
                state.LoseLeadership();
                await Task.Delay(_options.ElectionLoopDelaySeconds, stoppingToken);
            }
        }
    }

    private async Task ParticipateInElectionAsync(CancellationToken token)
    {
        log.LogInformation("Node {node} joining election with lease {lease}", 
            state.NodeId, state.GetLeaseId());

        try
        {
            var campaignRequest = new CampaignRequest
            {
                Name = ByteString.CopyFromUtf8(Constants.ElectionPrefix),
                Value = ByteString.CopyFromUtf8(state.NodeId),
                Lease = state.GetLeaseId()
            };

            await etcd.CampaignAsync(campaignRequest, cancellationToken: token);
            
            state.BecomeLeader();
            log.LogInformation("Node {node} became LEADER", state.NodeId);
            await WaitForLeadershipLossAsync(token);
        }
        finally
        {
            state.LoseLeadership();
            log.LogInformation("Node {node} is no longer leader", state.NodeId);
        }
    }

    private async Task WaitForLeadershipLossAsync(CancellationToken token)
    {
        while (state.HasLease() && state.IsLeader() &&  !token.IsCancellationRequested)
        {
            await Task.Delay(_options.ElectionLoopDelaySeconds, token);
        }
    }

    public override async Task StopAsync(CancellationToken cancellationToken)
    {
        log.LogInformation("Stopping LeaderElection for node {node}", state.NodeId);

        if (state.IsLeader())
        {
            try
            {
                await etcd.ResignAsync(new ResignRequest
                {
                    Leader = new LeaderKey
                    {
                        Name = ByteString.CopyFromUtf8(Constants.ElectionPrefix),
                        Key = ByteString.CopyFromUtf8(state.NodeId),
                        Lease = state.GetLeaseId()
                    }
                }, cancellationToken: cancellationToken);
                
                log.LogInformation("Node {node} resigned from leadership", state.NodeId);
            }
            catch (Exception ex)
            {
                log.LogWarning(ex, "Failed to resign gracefully");
            }
        }

        await base.StopAsync(cancellationToken);
    }
}