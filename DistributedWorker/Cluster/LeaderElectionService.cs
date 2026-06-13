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

    private static readonly TimeSpan InitialRetryDelay = TimeSpan.FromSeconds(1);
    private static readonly TimeSpan MaxRetryDelay = TimeSpan.FromSeconds(30);

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        log.LogInformation("LeaderElection starting for node {node}", state.NodeId);

        var retryDelay = InitialRetryDelay;

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                // Wait reactively for a lease instead of polling
                await state.WaitForLeaseAsync(stoppingToken);

                if (state.IsLeader())
                {
                    // Shouldn't happen, but guard against stale state
                    log.LogWarning("Node {node} still marked as leader before campaign", state.NodeId);
                    state.LoseLeadership();
                }

                await ParticipateInElectionAsync(stoppingToken);

                // Reset backoff on successful election cycle
                retryDelay = InitialRetryDelay;
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex)
            {
                log.LogError(ex, "Unexpected error during election");
                state.LoseLeadership();

                // Exponential backoff instead of fixed delay
                await Task.Delay(retryDelay, stoppingToken);
                retryDelay = TimeSpan.FromTicks(
                    Math.Min(retryDelay.Ticks * 2, MaxRetryDelay.Ticks));
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

            var response = await etcd.CampaignAsync(campaignRequest, cancellationToken: token);

            // Store the LeaderKey from CampaignResponse for proper resignation
            state.SetLeaderKey(response.Leader);
            state.BecomeLeader();
            log.LogInformation("Node {node} became LEADER", state.NodeId);

            // Wait reactively until leadership is lost (lease lost or flag cleared)
            await state.WaitForLeadershipLossAsync(token);
        }
        finally
        {
            state.SetLeaderKey(null);
            state.LoseLeadership();
            log.LogInformation("Node {node} is no longer leader", state.NodeId);
        }
    }

    public override async Task StopAsync(CancellationToken cancellationToken)
    {
        log.LogInformation("Stopping LeaderElection for node {node}", state.NodeId);

        if (state.IsLeader())
        {
            try
            {
                var leaderKey = state.GetLeaderKey();
                if (leaderKey != null)
                {
                    await etcd.ResignAsync(new ResignRequest
                    {
                        Leader = leaderKey
                    }, cancellationToken: cancellationToken);

                    log.LogInformation("Node {node} resigned from leadership", state.NodeId);
                }
                else
                {
                    log.LogWarning("Node {node} is leader but has no stored LeaderKey — cannot resign properly", state.NodeId);
                }
            }
            catch (Exception ex)
            {
                log.LogWarning(ex, "Failed to resign gracefully");
            }
        }

        await base.StopAsync(cancellationToken);
    }
}