using DistributedWorker.Cluster;
using Microsoft.Extensions.Options;

namespace DistributedWorker.Worker;

public class WorkerService(
    ClusterState state,
    ILogger<WorkerService> log,
    IOptions<ClusterOptions> options)
    : BackgroundService
{
    private readonly ClusterOptions _options = options.Value;

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        log.LogInformation("Worker starting for node {node}", state.NodeId);

        CancellationTokenSource? processingCts = null;

        await foreach (var _ in state.ProcessingStateChanged.ReadAllAsync(stoppingToken))
        {
            await CancelPreviousProcessingAsync(processingCts);

            if (!state.HasLease())
            {
                log.LogWarning("Node {node} lost lease — waiting to reacquire...", state.NodeId);
                continue;
            }

            processingCts = StartNewProcessingLoop(stoppingToken);
        }
    }

    private static async Task CancelPreviousProcessingAsync(CancellationTokenSource? cts)
    {
        if (cts != null)
            await cts.CancelAsync();
    }

    private CancellationTokenSource StartNewProcessingLoop(CancellationToken stoppingToken)
    {
        var cts = CancellationTokenSource.CreateLinkedTokenSource(stoppingToken);
        _ = RunProcessingLoopAsync(cts.Token);
        return cts;
    }

    private async Task RunProcessingLoopAsync(CancellationToken token)
    {
        using var timer = new PeriodicTimer(TimeSpan.FromSeconds(_options.WorkerProcessingIntervalSeconds));

        try
        {
            while (await timer.WaitForNextTickAsync(token))
            {
                if (!CanProcess())
                    continue;

                await TryProcessSegmentsAsync(token);
            }
        }
        catch (OperationCanceledException)
        {
            log.LogInformation("Processing stopped for node {node}", state.NodeId);
        }
    }

    private bool CanProcess()
    {
        if (!state.HasLease())
        {
            log.LogDebug("Node {node} has no lease, skipping iteration", state.NodeId);
            return false;
        }

        if (!state.GetMySegments().Any())
        {
            log.LogDebug("Node {node} has no segments assigned, skipping iteration", state.NodeId);
            return false;
        }

        return true;
    }

    private async Task TryProcessSegmentsAsync(CancellationToken token)
    {
        try
        {
            var segments = state.GetMySegments();
            await ProcessSegmentsAsync(segments, token);
        }
        catch (OperationCanceledException)
        {
            log.LogInformation("Processing cancelled for node {node}", state.NodeId);
        }
        catch (Exception ex)
        {
            log.LogError(ex, "Error in worker processing loop");
        }
    }

    private async Task ProcessSegmentsAsync(List<int> segments, CancellationToken token)
    {
        log.LogInformation("Node {node} processing {count} segments: [{segments}]",
            state.NodeId, segments.Count, string.Join(", ", segments));

        await Task.Delay(500, token); // Simulate work

        log.LogTrace("Segments processed successfully");
    }
}
