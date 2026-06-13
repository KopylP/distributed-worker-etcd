using DistributedWorker.Cluster;
using Microsoft.Extensions.Options;
using System.Threading.Channels;

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
        Task? processingTask = null;

        try
        {
            // Merge the two channels the worker cares about: lease and segments
            var merged = MergeChannels(state.LeaseStateChanged, state.SegmentsChanged, stoppingToken);

            await foreach (var _ in merged.ReadAllAsync(stoppingToken))
            {
                // Cancel and dispose previous processing
                if (processingCts != null)
                {
                    await processingCts.CancelAsync();

                    // Await the task so we observe any exceptions
                    if (processingTask != null)
                    {
                        try
                        {
                            await processingTask;
                        }
                        catch (OperationCanceledException)
                        {
                            // Expected when cancelling
                        }
                    }

                    processingCts.Dispose();
                    processingCts = null;
                    processingTask = null;
                }

                if (!state.HasLease())
                {
                    log.LogWarning("Node {node} lost lease — waiting to reacquire...", state.NodeId);
                    continue;
                }

                processingCts = CancellationTokenSource.CreateLinkedTokenSource(stoppingToken);
                processingTask = RunProcessingLoopAsync(processingCts.Token);
            }
        }
        finally
        {
            // Cleanup on shutdown
            if (processingCts != null)
            {
                await processingCts.CancelAsync();

                if (processingTask != null)
                {
                    try
                    {
                        await processingTask;
                    }
                    catch (OperationCanceledException)
                    {
                        // Expected
                    }
                }

                processingCts.Dispose();
            }
        }
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
