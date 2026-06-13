using System.Collections.ObjectModel;
using System.Threading.Channels;
using V3Electionpb;

namespace DistributedWorker.Cluster;

public class ClusterState
{
    private readonly object _lock = new();
    private readonly HashSet<string> _knownNodes = [];
    private List<int> _mySegments = [];
    private long _leaseId;
    private bool _hasLease;
    private bool _isLeader;
    private LeaderKey? _leaderKey;

    // --- Split channels by concern (Phase 2, Issue #4) ---
    private readonly Channel<bool> _membershipChanged = Channel.CreateUnbounded<bool>();
    private readonly Channel<bool> _leadershipChanged = Channel.CreateUnbounded<bool>();
    private readonly Channel<bool> _segmentsChanged = Channel.CreateUnbounded<bool>();
    private readonly Channel<bool> _leaseStateChanged = Channel.CreateUnbounded<bool>();

    // --- Point-to-point signals for election service (Phase 1) ---
    private TaskCompletionSource _leaseAcquired = new(TaskCreationOptions.RunContinuationsAsynchronously);
    private TaskCompletionSource _leadershipLost = new(TaskCreationOptions.RunContinuationsAsynchronously);

    public string NodeId { get; } =
        Environment.GetEnvironmentVariable("NODE_ID")
        ?? Guid.NewGuid().ToString("N");
    
    public ReadOnlyCollection<string> GetKnownNodes()
    {
        lock (_lock)
            return _knownNodes.ToList().AsReadOnly();
    }

    public long GetLeaseId()
    {
        lock (_lock)
            return _leaseId;
    }

    public bool HasLease()
    {
        lock (_lock)
            return _hasLease;
    }

    public bool IsLeader()
    {
        lock (_lock)
            return _isLeader;
    }

    public List<int> GetMySegments()
    {
        lock (_lock)
            return [.._mySegments];
    }

    public LeaderKey? GetLeaderKey()
    {
        lock (_lock)
            return _leaderKey;
    }

    public void SetLeaderKey(LeaderKey? key)
    {
        lock (_lock)
            _leaderKey = key;
    }

    public void SetupLease(long leaseId)
    {
        SetLeaseId(leaseId);
        SetHasLease(true);
    }

    public void ResetLease()
    {
        SetHasLease(false);
        SetLeaseId(0);
        RemoveKnownNode(NodeId);
    }

    public void BecomeLeader() => SetIsLeader(true);
    public void LoseLeadership() => SetIsLeader(false);

    public void ResetMySegments() => UpdateSegments([]);

    public void UpdateSegments(List<int> newSegments)
    {
        bool changed;
        lock (_lock)
        {
            changed = !_mySegments.SequenceEqual(newSegments);
            if (changed)
                _mySegments = [..newSegments];
        }

        if (changed)
            _segmentsChanged.Writer.TryWrite(true);
    }

    public void AddKnownNode(string nodeId)
    {
        bool added;
        lock (_lock)
            added = _knownNodes.Add(nodeId);

        if (added)
            _membershipChanged.Writer.TryWrite(true);
    }

    public void RemoveKnownNode(string nodeId)
    {
        bool removed;
        lock (_lock)
            removed = _knownNodes.Remove(nodeId);

        if (removed)
            _membershipChanged.Writer.TryWrite(true);
    }

    // --- Channel readers (split by concern) ---
    public ChannelReader<bool> MembershipChanged => _membershipChanged.Reader;
    public ChannelReader<bool> LeadershipChanged => _leadershipChanged.Reader;
    public ChannelReader<bool> SegmentsChanged => _segmentsChanged.Reader;
    public ChannelReader<bool> LeaseStateChanged => _leaseStateChanged.Reader;

    // --- Reactive wait methods for LeaderElectionService (Phase 1) ---

    /// <summary>
    /// Blocks until a lease is acquired. Used by LeaderElectionService
    /// to avoid polling for lease availability.
    /// </summary>
    public Task WaitForLeaseAsync(CancellationToken token)
    {
        TaskCompletionSource tcs;
        lock (_lock)
        {
            if (_hasLease)
                return Task.CompletedTask;
            tcs = _leaseAcquired;
        }
        return tcs.Task.WaitAsync(token);
    }

    /// <summary>
    /// Blocks until leadership is lost (lease lost or leadership flag cleared).
    /// Used by LeaderElectionService after winning an election.
    /// </summary>
    public Task WaitForLeadershipLossAsync(CancellationToken token)
    {
        TaskCompletionSource tcs;
        lock (_lock)
        {
            if (!_hasLease || !_isLeader)
                return Task.CompletedTask;
            // Reset the TCS so we can await it fresh
            _leadershipLost = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            tcs = _leadershipLost;
        }
        return tcs.Task.WaitAsync(token);
    }

    private void SetLeaseId(long value)
    {
        lock (_lock)
            _leaseId = value;
    }

    private void SetHasLease(bool value)
    {
        bool changed;
        lock (_lock)
        {
            changed = _hasLease != value;
            if (changed)
                _hasLease = value;
        }

        if (!changed) return;

        _leaseStateChanged.Writer.TryWrite(true);

        if (value)
        {
            // Signal that a lease was acquired — wake up election service
            lock (_lock)
            {
                _leaseAcquired.TrySetResult();
                // Prepare for next cycle
                _leaseAcquired = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            }
        }
        else
        {
            // Lease lost — signal leadership loss
            lock (_lock)
                _leadershipLost.TrySetResult();
        }
    }

    private void SetIsLeader(bool value)
    {
        bool changed;
        lock (_lock)
        {
            changed = _isLeader != value;
            if (changed)
                _isLeader = value;
        }

        if (!changed) return;

        _leadershipChanged.Writer.TryWrite(true);

        if (!value)
        {
            // Leadership explicitly lost — signal to WaitForLeadershipLossAsync
            lock (_lock)
                _leadershipLost.TrySetResult();
        }
    }
}