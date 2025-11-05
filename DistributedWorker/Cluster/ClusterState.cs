using System.Collections.ObjectModel;
using System.Threading.Channels;

namespace DistributedWorker.Cluster;

public class ClusterState
{
    private readonly object _lock = new();
    private readonly HashSet<string> _knownNodes = new();
    private List<int> _mySegments = new();
    private long _leaseId;
    private bool _hasLease;
    private bool _isLeader;

    // 🔄 Події стану
    private readonly Channel<bool> _clusterChanged = Channel.CreateUnbounded<bool>();
    private readonly Channel<bool> _processingStateChanged = Channel.CreateUnbounded<bool>();

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

    public void SetupLease(long leaseId)
    {
        SetLeaseId(leaseId);
        SetHasLease(true);
    }

    public void ResetLease()
    {
        SetHasLease(false);
        SetLeaseId(0);
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
            _processingStateChanged.Writer.TryWrite(true);
    }

    public void AddKnownNode(string nodeId)
    {
        bool added;
        lock (_lock)
            added = _knownNodes.Add(nodeId);

        if (added)
            _clusterChanged.Writer.TryWrite(true);
    }

    public void RemoveKnownNode(string nodeId)
    {
        bool removed;
        lock (_lock)
            removed = _knownNodes.Remove(nodeId);

        if (removed)
            _clusterChanged.Writer.TryWrite(true);
    }

    // --- Канали (читачі) ---
    public ChannelReader<bool> ClusterChanged => _clusterChanged.Reader;
    public ChannelReader<bool> ProcessingStateChanged => _processingStateChanged.Reader;

    // --- Приватні допоміжні методи ---
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

        if (changed)
            _processingStateChanged.Writer.TryWrite(true);
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

        if (changed)
            _clusterChanged.Writer.TryWrite(true);
    }
}