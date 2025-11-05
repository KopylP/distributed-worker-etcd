namespace DistributedWorker.Services;

public static class SegmentDistributor
{
    public static Dictionary<string, List<int>> DistributeEvenly(
        IReadOnlyList<string> activeNodes,
        int totalSegments)
    {
        var result = new Dictionary<string, List<int>>();

        if (activeNodes.Count == 0 || totalSegments <= 0)
            return result;

        int nodeCount = activeNodes.Count;
        int baseCount = totalSegments / nodeCount;
        int remainder = totalSegments % nodeCount;

        int currentSegment = 0;

        for (int i = 0; i < nodeCount; i++)
        {
            int countForNode = baseCount + (i < remainder ? 1 : 0);

            var segments = Enumerable
                .Range(currentSegment, countForNode)
                .ToList();

            result[activeNodes[i]] = segments;
            currentSegment += countForNode;
        }

        return result;
    }
}