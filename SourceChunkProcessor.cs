using Amazon.S3;
using Microsoft.EntityFrameworkCore;
using WordList.Common.Logging;
using WordList.Common.Messaging;
using WordList.Common.Messaging.Messages;
using WordList.Data.Sql;

namespace WordList.Processing.ProcessSourceChunk;

public class SourceChunkProcessor
{
    private readonly static AmazonS3Client s_s3 = new();
    private readonly static WordDbContext s_wordContext = new();

    public string SourceId { get; init; }
    public string ChunkId { get; init; }
    public string Key { get; init; }

    protected ILogger Log { get; init; }

    private SemaphoreSlim _shouldProcessBatchLimiter = new(3);

    private record struct SourceChunkWord
    {
        public string Word { get; set; }
        public bool ReplaceExisting { get; set; }
    }

    public SourceChunkProcessor(string sourceId, string chunkId, string key, ILogger logger)
    {
        SourceId = sourceId;
        ChunkId = chunkId;
        Key = key;
        Log = logger;
    }

    private async Task<string[]> ShouldProcessBatchAsync(SourceChunkWord[]? chunkWords)
    {
        await _shouldProcessBatchLimiter.WaitAsync().ConfigureAwait(false);
        try
        {
            if (chunkWords is null || chunkWords.Length == 0) return [];
            var words = chunkWords.Select(w => w.Word).ToArray();

            if (s_wordContext.Words is null) throw new InvalidOperationException("Words dbset is null");
            if (words is null) throw new InvalidOperationException("Words in chunk is null)");

            try
            {
                var foundWords = await s_wordContext.Words.Where(w => words.Contains(w.Text)).ToListAsync().ConfigureAwait(false);

                if (foundWords is null) throw new InvalidOperationException("FoundWords is null");

                return [.. words.Except(foundWords.Select(w => w.Text))];
            }
            catch (Exception ex)
            {
                Log.Error($"Failed to check status of existing batch of {chunkWords.Length} word(s):");
                Log.Error($"{ex.Message}");

                return [];
            }
        }
        finally
        {
            _shouldProcessBatchLimiter.Release();
        }
    }

    public async Task ProcessSourceChunkAsync()
    {
        var words = await GetSourceChunkWordsAsync().ConfigureAwait(false);

        Log.Info($"Filtering {words.Length} word(s) in chunk");

        var messages = new List<QueryWordMessage>();

        messages.AddRange(words.Where(w => w.ReplaceExisting).Select(w => new QueryWordMessage { Word = w.Word }));

        Log.Info($"Added {messages.Count} word(s) with ReplaceExisting=true");

        var checkWords = words.Where(w => !w.ReplaceExisting).ToList();

        Log.Info($"Checking {checkWords.Count} word(s) to see if they exist...");

        var shouldProcessWords = (await Task.WhenAll(
            checkWords
                .Chunk(100)
                .Select(ShouldProcessBatchAsync))
                .ConfigureAwait(false)
            ).SelectMany(
                w => w
            ).ToArray();

        Log.Info($"Should process {shouldProcessWords.Length} message(s)");

        await MessageQueues.QueryWords.SendBatchedMessagesAsync(Log, shouldProcessWords.Select(word => new QueryWordMessage { Word = word })).ConfigureAwait(false);

        Log.Info($"Finished processing chunk");
    }

    private async Task<SourceChunkWord[]> GetSourceChunkWordsAsync()
    {
        try
        {
            Log.Info($"Attempting to source chunk file from bucket");

            var words = new List<SourceChunkWord>();
            var response = await s_s3.GetObjectAsync(Environment.GetEnvironmentVariable("SOURCE_CHUNKS_BUCKET_NAME"), Key);

            using var reader = new StreamReader(response.ResponseStream);
            while (!reader.EndOfStream)
            {
                var line = await reader.ReadLineAsync();
                if (string.IsNullOrEmpty(line)) continue;

                var items = line.Split(',');
                var replaceExisting = items.Length > 1 && bool.Parse(items[1]);
                var word = items[0];

                if (string.IsNullOrEmpty(word) || !word.All(char.IsLetter)) continue;

                words.Add(new SourceChunkWord
                {
                    Word = word,
                    ReplaceExisting = replaceExisting
                });
            }

            Log.Info($"Retrieved {words.Count} word(s) in chunk");

            return [.. words];
        }
        catch (Exception ex)
        {
            Log.Info($"Failed to retrieve source chunk file from bucket: {ex.Message}");
            return [];
        }
    }
}