using Amazon.DynamoDBv2.DataModel;
using Amazon.Lambda.Core;
using Amazon.S3;
using Microsoft.EntityFrameworkCore;
using WordList.Data.Sql;
using WordList.Processing.ProcessSourceChunk.Models;

namespace WordList.Processing.ProcessSourceChunk;

public class SourceChunkProcessor
{
    private readonly static AmazonS3Client s_s3 = new();
    private readonly static WordDbContext s_wordContext = new();

    public string SourceId { get; init; }
    public string ChunkId { get; init; }
    public string Key { get; init; }

    protected string LogPrefix { get; init; }

    protected ILambdaLogger Logger { get; init; }

    private record struct SourceChunkWord
    {
        public string Word { get; set; }
        public bool ReplaceExisting { get; set; }
    }

    public SourceChunkProcessor(string sourceId, string chunkId, string key, ILambdaLogger logger)
    {
        SourceId = sourceId;
        ChunkId = chunkId;
        Key = key;
        Logger = logger;

        LogPrefix = $"[SourceId {sourceId}][ChunkId {chunkId}][Key {key}]";
    }

    private async Task<string[]> ShouldProcessBatchAsync(SourceChunkWord[]? chunkWords)
    {
        if (chunkWords is null || chunkWords.Length == 0) return [];
        var words = chunkWords.Select(w => w.Word).ToArray();

        try
        {
            var foundWords = await s_wordContext.Words.Where(w => words.Contains(w.Text)).ToListAsync().ConfigureAwait(false);

            return [.. words.Except(foundWords.Select(w => w.Text))];
        }
        catch (Exception ex)
        {
            Logger.LogError($"{LogPrefix} Failed to check status of existing batch of {chunkWords.Length} word(s):");
            Logger.LogError($"{LogPrefix} {ex.Message}");

            return [];
        }
    }

    public async Task ProcessSourceChunkAsync()
    {
        var words = await GetSourceChunkWordsAsync();

        Logger.LogInformation($"{LogPrefix} Filtering {words.Length} word(s) in chunk");

        var messages = new List<QueryWordMessage>();

        messages.AddRange(words.Where(w => w.ReplaceExisting).Select(w => new QueryWordMessage { Word = w.Word }));

        Logger.LogInformation($"{LogPrefix} Added {messages.Count} word(s) with ReplaceExisting=true");

        var checkWords = words.Where(w => !w.ReplaceExisting).ToList();

        Logger.LogInformation($"{LogPrefix} Checking {checkWords.Count} word(s) to see if they exist...");

        var batchNumber = 0;
        var batchTotal = (checkWords.Count + 99) / 100;
        foreach (var batch in checkWords.Chunk(100))
        {
            Logger.LogInformation($"{LogPrefix} Checking batch number {++batchNumber} of {batchTotal}");

            var wordsToProcess = await ShouldProcessBatchAsync(batch);

            messages.AddRange(wordsToProcess.Select(word => new QueryWordMessage { Word = word }));
        }

        Logger.LogInformation($"{LogPrefix} Should process {messages.Count} message(s)");

        var targetQueueUrl = Environment.GetEnvironmentVariable("QUERY_WORD_QUEUE_URL") ?? throw new Exception("QUERY_WORDS_QUEUE_NAME is not set");
        await new QueryWordMessageSender([.. messages], Logger, targetQueueUrl).SendAllMessagesAsync();

        Logger.LogInformation($"{LogPrefix} Finished processing chunk");
    }

    private async Task<SourceChunkWord[]> GetSourceChunkWordsAsync()
    {
        try
        {
            Logger.LogInformation($"{LogPrefix} Attempting to source chunk file from bucket");

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

            Logger.LogInformation($"{LogPrefix} Retrieved {words.Count} word(s) in chunk");

            return words.ToArray();
        }
        catch (Exception ex)
        {
            Logger.LogError($"{LogPrefix} Failed to retrieve source chunk file from bucket:");
            Logger.LogError($"{LogPrefix} {ex.Message}");
            return [];
        }
    }
}