using System.Security.Cryptography;
using Amazon.Lambda.Core;
using Amazon.Runtime.Internal.Util;
using Amazon.SQS;
using Amazon.SQS.Model;
using WordList.Processing.ProcessSourceChunk.Models;

namespace WordList.Processing.ProcessSourceChunk;

public class QueryWordMessageSender
{
    private static readonly AmazonSQSClient s_sqs = new();
    private readonly SemaphoreSlim _sendMessageLimiter = new(5);

    public QueryWordMessage[] Messages { get; init; }

    public string TargetQueueName { get; init; }

    protected ILambdaLogger Logger { get; init; }
    protected int TryCount { get; init; } = 3;

    public QueryWordMessageSender(QueryWordMessage[] messages, ILambdaLogger logger, string targetQueueName)
    {
        Messages = messages;
        Logger = logger;
        TargetQueueName = targetQueueName;
    }

    private string? TrySerialize(QueryWordMessage message)
    {
        try
        {
            return JsonHelpers.Serialize(message, LambdaFunctionJsonSerializerContext.Default.QueryWordMessage);
        }
        catch (Exception ex)
        {
            Logger.LogError($"[{message.Word}] Could not serialize message:");
            Logger.LogError(ex.Message);
            return null;
        }
    }

    private async Task SendMessageBatchAsync(QueryWordMessage[] messageBatch)
    {
        await _sendMessageLimiter.WaitAsync();
        try
        {
            var entryMap = messageBatch
                .Select(TrySerialize)
                .Where(text => text is not null)
                .Select(text => new SendMessageBatchRequestEntry(Guid.NewGuid().ToString(), text))
                .ToDictionary(entry => entry.Id);

            for (var tryNumber = 1; tryNumber <= TryCount && entryMap.Count != 0; tryNumber++)
            {
                var batchRequest = new SendMessageBatchRequest(TargetQueueName, [.. entryMap.Values]);

                if (tryNumber > 1) await Task.Delay(250);

                Logger.LogInformation($"Try number {tryNumber}: Sending batch of {batchRequest.Entries.Count} message(s)");
                try
                {
                    var response = await s_sqs.SendMessageBatchAsync(batchRequest);
                    foreach (var message in response.Successful)
                    {
                        entryMap.Remove(message.Id);
                    }
                }
                catch (Exception ex)
                {
                    Logger.LogError($"Failed to send message batch of {batchRequest.Entries.Count} message(s):");
                    Logger.LogError(ex.Message);
                }
            }

            if (entryMap.Count > 0)
            {
                Logger.LogError($"Failed to send message batch of {entryMap.Count} message(s) after {TryCount} attempt(s)");
            }
        }
        finally
        {
            _sendMessageLimiter.Release();
        }
    }

    public async Task SendAllMessagesAsync()
    {
        Logger.LogInformation($"Sending a total of {Messages.Length} message(s)");

        var tasks = Messages.Chunk(10).Select(SendMessageBatchAsync);

        Logger.LogInformation($"Waiting for all messages to send");

        await Task.WhenAll(tasks);
    }
}