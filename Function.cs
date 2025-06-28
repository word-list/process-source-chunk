using System.Text.Json.Serialization;
using Amazon.Lambda.Core;
using Amazon.Lambda.RuntimeSupport;
using Amazon.Lambda.Serialization.SystemTextJson;
using Amazon.Lambda.SQSEvents;
using WordList.Common.Messaging;
using WordList.Common.Status;
using WordList.Common.Status.Models;

namespace WordList.Processing.ProcessSourceChunk;

public class Function
{
    public static async Task<string> FunctionHandler(SQSEvent input, ILambdaContext context)
    {
        var log = new LambdaContextLogger(context);

        log.Info("Entering ProcessSourceChunk FunctionHandler");

        if (input.Records.Count > 1)
        {
            log.Warning($"Attempting to process {input.Records.Count} messages - SQS batch size should be set to 1!");
        }

        var messages = MessageQueues.ProcessSourceChunk.Receive(input, log);

        foreach (var message in messages)
        {
            if (message is null) continue;

            var status = new StatusClient(message.CorrelationId);
            await status.UpdateStatusAsync(SourceStatus.PROCESSING).ConfigureAwait(false);

            log.Info($"Starting source chunk processing for SourceId {message.SourceId}, ChunkId {message.ChunkId} at key {message.Key}");

            await new SourceChunkProcessor(message.SourceId, message.ChunkId, message.Key, status, log.WithPrefix($"[SourceId={message.SourceId}]")).ProcessSourceChunkAsync().ConfigureAwait(false);

            log.Info($"Finished source chunk processing for SourceId {message.SourceId}, ChunkId {message.ChunkId} at key {message.Key}");
        }

        log.Info("Exiting ProcessSourceChunk FunctionHandler");

        return "ok";
    }

    public static async Task Main()
    {
        Func<SQSEvent, ILambdaContext, Task<string>> handler = FunctionHandler;
        await LambdaBootstrapBuilder.Create(handler, new SourceGeneratorLambdaJsonSerializer<LambdaFunctionJsonSerializerContext>())
            .Build()
            .RunAsync();
    }
}

[JsonSerializable(typeof(string))]
[JsonSerializable(typeof(SQSEvent))]
public partial class LambdaFunctionJsonSerializerContext : JsonSerializerContext
{
}
