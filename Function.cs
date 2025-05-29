using System.Text.Json.Serialization;
using Amazon.Lambda.Core;
using Amazon.Lambda.RuntimeSupport;
using Amazon.Lambda.Serialization.SystemTextJson;
using Amazon.Lambda.SQSEvents;
using WordList.Processing.ProcessSourceChunk.Models;

namespace WordList.Processing.ProcessSourceChunk;

public class Function
{
    public static async Task<string> FunctionHandler(SQSEvent input, ILambdaContext context)
    {
        context.Logger.LogInformation("Entering ProcessSourceChunk FunctionHandler");

        if (input.Records.Count > 1)
        {
            context.Logger.LogWarning($"Attempting to process {input.Records.Count} messages - SQS batch size should be set to 1!");
        }

        var messages =
            input.Records.Select(record =>
                {
                    try
                    {
                        return JsonHelpers.Deserialize(record.Body, LambdaFunctionJsonSerializerContext.Default.ProcessSourceChunkMessage);
                    }
                    catch (Exception)
                    {
                        context.Logger.LogWarning($"Ignoring invalid message: {record.Body}");
                        return null;
                    }
                });

        foreach (var message in messages)
        {
            if (message is null) continue;

            context.Logger.LogInformation($"Starting source chunk processing for SourceId {message.SourceId}, ChunkId {message.ChunkId} at key {message.Key}");

            await new SourceChunkProcessor(message.SourceId, message.ChunkId, message.Key, context.Logger).ProcessSourceChunkAsync();

            context.Logger.LogInformation($"Finished source chunk processing for SourceId {message.SourceId}, ChunkId {message.ChunkId} at key {message.Key}");
        }

        context.Logger.LogInformation("Exiting ProcessSourceChunk FunctionHandler");

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
[JsonSerializable(typeof(ProcessSourceChunkMessage))]
[JsonSerializable(typeof(QueryWordMessage))]
public partial class LambdaFunctionJsonSerializerContext : JsonSerializerContext
{
}