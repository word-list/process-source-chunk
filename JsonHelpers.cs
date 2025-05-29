using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization.Metadata;

namespace WordList.Processing.ProcessSourceChunk;

public static class JsonHelpers
{
    public static string Serialize<T>(T obj, JsonTypeInfo<T> type)
    {
        return JsonSerializer.Serialize(obj, type);
    }

    public static T? Deserialize<T>(string text, JsonTypeInfo<T> type)
    {
        var bytes = Encoding.UTF8.GetBytes(text);
        using var stream = new MemoryStream(bytes);
        return JsonSerializer.Deserialize(stream, type);
    }

}