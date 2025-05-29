using Amazon.DynamoDBv2.DataModel;

[DynamoDBTable("word-table")]
public class Word
{
    [DynamoDBHashKey("id")]
    public required string Id { get; set; }
}