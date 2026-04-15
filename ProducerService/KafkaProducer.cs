using System.Text.Json;
using Confluent.Kafka;
using Microsoft.Extensions.Options;

namespace ProducerService;

public class KafkaProducer(
    ILogger<KafkaProducer> logger,
    IOptions<ProducerConfig> config) : IProducer, IDisposable
{
    private readonly IProducer<Null, string> _producer = new ProducerBuilder<Null, string>(config.Value).Build();
    private readonly ILogger<KafkaProducer> _logger = logger ?? throw new ArgumentNullException(nameof(logger));
    private const string Topic = "messages";

    public async Task SendMessageAsync(MessageDto messageDto)
    {
       var message = JsonSerializer.Serialize(messageDto);
       
       try
       {
           var result = await _producer.ProduceAsync(Topic, new Message<Null, string>()
           {
               Value = message
           });

           _logger.LogInformation("Message delivered to {Topic} [{Partition}]", result.Topic, result.Partition);
       }
       catch (ProduceException<string, string> ex)
       {
           _logger.LogError(ex, "Failed to send message to Kafka");
       }
    }

    public void Dispose()
    {
        _producer.Dispose();
    }
}