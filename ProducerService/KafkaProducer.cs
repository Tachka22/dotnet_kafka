using Confluent.Kafka;
using Confluent.SchemaRegistry;
using Confluent.SchemaRegistry.Serdes;
using KafkaSchemas;
using Microsoft.Extensions.Options;

namespace ProducerService;

public class KafkaProducer : IProducer
{
    private readonly IProducer<Null, MessageDto> _producer;
    private readonly ISchemaRegistryClient _schemaRegistryClient;
    private readonly ILogger<KafkaProducer> _logger;
    private readonly string _mainTopic;

    public KafkaProducer(
        ILogger<KafkaProducer> logger,
        IOptions<ProducerConfig> config,
        IConfiguration configuration)
    {
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
       
        var schemaRegistryUrl = configuration["SchemaRegistryUrl"] ?? throw new ArgumentNullException("SchemaRegistryUrl is not configured");
        
        _schemaRegistryClient = new CachedSchemaRegistryClient(new SchemaRegistryConfig
        {
            Url = schemaRegistryUrl 
        });
        
        _producer = new ProducerBuilder<Null, MessageDto>(config.Value)
            .SetValueSerializer(new AvroSerializer<MessageDto>(_schemaRegistryClient))
            .Build();
        
        _mainTopic = configuration["KafkaProducer:MainTopic"] ?? "messages";
    }

    public KafkaProducer(ISchemaRegistryClient schemaRegistryClient)
    {
        _schemaRegistryClient = schemaRegistryClient;
    }

    public async Task SendMessageAsync(MessageDto messageDto)
    {
       try
       {
           var result = await _producer.ProduceAsync(_mainTopic, new Message<Null, MessageDto>()
           {
               Value = messageDto
           });

           _logger.LogInformation("Avro message delivered to {Topic} [{Partition}] at offset {Offset}", result.Topic, result.Partition, result.Offset);
       }
       catch (ProduceException<string, string> ex)
       {
           _logger.LogError(ex, "Failed to send message to Kafka");
       }
    }

    public void Dispose()
    {
        _producer.Dispose();
        _schemaRegistryClient.Dispose();
    }
}