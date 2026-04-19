using Confluent.Kafka;
using Confluent.SchemaRegistry;
using KafkaSchemas;
using Microsoft.Extensions.Options;
using Polly;
using Polly.Retry;
using System.Text;
using Confluent.Kafka.SyncOverAsync;
using Confluent.SchemaRegistry.Serdes;

namespace ConsumerService;

public class KafkaConsumer : BackgroundService
{
    private readonly ConsumerConfig _consumerConfig;
    private readonly ILogger<KafkaConsumer> _logger;
    private readonly string _mainTopic;
    private readonly string _dlqTopic;
    private readonly string _schemaRegistryUrl;
    private readonly AsyncRetryPolicy _retryPolicy;
    private readonly int _maxRetryAttempts;

    private IConsumer<Null, MessageDto>? _consumer;
    private IProducer<string, string>? _dlqProducer;
    private ISchemaRegistryClient? _schemaRegistry;

    public KafkaConsumer(
        IOptions<ConsumerConfig> config,
        IConfiguration configuration,
        ILogger<KafkaConsumer> logger)
    {
        
        _consumerConfig = config.Value;
        _logger = logger;
        
        _mainTopic = configuration["KafkaConsumer:MainTopic"] ?? "messages";
        _dlqTopic = configuration["RetrySettings:DeadLetterTopic"] ?? "messages-dlq";
        _maxRetryAttempts = configuration.GetValue<int>("RetrySettings:MaxRetryAttempts", 3);
        _schemaRegistryUrl = configuration["SchemaRegistryUrl"] ?? "http://localhost:8081";
        

        _retryPolicy = Policy
            .Handle<Exception>()
            .WaitAndRetryAsync(_maxRetryAttempts, retryAttempt =>
                TimeSpan.FromSeconds(Math.Pow(2, retryAttempt)), // Экспоненциальная пауза: 2, 4, 8 сек.
                (exception, timeSpan, retryCount, _) =>
                {
                    _logger.LogWarning(exception, "Retry {RetryCount} after {Delay}s delay due to: {Message}", retryCount, timeSpan.TotalSeconds, exception.Message);
                });
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _schemaRegistry = new CachedSchemaRegistryClient(new SchemaRegistryConfig
        {
            Url = _schemaRegistryUrl
        });
        _dlqProducer = new ProducerBuilder<string, string>(new ProducerConfig
        {
            BootstrapServers = _consumerConfig.BootstrapServers
        }).Build();
        
        _consumer = new ConsumerBuilder<Null, MessageDto>(_consumerConfig)
            .SetValueDeserializer(new AvroDeserializer<MessageDto>(_schemaRegistry).AsSyncOverAsync())
            .Build();

        _consumer.Subscribe(_mainTopic);
        _logger.LogInformation("Consumer with Polly started. Listening to {Topic}", _mainTopic);

        while (!stoppingToken.IsCancellationRequested)
        {
            ConsumeResult<Null, MessageDto>? result = null;
            try
            {
                result = _consumer.Consume(stoppingToken);
                if (result?.Message?.Value == null) continue;

                var message = result.Message.Value;

                try
                {
                    await _retryPolicy.ExecuteAsync(async () =>
                    {
                        await ProcessMessage(message);
                    });
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "All retry attempts failed. Sending to DLQ...");
                    await SendToDeadLetterQueue(message.Content, ex.Message);
                }

                _consumer.Commit(result);
            }
            catch (ConsumeException ex) when (ex.Error.Code == ErrorCode.Local_ValueDeserialization)
            {
                _logger.LogCritical("Avro Deserialization Error. Skipping message.");
                await SendToDeadLetterQueue("RAW_BINARY_DATA", "Deserialization failed");
                _consumer.Commit();
            }
            catch (OperationCanceledException) { break; }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Unexpected error in consume loop");
            }
        }
    }

    private async Task ProcessMessage(MessageDto message)
    {
        _logger.LogInformation("Processing message: {Content}", message.Content);
        await Task.CompletedTask;
    }

    private async Task SendToDeadLetterQueue(string content, string reason)
    {
        await _dlqProducer!.ProduceAsync(_dlqTopic, new Message<string, string>
        {
            Key = Guid.NewGuid().ToString(),
            Value = content,
            Headers = new Headers { { "error-reason", Encoding.UTF8.GetBytes(reason) } }
        });
    }

    public override void Dispose()
    {
        _consumer?.Close();
        _consumer?.Dispose();
        _dlqProducer?.Dispose();
        _schemaRegistry?.Dispose();
        base.Dispose();
    }
}
