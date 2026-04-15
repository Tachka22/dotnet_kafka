using System.Text.Json;
using Confluent.Kafka;
using Microsoft.Extensions.Options;

namespace ConsumerService;

public class KafkaConsumer(
    IOptions<ConsumerConfig> config,
    IConfiguration configuration,
    ILogger<KafkaConsumer> logger)
    : BackgroundService
{
    private readonly IOptions<ConsumerConfig> _config = config ?? throw new ArgumentNullException(nameof(config));
    private readonly ILogger _logger = logger ?? throw new ArgumentNullException(nameof(logger));
    private IConsumer<string, string>? _consumer;

    protected override Task ExecuteAsync(CancellationToken stoppingToken)
    {
        try
        {
            _consumer = new ConsumerBuilder<string, string>(_config.Value).Build();
            const string topic = "messages";
            _consumer.Subscribe(topic);
            try
            {
                while (!stoppingToken.IsCancellationRequested)
                {
                    try
                    {
                        var result = _consumer.Consume(stoppingToken);
                        var messageJson = result.Message.Value;

                        _logger.LogInformation("Received raw: {Message}", messageJson);

                        try
                        {
                            var request = JsonSerializer.Deserialize<MessageDto>(messageJson);
                            _logger.LogInformation("Received Content: {Content}", request.Content);
                        }
                        catch (JsonException ex)
                        {
                            _logger.LogError(ex, "JSON Deserialization error for message: {Message}", messageJson);
                            continue;
                        }

                        _consumer.Commit(result);
                    }
                    catch (ConsumeException ex)
                    {
                        _logger.LogError(ex, "Kafka consume error");
                    }
                }
            }
            catch (OperationCanceledException)
            {
                _logger.LogInformation("Consumer stopped");
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Fatal Error");
            }
            finally
            {
                _consumer?.Close();
            }

            return Task.CompletedTask;
        }
        catch (Exception exception)
        {
            return Task.FromException(exception);
        }
    }

    public override void Dispose()
    {
        _consumer?.Dispose();
        base.Dispose();
    }
}