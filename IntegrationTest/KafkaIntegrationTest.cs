using System.Text.Json;
using Confluent.Kafka;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using ProducerService;
using Testcontainers.Kafka;
using MessageDto = ProducerService.MessageDto;

namespace IntegrationTest;

public class KafkaIntegrationTest : IAsyncLifetime
{
    private KafkaContainer? _kafkaContainer;
    private string _bootstrapServers = null!;

    public async Task InitializeAsync()
    {
        _kafkaContainer = new KafkaBuilder()
            .WithImage("confluentinc/cp-kafka:7.5.0")
            .Build();

        await _kafkaContainer.StartAsync();
        _bootstrapServers = _kafkaContainer.GetBootstrapAddress();
        
        // Даём Kafka время на инициализацию
        await Task.Delay(10000);
    }

    public async Task DisposeAsync()
    {
        if (_kafkaContainer != null)
        {
            await _kafkaContainer.StopAsync();
            await _kafkaContainer.DisposeAsync();
        }
    }

    [Fact]
    public async Task ProducerSendsMessage_ConsumerReceivesMessage()
    {
        // Arrange
        const string topic = "messages";
        var messageContent = "Hello!";
        var cts = new CancellationTokenSource(TimeSpan.FromSeconds(30));

        // Создаём продюсер
        var producerConfig = new ProducerConfig
        {
            BootstrapServers = _bootstrapServers,
        };

        var producerLogger = LoggerFactory.Create(builder => builder.AddConsole()).CreateLogger<KafkaProducer>();
        var producerOptions = Options.Create(producerConfig);
        
        // Act - отправляем сообщение через продюсер
        using (var producer = new KafkaProducer(producerLogger, producerOptions))
        {
            await producer.SendMessageAsync(new MessageDto(messageContent));
        }

        // Создаём консьюмер и читаем сообщение
        var consumerConfig = new ConsumerConfig
        {
            BootstrapServers = _bootstrapServers,
            GroupId = "test-group",
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnableAutoCommit = false,
        };

        using var consumer = new ConsumerBuilder<string, string>(consumerConfig).Build();
        consumer.Subscribe(topic);

        try
        {
            var consumeResult = consumer.Consume(cts.Token);
            var messageJson = consumeResult.Message.Value;
            var receivedMessage = JsonSerializer.Deserialize<MessageDto>(messageJson);

            // Assert
            Assert.NotNull(receivedMessage);
            Assert.Equal(messageContent, receivedMessage.Content);
        }
        catch (OperationCanceledException)
        {
            Assert.Fail("Timeout waiting for message");
        }
    }

    [Fact]
    public async Task ProducerSendsMultipleMessages_ConsumerReceivesAll()
    {
        // Arrange
        var messageContents = new[] { "message-1", "message-2", "message-3" };
        var receivedMessages = new List<string>();

        var producerConfig = new ProducerConfig
        {
            BootstrapServers = _bootstrapServers,
        };

        var producerLogger = LoggerFactory.Create(builder => builder.AddConsole()).CreateLogger<KafkaProducer>();
        var producerOptions = Options.Create(producerConfig);

        // Act - отправляем несколько сообщений
        using (var producer = new KafkaProducer(producerLogger, producerOptions))
        {
            foreach (var content in messageContents)
            {
                await producer.SendMessageAsync(new MessageDto(content));
            }
        }

        // Читаем все сообщения
        var consumerConfig = new ConsumerConfig
        {
            BootstrapServers = _bootstrapServers,
            GroupId = "test-group-multi",
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnableAutoCommit = true,
        };

        using var consumer = new ConsumerBuilder<string, string>(consumerConfig).Build();
        consumer.Subscribe("messages");

        var timeout = DateTime.Now.AddSeconds(10);
        while (receivedMessages.Count < messageContents.Length && DateTime.Now < timeout)
        {
            try
            {
                var result = consumer.Consume(TimeSpan.FromMilliseconds(100));
                if (result != null)
                {
                    var message = JsonSerializer.Deserialize<MessageDto>(result.Message.Value);
                    if (message != null)
                    {
                        receivedMessages.Add(message.Content);
                    }
                }
            }
            catch (ConsumeException)
            {
                break;
            }
        }

        // Assert
        Assert.Equal(messageContents.Length, receivedMessages.Count);
        Assert.Equal(messageContents, receivedMessages);
    }
}