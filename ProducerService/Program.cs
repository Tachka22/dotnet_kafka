using Confluent.Kafka;
using KafkaSchemas;
using ProducerService;

var builder = WebApplication.CreateBuilder(args);
builder.Services.Configure<ProducerConfig>(builder.Configuration.GetSection("KafkaProducer"));
builder.Services.AddSingleton<IProducer, KafkaProducer>();

var app = builder.Build();

app.MapPost("/send", async (MessageRequest request, IProducer producer) =>
    {
        var avroMessage = new MessageDto 
        { 
            Content = request.Content 
        };

        await producer.SendMessageAsync(avroMessage);
        
        return Results.Ok(new { status = "Sent", content = request.Content });
    })
    .WithName("SendMessage");

app.Run();

public record MessageRequest(string Content);

