using Confluent.Kafka;
using ProducerService;

var builder = WebApplication.CreateBuilder(args);
builder.Services.Configure<ProducerConfig>(builder.Configuration.GetSection("KafkaProducer"));
builder.Services.AddSingleton<IProducer, KafkaProducer>();

var app = builder.Build();

var producer = app.Services.GetRequiredService<IProducer>();

await producer.SendMessageAsync(new MessageDto("Hello World"));

// app.MapPost("/send", async ( MessageDto request) =>
//     {
//         await producer.SendMessageAsync(request);
//     })
//     .WithName("SendMessage");

app.Run();
