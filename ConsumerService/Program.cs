using Confluent.Kafka;
using ConsumerService;

var builder = WebApplication.CreateBuilder(args);

builder.Services.Configure<ConsumerConfig>(builder.Configuration.GetSection("KafkaConsumer"));
builder.Services.AddHostedService<KafkaConsumer>();

var app = builder.Build();
app.Run();