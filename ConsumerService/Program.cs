using Confluent.Kafka;
using ConsumerService;

var builder = WebApplication.CreateBuilder(args);
builder.Services.Configure<ConsumerConfig>(options =>
{
    var section = builder.Configuration.GetSection("KafkaConsumer");
    options.BootstrapServers = section["BootstrapServers"];
    options.GroupId = section["GroupId"];
});

builder.Services.AddHostedService<KafkaConsumer>();

var app = builder.Build();

app.Run();
