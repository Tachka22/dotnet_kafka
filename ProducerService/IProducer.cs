namespace ProducerService;

public interface IProducer
{
    Task SendMessageAsync(MessageDto messageDto);
}