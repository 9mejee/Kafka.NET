namespace Kafka.NET;

public class KafkaSettings
{
    public string Servers { get; set; }
    public string Group { get; set; }
    public string Topic { get; set; }
    public string SecurityProtocol { get; set; }
    public string SaslMechanism { get; set; }
    public string SaslUserName { get; set; }
    public string SaslPassword { get; set; }
}
