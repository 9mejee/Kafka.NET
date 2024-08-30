using Confluent.Kafka;
using Kafka.NET.Models;
using Microsoft.Extensions.Options;
using Spectre.Console;
using System.Reflection;
using System.Text.Json;

namespace Kafka.NET;

public class KafkaConsumerService : IHostedService, IDisposable
{
    private readonly ConsumerConfig _config;
    private readonly string _topic;
    private IConsumer<Ignore, string> _consumer;
    private CancellationTokenSource _cancellationTokenSource;
    private Task _consumingTask;

    public KafkaConsumerService(IOptions<KafkaSettings> kafkaSettings)
    {
        _config = new ConsumerConfig
        {
            BootstrapServers = kafkaSettings.Value.Servers,
            GroupId = kafkaSettings.Value.Group,
            AutoOffsetReset = AutoOffsetReset.Latest,
            EnableAutoCommit = false,
            SecurityProtocol = SecurityProtocol.SaslPlaintext,
            SaslMechanism = SaslMechanism.ScramSha256,
            SaslUsername = "user1",
            SaslPassword = "34vVbTpe5t"
        };
        _topic = kafkaSettings.Value.Topic;
    }

    public Task StartAsync(CancellationToken cancellationToken)
    {
        _cancellationTokenSource = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);

        _consumingTask = Task.Run(async () =>
        {
            try
            {
                using var _produce = new ProducerBuilder<Null, string>(_config).Build();
                using var _consumer = new ConsumerBuilder<Null, string>(_config).Build();
                _consumer.Subscribe(_topic);
                while (!_cancellationTokenSource.Token.IsCancellationRequested)
                    try
                    {
                        var results = _consumer.Consume(_cancellationTokenSource.Token);
                        var data = JsonSerializer.Deserialize<IEnumerable<TitanicModel>>(results?.Value);

                        DrawTable(data);
                        await Task.Delay(5000);
                        //Console.WriteLine($"Consumed message '{results.Value}' at: '{results.TopicPartitionOffset}'.");
                    }
                    catch (ConsumeException e)
                    {
                        Console.WriteLine($"Error occurred: {e.Error.Reason}");
                    }

            }
            catch (OperationCanceledException)
            {
                // Expected on shutdown
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Unexpected error: {ex.Message}");
            }
        }, _cancellationTokenSource.Token);

        return Task.CompletedTask;
    }

    public async Task StopAsync(CancellationToken cancellationToken)
    {
        if (_cancellationTokenSource != null)
        {
            _cancellationTokenSource.Cancel();
            await Task.WhenAny(_consumingTask, Task.Delay(Timeout.Infinite, cancellationToken));
        }
    }

    public void Dispose()
    {
        _consumer?.Close();
        _consumer?.Dispose();
        _cancellationTokenSource?.Dispose();
    }

    public void DrawTable<T>(IEnumerable<T> data)
    {
        if (data is null && !data.Any()) return;

        var table = new Table
        {
            Border = TableBorder.Ascii,
            BorderStyle = new Style(Color.Green)
        };
        var type = typeof(T);
        var properties = type.GetProperties(BindingFlags.Public | BindingFlags.Instance);

        AnsiConsole
            .Live(table)
            .AutoClear(false)
            .Start(ctx =>
            {
                foreach (var property in properties)
                {
                    table.AddColumn(property.Name);
                    ctx.Refresh();
                }

                foreach (var item in data)
                {
                    var row = new List<string>();
                    foreach (var property in properties)
                    {
                        var value = property.GetValue(item)?.ToString() ?? string.Empty;
                        row.Add(value);
                    }
                    table.AddRow(row.ToArray());
                    ctx.Refresh();
                }
            });

        AnsiConsole.Write(table);
    }
}
