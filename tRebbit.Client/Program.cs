using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Polly;
using Polly.Retry;
using RabbitMQ.Client.Exceptions;
using System;
using System.Net.Sockets;
using System.Threading.Tasks;
using tRebbit.Abstractions;

namespace tRebbit.Client
{
    class Program
    {
        static void Main(string[] args)
        {
            var host = CreateHostBuilder(args).Build();
            var serviceProvider = host.Services;

            var serv = serviceProvider.GetService<IServiceProvider>();
            var eq = serviceProvider == serv;

            var eventBus = serviceProvider.GetRequiredService<IEventBus>();

            // subscribe
            eventBus.Subscribe<PaymentCompletedEvent,
                PaymentCompletedIntegrationEventHandler>();

            eventBus.ActivateSubscriptionChannel();

            // publish
            eventBus.Publish(new PaymentCompletedEvent(100.0m, "bchornii"));

            Console.Read();
        }

        static IHostBuilder CreateHostBuilder(string[] args) =>
            Host.CreateDefaultBuilder(args)
                .ConfigureServices((_, services) =>
                {
                    var connectionRetryPolicy = RetryPolicy.Handle<SocketException>()
                        .Or<BrokerUnreachableException>()
                        .WaitAndRetry(5, retryAttempt =>
                            TimeSpan.FromSeconds(Math.Pow(2, retryAttempt)), (ex, time) =>
                            {

                                Console.WriteLine($"RabbitMQ Client could not connect after {time.TotalSeconds:n1}s ({ex.Message})");
                            });

                    var publishRetryPolicy = RetryPolicy.Handle<SocketException>()
                        .Or<BrokerUnreachableException>()
                        .WaitAndRetry(5, retryAttempt =>
                            TimeSpan.FromSeconds(Math.Pow(2, retryAttempt)), (ex, time) =>
                            {

                                Console.WriteLine($"RabbitMQ Client could not connect after {time.TotalSeconds:n1}s ({ex.Message})");
                            });

                    services.AddScoped<PaymentCompletedIntegrationEventHandler>();

                    services.AddEventBus(
                        exchangeName: "bc_exchange",
                        queueName: "bc_default_queue",
                        config: new RabbitMqConfig
                        {
                            Connection = "localhost",
                            UserName = "guest",
                            Password = "guest"
                        },
                        resillientConnectionPolicyAction: connectionRetryPolicy.Execute,
                        resillientPublishPolicyAction: publishRetryPolicy.Execute,
                        preprocessAction: (s, o) => Task.CompletedTask);
                });
    }
}
