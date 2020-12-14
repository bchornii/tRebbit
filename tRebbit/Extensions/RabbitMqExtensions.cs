using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using RabbitMQ.Client;
using System;
using System.Threading.Tasks;
using tRebbit.Abstractions;

namespace tRebbit
{
    public static class EventBusExtensions
    {
        public static IServiceCollection AddEventBus(this IServiceCollection services,
            string exchangeName, string queueName, RabbitMqConfig config,
            Action<Action> resillientConnectionPolicyAction,
            Action<Action> resillientPublishPolicyAction,
            Func<IServiceScope, object, Task> preprocessAction = null)
        {
            return services
                .AddSingleton<IRabbitMqPersistentConnection>(sp =>
                {
                    var logger = sp.GetRequiredService<ILogger<RabbitMqPersistentConnection>>();


                    if (string.IsNullOrEmpty(config.Password)
                        || string.IsNullOrEmpty(config.UserName))
                    {
                        throw new ArgumentNullException(nameof(config), "No RabbitMQ credentials");
                    }

                    var factory = new ConnectionFactory
                    {
                        HostName = config.Connection,
                        UserName = config.UserName,
                        Password = config.Password,
                        DispatchConsumersAsync = true
                    };

                    return new RabbitMqPersistentConnection(
                        factory, logger, resillientConnectionPolicyAction);
                })
                .AddSingleton<IEventBusSubscriptionsManager, EventBusSubscriptionsManager>()
                .AddSingleton<IEventBus, RabbitMqEventBus>(serviceProvider =>
                    new RabbitMqEventBus(
                        queueName: queueName,
                        exchangeName: exchangeName,
                        deadLettetingEnabled: true,
                        deadLetteringSufix: "DLX",
                        persistentConnection: serviceProvider.GetRequiredService<IRabbitMqPersistentConnection>(),
                        logger: serviceProvider.GetRequiredService<ILogger<RabbitMqEventBus>>(),
                        subscriptionsManager: serviceProvider.GetRequiredService<IEventBusSubscriptionsManager>(),
                        serviceProvider: serviceProvider.GetRequiredService<IServiceProvider>(),
                        resillientPublishPolicyAction: resillientPublishPolicyAction,
                        preprocessAction: preprocessAction));
        }
    }
}
