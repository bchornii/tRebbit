using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Threading.Tasks;
using tRebbit.Abstractions;

namespace tRebbit
{
    public class RabbitMqEventBus : IEventBus, IDisposable
    {
        private bool _disposed;

        private readonly string _queueName;
        private readonly string _exchangeName;
        private readonly bool _deadLettetingEnabled;
        private readonly string _deadLetteringSufix;
        private readonly IRabbitMqPersistentConnection _persistentConnection;
        private readonly ILogger<RabbitMqEventBus> _logger;
        private readonly IEventBusSubscriptionsManager _subscriptionsManager;
        private IModel _consumerChannel;
        private readonly IServiceProvider _serviceProvider;
        private readonly Action<Action> _resillientPublishPolicyAction;
        private Func<IServiceScope, object, Task> _preprocessAction;

        public RabbitMqEventBus(
            string queueName, string exchangeName,
            bool deadLettetingEnabled,
            string deadLetteringSufix,
            IRabbitMqPersistentConnection persistentConnection,
            ILogger<RabbitMqEventBus> logger,
            IEventBusSubscriptionsManager subscriptionsManager,
            IServiceProvider serviceProvider,
            Action<Action> resillientPublishPolicyAction,
            Func<IServiceScope, object, Task> preprocessAction)
        {
            _queueName = queueName;
            _exchangeName = exchangeName;
            _deadLettetingEnabled = deadLettetingEnabled;
            _deadLetteringSufix = deadLetteringSufix;
            _persistentConnection = persistentConnection;
            _logger = logger;
            _subscriptionsManager = subscriptionsManager;
            _serviceProvider = serviceProvider;
            _resillientPublishPolicyAction = resillientPublishPolicyAction;
            _preprocessAction = preprocessAction;

            DeclareExchangeAndQueue();
        }

        public void ActivateSubscriptionChannel()
            => _consumerChannel = CreateConsumerChannel();

        public void Publish(IntegrationEvent @event)
        {
            if (!_persistentConnection.IsConnected)
            {
                _persistentConnection.TryConnect();
            }

            var eventName = @event.GetType().Name;

            _logger.LogTrace(
                "Creating RabbitMQ channel to publish event: {EventId} ({EventName})",
                @event.Id, eventName);

            using (var channel = _persistentConnection.CreateModel())
            {
                // TODO: concider Publisher confirms - not possible with
                // TODO: disposable channels though
                //channel.ConfirmSelect();
                //channel.BasicNacks += (model, ea) =>
                //{
                //    _logger.LogTrace("Event {EventName} with id = {EventId} publish failure",
                //        eventName, @event.Id);
                //};

                var message = System.Text.Json
                    .JsonSerializer.Serialize(@event, @event.GetType());
                var body = Encoding.UTF8.GetBytes(message);

                _resillientPublishPolicyAction(() =>
                {
                    var properties = channel.CreateBasicProperties();
                    properties.DeliveryMode = 2; // persistent message

                    _logger.LogTrace("Publishing event to RabbitMQ: {EventId}", @event.Id);

                    channel.BasicPublish(
                        exchange: _exchangeName,
                        routingKey: eventName,
                        mandatory: true,
                        basicProperties: properties,
                        body: body);
                });
            }
        }

        public void Subscribe<T, TH>()
            where T : IntegrationEvent
            where TH : IIntegrationEventHandler<T>
        {
            var eventName = _subscriptionsManager.GetEventKey<T>();
            DoInternalSubscription(eventName);

            _logger.LogInformation(
                "Subscribing to event {EventName} with {EventHandler}", eventName, typeof(TH).GetGenericTypeName());

            _subscriptionsManager.AddSubscription<T, TH>();
        }

        public void Unsubscribe<T, TH>()
            where T : IntegrationEvent
            where TH : IIntegrationEventHandler<T>
        {
            var eventName = _subscriptionsManager.GetEventKey<T>();

            _logger.LogInformation("Unsubscribing from event {EventName}", eventName);

            _subscriptionsManager.RemoveSubscription<T, TH>();
        }

        ~RabbitMqEventBus()
            => Dispose(false);

        public void Dispose()
        {
            // Dispose of unmanaged resources.
            Dispose(true);

            // Suppress finalization.
            GC.SuppressFinalize(this);
        }

        protected virtual void Dispose(bool disposing)
        {
            if (_disposed)
            {
                return;
            }

            if (disposing)
            {
                try
                {
                    _persistentConnection?.Dispose();

                    _consumerChannel?.Close();
                    _consumerChannel.Dispose();
                }
                catch (IOException ex)
                {
                    _logger.LogCritical($"Exception message: {ex.Message} ---> {ex.InnerException?.Message}");
                }
            }

            _disposed = true;
        }

        private void DoInternalSubscription(string eventName)
        {
            if (!_subscriptionsManager
                    .HasSubscriptionsForEvent(eventName))
            {
                if (!_persistentConnection.IsConnected)
                {
                    _persistentConnection.TryConnect();
                }

                using (var channel = _persistentConnection.CreateModel())
                {
                    channel.QueueBind(
                        queue: _queueName,
                        exchange: _exchangeName,
                        routingKey: eventName);

                    if (_deadLettetingEnabled)
                    {
                        channel.QueueBind(
                            queue: $"{_queueName}_{_deadLetteringSufix}",
                            exchange: $"{_exchangeName}_{_deadLetteringSufix}",
                            routingKey: eventName);
                    }
                }
            }
        }

        private void DeclareExchangeAndQueue()
        {
            if (!_persistentConnection.IsConnected)
            {
                _persistentConnection.TryConnect();
            }

            using (var channel = _persistentConnection.CreateModel())
            {
                if (_deadLettetingEnabled)
                {
                    channel.ExchangeDeclare(
                        exchange: $"{_exchangeName}_{_deadLetteringSufix}",
                        type: ExchangeType.Direct,
                        durable: true);

                    channel.QueueDeclare(
                        queue: $"{_queueName}_{_deadLetteringSufix}",
                        durable: true,
                        exclusive: false,
                        autoDelete: false,
                        arguments: null);
                }

                channel.ExchangeDeclare(
                    exchange: _exchangeName,
                    type: ExchangeType.Direct,
                    durable: true);

                channel.QueueDeclare(
                    queue: _queueName,
                    durable: true,
                    exclusive: false,
                    autoDelete: false,
                    arguments: new Dictionary<string, object>
                    {
                        ["x-dead-letter-exchange"] = $"{_exchangeName}_{_deadLetteringSufix}"
                    });
            }
        }

        private IModel CreateConsumerChannel()
        {
            if (!_persistentConnection.IsConnected)
            {
                _persistentConnection.TryConnect();
            }

            _logger.LogTrace("Creating RabbitMQ consumer channel");

            var channel = _persistentConnection.CreateModel();

            _logger.LogTrace("Starting RabbitMQ basic consume");

            var consumer = new AsyncEventingBasicConsumer(channel);
            consumer.Received += async (model, ea) =>
            {
                try
                {
                    var eventName = ea.RoutingKey;
                    var message = Encoding.UTF8.GetString(ea.Body.ToArray());

                    await ProcessEvent(
                        eventName: eventName,
                        message: message,
                        isFirstDispatch: ea.Redelivered == false);

                    channel.BasicAck(ea.DeliveryTag, multiple: false);
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "----- ERROR Processing message \"{Message}\"", ex.Message);
                    channel.BasicNack(ea.DeliveryTag, multiple: false, requeue: false);
                }
            };

            channel.BasicConsume(
                queue: _queueName,
                autoAck: false,
                consumer: consumer);

            channel.CallbackException += (sender, ea) =>
            {
                _logger.LogWarning(ea.Exception, "Recreating RabbitMQ consumer channel");

                _consumerChannel.Dispose();
                _consumerChannel = CreateConsumerChannel();
            };

            return channel;
        }

        private async Task ProcessEvent(string eventName, string message, bool isFirstDispatch)
        {
            _logger.LogTrace("Processing RabbitMQ event: {EventName}", eventName);
            if (_subscriptionsManager.HasSubscriptionsForEvent(eventName))
            {
                var subscriptions = _subscriptionsManager.GetHandlersForEvent(eventName);
                foreach (var subscription in subscriptions)
                {
                    using (var scope = _serviceProvider.CreateScope())
                    {
                        var handler = scope.ServiceProvider.GetService(subscription.HandlerType);
                        if (handler == null)
                        {
                            return;
                        }

                        var eventType = _subscriptionsManager.GetEventTypeByName(eventName);
                        var integrationEvent = System.Text.Json.JsonSerializer.Deserialize(message, eventType);

                        _preprocessAction?.Invoke(scope, integrationEvent);

                        var concreteType = typeof(IIntegrationEventHandler<>).MakeGenericType(eventType);
                        var handleMethod = concreteType.GetMethod(nameof(IIntegrationEventHandler<IntegrationEvent>.Handle));
                        await (Task)handleMethod.Invoke(handler, new[] { integrationEvent, isFirstDispatch });
                    }                    
                }
            }

        }
    }
}
