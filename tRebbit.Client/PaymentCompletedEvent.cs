using tRebbit.Abstractions;

namespace tRebbit.Client
{
    public class PaymentCompletedEvent : IntegrationEvent
    {
        public decimal PayAmount { get; private set; }
        public string UserId { get; private set; }

        public PaymentCompletedEvent(decimal payAmount, string userId)
        {
            PayAmount = payAmount;
            UserId = userId;
        }

    }

    /*

    #region Events
    public record IntegrationEvent
    {
        public Guid Id { get; init; }
        public DateTime CreationDate { get; init; }
        public string CorrelationId { get; init; }

        public IntegrationEvent()
        {
            Id = Guid.NewGuid();
            CreationDate = DateTime.UtcNow;
        }

        [JsonConstructor]
        public IntegrationEvent(Guid id, DateTime createDate, string correlationId)
            => (Id, CreationDate, CorrelationId) = (id, createDate, correlationId);

        public void Deconstruct(out Guid id, out DateTime dt, out string correlationId) 
            => (id, dt, correlationId) = (Id, CreationDate, CorrelationId);
    }

    public record PaymentCompletedEvent(
        decimal PayAmount, string UsedId) : IntegrationEvent();

    #endregion

    #region Abstractions    
    public interface IIntegrationEventHandler<in TIntegrationEvent>
      where TIntegrationEvent : IntegrationEvent
    {
        Task Handle(TIntegrationEvent @event, bool isFirstDispatch);
    }

    public interface IEventBus
    {
        void ActivateSubscriptionChannel();

        void Publish(IntegrationEvent @event);

        void Subscribe<T, TH>()
            where T : IntegrationEvent
            where TH : IIntegrationEventHandler<T>;       

        void Unsubscribe<T, TH>()
            where TH : IIntegrationEventHandler<T>
            where T : IntegrationEvent;
    }
    #endregion

    #region Subscription Manager
    public record SubscriptionInfo(Type HandlerType);

    public interface IEventBusSubscriptionsManager
    {
        bool IsEmpty { get; }
        event EventHandler<string> OnEventRemoved;

        void AddSubscription<T, TH>()
           where T : IntegrationEvent
           where TH : IIntegrationEventHandler<T>;

        void RemoveSubscription<T, TH>()
             where TH : IIntegrationEventHandler<T>
             where T : IntegrationEvent;

        bool HasSubscriptionsForEvent<T>() where T : IntegrationEvent;
        bool HasSubscriptionsForEvent(string eventName);
        Type GetEventTypeByName(string eventName);
        void Clear();
        IEnumerable<SubscriptionInfo> GetHandlersForEvent<T>() where T : IntegrationEvent;
        IEnumerable<SubscriptionInfo> GetHandlersForEvent(string eventName);
        string GetEventKey<T>();
    }

    public class EventBusSubscriptionsManager : IEventBusSubscriptionsManager
    {
        private readonly Dictionary<string, List<SubscriptionInfo>> _handlers;
        private readonly List<Type> _eventTypes;

        public event EventHandler<string> OnEventRemoved;

        public EventBusSubscriptionsManager()
        {
            _handlers = new Dictionary<string, List<SubscriptionInfo>>();
            _eventTypes = new List<Type>();
        }

        public bool IsEmpty => !_handlers.Keys.Any();
        public void Clear() => _handlers.Clear();

        public void AddSubscription<T, TH>()
            where T : IntegrationEvent
            where TH : IIntegrationEventHandler<T>
        {
            var eventName = GetEventKey<T>();
            DoAddSubscription(typeof(TH), eventName);
            _eventTypes.Add(typeof(T));
        }

        private void DoAddSubscription(Type handlerType, string eventName)
        {
            if (!HasSubscriptionsForEvent(eventName))
            {
                _handlers.Add(eventName, new List<SubscriptionInfo>());
            }

            if (_handlers[eventName].Any(s => s.HandlerType == handlerType))
            {
                throw new ArgumentException(
                    $"Handler Type {handlerType.Name} already registered for '{eventName}'", nameof(handlerType));
            }

            _handlers[eventName].Add(new SubscriptionInfo(handlerType));
        }

        public void RemoveSubscription<T, TH>()
            where TH : IIntegrationEventHandler<T>
            where T : IntegrationEvent
        {
            var handlerToRemove = FindSubscriptionToRemove<T, TH>();
            var eventName = GetEventKey<T>();
            DoRemoveHandler(eventName, handlerToRemove);
        }


        private void DoRemoveHandler(string eventName, SubscriptionInfo subsToRemove)
        {
            if (subsToRemove != null)
            {
                _handlers[eventName].Remove(subsToRemove);
                if (!_handlers[eventName].Any())
                {
                    _handlers.Remove(eventName);
                    var eventType = _eventTypes.SingleOrDefault(e => e.Name == eventName);
                    if (eventType != null)
                    {
                        _eventTypes.Remove(eventType);
                    }
                    RaiseOnEventRemoved(eventName);
                }

            }
        }

        public IEnumerable<SubscriptionInfo> GetHandlersForEvent<T>() where T : IntegrationEvent
        {
            var key = GetEventKey<T>();
            return GetHandlersForEvent(key);
        }
        public IEnumerable<SubscriptionInfo> GetHandlersForEvent(string eventName) => _handlers[eventName];

        private void RaiseOnEventRemoved(string eventName)
        {
            var handler = OnEventRemoved;
            if (handler != null)
            {
                OnEventRemoved(this, eventName);
            }
        }

        private SubscriptionInfo FindSubscriptionToRemove<T, TH>()
            where T : IntegrationEvent
            where TH : IIntegrationEventHandler<T>
        {
            var eventName = GetEventKey<T>();
            return DoFindSubscriptionToRemove(eventName, typeof(TH));
        }

        private SubscriptionInfo DoFindSubscriptionToRemove(string eventName, Type handlerType)
        {
            if (!HasSubscriptionsForEvent(eventName))
            {
                return null;
            }

            return _handlers[eventName].SingleOrDefault(s => s.HandlerType == handlerType);

        }

        public bool HasSubscriptionsForEvent<T>() where T : IntegrationEvent
        {
            var key = GetEventKey<T>();
            return HasSubscriptionsForEvent(key);
        }
        public bool HasSubscriptionsForEvent(string eventName) 
            => _handlers.ContainsKey(eventName);

        public Type GetEventTypeByName(string eventName) 
            => _eventTypes.SingleOrDefault(t => t.Name == eventName);

        public string GetEventKey<T>() 
            => typeof(T).Name;
    }
    #endregion

    #region RabbitMq implementation

    public interface IRabbitMqPersistentConnection
        : IDisposable
    {
        bool IsConnected { get; }

        bool TryConnect();

        IModel CreateModel();
    }

    public class RabbitMqPersistentConnection :
        IRabbitMqPersistentConnection
    {
        private readonly IConnectionFactory _connectionFactory;        
        private readonly ILogger<RabbitMqPersistentConnection> _logger;
        private IConnection _connection;
        private readonly Action<Action> _resillientPolicyAction;
        private object _syncRoot = new object();
        private bool _disposed;

        public RabbitMqPersistentConnection(
            IConnectionFactory connectionFactory, 
            ILogger<RabbitMqPersistentConnection> logger,             
            Action<Action> resillientPolicyAction)
        {
            _connectionFactory = connectionFactory;
            _logger = logger;            
            _resillientPolicyAction = resillientPolicyAction;
        }

        public bool IsConnected => 
            _connection != null && _connection.IsOpen && !_disposed;

        public IModel CreateModel()
        {
            if (!IsConnected)
            {
                throw new InvalidOperationException(
                    "No RabbitMQ connections are available to perform this action");
            }

            return _connection.CreateModel();
        }

        ~RabbitMqPersistentConnection() 
            => Dispose(false);

        public void Dispose()
        {
            // Dispose of unmanaged resources.
            Dispose(true);

            // Suppress finalization.
            GC.SuppressFinalize(this);            
        }

        public bool TryConnect()
        {
            _logger.LogInformation("RabbitMQ Client is trying to connect");

            lock (_syncRoot)
            {
                _resillientPolicyAction(() =>
                {
                    _connection = _connectionFactory
                        .CreateConnection();
                });

                if (IsConnected)
                {
                    _connection.ConnectionShutdown += OnConnectionShutdown;
                    _connection.CallbackException += OnCallbackException;
                    _connection.ConnectionBlocked += OnConnectionBlocked;

                    _logger.LogInformation("RabbitMQ Client acquired a persistent connection to '{HostName}' and is subscribed to failure events", _connection.Endpoint.HostName);
                    return true;
                }
                else
                {
                    _logger.LogCritical("FATAL ERROR: RabbitMQ connections could not be created and opened");
                    return false;
                }
            }                       
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
                    _connection?.Close();
                    _connection?.Dispose();
                }
                catch (IOException ex)
                {
                    _logger.LogCritical(@$"Exception message: 
                        {ex.Message} ---> {ex.InnerException?.Message}");
                }
            }

            _disposed = true;               
        }

        private void OnConnectionBlocked(object sender, ConnectionBlockedEventArgs e)
        {
            if (_disposed) return;

            _logger.LogWarning("A RabbitMQ connection is shutdown. Trying to re-connect...");

            TryConnect();
        }

        private void OnCallbackException(object sender, CallbackExceptionEventArgs e)
        {
            if (_disposed) return;

            _logger.LogWarning("A RabbitMQ connection throw exception. Trying to re-connect...");

            TryConnect();
        }

        private void OnConnectionShutdown(object sender, ShutdownEventArgs reason)
        {
            if (_disposed) return;

            _logger.LogWarning("A RabbitMQ connection is on shutdown. Trying to re-connect...");

            TryConnect();
        }
    }

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

            using var channel = _persistentConnection.CreateModel();
            channel.ConfirmSelect();

            channel.BasicNacks += (model, ea) =>
            {
                _logger.LogTrace("Event {EventName} with id = {EventId} publish failure", 
                    eventName, @event.Id);
            };

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
                    _logger.LogCritical(@$"Exception message: 
                        {ex.Message} ---> {ex.InnerException?.Message}");
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

                using var channel = _persistentConnection.CreateModel();
                
                channel.QueueBind(queue: _queueName,
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

        private void DeclareExchangeAndQueue()
        {
            if (!_persistentConnection.IsConnected)
            {
                _persistentConnection.TryConnect();
            }

            using var channel = _persistentConnection.CreateModel();
            
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
                    using var scope = _serviceProvider.CreateScope();
                    var handler = scope.ServiceProvider.GetService(subscription.HandlerType);
                    if(handler == null)
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

    #endregion

    #region Extensions
    public static class GenericTypeExtensions
    {
        public static string GetGenericTypeName(this Type type)
        {
            var typeName = string.Empty;

            if (type.IsGenericType)
            {
                var genericTypes = string.Join(",", type.GetGenericArguments().Select(t => t.Name).ToArray());
                typeName = $"{type.Name.Remove(type.Name.IndexOf('`'))}<{genericTypes}>";
            }
            else
            {
                typeName = type.Name;
            }

            return typeName;
        }
    }

    public record EventBusConfig(string Connection, string UserName, string Password);

    public static class EventBusExtensions
    {
        public static IServiceCollection AddEventBus(this IServiceCollection services, 
            string exchangeName, string queueName, EventBusConfig config, 
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
    #endregion

    */
}
