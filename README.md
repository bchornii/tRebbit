# tRebbit

.NET Standart library for subscribing/publishing messages to RabbitMq. Under the hood uses official RabbitMq .NET client, but provides more abstract interface.

## Goals
 - Simplify publising/subscribing to events, without referring to RabbitMq building blocks. 
 - Provide basic implementation wich could be extended.
 
## Main features
 - Separated abstractions and implementations into different libraries. It allows you to use interfaces only inside Application/Domain Layer and bind them to implemenrations only
   inside Infrastructure Layer.
 - Dead-lettering is optional and could help to keep track of failures during original event processing.
 - IServiceCollection.AddEventBus helps to hook up everything in single place to be good to go.
 - In-memory subscription management keeps track of events/handles registered by your app.
 - IIntegrationEventHandler is a basic interface to implement for particular type of event to handle.
 - Is not opinionated about resillience library or approach you use by accepting delegates for this from client code.
 
## Sample
   
  1. Define event message by inheriting IntegrationEvent type: 
   
```    
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
```

2. Define event handles by implementing IIntegrationEventHandler interface:
```
    public class PaymentCompletedIntegrationEventHandler : 
        IIntegrationEventHandler<PaymentCompletedEvent>
    {
        public Task Handle(PaymentCompletedEvent @event, bool isFirstDispatch)
        {
            // uncomment to see how event flows into DLX
            //throw new ArgumentNullException();

            Console.WriteLine("Payment completed");
            Console.WriteLine(System.Text.Json.JsonSerializer.Serialize(@event));
            return Task.CompletedTask;
        }
    }
```

3. Register event handles with DI: 

```
 services.AddScoped<PaymentCompletedIntegrationEventHandler>();
```

4. Wire up everything by calling .AddEventBus:

```
    services.AddEventBus(
      exchangeName: "your_exchange_name",
      queueName: "your_task_queue_name",
      config: new RabbitMqConfig
      {
        Connection = "localhost",
        UserName = "guest",
        Password = "guest"
      },
      resillientConnectionPolicyAction: , // here place reference to resillience action which will handle connections, for example RetryPolicy.Execute
      resillientPublishPolicyAction: ,    // here place reference to resillience action which will handle publishes, for example RetryPolicy.Execute
      preprocessAction:                   // any preprocess action before event is actually handled by your custom event handles
  );
```
  
    