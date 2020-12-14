using System;
using System.Text.Json.Serialization;

namespace tRebbit.Abstractions
{
    public class IntegrationEvent
    {
        public Guid Id { get; private set; }
        public DateTime CreationDate { get; private set; }
        public string CorrelationId { get; private set; }

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
}
