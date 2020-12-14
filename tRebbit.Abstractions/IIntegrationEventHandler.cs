using System.Threading.Tasks;

namespace tRebbit.Abstractions
{
    public interface IIntegrationEventHandler<in TIntegrationEvent>
      where TIntegrationEvent : IntegrationEvent
    {
        Task Handle(TIntegrationEvent @event, bool isFirstDispatch);
    }
}
