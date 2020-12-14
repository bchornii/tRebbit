using Microsoft.Extensions.Logging;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System;
using System.IO;

namespace tRebbit
{
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
                    _logger.LogCritical($"Exception message: {ex.Message} ---> {ex.InnerException?.Message}");
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

}
