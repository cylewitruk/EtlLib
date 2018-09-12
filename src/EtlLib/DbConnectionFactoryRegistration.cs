using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data;

namespace EtlLib
{
    public interface IDbConnectionFactory
    {
        IDbConnection CreateNamedConnection(string name);
    }

    public interface IDbConnectionRegistrar
    {
        IDbConnectionRegistrar For<T>(Action<IDbConnectionRegistrarProviderContext<T>> con)
            where T : class, IDbConnection, new();

        IDbConnectionRegistrar For(Func<string, IDbConnection> provider, Action<IDbConnectionRegistrarProviderContext> con);
    }

    public interface IDbConnectionRegistrarProviderContext<T>
        where T : class, IDbConnection, new()
    {
        IDbConnectionRegistrarProviderContext<T> Register(string name, string connectionString);
        IDbConnectionRegistrarProviderContext<T> RegisterConnectionString(string connectionStringName);
    }

    public interface IDbConnectionRegistrarProviderContext
    {
        IDbConnectionRegistrarProviderContext Register(string name, string connectionString);
        IDbConnectionRegistrarProviderContext RegisterConnectionString(string connectionStringName);
    }

    public class DbConnectionFactory : IDbConnectionRegistrar, IDbConnectionFactory
    {
        private readonly Dictionary<string, DbConnectionFactoryRegistration> _registrations;

        private IReadOnlyDictionary<string, DbConnectionFactoryRegistration> Registrations => _registrations;

        public DbConnectionFactory()
        {
            _registrations = new Dictionary<string, DbConnectionFactoryRegistration>();
        }

        public IDbConnectionRegistrar For<T>(Action<IDbConnectionRegistrarProviderContext<T>> con) 
            where T : class, IDbConnection, new()
        {
            var providerContext = new DbConnectionRegistrarProviderContext<T>();
            con(providerContext);
            foreach(var reg in providerContext.Registrations)
                _registrations.Add(reg.Key, reg.Value);
            return this;
        }

        public IDbConnectionRegistrar For(Func<string, IDbConnection> provider,
            Action<IDbConnectionRegistrarProviderContext> con)
        {
            var providerContext = new DbConnectionRegistrarProviderContext(provider);
            con(providerContext);
            foreach (var reg in providerContext.Registrations)
                _registrations.Add(reg.Key, reg.Value);
            return this;
        }

        public IDbConnection CreateNamedConnection(string name)
        {
            var reg = _registrations[name];

            return reg.CreateConnectionFn(reg.ConnectionString);
        }
    }

    public class DbConnectionRegistrarProviderContext<T> : IDbConnectionRegistrarProviderContext<T>
        where T : class, IDbConnection, new()
    {
        private readonly Dictionary<string, DbConnectionFactoryRegistration> _registrations;

        public IReadOnlyDictionary<string, DbConnectionFactoryRegistration> Registrations => _registrations;

        public DbConnectionRegistrarProviderContext()
        {
            _registrations = new Dictionary<string, DbConnectionFactoryRegistration>();
        }

        public IDbConnectionRegistrarProviderContext<T> Register(string name, string connectionString)
        {
            _registrations.Add(name, new DbConnectionFactoryRegistration(name, connectionString, cs => new T
            {
                ConnectionString = cs
            }));
            return this;
        }

        public IDbConnectionRegistrarProviderContext<T> RegisterConnectionString(string connectionStringName)
        {
            _registrations.Add(connectionStringName, new DbConnectionFactoryRegistration(connectionStringName, 
                ConfigurationManager.ConnectionStrings[connectionStringName].ConnectionString, 
                cs => new T
                {
                    ConnectionString = cs
                }));
            return this;
        }
    }

    public class DbConnectionRegistrarProviderContext : IDbConnectionRegistrarProviderContext
    {
        private readonly Dictionary<string, DbConnectionFactoryRegistration> _registrations;
        private readonly Func<string, IDbConnection> _createFn;

        public IReadOnlyDictionary<string, DbConnectionFactoryRegistration> Registrations => _registrations;

        public DbConnectionRegistrarProviderContext(Func<string, IDbConnection> createFn)
        {
            _registrations = new Dictionary<string, DbConnectionFactoryRegistration>();
            _createFn = createFn;
        }

        public IDbConnectionRegistrarProviderContext Register(string name, string connectionString)
        {
            _registrations.Add(name, new DbConnectionFactoryRegistration(name, connectionString, _createFn));
            return this;
        }

        public IDbConnectionRegistrarProviderContext RegisterConnectionString(string connectionStringName)
        {
            _registrations.Add(connectionStringName, 
                new DbConnectionFactoryRegistration(connectionStringName, 
                    ConfigurationManager.ConnectionStrings[connectionStringName].ConnectionString, _createFn));
            return this;
        }
    }

    public class DbConnectionFactoryRegistration
    {
        public string Name { get; }
        public Func<string, IDbConnection> CreateConnectionFn { get; }
        public string ConnectionString { get; }

        public DbConnectionFactoryRegistration(string name, string connectionString,
            Func<string, IDbConnection> createConnectionFn)
        {
            Name = name;
            ConnectionString = connectionString;
            CreateConnectionFn = createConnectionFn;
        }

        public IDbConnection CreateDbConnection()
        {
            return CreateConnectionFn(ConnectionString);
        }
    }
}