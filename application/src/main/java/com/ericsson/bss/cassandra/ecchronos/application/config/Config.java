/*
 * Copyright 2020 Telefonaktiebolaget LM Ericsson
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.ericsson.bss.cassandra.ecchronos.application.config; // NOPMD

import java.io.File;
import java.util.Locale;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import org.springframework.context.ApplicationContext;

import com.ericsson.bss.cassandra.ecchronos.application.AbstractRepairConfigurationProvider;
import com.ericsson.bss.cassandra.ecchronos.application.FileBasedRepairConfiguration;
import com.ericsson.bss.cassandra.ecchronos.connection.JmxConnectionProvider;
import com.ericsson.bss.cassandra.ecchronos.connection.NativeConnectionProvider;
import com.ericsson.bss.cassandra.ecchronos.connection.StatementDecorator;
import com.ericsson.bss.cassandra.ecchronos.core.repair.RepairLockType;

public class Config
{
    private ConnectionConfig connection;
    private GlobalRepairConfig repair;
    private StatisticsConfig statistics;
    private LockFactoryConfig lock_factory;
    private RunPolicyConfig run_policy;
    private SchedulerConfig scheduler;
    private RestServerConfig rest_server;

    public ConnectionConfig getConnectionConfig()
    {
        return connection;
    }

    public void setConnection(ConnectionConfig connection)
    {
        this.connection = connection;
    }

    public GlobalRepairConfig getRepair()
    {
        return repair;
    }

    public void setRepair(GlobalRepairConfig repair)
    {
        this.repair = repair;
    }

    public StatisticsConfig getStatistics()
    {
        return statistics;
    }

    public void setStatistics(StatisticsConfig statistics)
    {
        this.statistics = statistics;
    }

    public LockFactoryConfig getLockFactory()
    {
        return lock_factory;
    }

    public void setLock_factory(LockFactoryConfig lock_factory)
    {
        this.lock_factory = lock_factory;
    }

    public RunPolicyConfig getRunPolicy()
    {
        return run_policy;
    }

    public void setRun_policy(RunPolicyConfig run_policy)
    {
        this.run_policy = run_policy;
    }

    public SchedulerConfig getScheduler()
    {
        return scheduler;
    }

    public void setScheduler(SchedulerConfig scheduler)
    {
        this.scheduler = scheduler;
    }

    public RestServerConfig getRestServer()
    {
        return rest_server;
    }

    public void setRest_server(RestServerConfig rest_server)
    {
        this.rest_server = rest_server;
    }

    public static class ConnectionConfig
    {
        private NativeConnection cql;
        private JmxConnection jmx;

        public NativeConnection getCql()
        {
            return cql;
        }

        public JmxConnection getJmx()
        {
            return jmx;
        }

        public void setCql(NativeConnection cql)
        {
            this.cql = cql;
        }

        public void setJmx(JmxConnection jmx)
        {
            this.jmx = jmx;
        }

        @Override
        public String toString()
        {
            return String.format("Connection(native=%s, jmx=%s)", cql, jmx);
        }
    }

    public static abstract class Connection<T>
    {
        private String host;
        private int port;
        private Class<T> provider;
        private Timeout timeout;

        public String getHost()
        {
            return host;
        }

        public int getPort()
        {
            return port;
        }

        public Class<T> getProviderClass()
        {
            return provider;
        }

        public Timeout getTimeout()
        {
            return timeout;
        }

        public void setHost(String host)
        {
            this.host = host;
        }

        public void setPort(int port)
        {
            this.port = port;
        }

        public void setTimeout(Timeout timeout)
        {
            this.timeout = timeout;
        }

        public void setProvider(Class<T> provider) throws NoSuchMethodException
        {
            provider.getDeclaredConstructor(expectedConstructor());

            this.provider = provider;
        }

        protected abstract Class<?>[] expectedConstructor();

        @Override
        public String toString()
        {
            return String.format("(%s:%d:%s),provider=%s", host, port, timeout, provider);
        }

        public static class Timeout
        {
            private long time;
            private TimeUnit unit;

            public long getConnectionTimeout(TimeUnit unit)
            {
                return unit.convert(time, this.unit);
            }

            public void setTime(long time)
            {
                this.time = time;
            }

            public void setUnit(String unit)
            {
                this.unit = TimeUnit.valueOf(unit.toUpperCase(Locale.US));
            }
        }
    }

    public static class NativeConnection extends Connection<NativeConnectionProvider>
    {
        private Class<StatementDecorator> decoratorClass;

        public Class<StatementDecorator> getDecoratorClass()
        {
            return decoratorClass;
        }

        public void setDecoratorClass(Class<StatementDecorator> decoratorClass) throws NoSuchMethodException
        {
            decoratorClass.getDeclaredConstructor(Config.class);

            this.decoratorClass = decoratorClass;
        }

        @Override
        protected Class<?>[] expectedConstructor()
        {
            return new Class<?>[] { Config.class, Supplier.class };
        }

        @Override
        public String toString()
        {
            return String.format("(%s:%d),provider=%s,decorator=%s", getHost(), getPort(), getProviderClass(),
                    decoratorClass);
        }
    }

    public static class JmxConnection extends Connection<JmxConnectionProvider>
    {
        @Override
        protected Class<?>[] expectedConstructor()
        {
            return new Class<?>[] { Config.class, Supplier.class };
        }
    }

    public static class GlobalRepairConfig extends RepairConfig
    {
        private RepairLockType lock_type;
        private Interval history_lookback;
        private RepairHistory history;
        private Class<? extends AbstractRepairConfigurationProvider> provider = FileBasedRepairConfiguration.class;
        private boolean enable_twcs_repairs = false;

        public RepairLockType getLockType()
        {
            return lock_type;
        }

        public Interval getHistoryLookback()
        {
            return history_lookback;
        }

        public void setLock_type(String lock_type)
        {
            this.lock_type = RepairLockType.valueOf(lock_type.toUpperCase(Locale.US));
        }

        public void setHistory_lookback(Interval history_lookback)
        {
            this.history_lookback = history_lookback;
        }

        public RepairHistory getHistory()
        {
            return history;
        }

        public void setHistory(RepairHistory history)
        {
            this.history = history;
        }

        public Class<? extends AbstractRepairConfigurationProvider> getProvider()
        {
            return provider;
        }

        public void setProvider(Class<? extends AbstractRepairConfigurationProvider> provider)
                throws NoSuchMethodException
        {
            provider.getDeclaredConstructor(ApplicationContext.class);

            this.provider = provider;
        }

        public boolean isTwcsEnabled()
        {
            return enable_twcs_repairs;
        }

        public void setEnable_twcs_repairs(boolean enable_twcs_repairs)
        {
            this.enable_twcs_repairs = enable_twcs_repairs;
        }
    }

    public static class RepairHistory
    {
        public enum Provider
        {
            CASSANDRA, UPGRADE, ECC
        }

        private Provider provider;
        private String keyspace;

        public Provider getProvider()
        {
            return provider;
        }

        public void setProvider(String provider)
        {
            this.provider = Provider.valueOf(provider.toUpperCase(Locale.US));
        }

        public String getKeyspace()
        {
            return keyspace;
        }

        public void setKeyspace(String keyspace)
        {
            this.keyspace = keyspace;
        }
    }

    public static class Alarm
    {
        private Interval warn;
        private Interval error;

        public Alarm()
        {
            // Default constructor for jackson
        }

        public Alarm(Interval warn, Interval error)
        {
            this.warn = warn;
            this.error = error;
        }

        public Interval getWarn()
        {
            return warn;
        }

        public Interval getError()
        {
            return error;
        }

        public void setWarn(Interval warn)
        {
            this.warn = warn;
        }

        public void setError(Interval error)
        {
            this.error = error;
        }
    }

    public static class StatisticsConfig
    {
        private boolean enabled;
        private File directory;

        public boolean isEnabled()
        {
            return enabled;
        }

        public File getDirectory()
        {
            return directory;
        }

        public void setEnabled(boolean enabled)
        {
            this.enabled = enabled;
        }

        public void setDirectory(String directory)
        {
            this.directory = new File(directory);
        }
    }

    public static class LockFactoryConfig
    {
        private CasLockFactoryConfig cas;

        public CasLockFactoryConfig getCas()
        {
            return cas;
        }

        public void setCas(CasLockFactoryConfig cas)
        {
            this.cas = cas;
        }
    }

    public static class CasLockFactoryConfig
    {
        private String keyspace;

        public String getKeyspace()
        {
            return keyspace;
        }

        public void setKeyspace(String keyspace)
        {
            this.keyspace = keyspace;
        }
    }

    public static class RunPolicyConfig
    {
        private TimeBasedConfig time_based;

        public TimeBasedConfig getTimeBased()
        {
            return time_based;
        }

        public void setTime_based(TimeBasedConfig time_based)
        {
            this.time_based = time_based;
        }
    }

    public static class TimeBasedConfig
    {
        private String keyspace;

        public String getKeyspace()
        {
            return keyspace;
        }

        public void setKeyspace(String keyspace)
        {
            this.keyspace = keyspace;
        }
    }

    public static class SchedulerConfig
    {
        private Interval frequency;

        public Interval getFrequency()
        {
            return frequency;
        }

        public void setFrequency(Interval frequency)
        {
            this.frequency = frequency;
        }
    }

    public static class RestServerConfig
    {
        private String host;
        private int port;

        public String getHost()
        {
            return host;
        }

        public void setHost(String host)
        {
            this.host = host;
        }

        public int getPort()
        {
            return port;
        }

        public void setPort(int port)
        {
            this.port = port;
        }
    }

    public static class Interval
    {
        private long time;
        private TimeUnit unit;

        public Interval()
        {
            // Default constructor for jackson
        }

        public Interval(long time, TimeUnit unit)
        {
            this.time = time;
            this.unit = unit;
        }

        public long getInterval(TimeUnit unit)
        {
            return unit.convert(time, this.unit);
        }

        public void setTime(long time)
        {
            this.time = time;
        }

        public void setUnit(String unit)
        {
            this.unit = TimeUnit.valueOf(unit.toUpperCase(Locale.US));
        }
    }
}
