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
package com.ericsson.bss.cassandra.ecchronos.application;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.springframework.context.ApplicationContext;

import com.datastax.driver.core.*;
import com.ericsson.bss.cassandra.ecchronos.application.config.Config;
import com.ericsson.bss.cassandra.ecchronos.application.config.ConfigurationHelper;
import com.ericsson.bss.cassandra.ecchronos.connection.NativeConnectionProvider;
import com.ericsson.bss.cassandra.ecchronos.core.repair.RepairConfiguration;
import com.ericsson.bss.cassandra.ecchronos.core.utils.TableReference;
import com.ericsson.bss.cassandra.ecchronos.core.utils.UnitConverter;
import com.google.common.collect.ImmutableMap;

@RunWith(MockitoJUnitRunner.class)
public class TestFileBasedRepairConfiguration
{
    @Mock
    private ApplicationContext mockApplicationContext;

    @Mock
    private Config config;

    @Mock
    private NativeConnectionProvider mockNativeConnectionProvider;

    @Mock
    private Session mockSession;

    @Mock
    private Cluster mockCluster;

    @Mock
    private Metadata mockMetadata;

    private Map<String, KeyspaceMetadata> mockedKeyspaces = new HashMap<>();

    private Map<String, Map<String, TableMetadata>> mockedTables = new HashMap<>();

    @Before
    public void setup()
    {
        when(mockApplicationContext.getBean(eq(Config.class))).thenReturn(config);
        when(mockApplicationContext.getBean(eq(NativeConnectionProvider.class)))
                .thenReturn(mockNativeConnectionProvider);
        when(config.getRepair()).thenReturn(new Config.GlobalRepairConfig());

        when(mockNativeConnectionProvider.getSession()).thenReturn(mockSession);
        when(mockSession.getCluster()).thenReturn(mockCluster);
        when(mockCluster.getMetadata()).thenReturn(mockMetadata);

        when(mockMetadata.getKeyspace(anyString())).thenAnswer(inv -> {
            String keyspace = inv.getArgumentAt(0, String.class);
            return mockedKeyspaces.get(keyspace);
        });
    }

    @Test
    public void testNoSchedule() throws Exception
    {
        AbstractRepairConfigurationProvider repairConfigProvider = withSchedule("schedule.yml");

        assertThat(repairConfigProvider.get(tableReference("any", "table"))).isEqualTo(RepairConfiguration.DEFAULT);
    }

    @Test
    public void testAllSchedules() throws Exception
    {
        AbstractRepairConfigurationProvider repairConfigProvider = withSchedule("regex_schedule.yml");

        RepairConfiguration allKeyspacesPattern = RepairConfiguration.newBuilder()
                .withRepairInterval(8, TimeUnit.DAYS)
                .build();

        RepairConfiguration allKeyspacesTb2 = RepairConfiguration.newBuilder()
                .withRepairInterval(5, TimeUnit.DAYS)
                .build();

        RepairConfiguration ks2Tb1 = RepairConfiguration.newBuilder()
                .withRepairInterval(1, TimeUnit.DAYS)
                .build();
        RepairConfiguration ks2Tb2 = RepairConfiguration.newBuilder()
                .withRepairInterval(2, TimeUnit.DAYS)
                .build();

        assertConfig(repairConfigProvider, "any", "nonexisting", RepairConfiguration.DEFAULT);

        assertConfig(repairConfigProvider, "any", "table_abc", allKeyspacesPattern);
        assertConfig(repairConfigProvider, "ks2", "table_abc", allKeyspacesPattern);

        assertConfig(repairConfigProvider, "any", "tb2", allKeyspacesTb2);

        assertConfig(repairConfigProvider, "ks2", "tb1", ks2Tb1);
        assertConfig(repairConfigProvider, "ks2", "tb2", ks2Tb2);

        assertConfig(repairConfigProvider, "ks2", "tb23", RepairConfiguration.DEFAULT);

        assertConfig(repairConfigProvider, "any", "table", RepairConfiguration.DEFAULT);
    }

    @Test
    public void testTwcsTablesDisabled() throws Exception
    {
        AbstractRepairConfigurationProvider repairConfigProvider = withSchedule("test_schedule.yml");

        mockTWCSTable("ks1", "tb1");
        mockDTCSTable("ks1", "tb2");
        mockTWCSTable("another", "tb1"); // Should be disabled
        mockTWCSTable("another", "tb2"); // Should be disabled

        RepairConfiguration ks1tb1 = RepairConfiguration.newBuilder()
                .withRepairInterval(8, TimeUnit.DAYS)
                .withRepairWarningTime(9, TimeUnit.DAYS)
                .withRepairErrorTime(11, TimeUnit.DAYS)
                .withRepairUnwindRatio(1.0d)
                .withTargetRepairSizeInBytes(UnitConverter.toBytes("15m"))
                .build();

        RepairConfiguration ks1tb2 = RepairConfiguration.newBuilder()
                .withRepairInterval(5, TimeUnit.DAYS)
                .build();

        assertThat(repairConfigProvider.get(tableReference("ks1", "tb1"))).isEqualTo(ks1tb1);
        assertThat(repairConfigProvider.get(tableReference("ks1", "tb2"))).isEqualTo(ks1tb2);

        assertThat(repairConfigProvider.get(tableReference("another", "tb1"))).isEqualTo(RepairConfiguration.DISABLED);
        assertThat(repairConfigProvider.get(tableReference("another", "tb2"))).isEqualTo(RepairConfiguration.DISABLED);
    }

    @Test
    public void testTwcsTablesEnabled() throws Exception
    {
        Config.GlobalRepairConfig repairConfig = new Config.GlobalRepairConfig();
        repairConfig.setEnable_twcs_repairs(true);
        when(config.getRepair()).thenReturn(repairConfig);

        AbstractRepairConfigurationProvider repairConfigProvider = withSchedule("test_schedule.yml");

        mockTWCSTable("ks1", "tb1");
        mockDTCSTable("ks1", "tb2");
        mockTWCSTable("another", "tb1");
        mockTWCSTable("another", "tb2");

        RepairConfiguration ks1tb1 = RepairConfiguration.newBuilder()
                .withRepairInterval(8, TimeUnit.DAYS)
                .withRepairWarningTime(9, TimeUnit.DAYS)
                .withRepairErrorTime(11, TimeUnit.DAYS)
                .withRepairUnwindRatio(1.0d)
                .withTargetRepairSizeInBytes(UnitConverter.toBytes("15m"))
                .build();

        RepairConfiguration ks1tb2 = RepairConfiguration.newBuilder()
                .withRepairInterval(5, TimeUnit.DAYS)
                .build();

        assertThat(repairConfigProvider.get(tableReference("ks1", "tb1"))).isEqualTo(ks1tb1);
        assertThat(repairConfigProvider.get(tableReference("ks1", "tb2"))).isEqualTo(ks1tb2);

        assertThat(repairConfigProvider.get(tableReference("another", "tb1"))).isEqualTo(RepairConfiguration.DEFAULT);
        assertThat(repairConfigProvider.get(tableReference("another", "tb2"))).isEqualTo(RepairConfiguration.DEFAULT);
    }

    private void assertConfig(AbstractRepairConfigurationProvider repairConfigProvider, String keyspace, String table,
            RepairConfiguration repairConfiguration)
    {
        assertThat(repairConfigProvider.get(tableReference(keyspace, table))).isEqualTo(repairConfiguration);
    }

    private AbstractRepairConfigurationProvider withSchedule(String schedule) throws Exception
    {
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        File configFile = new File(classLoader.getResource(schedule).toURI());
        ConfigurationHelper configurationHelper = new ConfigurationHelper(configFile.getParent());

        return new FileBasedRepairConfiguration(mockApplicationContext, configurationHelper, schedule);
    }

    private void mockTWCSTable(String keyspace, String table)
    {
        mockTable(keyspace, table, "org.apache.cassandra.db.compaction.TimeWindowCompactionStrategy");
    }

    private void mockDTCSTable(String keyspace, String table)
    {
        mockTable(keyspace, table, "org.apache.cassandra.db.compaction.DateTieredCompactionStrategy");
    }

    private void mockTable(String keyspace, String table, String compactionStrategy)
    {
        KeyspaceMetadata keyspaceMetadata = mockedKeyspaces.computeIfAbsent(keyspace,
                ks -> mock(KeyspaceMetadata.class));

        Map<String, TableMetadata> tables = mockedTables.computeIfAbsent(keyspace, ks -> new HashMap<>());
        TableMetadata tableMetadata = tables.computeIfAbsent(table, tb -> mock(TableMetadata.class));

        when(keyspaceMetadata.getTable(eq(table))).thenReturn(tableMetadata);
        when(mockMetadata.getKeyspace(eq(keyspace))).thenReturn(keyspaceMetadata);

        TableOptionsMetadata tableOptionsMetadata = mock(TableOptionsMetadata.class);
        when(tableOptionsMetadata.getCompaction()).thenReturn(ImmutableMap.of("class", compactionStrategy));

        when(tableMetadata.getOptions()).thenReturn(tableOptionsMetadata);
    }

    private TableReference tableReference(String keyspace, String table)
    {
        TableReference tableReference = mock(TableReference.class);
        when(tableReference.getKeyspace()).thenReturn(keyspace);
        when(tableReference.getTable()).thenReturn(table);
        return tableReference;
    }
}
