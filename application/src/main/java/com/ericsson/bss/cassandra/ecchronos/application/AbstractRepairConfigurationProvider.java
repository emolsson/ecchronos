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

import java.util.Map;
import java.util.Optional;

import org.springframework.context.ApplicationContext;

import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.TableMetadata;
import com.ericsson.bss.cassandra.ecchronos.application.config.Config;
import com.ericsson.bss.cassandra.ecchronos.connection.NativeConnectionProvider;
import com.ericsson.bss.cassandra.ecchronos.core.repair.RepairConfiguration;
import com.ericsson.bss.cassandra.ecchronos.core.utils.TableReference;

public abstract class AbstractRepairConfigurationProvider
{
    private static final String TWCS_CLASS = "org.apache.cassandra.db.compaction.TimeWindowCompactionStrategy";
    private static final String DTCS_CLASS = "org.apache.cassandra.db.compaction.DateTieredCompactionStrategy";

    protected final ApplicationContext applicationContext;

    private final RepairConfiguration defaultRepairConfiguration;
    private final NativeConnectionProvider nativeConnectionProvider;
    private final boolean twcsRepair;

    protected AbstractRepairConfigurationProvider(ApplicationContext applicationContext)
    {
        this.applicationContext = applicationContext;

        Config config = applicationContext.getBean(Config.class);
        this.twcsRepair = config.getRepair().isTwcsEnabled();
        this.defaultRepairConfiguration = config.getRepair().asRepairConfiguration();
        this.nativeConnectionProvider = applicationContext.getBean(NativeConnectionProvider.class);
    }

    public RepairConfiguration get(TableReference tableReference)
    {
        return forTable(tableReference).orElse(resolveDefaultConfig(tableReference));
    }

    public abstract Optional<RepairConfiguration> forTable(TableReference tableReference);

    private RepairConfiguration resolveDefaultConfig(TableReference tableReference)
    {
        if (!twcsRepair && isTwcsTable(tableReference))
        {
            return RepairConfiguration.DISABLED;
        }

        return defaultRepairConfiguration;
    }

    private boolean isTwcsTable(TableReference tableReference)
    {
        Metadata metadata = nativeConnectionProvider.getSession().getCluster().getMetadata();
        KeyspaceMetadata keyspaceMetadata = metadata.getKeyspace(tableReference.getKeyspace());
        if (keyspaceMetadata != null)
        {
            TableMetadata tableMetadata = keyspaceMetadata.getTable(tableReference.getTable());
            if (tableMetadata != null)
            {
                Map<String, String> compactionOptions = tableMetadata.getOptions().getCompaction();
                String compactionClass = compactionOptions.get("class");

                return TWCS_CLASS.equals(compactionClass) || DTCS_CLASS.equals(compactionClass);
            }
        }

        return false;
    }
}
