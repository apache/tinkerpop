/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.tinkerpop.gremlin.hadoop.structure.io;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationUtils;
import org.apache.tinkerpop.gremlin.hadoop.structure.HadoopGraph;
import org.apache.tinkerpop.gremlin.hadoop.structure.util.ConfUtil;
import org.apache.tinkerpop.gremlin.structure.io.IoRegistry;
import org.apache.tinkerpop.gremlin.structure.io.gryo.GryoPool;
import org.apache.tinkerpop.gremlin.structure.io.gryo.GryoVersion;
import org.apache.tinkerpop.gremlin.util.SystemUtil;

import java.util.Collections;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class HadoopPools {

    private HadoopPools() {
    }

    private static GryoPool GRYO_POOL = null;
    private static boolean INITIALIZED = false;

    public synchronized static void initialize(final Configuration configuration) {
        if (!INITIALIZED) {
            INITIALIZED = true;
            GRYO_POOL = GryoPool.build().
                    poolSize(configuration.getInt(GryoPool.CONFIG_IO_GRYO_POOL_SIZE, 256)).
                    version(GryoVersion.valueOf(configuration.getString(GryoPool.CONFIG_IO_GRYO_VERSION, GryoPool.CONFIG_IO_GRYO_POOL_VERSION_DEFAULT.name()))).
                    ioRegistries(configuration.getList(IoRegistry.IO_REGISTRY, Collections.emptyList())).
                    initializeMapper(m -> m.registrationRequired(false)).
                    create();
        }
    }

    public synchronized static void initialize(final org.apache.hadoop.conf.Configuration configuration) {
        HadoopPools.initialize(ConfUtil.makeApacheConfiguration(configuration));
    }

    public synchronized static void initialize(final GryoPool gryoPool) {
        GRYO_POOL = gryoPool;
        INITIALIZED = true;
    }

    public static GryoPool getGryoPool() {
        if (!INITIALIZED) {
            final Configuration configuration = SystemUtil.getSystemPropertiesConfiguration("tinkerpop", true);
            HadoopGraph.LOGGER.warn("The " + HadoopPools.class.getSimpleName() + " has not been initialized, using system properties configuration: " + ConfigurationUtils.toString(configuration));
            initialize(configuration);
        }
        return GRYO_POOL;
    }

    public static void close() {
        INITIALIZED = false;
        GRYO_POOL = null;
    }
}
