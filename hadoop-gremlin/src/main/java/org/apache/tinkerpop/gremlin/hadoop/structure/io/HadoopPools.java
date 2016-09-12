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

import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.Configuration;
import org.apache.tinkerpop.gremlin.hadoop.structure.HadoopGraph;
import org.apache.tinkerpop.gremlin.hadoop.structure.util.ConfUtil;
import org.apache.tinkerpop.gremlin.structure.io.gryo.GryoPool;

import java.util.Collections;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class HadoopPools {

    private HadoopPools() {
    }

    private static GryoPool GRYO_POOL = GryoPool.build().create();
    private static boolean INITIALIZED = false;

    public synchronized static void initialize(final Configuration configuration) {
        if (!INITIALIZED) {
            INITIALIZED = true;
            GRYO_POOL = GryoPool.build().
                    poolSize(configuration.getInt(GryoPool.CONFIG_IO_GRYO_POOL_SIZE, 256)).
                    ioRegistries(configuration.getList(GryoPool.CONFIG_IO_REGISTRY, Collections.emptyList())).
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
            HadoopGraph.LOGGER.warn("The " + HadoopPools.class.getSimpleName() + " has not been initialized, using the default pool");     // TODO: this is necessary because we can't get the pool intialized in the Merger code of the Hadoop process.
            initialize(new BaseConfiguration());
        }
        return GRYO_POOL;
    }
}
