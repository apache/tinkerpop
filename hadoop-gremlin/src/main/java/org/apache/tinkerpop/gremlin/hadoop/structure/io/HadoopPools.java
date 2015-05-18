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
import org.apache.tinkerpop.gremlin.hadoop.structure.util.ConfUtil;
import org.apache.tinkerpop.gremlin.structure.io.gryo.GryoPool;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class HadoopPools {

    private HadoopPools() {
    }

    private static GryoPool GRYO_POOL;

    public synchronized static void initialize(final Configuration configuration) {
        if (null == GRYO_POOL) {
            GRYO_POOL = new GryoPool(configuration);
        }
    }

    public synchronized static void initialize(final org.apache.hadoop.conf.Configuration configuration) {
        if (null == GRYO_POOL) {
            GRYO_POOL = new GryoPool(ConfUtil.makeApacheConfiguration(configuration));
        }
    }

    public static GryoPool getGryoPool() {
        if (null == GRYO_POOL)
            throw new IllegalStateException("The GryoPool has not been initialized");
        return GRYO_POOL;
    }
}
