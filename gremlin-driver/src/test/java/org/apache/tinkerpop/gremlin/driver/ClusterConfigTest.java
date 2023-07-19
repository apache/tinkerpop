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
package org.apache.tinkerpop.gremlin.driver;

import org.apache.commons.configuration2.BaseConfiguration;
import org.apache.commons.configuration2.Configuration;
import org.apache.tinkerpop.gremlin.util.ser.GraphSONMessageSerializerV2;
import org.apache.tinkerpop.gremlin.util.ser.GraphSONMessageSerializerV3;
import org.apache.tinkerpop.shaded.jackson.core.StreamReadConstraints;
import org.junit.Test;

import java.util.Arrays;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class ClusterConfigTest {

    @Test
    public void shouldPropagateSerializerConstraintsForGraphSON3() {
        final Configuration config = new BaseConfiguration();
        config.setProperty("serializer.config.maxNumberLength", 999);
        config.setProperty("serializer.config.maxStringLength", 123456);
        config.setProperty("serializer.config.maxNestingDepth", 55);
        config.setProperty("hosts", Arrays.asList("localhost"));

        config.setProperty("serializer.className", GraphSONMessageSerializerV3.class.getCanonicalName());
        final Cluster cluster = Cluster.open(config);
        assertTrue(cluster.getSerializer() instanceof GraphSONMessageSerializerV3);
        final GraphSONMessageSerializerV3 serV3 = (GraphSONMessageSerializerV3) cluster.getSerializer();
        final StreamReadConstraints constraints = serV3.getMapper().getFactory().streamReadConstraints();

        assertEquals(999, constraints.getMaxNumberLength());
        assertEquals(123456, constraints.getMaxStringLength());
        assertEquals(55, constraints.getMaxNestingDepth());
    }

    @Test
    public void shouldPropagateSerializerConstraintsForGraphSON2() {
        final Configuration config = new BaseConfiguration();
        config.setProperty("serializer.config.maxNumberLength", 999);
        config.setProperty("serializer.config.maxStringLength", 123456);
        config.setProperty("serializer.config.maxNestingDepth", 55);
        config.setProperty("hosts", Arrays.asList("localhost"));

        config.setProperty("serializer.className", GraphSONMessageSerializerV2.class.getCanonicalName());
        final Cluster cluster = Cluster.open(config);
        assertTrue(cluster.getSerializer() instanceof GraphSONMessageSerializerV2);
        final GraphSONMessageSerializerV2 serV2 = (GraphSONMessageSerializerV2) cluster.getSerializer();
        final StreamReadConstraints constraints = serV2.getMapper().getFactory().streamReadConstraints();

        assertEquals(999, constraints.getMaxNumberLength());
        assertEquals(123456, constraints.getMaxStringLength());
        assertEquals(55, constraints.getMaxNestingDepth());
    }
}
