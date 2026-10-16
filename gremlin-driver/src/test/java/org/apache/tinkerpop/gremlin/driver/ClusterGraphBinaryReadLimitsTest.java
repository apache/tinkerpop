/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
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
import org.apache.tinkerpop.gremlin.structure.io.binary.GraphBinaryReadLimits;
import org.apache.tinkerpop.gremlin.util.ser.GraphBinaryMessageSerializerV4;
import org.apache.tinkerpop.gremlin.util.ser.Serializers;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.Collections;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

public class ClusterGraphBinaryReadLimitsTest {

    private static final GraphBinaryReadLimits CONFIGURED = GraphBinaryReadLimits.of(1000, 2000, 3000);

    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    private Cluster cluster;

    @After
    public void closeCluster() {
        if (cluster != null) cluster.close();
    }

    private static GraphBinaryReadLimits limitsOf(final Cluster cluster) {
        return ((GraphBinaryMessageSerializerV4) cluster.getSerializer()).getMapper().getReader().getLimits();
    }

    @Test
    public void shouldUseDefaultLimits() {
        cluster = Cluster.build("localhost").create();

        assertEquals(GraphBinaryReadLimits.DEFAULT, limitsOf(cluster));
    }

    @Test
    public void shouldApplyLimitsFromBuilder() {
        cluster = Cluster.build("localhost")
                .maxGraphBinaryValueLength(1000)
                .maxGraphBinaryArrayLength(2000)
                .maxGraphBinaryInitialCapacity(3000)
                .create();

        assertEquals(CONFIGURED, limitsOf(cluster));
    }

    @Test
    public void shouldApplyLimitsToProvidedSerializer() {
        final GraphBinaryMessageSerializerV4 serializer = new GraphBinaryMessageSerializerV4();
        cluster = Cluster.build("localhost")
                .responseSerializer(serializer)
                .maxGraphBinaryValueLength(1000)
                .maxGraphBinaryArrayLength(2000)
                .maxGraphBinaryInitialCapacity(3000)
                .create();

        assertSame(serializer, cluster.getSerializer());
        assertEquals(CONFIGURED, limitsOf(cluster));
    }

    @Test
    public void shouldApplyLimitsFromSettingsFile() throws Exception {
        final File file = temporaryFolder.newFile("cluster.yaml");
        Files.write(file.toPath(), ("hosts: [localhost]\n" +
                "maxGraphBinaryValueLength: 1000\n" +
                "maxGraphBinaryArrayLength: 2000\n" +
                "maxGraphBinaryInitialCapacity: 3000\n").getBytes(StandardCharsets.UTF_8));

        cluster = Cluster.build(file).create();

        assertEquals(CONFIGURED, limitsOf(cluster));
    }

    @Test
    public void shouldApplyLimitsFromSettingsFileWithConfiguredSerializer() throws Exception {
        final File file = temporaryFolder.newFile("cluster-with-serializer.yaml");
        Files.write(file.toPath(), ("hosts: [localhost]\n" +
                "responseSerializer:\n" +
                "  className: " + GraphBinaryMessageSerializerV4.class.getName() + "\n" +
                "  config: { }\n" +
                "maxGraphBinaryValueLength: 1000\n" +
                "maxGraphBinaryArrayLength: 2000\n" +
                "maxGraphBinaryInitialCapacity: 3000\n").getBytes(StandardCharsets.UTF_8));

        cluster = Cluster.build(file).create();

        assertEquals(CONFIGURED, limitsOf(cluster));
    }

    @Test
    public void shouldApplyLimitsFromConfiguration() {
        final Configuration conf = new BaseConfiguration();
        conf.setProperty("hosts", Collections.singletonList("localhost"));
        conf.setProperty("maxGraphBinaryValueLength", "1000");
        conf.setProperty("maxGraphBinaryArrayLength", "2000");
        conf.setProperty("maxGraphBinaryInitialCapacity", "3000");

        cluster = Cluster.open(conf);

        assertEquals(CONFIGURED, limitsOf(cluster));
    }

    @Test
    public void shouldIgnoreLimitsForSerializersOtherThanGraphBinary() {
        cluster = Cluster.build("localhost")
                .responseSerializer(Serializers.GRAPHSON_V4)
                .maxGraphBinaryValueLength(1000)
                .create();

        assertTrue(Arrays.asList(cluster.getSerializers()).contains(Serializers.GRAPHSON_V4.getValue()));
    }
}
