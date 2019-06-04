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
package org.apache.tinkerpop.gremlin.structure.io.graphson;

import org.apache.tinkerpop.gremlin.structure.io.AbstractTypedCompatibilityTest;
import org.apache.tinkerpop.gremlin.structure.io.Compatibility;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerIoRegistryV2d0;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerIoRegistryV3d0;
import org.apache.tinkerpop.shaded.jackson.databind.ObjectMapper;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
@RunWith(Parameterized.class)
public class GraphSONTypedCompatibilityTest extends AbstractTypedCompatibilityTest {

    private static ObjectMapper mapperV2 = GraphSONMapper.build().
            addRegistry(TinkerIoRegistryV2d0.instance()).
            typeInfo(TypeInfo.PARTIAL_TYPES).
            addCustomModule(GraphSONXModuleV2d0.build().create(false)).
            addCustomModule(new org.apache.tinkerpop.gremlin.driver.ser.AbstractGraphSONMessageSerializerV2d0.GremlinServerModule()).
            version(GraphSONVersion.V2_0).create().createMapper();

    private static ObjectMapper mapperV3 = GraphSONMapper.build().
            addRegistry(TinkerIoRegistryV3d0.instance()).
            addCustomModule(GraphSONXModuleV3d0.build().create(false)).
            addCustomModule(new org.apache.tinkerpop.gremlin.driver.ser.GraphSONMessageSerializerV3d0.GremlinServerModule()).
            version(GraphSONVersion.V3_0).create().createMapper();

    @Parameterized.Parameters(name = "expect({0})")
    public static Iterable<Object[]> data() {
        return Arrays.asList(new Object[][]{
                {GraphSONCompatibility.V2D0_PARTIAL_3_2_3, mapperV2 },
                {GraphSONCompatibility.V2D0_PARTIAL_3_2_4, mapperV2 },
                {GraphSONCompatibility.V2D0_PARTIAL_3_2_5, mapperV2 },
                {GraphSONCompatibility.V2D0_PARTIAL_3_2_6, mapperV2 },
                {GraphSONCompatibility.V2D0_PARTIAL_3_2_7, mapperV2 },
                {GraphSONCompatibility.V2D0_PARTIAL_3_2_8, mapperV2 },
                {GraphSONCompatibility.V2D0_PARTIAL_3_2_9, mapperV2 },
                {GraphSONCompatibility.V2D0_PARTIAL_3_2_10, mapperV2 },
                {GraphSONCompatibility.V2D0_PARTIAL_3_3_0, mapperV2 },
                {GraphSONCompatibility.V3D0_PARTIAL_3_3_0, mapperV3 },
                {GraphSONCompatibility.V2D0_PARTIAL_3_3_1, mapperV2 },
                {GraphSONCompatibility.V3D0_PARTIAL_3_3_1, mapperV3 },
                {GraphSONCompatibility.V2D0_PARTIAL_3_3_2, mapperV2 },
                {GraphSONCompatibility.V3D0_PARTIAL_3_3_2, mapperV3 },
                {GraphSONCompatibility.V2D0_PARTIAL_3_3_3, mapperV2 },
                {GraphSONCompatibility.V3D0_PARTIAL_3_3_3, mapperV3 },
                {GraphSONCompatibility.V2D0_PARTIAL_3_3_4, mapperV2 },
                {GraphSONCompatibility.V3D0_PARTIAL_3_3_4, mapperV3 },
                {GraphSONCompatibility.V2D0_PARTIAL_3_3_5, mapperV2 },
                {GraphSONCompatibility.V3D0_PARTIAL_3_3_5, mapperV3 },
                {GraphSONCompatibility.V2D0_PARTIAL_3_3_6, mapperV2 },
                {GraphSONCompatibility.V3D0_PARTIAL_3_3_6, mapperV3 },
                {GraphSONCompatibility.V2D0_PARTIAL_3_3_7, mapperV2 },
                {GraphSONCompatibility.V3D0_PARTIAL_3_3_7, mapperV3 },
                {GraphSONCompatibility.V2D0_PARTIAL_3_3_8, mapperV2 },
                {GraphSONCompatibility.V3D0_PARTIAL_3_3_8, mapperV3 },
                {GraphSONCompatibility.V2D0_PARTIAL_3_4_0, mapperV2 },
                {GraphSONCompatibility.V3D0_PARTIAL_3_4_0, mapperV3 },
                {GraphSONCompatibility.V2D0_PARTIAL_3_4_1, mapperV2 },
                {GraphSONCompatibility.V3D0_PARTIAL_3_4_1, mapperV3 },
                {GraphSONCompatibility.V2D0_PARTIAL_3_4_2, mapperV2 },
                {GraphSONCompatibility.V3D0_PARTIAL_3_4_2, mapperV3 },
                {GraphSONCompatibility.V2D0_PARTIAL_3_4_3, mapperV2 },
                {GraphSONCompatibility.V3D0_PARTIAL_3_4_3, mapperV3 }
        });
    }

    @Parameterized.Parameter(value = 0)
    public Compatibility compatibility;

    @Parameterized.Parameter(value = 1)
    public ObjectMapper mapper;

    @Override
    public <T> T read(final byte[] bytes, final Class<T> clazz) throws Exception {
        return mapper.readValue(bytes, clazz);
    }

    @Override
    public byte[] write(final Object o, final Class<?> clazz) throws Exception  {
        return mapper.writeValueAsBytes(o);
    }

    @Override
    public Compatibility getCompatibility() {
        return compatibility;
    }
}
