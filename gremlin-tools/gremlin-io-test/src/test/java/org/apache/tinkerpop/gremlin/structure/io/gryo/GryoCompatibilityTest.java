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
package org.apache.tinkerpop.gremlin.structure.io.gryo;

import org.apache.commons.io.output.ByteArrayOutputStream;
import org.apache.tinkerpop.gremlin.structure.io.AbstractTypedCompatibilityTest;
import org.apache.tinkerpop.gremlin.structure.io.Compatibility;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerIoRegistryV2d0;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerIoRegistryV3d0;
import org.apache.tinkerpop.shaded.kryo.Kryo;
import org.apache.tinkerpop.shaded.kryo.io.Input;
import org.apache.tinkerpop.shaded.kryo.io.Output;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
@RunWith(Parameterized.class)
public class GryoCompatibilityTest extends AbstractTypedCompatibilityTest {

    private static Kryo mapperV1 = GryoMapper.build().
            version(GryoVersion.V1_0).
            addRegistry(TinkerIoRegistryV2d0.instance()).create().createMapper();
    private static Kryo mapperV3 = GryoMapper.build().
            version(GryoVersion.V3_0).
            addRegistry(TinkerIoRegistryV3d0.instance()).create().createMapper();

    @Parameterized.Parameters(name = "expect({0})")
    public static Iterable<Object[]> data() {
        return Arrays.asList(new Object[][]{
                {GryoCompatibility.V1D0_3_2_3, mapperV1 },
                {GryoCompatibility.V1D0_3_2_4, mapperV1 },
                {GryoCompatibility.V1D0_3_2_5, mapperV1 },
                {GryoCompatibility.V1D0_3_2_6, mapperV1 },
                {GryoCompatibility.V1D0_3_2_7, mapperV1 },
                {GryoCompatibility.V1D0_3_2_8, mapperV1 },
                {GryoCompatibility.V1D0_3_2_9, mapperV1 },
                {GryoCompatibility.V1D0_3_2_10, mapperV1 },
                {GryoCompatibility.V1D0_3_3_0, mapperV1 },
                {GryoCompatibility.V3D0_3_3_0, mapperV3 },
                {GryoCompatibility.V1D0_3_3_1, mapperV1 },
                {GryoCompatibility.V3D0_3_3_1, mapperV3 },
                {GryoCompatibility.V1D0_3_3_2, mapperV1 },
                {GryoCompatibility.V3D0_3_3_2, mapperV3 },
                {GryoCompatibility.V1D0_3_3_3, mapperV1 },
                {GryoCompatibility.V3D0_3_3_3, mapperV3 },
                {GryoCompatibility.V1D0_3_3_4, mapperV1 },
                {GryoCompatibility.V3D0_3_3_4, mapperV3 },
                {GryoCompatibility.V1D0_3_3_5, mapperV1 },
                {GryoCompatibility.V3D0_3_3_5, mapperV3 },
                {GryoCompatibility.V1D0_3_3_6, mapperV1 },
                {GryoCompatibility.V3D0_3_3_6, mapperV3 },
                {GryoCompatibility.V1D0_3_3_7, mapperV1 },
                {GryoCompatibility.V3D0_3_3_7, mapperV3 },
                {GryoCompatibility.V1D0_3_3_8, mapperV1 },
                {GryoCompatibility.V3D0_3_3_8, mapperV3 },
                {GryoCompatibility.V1D0_3_3_9, mapperV1 },
                {GryoCompatibility.V3D0_3_3_9, mapperV3 },
                {GryoCompatibility.V1D0_3_3_10, mapperV1 },
                {GryoCompatibility.V3D0_3_3_10, mapperV3 },
                {GryoCompatibility.V1D0_3_3_11, mapperV1 },
                {GryoCompatibility.V3D0_3_3_11, mapperV3 },
                {GryoCompatibility.V1D0_3_4_0, mapperV1 },
                {GryoCompatibility.V3D0_3_4_0, mapperV3 },
                {GryoCompatibility.V1D0_3_4_1, mapperV1 },
                {GryoCompatibility.V3D0_3_4_1, mapperV3 },
                {GryoCompatibility.V1D0_3_4_2, mapperV1 },
                {GryoCompatibility.V3D0_3_4_2, mapperV3 },
                {GryoCompatibility.V1D0_3_4_3, mapperV1 },
                {GryoCompatibility.V3D0_3_4_3, mapperV3 },
                {GryoCompatibility.V1D0_3_4_4, mapperV1 },
                {GryoCompatibility.V3D0_3_4_4, mapperV3 },
                {GryoCompatibility.V1D0_3_4_5, mapperV1 },
                {GryoCompatibility.V3D0_3_4_5, mapperV3 },
                {GryoCompatibility.V1D0_3_4_6, mapperV1 },
                {GryoCompatibility.V3D0_3_4_6, mapperV3 },
                {GryoCompatibility.V1D0_3_4_7, mapperV1 },
                {GryoCompatibility.V3D0_3_4_7, mapperV3 },

                {GryoCompatibility.V1D0_3_5_0, mapperV1 },
                {GryoCompatibility.V3D0_3_5_0, mapperV3 }
        });
    }

    @Parameterized.Parameter(value = 0)
    public Compatibility compatibility;

    @Parameterized.Parameter(value = 1)
    public Kryo mapper;

    @Override
    public <T> T read(final byte[] bytes, final Class<T> clazz) throws Exception {
        final Input input = new Input(bytes);
        return mapper.readObject(input, clazz);
    }

    @Override
    public byte[] write(final Object o, final Class<?> clazz) throws Exception  {
        try (ByteArrayOutputStream stream = new ByteArrayOutputStream()) {
            final Output output = new Output(stream);
            mapper.writeObject(output, o);
            output.flush();
            return stream.toByteArray();
        }
    }

    @Override
    public Compatibility getCompatibility() {
        return compatibility;
    }
}
