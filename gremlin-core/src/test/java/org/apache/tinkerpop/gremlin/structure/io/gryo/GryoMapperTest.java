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

import org.apache.tinkerpop.gremlin.structure.io.IoX;
import org.apache.tinkerpop.gremlin.structure.io.IoXIoRegistry;
import org.apache.tinkerpop.gremlin.structure.io.IoY;
import org.apache.tinkerpop.gremlin.structure.io.IoYIoRegistry;
import org.apache.tinkerpop.shaded.kryo.Kryo;
import org.apache.tinkerpop.shaded.kryo.io.Input;
import org.apache.tinkerpop.shaded.kryo.io.Output;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.OutputStream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotSame;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class GryoMapperTest {
    @Test
    public void shouldMakeNewInstance() {
        final GryoMapper.Builder b = GryoMapper.build();
        assertNotSame(b, GryoMapper.build());
    }

    @Test
    public void shouldRegisterMultipleIoRegistryToSerialize() throws Exception {
        final GryoMapper mapper = GryoMapper.build()
                .addRegistry(IoXIoRegistry.InstanceBased.getInstance())
                .addRegistry(IoYIoRegistry.InstanceBased.getInstance()).create();
        final Kryo kryo = mapper.createMapper();
        try (final OutputStream stream = new ByteArrayOutputStream()) {
            final Output out = new Output(stream);
            final IoX x = new IoX("x");
            final IoY y = new IoY(100, 200);
            kryo.writeClassAndObject(out, x);
            kryo.writeClassAndObject(out, y);

            try (final InputStream inputStream = new ByteArrayInputStream(out.toBytes())) {
                final Input input = new Input(inputStream);
                final IoX readX = (IoX) kryo.readClassAndObject(input);
                final IoY readY = (IoY) kryo.readClassAndObject(input);
                assertEquals(x, readX);
                assertEquals(y, readY);
            }
        }
    }

    @Test
    public void shouldExpectReadFailureAsIoRegistryOrderIsNotRespected() throws Exception {
        final GryoMapper mapperWrite = GryoMapper.build()
                .addRegistry(IoXIoRegistry.InstanceBased.getInstance())
                .addRegistry(IoYIoRegistry.InstanceBased.getInstance()).create();

        final GryoMapper mapperRead = GryoMapper.build()
                .addRegistry(IoYIoRegistry.InstanceBased.getInstance())
                .addRegistry(IoXIoRegistry.InstanceBased.getInstance()).create();

        final Kryo kryoWriter = mapperWrite.createMapper();
        final Kryo kryoReader = mapperRead.createMapper();
        try (final OutputStream stream = new ByteArrayOutputStream()) {
            final Output out = new Output(stream);
            final IoX x = new IoX("x");
            final IoY y = new IoY(100, 200);
            kryoWriter.writeClassAndObject(out, x);
            kryoWriter.writeClassAndObject(out, y);

            try (final InputStream inputStream = new ByteArrayInputStream(out.toBytes())) {
                final Input input = new Input(inputStream);

                // kryo will read a IoY instance as we've reversed the registries.  it is neither an X or a Y
                // so assert that both are incorrect
                final IoY readY = (IoY) kryoReader.readClassAndObject(input);
                assertNotEquals(y, readY);
                assertNotEquals(x, readY);
            }
        }
    }
}
