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

import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.io.IoX;
import org.apache.tinkerpop.gremlin.structure.io.IoXIoRegistry;
import org.apache.tinkerpop.gremlin.structure.io.IoY;
import org.apache.tinkerpop.gremlin.structure.io.IoYIoRegistry;
import org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONTokens;
import org.apache.tinkerpop.gremlin.structure.util.detached.DetachedVertex;
import org.apache.tinkerpop.shaded.kryo.ClassResolver;
import org.apache.tinkerpop.shaded.kryo.Kryo;
import org.apache.tinkerpop.shaded.kryo.Registration;
import org.apache.tinkerpop.shaded.kryo.io.Input;
import org.apache.tinkerpop.shaded.kryo.io.Output;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

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
    public void shouldSerializeDeserialize() throws Exception {
        final GryoMapper mapper = GryoMapper.build().create();
        final Kryo kryo = mapper.createMapper();
        try (final OutputStream stream = new ByteArrayOutputStream()) {
            final Output out = new Output(stream);

            final Map<String,Object> props = new HashMap<>();
            final List<Map<String, Object>> propertyNames = new ArrayList<>(1);
            final Map<String,Object> propertyName = new HashMap<>();
            propertyName.put(GraphSONTokens.ID, "x");
            propertyName.put(GraphSONTokens.KEY, "x");
            propertyName.put(GraphSONTokens.VALUE, "no-way-this-will-ever-work");
            propertyNames.add(propertyName);
            props.put("x", propertyNames);
            final DetachedVertex v = new DetachedVertex(100, Vertex.DEFAULT_LABEL, props);

            kryo.writeClassAndObject(out, v);

            try (final InputStream inputStream = new ByteArrayInputStream(out.toBytes())) {
                final Input input = new Input(inputStream);
                final DetachedVertex readX = (DetachedVertex) kryo.readClassAndObject(input);
                assertEquals("no-way-this-will-ever-work", readX.value("x"));
            }
        }
    }

    @Test
    public void shouldSerializeWithCustomClassResolverToDetachedVertex() throws Exception {
        final Supplier<ClassResolver> classResolver = new CustomClassResolverSupplier();
        final GryoMapper mapper = GryoMapper.build().classResolver(classResolver).create();
        final Kryo kryo = mapper.createMapper();
        try (final OutputStream stream = new ByteArrayOutputStream()) {
            final Output out = new Output(stream);
            final IoX x = new IoX("no-way-this-will-ever-work");

            kryo.writeClassAndObject(out, x);

            final GryoMapper mapperWithoutKnowledgeOfIox = GryoMapper.build().create();
            final Kryo kryoWithoutKnowledgeOfIox = mapperWithoutKnowledgeOfIox.createMapper();
            try (final InputStream inputStream = new ByteArrayInputStream(out.toBytes())) {
                final Input input = new Input(inputStream);
                final DetachedVertex readX = (DetachedVertex) kryoWithoutKnowledgeOfIox.readClassAndObject(input);
                assertEquals("no-way-this-will-ever-work", readX.value("x"));
            }
        }
    }

    @Test
    public void shouldSerializeWithCustomClassResolverToHashMap() throws Exception {
        final Supplier<ClassResolver> classResolver = new CustomClassResolverSupplier();
        final GryoMapper mapper = GryoMapper.build().classResolver(classResolver).create();
        final Kryo kryo = mapper.createMapper();
        try (final OutputStream stream = new ByteArrayOutputStream()) {
            final Output out = new Output(stream);
            final IoY y = new IoY(100, 200);

            kryo.writeClassAndObject(out, y);

            final GryoMapper mapperWithoutKnowledgeOfIoy = GryoMapper.build().create();
            final Kryo kryoWithoutKnowledgeOfIox = mapperWithoutKnowledgeOfIoy.createMapper();
            try (final InputStream inputStream = new ByteArrayInputStream(out.toBytes())) {
                final Input input = new Input(inputStream);
                final Map readY = (HashMap) kryoWithoutKnowledgeOfIox.readClassAndObject(input);
                assertEquals("100-200", readY.get("y"));
            }
        }
    }

    @Test
    public void shouldSerializeWithoutRegistration() throws Exception {
        final GryoMapper mapper = GryoMapper.build().registrationRequired(false).create();
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

    /**
     * Creates new {@link CustomClassResolver} when requested.
     */
    public static class CustomClassResolverSupplier implements Supplier<ClassResolver> {
        @Override
        public ClassResolver get() {
            return new CustomClassResolver();
        }
    }

    /**
     * A custom {@code ClassResolver} that alters the {@code Registration} returned to Kryo when an {@link IoX} class
     * is requested, coercing it to a totally different class (a {@link DetachedVertex}).  This coercion demonstrates
     * how a TinkerPop provider might take a custom internal class and serialize it into something core to
     * TinkerPop which then removes the requirement for providers to expose serializers on the client side for user
     * consumption.
     */
    public static class CustomClassResolver extends GryoClassResolver {
        private IoXIoRegistry.IoXToVertexSerializer ioXToVertexSerializer = new IoXIoRegistry.IoXToVertexSerializer();
        private IoYIoRegistry.IoYToHashMapSerializer ioYToHashMapSerializer = new IoYIoRegistry.IoYToHashMapSerializer();

        public Registration getRegistration(final Class clazz) {
            if (IoX.class.isAssignableFrom(clazz)) {
                final Registration registration = super.getRegistration(DetachedVertex.class);
                return new Registration(registration.getType(), ioXToVertexSerializer, registration.getId());
            } else if (IoY.class.isAssignableFrom(clazz)) {
                final Registration registration = super.getRegistration(HashMap.class);
                return new Registration(registration.getType(), ioYToHashMapSerializer, registration.getId());
            } else {
                return super.getRegistration(clazz);
            }
        }
    }
}
