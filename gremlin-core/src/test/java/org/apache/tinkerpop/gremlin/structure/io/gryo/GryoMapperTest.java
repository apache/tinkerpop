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

import org.apache.tinkerpop.gremlin.process.remote.traversal.DefaultRemoteTraverser;
import org.apache.tinkerpop.gremlin.process.traversal.GremlinLang;
import org.apache.tinkerpop.gremlin.process.traversal.Merge;
import org.apache.tinkerpop.gremlin.process.traversal.TextP;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalExplanation;
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
import org.apache.tinkerpop.shaded.kryo.Serializer;
import org.apache.tinkerpop.shaded.kryo.io.Input;
import org.apache.tinkerpop.shaded.kryo.io.Output;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.MonthDay;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.time.Period;
import java.time.Year;
import java.time.YearMonth;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.__;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsInstanceOf.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.fail;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
@RunWith(Parameterized.class)
public class GryoMapperTest {

    @Parameterized.Parameters
    public static Iterable<Object[]> data() {
        return Arrays.asList(new Object[][]{
                {"1_0", (Supplier<GryoMapper.Builder>) () -> GryoMapper.build().version(GryoVersion.V1_0)},
                {"3_0", (Supplier<GryoMapper.Builder>) () -> GryoMapper.build().version(GryoVersion.V3_0)}});
    }

    @Parameterized.Parameter
    public String name;

    @Parameterized.Parameter(value = 1)
    public Supplier<GryoMapper.Builder> builder;

    @Test
    public void shouldMakeNewInstance() {
        final GryoMapper.Builder b = GryoMapper.build();
        assertNotSame(b, GryoMapper.build());
    }

    @Test
    public void shouldSerializeDeserialize() throws Exception {
        final GryoMapper mapper = builder.get().create();
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
        final GryoMapper mapper = builder.get().classResolver(classResolver).create();
        final Kryo kryo = mapper.createMapper();
        try (final OutputStream stream = new ByteArrayOutputStream()) {
            final Output out = new Output(stream);
            final IoX x = new IoX("no-way-this-will-ever-work");

            kryo.writeClassAndObject(out, x);

            final GryoMapper mapperWithoutKnowledgeOfIox = builder.get().create();
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
        final GryoMapper mapper = builder.get().classResolver(classResolver).create();
        final Kryo kryo = mapper.createMapper();
        try (final OutputStream stream = new ByteArrayOutputStream()) {
            final Output out = new Output(stream);
            final IoY y = new IoY(100, 200);

            kryo.writeClassAndObject(out, y);

            final GryoMapper mapperWithoutKnowledgeOfIoy = builder.get().create();
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
        final GryoMapper mapper = builder.get().registrationRequired(false).create();
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
        final GryoMapper mapper = builder.get().addRegistry(IoXIoRegistry.InstanceBased.instance())
                .addRegistry(IoYIoRegistry.InstanceBased.instance()).create();
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
        final GryoMapper mapperWrite = builder.get().addRegistry(IoXIoRegistry.InstanceBased.instance())
                .addRegistry(IoYIoRegistry.InstanceBased.instance()).create();

        final GryoMapper mapperRead = GryoMapper.build()
                .addRegistry(IoYIoRegistry.InstanceBased.instance())
                .addRegistry(IoXIoRegistry.InstanceBased.instance()).create();

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

    @Test
    public void shouldOverrideExistingSerializer() throws Exception {
        final GryoMapper mapper = builder.get().addCustom(Duration.class, new OverrideDurationSerializer()).create();

        try (final OutputStream stream = new ByteArrayOutputStream()) {
            final Output out = new Output(stream);
            mapper.createMapper().writeObject(out, Duration.ZERO);
            fail("The OverrideDurationSerializer throws exceptions so this should not have worked");
        } catch (Exception ex) {
            assertThat(ex, instanceOf(UnsupportedOperationException.class));
            assertEquals("I don't do anything", ex.getMessage());
        }
    }

    @Test
    public void shouldHandleDefaultRemoteTraverser() throws Exception  {
        final DefaultRemoteTraverser<Integer> br = new DefaultRemoteTraverser<>(123, 1000);
        final DefaultRemoteTraverser inOut = serializeDeserialize(br, DefaultRemoteTraverser.class);
        assertEquals(br.bulk(), inOut.bulk());
        assertEquals(br.get(), inOut.get());
    }

    @Test
    public void shouldHandleDuration() throws Exception  {
        final Duration o = Duration.ZERO;
        assertEquals(o, serializeDeserialize(o, Duration.class));
    }

    @Test
    public void shouldHandleInstant() throws Exception  {
        final Instant o = Instant.ofEpochMilli(System.currentTimeMillis());
        assertEquals(o, serializeDeserialize(o, Instant.class));
    }

    @Test
    public void shouldHandleLocalDate() throws Exception  {
        final LocalDate o = LocalDate.now();
        assertEquals(o, serializeDeserialize(o, LocalDate.class));
    }

    @Test
    public void shouldHandleLocalDateTime() throws Exception  {
        final LocalDateTime o = LocalDateTime.now();
        assertEquals(o, serializeDeserialize(o, LocalDateTime.class));
    }

    @Test
    public void shouldHandleLocalTime() throws Exception  {
        final LocalTime o = LocalTime.now();
        assertEquals(o, serializeDeserialize(o, LocalTime.class));
    }

    @Test
    public void shouldHandleMonthDay() throws Exception  {
        final MonthDay o = MonthDay.now();
        assertEquals(o, serializeDeserialize(o, MonthDay.class));
    }

    @Test
    public void shouldHandleOffsetDateTime() throws Exception  {
        final OffsetDateTime o = OffsetDateTime.now();
        assertEquals(o, serializeDeserialize(o, OffsetDateTime.class));
    }

    @Test
    public void shouldHandleOffsetTime() throws Exception  {
        final OffsetTime o = OffsetTime.now();
        assertEquals(o, serializeDeserialize(o, OffsetTime.class));
    }

    @Test
    public void shouldHandlePeriod() throws Exception  {
        final Period o = Period.ofDays(3);
        assertEquals(o, serializeDeserialize(o, Period.class));
    }

    @Test
    public void shouldHandleYear() throws Exception  {
        final Year o = Year.now();
        assertEquals(o, serializeDeserialize(o, Year.class));
    }

    @Test
    public void shouldHandleYearMonth() throws Exception  {
        final YearMonth o = YearMonth.now();
        assertEquals(o, serializeDeserialize(o, YearMonth.class));
    }

    @Test
    public void shouldHandleZonedDateTime() throws Exception  {
        final ZonedDateTime o = ZonedDateTime.now();
        assertEquals(o, serializeDeserialize(o, ZonedDateTime.class));
    }

    @Test
    public void shouldHandleZonedOffset() throws Exception  {
        final ZoneOffset o  = ZonedDateTime.now().getOffset();
        assertEquals(o, serializeDeserialize(o, ZoneOffset.class));
    }

    @Test
    public void shouldHandleTraversalExplanation() throws Exception  {
        final TraversalExplanation te = __().out().outV().outE().explain();
        assertEquals(te.toString(), serializeDeserialize(te, TraversalExplanation.class).toString());
    }

    @Test
    public void shouldHandleClass() throws Exception {
        final Class<?> clazz = java.io.File.class;
        assertEquals(clazz, serializeDeserialize(clazz, Class.class));
    }

    @Test
    public void shouldHandleTimestamp() throws Exception {
        final Timestamp ts = new java.sql.Timestamp(1481750076295L);
        assertEquals(ts, serializeDeserialize(ts, java.sql.Timestamp.class));
    }

    @Test
    public void shouldHandleINetAddress() throws Exception {
        final InetAddress addy = InetAddress.getByName("localhost");
        assertEquals(addy, serializeDeserialize(addy, InetAddress.class));
    }

    @Test
    public void shouldHandleByteBuffer() throws Exception {
        final ByteBuffer bb = ByteBuffer.wrap("some bytes for you".getBytes());
        assertThat(Arrays.equals(bb.array(), serializeDeserialize(bb, ByteBuffer.class).array()), is(true));
    }

    @Test
    public void shouldHandleMerge() throws Exception {
        final Merge merge = Merge.onCreate;
        assertEquals(merge, serializeDeserialize(merge, Merge.class));
    }

    @Test
    public void shouldHandleTextP() throws Exception {
        final TextP startingWith = TextP.startingWith("meh");
        assertEquals(startingWith, serializeDeserialize(startingWith, TextP.class));
        final TextP regex = TextP.regex("meh");
        assertEquals(regex, serializeDeserialize(regex, TextP.class));
    }

    public <T> T serializeDeserialize(final Object o, final Class<T> clazz) throws Exception {
        final Kryo kryo = builder.get().create().createMapper();
        try (final ByteArrayOutputStream stream = new ByteArrayOutputStream()) {
            final Output out = new Output(stream);
            kryo.writeObject(out, o);
            out.flush();

            try (final InputStream inputStream = new ByteArrayInputStream(stream.toByteArray())) {
                final Input input = new Input(inputStream);
                return kryo.readObject(input, clazz);
            }
        }
    }

    /**
     * Creates new {@link CustomClassResolver} when requested.
     */
    private static class CustomClassResolverSupplier implements Supplier<ClassResolver> {
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
    private static class CustomClassResolver extends GryoClassResolverV1 {
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

    private final static class OverrideDurationSerializer extends Serializer<Duration>
    {
        @Override
        public void write(final Kryo kryo, final Output output, final Duration duration)
        {
            throw new UnsupportedOperationException("I don't do anything");
        }

        @Override
        public Duration read(final Kryo kryo, final Input input, final Class<Duration> durationClass)
        {
            throw new UnsupportedOperationException("I don't do anything");
        }
    }

}
