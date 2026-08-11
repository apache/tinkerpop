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
package org.apache.tinkerpop.gremlin.structure.io.gryo;

import org.apache.tinkerpop.gremlin.process.remote.traversal.DefaultRemoteTraverser;
import org.apache.tinkerpop.gremlin.process.traversal.GremlinLang;
import org.apache.tinkerpop.gremlin.process.traversal.Merge;
import org.apache.tinkerpop.gremlin.process.traversal.TextP;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.OptionsStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalExplanation;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.io.Io;
import org.apache.tinkerpop.gremlin.structure.io.IoX;
import org.apache.tinkerpop.gremlin.structure.io.IoXIoRegistry;
import org.apache.tinkerpop.gremlin.structure.io.IoY;
import org.apache.tinkerpop.gremlin.structure.io.IoYIoRegistry;
import org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONTokens;
import org.apache.tinkerpop.gremlin.structure.util.detached.DetachedVertex;
import org.apache.tinkerpop.gremlin.structure.util.empty.EmptyGraph;
import org.apache.tinkerpop.shaded.kryo.ClassResolver;
import org.apache.tinkerpop.shaded.kryo.Kryo;
import org.apache.tinkerpop.shaded.kryo.Registration;
import org.apache.tinkerpop.shaded.kryo.Serializer;
import org.apache.tinkerpop.shaded.kryo.io.Input;
import org.apache.tinkerpop.shaded.kryo.io.Output;
import org.apache.tinkerpop.shaded.kryo.serializers.JavaSerializer;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.io.Serializable;
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
import java.util.function.Function;
import java.util.function.Supplier;

import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.__;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsInstanceOf.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
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

    /**
     * The Gryo type id of {@code OptionsStrategy}, registered with the shaded {@code JavaSerializer} in both
     * {@link GryoVersion#V1_0} and {@link GryoVersion#V3_0}.
     */
    private static final int OPTIONS_STRATEGY_GRYO_ID = 187;

    /**
     * Kryo shifts written class ids to leave room for its {@code NULL} and {@code NAME} markers, as
     * {@link AbstractGryoClassResolver#readClass(Input)} shows.
     */
    private static final int CLASS_ID_OFFSET = 2;

    /**
     * Kryo's reference marker for an object being seen for the first time.
     */
    private static final int KRYO_NOT_NULL = 1;

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

    /**
     * Dropping the {@code JavaSerializer} registrations only holds while registration stays required. Without it a
     * stream may name a class as a string instead of using a registered id, and Kryo resolves that class implicitly
     * with its default serializer, which is a {@code JavaSerializer} for any type declaring one. The combination is
     * therefore refused rather than producing a mapper that looks hardened and is not.
     */
    @Test
    public void shouldRejectJavaSerializationDisabledWithoutRegistrationRequired() {
        try {
            builder.get().javaSerializationAllowed(false).registrationRequired(false).create();
            fail("javaSerializationAllowed(false) with registrationRequired(false) must be refused");
        } catch (IllegalStateException expected) {
            assertEquals(GryoMapper.Builder.UNSAFE_COMBINATION_MESSAGE, expected.getMessage());
        }
    }

    /**
     * The guard above must not reject either flag on its own, since both remain legitimate: full fidelity without
     * registration is how the OLAP pools run, and hardening with registration required is the IO default.
     */
    @Test
    public void shouldAllowEitherRegistrationRequiredOrJavaSerializationSettingAlone() {
        builder.get().registrationRequired(false).create();
        builder.get().javaSerializationAllowed(false).create();
        builder.get().javaSerializationAllowed(false).registrationRequired(true).create();
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

    /**
     * Gryo registers a handful of types with the shaded Kryo {@code JavaSerializer}, which deserializes by way of
     * {@code java.io.ObjectInputStream.readObject()}. A stream that presents one of those type ids therefore
     * reconstructs whatever {@code Serializable} object graph follows and runs its {@code readObject()} methods,
     * which is an unsafe-deserialization sink on caller-supplied bytes. The canary used here carries no
     * payload at all: the mere execution of its {@code readObject()} is the proof.
     */
    @Test
    public void shouldNotInvokeJavaDeserializationOnGryoRead() throws Exception {
        final Kryo kryo = builder.get().javaSerializationAllowed(false).create().createMapper();

        // Kryo frames an object as varint(class id + 2) followed by a NOT_NULL reference marker. Confirm that
        // framing against a known registration (HashMap is id 11 in both V1_0 and V3_0) rather than trusting a
        // hard coded Kryo internal, so the crafted stream below cannot silently stop reaching the serializer.
        final int hashMapId = kryo.getRegistration(HashMap.class).getId();
        assertEquals(11, hashMapId);
        final Output probe = new Output(64, -1);
        kryo.writeClassAndObject(probe, new HashMap<String, Object>());
        probe.flush();
        final Input probeInput = new Input(probe.toBytes());
        assertEquals(hashMapId + CLASS_ID_OFFSET, probeInput.readVarInt(true));
        assertEquals(KRYO_NOT_NULL, probeInput.readVarInt(true));

        final byte[] malicious = maliciousGryoBytes();

        DeserializationCanary.FIRED = false;
        try {
            kryo.readClassAndObject(new Input(new ByteArrayInputStream(malicious)));
        } catch (Exception ignored) {
            // refusing the stream outright is the expected outcome. what matters is that nothing was deserialized on
            // the way to that decision.
        }

        assertFalse("Reading Gryo " + name + " bytes must not invoke ObjectInputStream.readObject() on the stream, " +
                        "since a crafted stream presenting a JavaSerializer-backed type id such as " +
                        OPTIONS_STRATEGY_GRYO_ID + " (OptionsStrategy) could otherwise carry an arbitrary Java " +
                        "object graph",
                DeserializationCanary.FIRED);
    }

    /**
     * Positive control for {@link #shouldNotInvokeJavaDeserializationOnGryoRead()}. Without hardening, the same bytes
     * do reach {@code ObjectInputStream.readObject()}, which is what gives the assertion there any meaning: were the
     * crafted framing ever to stop selecting the {@code JavaSerializer}, this test would fail and say so. It also
     * documents that a directly built mapper stays full fidelity and must only be pointed at trusted bytes.
     */
    @Test
    public void shouldInvokeJavaDeserializationOnDefaultMapperRead() throws Exception {
        final Kryo kryo = builder.get().create().createMapper();
        final byte[] malicious = maliciousGryoBytes();

        DeserializationCanary.FIRED = false;
        try {
            kryo.readClassAndObject(new Input(new ByteArrayInputStream(malicious)));
        } catch (Exception ignored) {
            // the payload deserializes to the canary rather than to an OptionsStrategy, so a failure is possible
            // here, but it would come after readObject() has already run
        }

        assertTrue("the crafted stream must reach ObjectInputStream.readObject() on a full fidelity mapper, " +
                        "otherwise the hardened assertions prove nothing",
                DeserializationCanary.FIRED);
    }

    /**
     * The same crafted stream fed through the reader that {@code io()} and graph persistence use, whose default
     * mapper is hardened.
     */
    @Test
    public void shouldNotInvokeJavaDeserializationOnGryoReaderRead() throws Exception {
        final GryoReader reader = GryoReader.build().create();

        DeserializationCanary.FIRED = false;
        try (final InputStream stream = new ByteArrayInputStream(maliciousGryoBytes())) {
            reader.readObject(stream, Object.class);
        } catch (Exception ignored) {
            // as above, refusing the stream is the expected outcome
        }

        assertFalse("GryoReader must not invoke ObjectInputStream.readObject() on the bytes it reads",
                DeserializationCanary.FIRED);
    }

    /**
     * Positive control for {@link #shouldNotInvokeJavaDeserializationOnGryoReaderRead()}. Supplying a full fidelity
     * mapper explicitly must let the same crafted stream reach {@code ObjectInputStream.readObject()} through
     * {@link GryoReader}. Without this, that test could pass for the wrong reason. {@code readObject} is the one
     * entry point that does not check the header, so were it ever to start doing so, the crafted bytes would be
     * rejected on the first byte, the canary would never run, and the assertion there would hold while proving
     * nothing.
     */
    @Test
    public void shouldInvokeJavaDeserializationOnDefaultMapperGryoReaderRead() throws Exception {
        final GryoReader reader = GryoReader.build().mapper(builder.get().create()).create();

        DeserializationCanary.FIRED = false;
        try (final InputStream stream = new ByteArrayInputStream(maliciousGryoBytes())) {
            reader.readObject(stream, Object.class);
        } catch (Exception ignored) {
            // the payload deserializes to the canary rather than to an OptionsStrategy, so a failure is possible
            // here, but it would come after readObject() has already run
        }

        assertTrue("the crafted stream must reach ObjectInputStream.readObject() through GryoReader, otherwise " +
                        "shouldNotInvokeJavaDeserializationOnGryoReaderRead proves nothing",
                DeserializationCanary.FIRED);
    }

    /**
     * Hardening the mapper must not cost anything on the graph structure that a Gryo document actually carries.
     */
    @Test
    public void shouldRoundTripGraphStructureWithJavaSerializationDisabled() throws Exception {
        final Kryo kryo = builder.get().javaSerializationAllowed(false).create().createMapper();

        final Map<String, Object> props = new HashMap<>();
        final List<Map<String, Object>> propertyNames = new ArrayList<>(1);
        final Map<String, Object> propertyName = new HashMap<>();
        propertyName.put(GraphSONTokens.ID, "x");
        propertyName.put(GraphSONTokens.KEY, "x");
        propertyName.put(GraphSONTokens.VALUE, "no-way-this-will-ever-work");
        propertyNames.add(propertyName);
        props.put("x", propertyNames);

        final Output out = new Output(1024, -1);
        kryo.writeClassAndObject(out, new DetachedVertex(100, Vertex.DEFAULT_LABEL, props));
        out.flush();

        final DetachedVertex readX = (DetachedVertex) kryo.readClassAndObject(
                new Input(new ByteArrayInputStream(out.toBytes())));
        assertEquals("no-way-this-will-ever-work", readX.value("x"));
    }

    /**
     * A Gryo stream that presents {@code OptionsStrategy}'s type id and then a raw Java-serialized payload. Crafting
     * it needs no cooperation from the Gryo writer, which is why the sink was reachable from untrusted bytes.
     */
    private byte[] maliciousGryoBytes() throws Exception {
        final ByteArrayOutputStream javaPayload = new ByteArrayOutputStream();
        try (final ObjectOutputStream oos = new ObjectOutputStream(javaPayload)) {
            oos.writeObject(new DeserializationCanary());
        }

        final Output malicious = new Output(javaPayload.size() + 64, -1);
        malicious.writeVarInt(OPTIONS_STRATEGY_GRYO_ID + CLASS_ID_OFFSET, true);
        malicious.writeVarInt(KRYO_NOT_NULL, true);
        malicious.writeBytes(javaPayload.toByteArray());
        malicious.flush();
        return malicious.toBytes();
    }

    /**
     * Companion to {@link #shouldNotInvokeJavaDeserializationOnGryoRead()} that covers the whole sink surface rather
     * than one carrier type. The assertion is made against the {@code Kryo} instance that actually decodes bytes, so
     * that it cannot pass merely by asking the same question of the same metadata the filter itself used.
     */
    @Test
    public void shouldNotRegisterTypesWithJavaSerializerWhenDisabled() {
        final Kryo hardened = builder.get().javaSerializationAllowed(false).create().createMapper();

        for (final TypeRegistration<?> tr : javaSerializedRegistrations()) {
            final Class<?> clazz = tr.getTargetClass();
            try {
                hardened.getRegistration(clazz);
                fail(clazz.getSimpleName() + " must not be registered on a hardened mapper");
            } catch (IllegalArgumentException expected) {
                // Kryo refuses an unregistered class while registration is required
            }
        }
    }

    /**
     * A custom type contributed with Kryo's {@code JavaSerializer} is filtered on the same terms as the defaults,
     * since an {@link org.apache.tinkerpop.gremlin.structure.io.IoRegistry} is an untrusted-input path too.
     */
    @Test
    public void shouldNotRegisterCustomTypesWithJavaSerializerWhenDisabled() {
        final Kryo hardened = builder.get().addCustom(IoX.class, new JavaSerializer()).
                javaSerializationAllowed(false).create().createMapper();

        try {
            hardened.getRegistration(IoX.class);
            fail("a custom JavaSerializer registration must not survive on a hardened mapper");
        } catch (IllegalArgumentException expected) {
            // as above
        }
    }

    /**
     * A custom type whose serializer is supplied as a {@code Function} resolving to a {@code JavaSerializer} can only
     * be recognized once a {@code Kryo} exists, so it is dropped at mapper-creation time rather than at build time.
     */
    @Test
    public void shouldNotRegisterCustomFunctionTypesWithJavaSerializerWhenDisabled() {
        final Kryo hardened = builder.get().addCustom(IoX.class, (Function<Kryo, Serializer>) k -> new JavaSerializer()).
                javaSerializationAllowed(false).create().createMapper();

        try {
            hardened.getRegistration(IoX.class);
            fail("a custom Function supplied JavaSerializer must not survive on a hardened mapper");
        } catch (IllegalArgumentException expected) {
            // Kryo refuses an unregistered class while registration is required
        }
    }

    /**
     * A type carrying {@code @DefaultSerializer(JavaSerializer.class)} and registered without an explicit serializer
     * resolves to a {@code JavaSerializer} through Kryo's default, which is likewise dropped at mapper-creation time.
     */
    @Test
    public void shouldNotRegisterDefaultSerializerJavaSerializerTypesWhenDisabled() {
        final Kryo hardened = builder.get().addCustom(JavaSerializedByDefault.class).
                javaSerializationAllowed(false).create().createMapper();

        try {
            hardened.getRegistration(JavaSerializedByDefault.class);
            fail("a @DefaultSerializer(JavaSerializer) registration must not survive on a hardened mapper");
        } catch (IllegalArgumentException expected) {
            // as above
        }
    }

    /**
     * The full fidelity mapper is unchanged and remains available for trusted, in-process round-trips. This test
     * documents which registrations that leaves on native Java serialization.
     */
    @Test
    public void shouldRegisterTypesWithJavaSerializerByDefault() {
        final List<String> found = new ArrayList<>();
        for (final TypeRegistration<?> tr : javaSerializedRegistrations())
            found.add(String.format("%s(%d)", tr.getTargetClass().getSimpleName(), tr.getId()));

        final List<String> expected = name.equals("1_0") ?
                Arrays.asList("TraversalExplanation(106)", "GroupBiOperator(117)", "OrderBiOperator(118)",
                        "PartitionStrategy(140)", "SubgraphStrategy(141)", "SeedStrategy(192)",
                        "VertexProgramStrategy(142)", "ProductiveByStrategy(195)", "OptionsStrategy(187)",
                        "GValue(199)") :
                Arrays.asList("PartitionStrategy(140)", "SubgraphStrategy(141)", "SeedStrategy(192)",
                        "VertexProgramStrategy(142)", "ProductiveByStrategy(195)", "OptionsStrategy(187)",
                        "TraversalExplanation(106)", "GValue(199)");
        assertEquals(expected, found);
    }

    /**
     * The inverse of {@link #shouldNotRegisterTypesWithJavaSerializerWhenDisabled()}. Setting the value explicitly
     * keeps the affected types usable, which is what a trusted, in-process round-trip relies on.
     */
    @Test
    public void shouldRoundTripStrategyWhenJavaSerializationAllowed() throws Exception {
        final Kryo kryo = builder.get().javaSerializationAllowed(true).create().createMapper();

        final Output out = new Output(1024, -1);
        kryo.writeClassAndObject(out, OptionsStrategy.build().with("some-key", "some-value").create());
        out.flush();

        final OptionsStrategy read = (OptionsStrategy) kryo.readClassAndObject(
                new Input(new ByteArrayInputStream(out.toBytes())));
        assertEquals("some-value", read.getOptions().get("some-key"));
    }

    /**
     * {@link GryoIo} hardens its mapper, and the {@code onMapper} consumer is the documented way to restore full
     * fidelity where the bytes are trusted. This pins the form shown in the upgrade documentation, including the cast.
     */
    @Test
    public void shouldRestoreJavaSerializationThroughGryoIoOnMapper() {
        final Io.Builder<GryoIo> io = GryoIo.build(gryoVersion());
        io.graph(EmptyGraph.instance());
        io.onMapper(m -> ((GryoMapper.Builder) m).javaSerializationAllowed(true));

        final Kryo restored = io.create().mapper().create().createMapper();
        assertEquals(OPTIONS_STRATEGY_GRYO_ID, restored.getRegistration(OptionsStrategy.class).getId());
    }

    /**
     * {@link GryoIo} applies its hardening before the {@code onMapper} consumer, so turning registration off there
     * leaves the combination the mapper refuses. This is the likelier way to write it than the direct builder form,
     * and it changes behaviour for anyone who did, so it is pinned separately.
     */
    @Test
    public void shouldRejectRegistrationNotRequiredThroughGryoIoOnMapper() {
        final Io.Builder<GryoIo> io = GryoIo.build(gryoVersion());
        io.graph(EmptyGraph.instance());
        io.onMapper(m -> ((GryoMapper.Builder) m).registrationRequired(false));

        try {
            io.create().mapper().create();
            fail("registrationRequired(false) through onMapper must be refused, since GryoIo hardens the mapper");
        } catch (IllegalStateException expected) {
            assertEquals(GryoMapper.Builder.UNSAFE_COMBINATION_MESSAGE, expected.getMessage());
        }
    }

    /**
     * Without such a consumer, {@link GryoIo} is hardened like the reader and writer defaults.
     */
    @Test
    public void shouldNotRegisterTypesWithJavaSerializerOnGryoIoDefault() {
        final Io.Builder<GryoIo> io = GryoIo.build(gryoVersion());
        io.graph(EmptyGraph.instance());

        final Kryo hardened = io.create().mapper().create().createMapper();
        try {
            hardened.getRegistration(OptionsStrategy.class);
            fail("GryoIo must not register OptionsStrategy by default");
        } catch (IllegalArgumentException expected) {
            // Kryo refuses an unregistered class while registration is required
        }
    }

    /**
     * The writer default is hardened too, so a document carrying one of the dropped types cannot be produced by the
     * paths that could not read it back.
     */
    @Test
    public void shouldNotWriteTypesWithJavaSerializerOnGryoWriterDefault() throws Exception {
        final GryoWriter writer = GryoWriter.build().create();

        try (final OutputStream stream = new ByteArrayOutputStream()) {
            writer.writeObject(stream, OptionsStrategy.build().with("some-key", "some-value").create());
            fail("the GryoWriter default must not write a JavaSerializer backed type");
        } catch (IllegalArgumentException expected) {
            // as above, Kryo refuses the unregistered class
        }
    }

    private GryoVersion gryoVersion() {
        return name.equals("1_0") ? GryoVersion.V1_0 : GryoVersion.V3_0;
    }

    /**
     * The registrations that the full fidelity mapper of the version under test backs with Kryo's
     * {@code JavaSerializer}, each of which is a carrier for the sink.
     */
    private List<TypeRegistration<?>> javaSerializedRegistrations() {
        final List<TypeRegistration<?>> found = new ArrayList<>();
        for (final TypeRegistration<?> tr : builder.get().create().getTypeRegistrations()) {
            if (tr.getShadedSerializer() instanceof JavaSerializer) found.add(tr);
        }

        // if detection ever breaks, the tests that loop over this would pass without checking anything
        assertThat(found.size(), greaterThan(0));
        return found;
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

    /**
     * A type that resolves to Kryo's {@code JavaSerializer} through the class-level {@code @DefaultSerializer}
     * annotation rather than an explicit registration, exercising the default-serializer branch of the filter.
     */
    @org.apache.tinkerpop.shaded.kryo.DefaultSerializer(JavaSerializer.class)
    public static class JavaSerializedByDefault implements Serializable {
        private static final long serialVersionUID = 1L;
    }

    /**
     * A deliberately inert {@code Serializable} used to detect whether native Java deserialization ran during a Gryo
     * read. It touches nothing outside this class: no process execution, no filesystem, no reflection.
     */
    private static class DeserializationCanary implements Serializable {
        private static final long serialVersionUID = 1L;

        static volatile boolean FIRED = false;

        private void readObject(final ObjectInputStream in) throws IOException, ClassNotFoundException {
            in.defaultReadObject();
            FIRED = true;
        }
    }
}
