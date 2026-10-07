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

import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.AbstractTraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.SubgraphStrategy;
import org.apache.tinkerpop.gremlin.structure.io.ClassRegistry;
import org.apache.tinkerpop.gremlin.structure.io.gryo.kryoshim.shaded.ShadedInputAdapter;
import org.apache.tinkerpop.shaded.kryo.Kryo;
import org.apache.tinkerpop.shaded.kryo.io.Input;
import org.apache.tinkerpop.shaded.kryo.io.Output;
import org.junit.After;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Covers both serializers that read a class name, {@code ClassSerializer} for a single value and
 * {@code ClassArraySerializer} for the {@code withoutStrategies()} argument. Each test reads through both Gryo
 * versions, which share them. The file is not parameterized by version because the canary can report only one load, so
 * a second run would see a flag the first had already set.
 */
public class ClassSerializerTest {

    private static final GryoVersion[] VERSIONS = new GryoVersion[]{GryoVersion.V1_0, GryoVersion.V3_0};

    @After
    public void unregisterProviderType() {
        ClassRegistry.unregister(ProviderType.class);
    }

    /**
     * A provider makes a class of its own nameable by calling {@link ClassRegistry#register(Class)}, so that call is
     * pinned here through the serializer rather than only through the registry's fall-back to {@code GlobalCache}.
     * {@code ProviderType} is registered rather than refused, so do not write a refusal test against it.
     */
    @Test
    public void shouldReadClassRegisteredThroughClassRegistry() throws Exception {
        ClassRegistry.register(ProviderType.class);

        for (final GryoVersion version : VERSIONS) {
            assertEquals(ProviderType.class, roundTrip(version, ProviderType.class, Class.class));
        }
    }

    /**
     * {@code SubgraphStrategy} is registered with {@code TraversalStrategies.GlobalCache}, which the registry falls
     * back to, so only {@code ProviderType} has to be registered here. Two elements are written so that the read
     * covers the loop rather than one name.
     */
    @Test
    public void shouldReadClassArrayRegisteredThroughClassRegistry() throws Exception {
        ClassRegistry.register(ProviderType.class);

        final Class[] classes = new Class[]{SubgraphStrategy.class, ProviderType.class};
        for (final GryoVersion version : VERSIONS) {
            assertArrayEquals(classes, roundTrip(version, classes, Class[].class));
        }
    }

    @Test
    public void shouldRejectClassThatIsNotRegistered() throws Exception {
        for (final GryoVersion version : VERSIONS) {
            assertEquals("Class not recognized - " + File.class.getName(),
                    refusalOf(version, File.class, Class.class).getMessage());
        }
    }

    /**
     * The unregistered name is the second element, so the refusal covers every element rather than only the first.
     * Refusing part way through leaves the stream partly consumed, which costs nothing because the whole read fails.
     */
    @Test
    public void shouldRejectClassArrayThatIsNotRegistered() throws Exception {
        final Class[] classes = new Class[]{SubgraphStrategy.class, File.class};
        for (final GryoVersion version : VERSIONS) {
            assertEquals("Class not recognized - " + File.class.getName(),
                    refusalOf(version, classes, Class[].class).getMessage());
        }
    }

    /**
     * The counterpart to the refusal below, without which a blanket refusal of every list holding a {@code Class} would
     * pass that test. Both names resolve, one through {@code GlobalCache} and one through {@link ClassRegistry}, so both
     * resolution paths are covered inside a container, and both elements are pinned by identity and in order so a
     * partly read or empty list cannot pass.
     */
    @Test
    public void shouldReadListElementsThatAreRegistered() throws Exception {
        ClassRegistry.register(ProviderType.class);

        final ArrayList<Class> classes = new ArrayList<>(Arrays.asList(SubgraphStrategy.class, ProviderType.class));
        for (final GryoVersion version : VERSIONS) {
            final ArrayList<?> read = roundTrip(version, classes, ArrayList.class);

            assertEquals(2, read.size());
            assertSame(SubgraphStrategy.class, read.get(0));
            assertSame(ProviderType.class, read.get(1));
        }
    }

    /**
     * A {@code Class} held inside a container reaches the same serializer as one read on its own, because Kryo reads
     * each element of a collection that carries no element type with the serializer registered for the class the
     * element itself names. The unregistered name is the second element, so the refusal covers every element rather
     * than only the first.
     */
    @Test
    public void shouldRejectListElementThatIsNotRegistered() throws Exception {
        final ArrayList<Class> classes = new ArrayList<>(Arrays.asList(SubgraphStrategy.class, File.class));
        for (final GryoVersion version : VERSIONS) {
            assertEquals("Class not recognized - " + File.class.getName(),
                    refusalOf(version, classes, ArrayList.class).getMessage());
        }
    }

    /**
     * The refusal test above would pass against an implementation that resolved the name through a class loader before
     * refusing, so this is the only test that distinguishes a refusal from never having loaded the class. It detects a
     * return to the initialising {@code Class.forName(name)}. The three-argument form loads without initialising and
     * would not be detected.
     */
    @Test
    public void shouldNotLoadClassNamedByUnregisteredStrategy() throws Exception {
        assertFalse("the canary was already initialised before the read, so this fixture no longer proves anything",
                CanaryFlag.initialised);

        final String fqcn = ClassSerializerTest.class.getName() + "$CanaryStrategy";
        try {
            readName(fqcn);
            fail("A class that is not a registered strategy must not deserialize - " + fqcn);
        } catch (RuntimeException ex) {
            assertEquals("Class not recognized - " + fqcn, ex.getMessage());
        }

        assertFalse("reading the name of an unregistered strategy initialised the class it named",
                CanaryFlag.initialised);

        Class.forName(fqcn);
        assertTrue("the canary cannot report a load, so the assertions above cannot fail", CanaryFlag.initialised);
    }

    private <T> T roundTrip(final GryoVersion version, final Object value, final Class<T> clazz) throws Exception {
        final Kryo kryo = GryoMapper.build().version(version).create().createMapper();
        try (final ByteArrayOutputStream stream = new ByteArrayOutputStream()) {
            final Output output = new Output(stream);
            kryo.writeObject(output, value);
            output.flush();

            try (final InputStream inputStream = new ByteArrayInputStream(stream.toByteArray())) {
                return kryo.readObject(new Input(inputStream), clazz);
            }
        }
    }

    /**
     * Round trips {@code value} and hands back the refusal the read produced, failing the calling test if it
     * deserialized instead.
     */
    private RuntimeException refusalOf(final GryoVersion version, final Object value,
                                       final Class<?> clazz) throws Exception {
        try {
            roundTrip(version, value, clazz);
        } catch (RuntimeException ex) {
            return ex;
        }

        fail("A class that is not a registered strategy must not deserialize - " + value);
        return null;
    }

    /**
     * Hands a name to {@code ClassSerializer} written the way its own {@code write} writes one. The writer takes a
     * loaded {@code Class}, so a name that must stay unloaded cannot be produced by round tripping a class literal.
     */
    private Class readName(final String fqcn) {
        final Output output = new Output(64, -1);
        output.writeString(fqcn);
        output.flush();

        return new UtilSerializers.ClassSerializer().read(null,
                new ShadedInputAdapter(new Input(output.toBytes())), Class.class);
    }

    /**
     * Records that the static initialiser of {@code CanaryStrategy} ran. It is held here, on a class of its own,
     * rather than on {@code CanaryStrategy} itself, because reading a static field initialises the class that declares
     * it: a flag on {@code CanaryStrategy} could not be read without loading the very class it reports on.
     */
    static final class CanaryFlag {
        static boolean initialised;
    }

    /**
     * A {@link TraversalStrategy} on the test classpath, never registered, so refusing it tests registry membership
     * rather than the class not being a strategy. Never name it as a class literal and never register it.
     */
    static final class CanaryStrategy
            extends AbstractTraversalStrategy<TraversalStrategy.DecorationStrategy>
            implements TraversalStrategy.DecorationStrategy {

        static {
            CanaryFlag.initialised = true;
        }

        @Override
        public void apply(final Traversal.Admin<?, ?> traversal) {
            // do nothing
        }
    }

    /**
     * Stands in for a class a provider ships, registered by each test that reads it and unregistered again in
     * {@code unregisterProviderType}. It is deliberately not a {@link TraversalStrategy}, which the registry refuses,
     * and it is registered rather than refused, so it is not a refusal fixture: {@code CanaryStrategy} is the class
     * that is never registered.
     */
    private static final class ProviderType {
    }
}
