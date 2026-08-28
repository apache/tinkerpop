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
package org.apache.tinkerpop.gremlin.util.ser.binary.types;

import io.netty.buffer.ByteBufAllocator;
import org.apache.tinkerpop.gremlin.process.traversal.Bytecode;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategies;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.AbstractTraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.SubgraphStrategy;
import org.apache.tinkerpop.gremlin.structure.io.Buffer;
import org.apache.tinkerpop.gremlin.structure.io.ClassRegistry;
import org.apache.tinkerpop.gremlin.structure.io.binary.DataType;
import org.apache.tinkerpop.gremlin.structure.io.binary.GraphBinaryReader;
import org.apache.tinkerpop.gremlin.structure.io.binary.GraphBinaryWriter;
import org.apache.tinkerpop.gremlin.structure.io.binary.TypeSerializerRegistry;
import org.apache.tinkerpop.gremlin.structure.io.binary.types.SimpleTypeSerializer;
import org.apache.tinkerpop.gremlin.structure.util.empty.EmptyGraph;
import org.apache.tinkerpop.gremlin.util.ser.NettyBufferFactory;
import org.junit.After;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.StringContains.containsString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * A {@code Class} value carries a class name off the wire, and that name is resolved through {@link ClassRegistry}
 * rather than through a class loader, so only a class that registry holds, or a {@link TraversalStrategy} registered
 * with {@code TraversalStrategies.GlobalCache}, can be named. The write path is deliberately left ungated because the
 * writer is handed a class the server already holds.
 */
public class ClassSerializerTest {

    private static final NettyBufferFactory bufferFactory = new NettyBufferFactory();

    private final ByteBufAllocator allocator = ByteBufAllocator.DEFAULT;

    @After
    public void unregisterFixtures() {
        TraversalStrategies.GlobalCache.unregisterStrategy(NestedStrategy.class);
        ClassRegistry.unregister(ProviderType.class);
    }

    @Test
    public void shouldReadClassRegisteredAsABuiltIn() throws Exception {
        assertEquals(SubgraphStrategy.class, readClass(SubgraphStrategy.class.getName()));
    }

    /**
     * The generic {@code Class} value exists for {@code withoutStrategies()}, so the decode is also pinned against the
     * bytecode a client produces rather than only against a name written on its own.
     */
    @Test
    public void shouldDecodeRegisteredStrategyFromWithoutStrategiesBytecode() throws Exception {
        final Bytecode bytecode = EmptyGraph.instance().traversal().
                withoutStrategies(SubgraphStrategy.class).V().asAdmin().getBytecode();

        final Buffer buffer = bufferFactory.create(allocator.buffer());
        new GraphBinaryWriter().write(bytecode, buffer);

        assertEquals(bytecode, new GraphBinaryReader().read(buffer));
    }

    @Test
    public void shouldRejectClassThatIsNotRegistered() throws Exception {
        try {
            readClass(File.class.getName());
            fail("A class that is not a registered strategy must not deserialize");
        } catch (IOException ex) {
            assertThat(ex.getMessage(), containsString("Class not recognized - java.io.File"));
        }
    }

    /**
     * A refusal must not reveal whether the named class is on the classpath. Present-but-unregistered and absent
     * entirely get the same checked {@link IOException}, same message apart from the name, no cause - that
     * indistinguishability is what closing the oracle means. It holds by construction: the registry lookup never
     * consults a class loader. Pinned because a change that resolved the name before refusing would still pass every
     * other refusal test here.
     */
    @Test
    public void shouldRefusePresentAndAbsentClassesIndistinguishably() throws Exception {
        final String present = File.class.getName();
        final String absent = "org.apache.tinkerpop.gremlin.util.ser.binary.types.NotOnTheClasspath";

        // the test only means something while one name resolves and the other does not, so both are pinned first
        Class.forName(present);
        try {
            Class.forName(absent);
            fail("the absent name must not be on the classpath for this test to mean anything");
        } catch (ClassNotFoundException expected) {
            // the fixture holds
        }

        final IOException presentRefusal = refusalOf(present);
        final IOException absentRefusal = refusalOf(absent);

        assertEquals(presentRefusal.getClass(), absentRefusal.getClass());
        assertEquals("Class not recognized - " + present, presentRefusal.getMessage());
        assertEquals("Class not recognized - " + absent, absentRefusal.getMessage());

        // the messages are the same once the name each one echoes is taken out of it
        assertEquals(presentRefusal.getMessage().replace(present, "?"),
                absentRefusal.getMessage().replace(absent, "?"));

        // nor does a cause answer what the message does not, such as a ClassNotFoundException on one side only
        assertNull(presentRefusal.getCause());
        assertNull(absentRefusal.getCause());
    }

    @Test
    public void shouldRejectEmptyName() throws Exception {
        try {
            readClass("");
            fail("An empty class name must not deserialize");
        } catch (IOException ex) {
            assertEquals("Class not recognized - ", ex.getMessage());
        }
    }

    /**
     * A {@code String} value is not nullable at this position, so a null name cannot arrive from a well-formed
     * message. It is still worth pinning that a null reaches the same refusal rather than a {@code NullPointerException}
     * that would escape the request handlers and leave a client with no response at all.
     */
    @Test
    public void shouldRejectNullName() throws Exception {
        final GraphBinaryReader reader = new GraphBinaryReader(TypeSerializerRegistry.build().
                add(String.class, new NullStringSerializer()).create());
        try {
            reader.readValue(bufferFactory.create(allocator.buffer()), Class.class, false);
            fail("A null class name must not deserialize");
        } catch (IOException ex) {
            assertEquals("Class not recognized - null", ex.getMessage());
        }
    }

    @Test
    public void shouldRejectArrayDescriptor() throws Exception {
        try {
            readClass("[Ljava.lang.String;");
            fail("An array descriptor must not deserialize");
        } catch (IOException ex) {
            assertEquals("Class not recognized - [Ljava.lang.String;", ex.getMessage());
        }
    }

    @Test
    public void shouldReadRegisteredNestedClass() throws Exception {
        TraversalStrategies.GlobalCache.registerStrategy(NestedStrategy.class);

        assertEquals(NestedStrategy.class, readClass(NestedStrategy.class.getName()));
    }

    /**
     * A provider makes a class of its own nameable by calling {@link ClassRegistry#register(Class)}, so that call is
     * pinned here through the deserializer rather than only through the registry's fall-back to {@code GlobalCache}.
     * {@code ProviderType} is registered rather than refused, so do not write a refusal test against it.
     */
    @Test
    public void shouldReadClassRegisteredThroughClassRegistry() throws Exception {
        ClassRegistry.register(ProviderType.class);

        assertEquals(ProviderType.class, readClass(ProviderType.class.getName()));
    }

    /**
     * Every other refusal test here would pass against an implementation that resolved the name through a class loader
     * before refusing, so this is the only test that distinguishes a refusal from never having loaded the class. It
     * detects a return to the initialising {@code Class.forName(name)}; the three-argument form loads without
     * initialising and would not be detected.
     */
    @Test
    public void shouldNotLoadClassNamedByUnregisteredStrategy() throws Exception {
        assertFalse("the canary was already initialised before the read, so this fixture no longer proves anything",
                CanaryFlag.initialised);

        final String fqcn = ClassSerializerTest.class.getName() + "$CanaryStrategy";
        final IOException refusal = refusalOf(fqcn);
        assertEquals("Class not recognized - " + fqcn, refusal.getMessage());

        assertFalse("reading the name of an unregistered strategy initialised the class it named",
                CanaryFlag.initialised);

        Class.forName(fqcn);
        assertTrue("the canary cannot report a load, so the assertions above cannot fail", CanaryFlag.initialised);
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

        final Buffer buffer = bufferFactory.create(allocator.buffer());
        new GraphBinaryWriter().write(Arrays.asList(SubgraphStrategy.class, ProviderType.class), buffer);

        final List<?> read = (List<?>) new GraphBinaryReader().read(buffer);

        assertEquals(2, read.size());
        assertSame(SubgraphStrategy.class, read.get(0));
        assertSame(ProviderType.class, read.get(1));
    }

    /**
     * A {@code Class} held inside a container is read by the same serializer as one read on its own, because a
     * collection reads each element as a fully qualified value and so dispatches on the element's own type code. The
     * unregistered name is the second element, so the refusal covers every element rather than only the first.
     */
    @Test
    public void shouldRejectListElementThatIsNotRegistered() throws Exception {
        final Buffer buffer = bufferFactory.create(allocator.buffer());
        new GraphBinaryWriter().write(Arrays.asList(SubgraphStrategy.class, File.class), buffer);

        try {
            new GraphBinaryReader().read(buffer);
            fail("A list holding a class that is not a registered strategy must not deserialize");
        } catch (IOException ex) {
            assertThat(ex.getMessage(), containsString("Class not recognized - " + File.class.getName()));
        }
    }

    /**
     * The writer is handed a class the server itself holds rather than a name off the wire, so it is not gated. The
     * consequence, recorded here, is that a server can write a {@code Class} value that it would refuse to read.
     */
    @Test
    public void shouldWriteClassThatIsNotRegistered() throws Exception {
        final Buffer buffer = bufferFactory.create(allocator.buffer());
        new GraphBinaryWriter().writeValue(File.class, buffer, false);

        assertEquals(File.class.getName(), new GraphBinaryReader().readValue(buffer, String.class, false));
    }

    /**
     * The value of a {@code Class} is the class name written as a non-nullable {@code String} value, so writing the
     * name that way produces exactly what a client sends for a {@code Class}.
     */
    private Class readClass(final String fqcn) throws IOException {
        final Buffer buffer = bufferFactory.create(allocator.buffer());
        new GraphBinaryWriter().writeValue(fqcn, buffer, false);

        return new GraphBinaryReader().readValue(buffer, Class.class, false);
    }

    /**
     * Reads a {@code Class} value naming {@code fqcn} and hands back the refusal it produced, failing the calling test
     * if the name deserialized instead.
     */
    private IOException refusalOf(final String fqcn) throws IOException {
        try {
            readClass(fqcn);
        } catch (IOException ex) {
            return ex;
        }

        fail("A class that is not a registered strategy must not deserialize - " + fqcn);
        return null;
    }

    private static final class NestedStrategy
            extends AbstractTraversalStrategy<TraversalStrategy.DecorationStrategy>
            implements TraversalStrategy.DecorationStrategy {

        @Override
        public void apply(final Traversal.Admin<?, ?> traversal) {
            // do nothing
        }
    }

    /**
     * Stands in for a class a provider ships, registered by each test that reads it and unregistered again in
     * {@code unregisterFixtures}. It is deliberately not a {@link TraversalStrategy}, which the
     * registry refuses, and it is registered rather than refused, so it is not a refusal fixture:
     * {@code CanaryStrategy} is the class that is never registered.
     */
    private static final class ProviderType {
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
     * rather than the class not being a strategy. Never name it as a class literal and never register it -
     * {@code shouldNotLoadClassNamedByUnregisteredStrategy} names it as a string and watches its static initialiser.
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
     * Stands in for the {@code String} serializer to show that a null name is refused the same way, which a
     * well-formed message cannot produce at a non-nullable position.
     */
    private static class NullStringSerializer extends SimpleTypeSerializer<String> {

        NullStringSerializer() {
            super(DataType.STRING);
        }

        @Override
        protected String readValue(final Buffer buffer, final GraphBinaryReader context) throws IOException {
            return null;
        }

        @Override
        protected void writeValue(final String value, final Buffer buffer,
                                  final GraphBinaryWriter context) throws IOException {
            throw new IOException("the String serializer must not be asked to write");
        }
    }
}
