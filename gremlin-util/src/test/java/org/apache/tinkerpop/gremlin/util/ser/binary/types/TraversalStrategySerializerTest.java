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
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategies;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.AbstractTraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.TraversalStrategyProxy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.SubgraphStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.verification.ReadOnlyStrategy;
import org.apache.tinkerpop.gremlin.structure.io.Buffer;
import org.apache.tinkerpop.gremlin.structure.io.ClassRegistry;
import org.apache.tinkerpop.gremlin.structure.io.binary.DataType;
import org.apache.tinkerpop.gremlin.structure.io.binary.GraphBinaryReader;
import org.apache.tinkerpop.gremlin.structure.io.binary.GraphBinaryWriter;
import org.apache.tinkerpop.gremlin.structure.io.binary.TypeSerializerRegistry;
import org.apache.tinkerpop.gremlin.structure.io.binary.types.SimpleTypeSerializer;
import org.apache.tinkerpop.gremlin.util.ser.NettyBufferFactory;
import org.junit.After;
import org.junit.Test;

import java.io.IOException;
import java.util.Collections;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.StringContains.containsString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;

public class TraversalStrategySerializerTest {

    private static final NettyBufferFactory bufferFactory = new NettyBufferFactory();
    private static boolean loadRecordingStrategyInitialized;
    private final ByteBufAllocator allocator = ByteBufAllocator.DEFAULT;

    @After
    public void unregisterStrategy() {
        TraversalStrategies.GlobalCache.unregisterStrategy(LoadRecordingStrategy.class);
    }

    @Test
    public void shouldRejectStrategyThatIsNotRegistered() throws Exception {
        final String fqcn = LoadRecordingStrategy.class.getName();
        try {
            readStrategy(reader(), fqcn);
            fail("A strategy that is not registered must not deserialize");
        } catch (IOException ex) {
            assertThat(ex.getMessage(), containsString("TraversalStrategy not recognized - " + fqcn));
        }
    }

    /**
     * The other half of the one-way containment between the two registries, seen at the wire. A name
     * {@link ClassRegistry} holds decodes as a {@code Class} value, as
     * {@code ClassSerializerTest.shouldReadClassRegisteredThroughClassRegistry} shows, and must still be refused here,
     * because this position reflectively constructs what it resolves and only {@code GlobalCache} admits a class to it.
     */
    @Test
    public void shouldRejectStrategyNamedByAClassRegistryEntry() throws Exception {
        final String fqcn = ProviderType.class.getName();
        ClassRegistry.register(ProviderType.class);

        try {
            readStrategy(reader(), fqcn);
            fail("A class that is only nameable must not deserialize at the strategy selector");
        } catch (IOException ex) {
            assertThat(ex.getMessage(), containsString("TraversalStrategy not recognized - " + fqcn));
        } finally {
            ClassRegistry.unregister(ProviderType.class);
        }
    }

    @Test
    public void shouldRejectStrategyThatIsNotRegisteredWithoutInitializingIt() throws Exception {
        // a class literal does not initialize the class, so naming it this way keeps the assertion below meaningful
        final String fqcn = LoadRecordingStrategy.class.getName();
        try {
            readStrategy(reader(), fqcn);
            fail("A strategy that is not registered must not deserialize");
        } catch (IOException ignored) {
            // asserted on by shouldRejectStrategyThatIsNotRegistered
        }

        assertFalse("The rejected strategy was initialized, so the check ran after the class was loaded",
                loadRecordingStrategyInitialized);
    }

    @Test
    public void shouldAdmitStrategyRegisteredAsABuiltIn() throws Exception {
        assertEquals(SubgraphStrategy.class,
                readStrategy(reader(), SubgraphStrategy.class.getName()).getStrategyClass());
    }

    @Test
    public void shouldAdmitStrategyRegisteredByAProvider() throws Exception {
        TraversalStrategies.GlobalCache.registerStrategy(LoadRecordingStrategy.class);

        assertEquals(LoadRecordingStrategy.class,
                readStrategy(reader(), LoadRecordingStrategy.class.getName()).getStrategyClass());
    }

    @Test
    public void shouldRejectDeniedStrategyAfterRegistrationAttempt() throws Exception {
        TraversalStrategies.GlobalCache.registerStrategy(DeniedStrategy.class);
        TraversalStrategies.GlobalCache.denyStrategy(DeniedStrategy.class);
        TraversalStrategies.GlobalCache.registerStrategy(DeniedStrategy.class);

        final String fqcn = DeniedStrategy.class.getName();
        try {
            readStrategy(reader(), fqcn);
            fail("A denied strategy must not deserialize");
        } catch (IOException ex) {
            assertThat(ex.getMessage(), containsString("TraversalStrategy not recognized - " + fqcn));
        }
    }

    /**
     * Reading the class as a name rather than as a {@code Class} value must not change the format, since the value of a
     * {@code Class} is the class name written as a {@code String} value. Reading back what the writer produced the way
     * a reader before this change did shows that the two agree.
     */
    @Test
    public void shouldWriteTheStrategyClassInTheClassValueFormat() throws Exception {
        final Buffer buffer = bufferFactory.create(allocator.buffer());
        new GraphBinaryWriter().writeValue(ReadOnlyStrategy.instance(), buffer, false);

        assertEquals(ReadOnlyStrategy.class, new GraphBinaryReader().readValue(buffer, Class.class, false));
    }

    /**
     * The strategy class no longer reaches the {@code ClassSerializer}, which matters because that serializer resolves
     * whatever name it is given. A registry whose {@code Class} serializer refuses to read anything still reads a
     * strategy.
     */
    @Test
    public void shouldNotReadTheStrategyClassThroughTheClassSerializer() throws Exception {
        final GraphBinaryReader reader = new GraphBinaryReader(TypeSerializerRegistry.build().
                add(Class.class, new RefusingClassSerializer()).create());

        assertEquals(SubgraphStrategy.class, readStrategy(reader, SubgraphStrategy.class.getName()).getStrategyClass());
    }

    private GraphBinaryReader reader() {
        return new GraphBinaryReader(TypeSerializerRegistry.build().create());
    }

    /**
     * Writes the value of a {@code TraversalStrategy} as a name followed by an empty configuration, which is what a
     * client sends for a strategy that takes no configuration.
     */
    private TraversalStrategyProxy readStrategy(final GraphBinaryReader reader, final String fqcn) throws IOException {
        final GraphBinaryWriter writer = new GraphBinaryWriter();
        final Buffer buffer = bufferFactory.create(allocator.buffer());
        writer.writeValue(fqcn, buffer, false);
        writer.writeValue(Collections.emptyMap(), buffer, false);

        return (TraversalStrategyProxy) reader.readValue(buffer, TraversalStrategy.class, false);
    }

    private static final class LoadRecordingStrategy
            extends AbstractTraversalStrategy<TraversalStrategy.DecorationStrategy>
            implements TraversalStrategy.DecorationStrategy {

        static {
            loadRecordingStrategyInitialized = true;
        }

        @Override
        public void apply(final Traversal.Admin<?, ?> traversal) {
            // do nothing
        }
    }

    private static final class DeniedStrategy
            extends AbstractTraversalStrategy<TraversalStrategy.DecorationStrategy>
            implements TraversalStrategy.DecorationStrategy {

        @Override
        public void apply(final Traversal.Admin<?, ?> traversal) {
            // do nothing
        }
    }

    /**
     * Stands in for a class a provider registers with {@link ClassRegistry}, which refuses a {@link TraversalStrategy},
     * so a name that registry holds is never one the strategy selector may construct.
     */
    private static final class ProviderType {
    }

    /**
     * Stands in for the {@code ClassSerializer} to show that nothing consults it while a strategy is read.
     */
    private static class RefusingClassSerializer extends SimpleTypeSerializer<Class> {

        RefusingClassSerializer() {
            super(DataType.CLASS);
        }

        @Override
        protected Class readValue(final Buffer buffer, final GraphBinaryReader context) throws IOException {
            throw new IOException("the Class serializer must not be consulted");
        }

        @Override
        protected void writeValue(final Class value, final Buffer buffer,
                                  final GraphBinaryWriter context) throws IOException {
            throw new IOException("the Class serializer must not be consulted");
        }
    }
}
