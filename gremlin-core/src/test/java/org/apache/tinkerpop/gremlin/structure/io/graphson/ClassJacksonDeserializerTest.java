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
package org.apache.tinkerpop.gremlin.structure.io.graphson;

import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.AbstractTraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.SubgraphStrategy;
import org.apache.tinkerpop.gremlin.structure.io.ClassRegistry;
import org.apache.tinkerpop.shaded.jackson.databind.ObjectMapper;
import org.apache.tinkerpop.shaded.jackson.databind.exc.MismatchedInputException;
import org.junit.After;
import org.junit.Test;

import java.io.File;
import java.util.Arrays;
import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.StringContains.containsString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Each test reads through both the V2 and V3 mappers, which share one deserializer. The file is not parameterized by
 * version because the canary can report only one load, so a second run would see a flag the first had already set.
 */
public class ClassJacksonDeserializerTest extends AbstractGraphSONTest {

    private final ObjectMapper mapperV2 = GraphSONMapper.build().version(GraphSONVersion.V2_0).
            typeInfo(TypeInfo.PARTIAL_TYPES).create().createMapper();

    private final ObjectMapper mapperV3 = GraphSONMapper.build().version(GraphSONVersion.V3_0).
            typeInfo(TypeInfo.PARTIAL_TYPES).create().createMapper();

    @After
    public void unregisterProviderType() {
        ClassRegistry.unregister(ProviderType.class);
    }

    /**
     * {@code SubgraphStrategy} is registered with {@code TraversalStrategies.GlobalCache}, which the registry falls
     * back to, so nothing has to register it here.
     */
    @Test
    public void shouldReadClassOfRegisteredStrategy() throws Exception {
        assertEquals(SubgraphStrategy.class, serializeDeserialize(mapperV2, SubgraphStrategy.class, Class.class));
        assertEquals(SubgraphStrategy.class, serializeDeserialize(mapperV3, SubgraphStrategy.class, Class.class));
    }

    /**
     * A provider makes a class of its own nameable by calling {@link ClassRegistry#register(Class)}, so that call is
     * pinned here through the deserializer rather than only through the registry's fall-back to {@code GlobalCache}.
     * {@code ProviderType} is registered rather than refused, so do not write a refusal test against it.
     */
    @Test
    public void shouldReadClassRegisteredThroughClassRegistry() throws Exception {
        ClassRegistry.register(ProviderType.class);

        assertEquals(ProviderType.class, serializeDeserialize(mapperV2, ProviderType.class, Class.class));
        assertEquals(ProviderType.class, serializeDeserialize(mapperV3, ProviderType.class, Class.class));
    }

    @Test
    public void shouldRejectClassThatIsNotRegistered() throws Exception {
        final String fqcn = File.class.getName();

        assertThat(refusalOf(mapperV2, fqcn).getMessage(), containsString(fqcn));
        assertThat(refusalOf(mapperV3, fqcn).getMessage(), containsString(fqcn));
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

        final String elements = classValue(SubgraphStrategy.class.getName()) + "," +
                classValue(ProviderType.class.getName());

        // V2 writes a list as a bare JSON array while V3 wraps it in g:List, as in the refusal below
        final List<?> fromV2 = readList(mapperV2, "[" + elements + "]");
        final List<?> fromV3 = readList(mapperV3, "{\"@type\":\"g:List\",\"@value\":[" + elements + "]}");

        for (final List<?> read : Arrays.asList(fromV2, fromV3)) {
            assertEquals(2, read.size());
            assertSame(SubgraphStrategy.class, read.get(0));
            assertSame(ProviderType.class, read.get(1));
        }
    }

    /**
     * A {@code Class} held inside a container reaches the same deserializer as one read on its own, because a list
     * reads each element as an {@code Object} and so resolves the type the element itself carries. The unregistered
     * name is the second element, so the refusal covers every element rather than only the first. V2 writes a list as a
     * bare JSON array while V3 wraps it in {@code g:List}, so the two reads are not handed the same document.
     */
    @Test
    public void shouldRejectListElementThatIsNotRegistered() throws Exception {
        final String refusal = "Class not recognized - " + File.class.getName();
        final String elements = classValue(SubgraphStrategy.class.getName()) + "," +
                classValue(File.class.getName());

        assertThat(refusalOfJson(mapperV2, "[" + elements + "]").getMessage(), containsString(refusal));
        assertThat(refusalOfJson(mapperV3, "{\"@type\":\"g:List\",\"@value\":[" + elements + "]}").getMessage(),
                containsString(refusal));
    }

    @Test
    public void shouldNotLoadClassNamedByUnregisteredStrategy() throws Exception {
        assertFalse("the canary was already initialised before the read, so this fixture no longer proves anything",
                CanaryFlag.initialised);

        final String fqcn = ClassJacksonDeserializerTest.class.getName() + "$CanaryStrategy";
        assertThat(refusalOf(mapperV2, fqcn).getMessage(), containsString(fqcn));
        assertThat(refusalOf(mapperV3, fqcn).getMessage(), containsString(fqcn));

        assertFalse("reading the name of an unregistered strategy initialised the class it named",
                CanaryFlag.initialised);

        Class.forName(fqcn);
        assertTrue("the canary cannot report a load, so the assertions above cannot fail", CanaryFlag.initialised);
    }

    /**
     * Reads a {@code Class} value naming {@code fqcn} into an {@code Object}, as a bytecode argument arrives, and
     * returns the refusal. The deserializer's {@code IOException} surfaces as a {@link MismatchedInputException}.
     */
    private MismatchedInputException refusalOf(final ObjectMapper mapper, final String fqcn) throws Exception {
        return refusalOfJson(mapper, classValue(fqcn));
    }

    /**
     * Reads {@code json} into an {@code Object} and hands back the refusal it produced, failing the calling test if it
     * deserialized instead.
     */
    private MismatchedInputException refusalOfJson(final ObjectMapper mapper, final String json) throws Exception {
        try {
            mapper.readValue(json, Object.class);
        } catch (MismatchedInputException ex) {
            return ex;
        }

        fail("A class that is not a registered strategy must not deserialize - " + json);
        return null;
    }

    /**
     * Reads {@code json} into an {@code Object}, as a bytecode argument arrives, and hands back the list it produced.
     */
    private List<?> readList(final ObjectMapper mapper, final String json) throws Exception {
        return (List<?>) mapper.readValue(json, Object.class);
    }

    /**
     * The {@code Class} value a client sends, as it appears both on its own and as an element of a list.
     */
    private static String classValue(final String fqcn) {
        return "{\"@type\":\"g:Class\",\"@value\":\"" + fqcn + "\"}";
    }

    /**
     * The flag lives here rather than on {@code CanaryStrategy} because reading a static field initialises the class
     * declaring it, so a flag on {@code CanaryStrategy} could not be read without loading what it reports on.
     */
    static final class CanaryFlag {
        static boolean initialised;
    }

    /**
     * A {@link TraversalStrategy} that is never registered, so refusing it tests registry membership rather than the
     * class not being a strategy. Never name it as a class literal and never register it.
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
     * {@code unregisterProviderType}. It is deliberately not a {@link TraversalStrategy}, which
     * the registry refuses, and it is registered rather than refused, so it is not a refusal fixture:
     * {@code CanaryStrategy} is the class that is never registered.
     */
    private static final class ProviderType {
    }
}
