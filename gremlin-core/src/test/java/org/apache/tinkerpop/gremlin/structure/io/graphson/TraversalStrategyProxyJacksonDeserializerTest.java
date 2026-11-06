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
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategies;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.AbstractTraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.TraversalStrategyProxy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.SubgraphStrategy;
import org.apache.tinkerpop.shaded.jackson.databind.ObjectMapper;
import org.junit.After;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.util.Arrays;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.StringContains.containsString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;

@RunWith(Parameterized.class)
public class TraversalStrategyProxyJacksonDeserializerTest {

    private static boolean loadRecordingStrategyInitialized;

    @Parameterized.Parameters(name = "{0}")
    public static Iterable<Object[]> data() {
        return Arrays.asList(new Object[][]{
                {"v2", GraphSONMapper.build().version(GraphSONVersion.V2_0).
                        typeInfo(TypeInfo.PARTIAL_TYPES).create().createMapper()},
                {"v3", GraphSONMapper.build().version(GraphSONVersion.V3_0).
                        typeInfo(TypeInfo.PARTIAL_TYPES).create().createMapper()}
        });
    }

    @Parameterized.Parameter(0)
    public String version;

    @Parameterized.Parameter(1)
    public ObjectMapper mapper;

    @After
    public void unregisterStrategy() {
        TraversalStrategies.GlobalCache.unregisterStrategy(LoadRecordingStrategy.class);
    }

    @Test
    public void shouldRejectStrategyThatIsNotRegistered() throws Exception {
        final String fqcn = LoadRecordingStrategy.class.getName();
        try {
            readStrategy(fqcn);
            fail("A strategy that is not registered must not deserialize");
        } catch (IOException ex) {
            assertThat(ex.getMessage(), containsString("TraversalStrategy not recognized - " + fqcn));
        }
    }

    @Test
    public void shouldRejectStrategyThatIsNotRegisteredWithoutInitializingIt() throws Exception {
        final String fqcn = LoadRecordingStrategy.class.getName();
        try {
            readStrategy(fqcn);
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
                readStrategy(SubgraphStrategy.class.getName()).getStrategyClass());
    }

    @Test
    public void shouldAdmitStrategyRegisteredByAProvider() throws Exception {
        TraversalStrategies.GlobalCache.registerStrategy(LoadRecordingStrategy.class);

        assertEquals(LoadRecordingStrategy.class,
                readStrategy(LoadRecordingStrategy.class.getName()).getStrategyClass());
    }

    private TraversalStrategyProxy readStrategy(final String fqcn) throws IOException {
        return mapper.readValue(
                String.format("{\"@type\":\"g:TraversalStrategy\",\"@value\":{\"fqcn\":\"%s\",\"conf\":{}}}",
                        fqcn),
                TraversalStrategyProxy.class);
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
}
