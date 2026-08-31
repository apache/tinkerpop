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

package org.apache.tinkerpop.gremlin.jsr223;

import org.apache.commons.configuration2.BaseConfiguration;
import org.apache.tinkerpop.gremlin.process.traversal.Bytecode;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.AbstractTraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.TraversalStrategyProxy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.verification.ReadOnlyStrategy;
import org.apache.tinkerpop.gremlin.structure.util.empty.EmptyGraph;
import org.junit.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

public class JavaTranslatorTest {
    private final GraphTraversalSource g = EmptyGraph.instance().traversal();
    private final JavaTranslator<GraphTraversalSource, Traversal.Admin<?, ?>> translator = JavaTranslator.of(g);

    @Test
    public void shouldTranslateHasWithObjectThirdArgValue() {
        final Bytecode bytecode = new Bytecode();
        bytecode.addStep("E");
        bytecode.addStep("has", "knows", "weight", 1.0);
        final Traversal.Admin<?, ?> translation = translator.translate(bytecode);
        assertEquals(g.E().has("knows", "weight", 1.0).asAdmin(), translation);
    }

    @Test
    public void shouldTranslateHasWithPredicateThirdArgValue() {
        final Bytecode bytecode = new Bytecode();
        bytecode.addStep("E");
        bytecode.addStep("has", "knows", "weight", P.eq(1.0));
        final Traversal.Admin<?, ?> translation = translator.translate(bytecode);
        assertEquals(g.E().has("knows", "weight", P.eq(1.0)).asAdmin(), translation);
    }

    @Test
    public void shouldTranslateHasWithNullThirdArgValue() {
        final Bytecode bytecode = new Bytecode();
        bytecode.addStep("E");
        bytecode.addStep("has", "knows", "weight", null);
        final Traversal.Admin<?, ?> translation = translator.translate(bytecode);
        assertEquals(g.E().has("knows", "weight", (String) null).asAdmin(), translation);
    }

    @Test
    public void shouldTranslateHasWithObjectSecondArgValue() {
        final Bytecode bytecode = new Bytecode();
        bytecode.addStep("E");
        bytecode.addStep("has", "weight", 1.0);
        final Traversal.Admin<?, ?> translation = translator.translate(bytecode);
        assertEquals(g.E().has("weight", 1.0).asAdmin(), translation);
    }

    @Test
    public void shouldTranslateHasWithPredicateSecondArgValue() {
        final Bytecode bytecode = new Bytecode();
        bytecode.addStep("E");
        bytecode.addStep("has", "weight", P.eq(1.0));
        final Traversal.Admin<?, ?> translation = translator.translate(bytecode);
        assertEquals(g.E().has("weight", P.eq(1.0)).asAdmin(), translation);
    }

    @Test
    public void shouldTranslateHasWithNullSecondArgValue() {
        final Bytecode bytecode = new Bytecode();
        bytecode.addStep("E");
        bytecode.addStep("has", "weight", null);
        final Traversal.Admin<?, ?> translation = translator.translate(bytecode);
        assertEquals(g.E().has("weight", (String) null).asAdmin(), translation);
    }

    @Test
    public void shouldTranslateRegisteredTraversalStrategyProxy() {
        final Bytecode bytecode = new Bytecode();
        bytecode.addSource(TraversalSource.Symbols.withStrategies,
                new TraversalStrategyProxy<>(ReadOnlyStrategy.instance()));
        bytecode.addStep("V");

        assertEquals(g.withStrategies(ReadOnlyStrategy.instance()).V().asAdmin(), translator.translate(bytecode));
    }

    @Test
    public void shouldRejectUnregisteredTraversalStrategyProxyBeforeInvokingFactory() {
        final Bytecode bytecode = new Bytecode();
        bytecode.addSource(TraversalSource.Symbols.withStrategies,
                new TraversalStrategyProxy<>(UnregisteredStrategy.class, new BaseConfiguration()));
        bytecode.addStep("V");

        final IllegalStateException exception =
                assertThrows(IllegalStateException.class, () -> translator.translate(bytecode));
        assertThat(exception.getMessage(), containsString(
                "TraversalStrategy not recognized - " + UnregisteredStrategy.class.getName()));
        assertThat(UnregisteredStrategy.instanceInvoked, is(false));
    }

    @Test
    @SuppressWarnings({"rawtypes", "unchecked"})
    public void shouldRejectTraversalStrategyProxyForNonStrategyClass() {
        final Bytecode bytecode = new Bytecode();
        bytecode.addSource(TraversalSource.Symbols.withStrategies,
                new TraversalStrategyProxy(String.class, new BaseConfiguration()));
        bytecode.addStep("V");

        final IllegalStateException exception =
                assertThrows(IllegalStateException.class, () -> translator.translate(bytecode));
        assertThat(exception.getMessage(), containsString(
                "Class is not a TraversalStrategy - " + String.class.getName()));
    }

    private static final class UnregisteredStrategy
            extends AbstractTraversalStrategy<TraversalStrategy.DecorationStrategy>
            implements TraversalStrategy.DecorationStrategy {

        private static final UnregisteredStrategy INSTANCE = new UnregisteredStrategy();
        private static boolean instanceInvoked = false;

        public static UnregisteredStrategy instance() {
            instanceInvoked = true;
            return INSTANCE;
        }

        @Override
        public void apply(final Traversal.Admin<?, ?> traversal) {
        }
    }
}
