/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */

package org.apache.tinkerpop.gremlin.tinkergraph.structure.io.graphson;

import org.apache.tinkerpop.gremlin.GraphProvider;
import org.apache.tinkerpop.gremlin.LoadGraphWith;
import org.apache.tinkerpop.gremlin.jsr223.JavaTranslator;
import org.apache.tinkerpop.gremlin.process.traversal.CoreTraversalTest;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalInterruptionComputerTest;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalInterruptionTest;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.PageRankTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.ProgramTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.ReadTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.WriteTest;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.ElementIdStrategyProcessTest;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.EventStrategyProcessTest;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.PartitionStrategyProcessTest;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.TranslationStrategy;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONVersion;
import org.apache.tinkerpop.gremlin.tinkergraph.TinkerGraphProvider;
import org.apache.tinkerpop.gremlin.tinkergraph.process.computer.TinkerGraphComputer;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public abstract class AbstractTinkerGraphGraphSONTranslatorProvider extends TinkerGraphProvider {

    private static Set<String> SKIP_TESTS = new HashSet<>(Arrays.asList(
            "testProfileStrategyCallback",
            "testProfileStrategyCallbackSideEffect",
            // TODO: read and write tests don't translate locally well because of calling iterate() inside read()/write() add a none() - fix????
            ReadTest.Traversals.class.getCanonicalName(),
            WriteTest.Traversals.class.getCanonicalName(),
            //
            ProgramTest.Traversals.class.getCanonicalName(),
            TraversalInterruptionTest.class.getCanonicalName(),
            TraversalInterruptionComputerTest.class.getCanonicalName(),
            EventStrategyProcessTest.class.getCanonicalName(),
            ElementIdStrategyProcessTest.class.getCanonicalName()));

    private final GraphSONVersion version;

    AbstractTinkerGraphGraphSONTranslatorProvider(final GraphSONVersion version) {
        this.version = version;
    }

    @Override
    public Map<String, Object> getBaseConfiguration(final String graphName, final Class<?> test, final String testMethodName,
                                                    final LoadGraphWith.GraphData loadGraphWith) {
        final Map<String, Object> config = super.getBaseConfiguration(graphName, test, testMethodName, loadGraphWith);
        config.put("skipTest", SKIP_TESTS.contains(testMethodName) || SKIP_TESTS.contains(test.getCanonicalName()));
        return config;
    }

    @Override
    public GraphTraversalSource traversal(final Graph graph) {
        if ((Boolean) graph.configuration().getProperty("skipTest"))
            return graph.traversal();
        else {
            final GraphTraversalSource g = graph.traversal();
            return g.withStrategies(new TranslationStrategy(g, new GraphSONTranslator<>(JavaTranslator.of(g), version)));
        }
    }

    public static class TinkerGraphGraphSONv2TranslatorProvider extends AbstractTinkerGraphGraphSONTranslatorProvider {
        public TinkerGraphGraphSONv2TranslatorProvider() {
            super(GraphSONVersion.V2_0);
        }
    }

    public static class TinkerGraphGraphSONv3TranslatorProvider extends AbstractTinkerGraphGraphSONTranslatorProvider {
        public TinkerGraphGraphSONv3TranslatorProvider() {
            super(GraphSONVersion.V3_0);
        }
    }

    @GraphProvider.Descriptor(computer = TinkerGraphComputer.class)
    public static class TinkerGraphGraphSONv2TranslatorComputerProvider extends TinkerGraphGraphSONv2TranslatorProvider {

        @Override
        public GraphTraversalSource traversal(final Graph graph) {
            return super.traversal(graph).withComputer();
        }
    }

    @GraphProvider.Descriptor(computer = TinkerGraphComputer.class)
    public static class TinkerGraphGraphSONv3TranslatorComputerProvider extends TinkerGraphGraphSONv3TranslatorProvider {

        @Override
        public GraphTraversalSource traversal(final Graph graph) {
            return super.traversal(graph).withComputer();
        }
    }
}