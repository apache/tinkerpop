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
import org.apache.tinkerpop.gremlin.jsr223.JavaTranslator;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.TranslationStrategy;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONVersion;
import org.apache.tinkerpop.gremlin.tinkergraph.TinkerGraphProvider;
import org.apache.tinkerpop.gremlin.tinkergraph.process.computer.TinkerGraphComputer;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.ProfileTest",
        method = "*",
        reason = "Tests for profile() are not supported for remotes")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.LambdaStepTest",
        method = "*",
        reason = "Tests that include lambdas are not supported by the test suite for remotes")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.filter.DedupTest",
        method = "g_V_both_name_order_byXa_bX_dedup_value",
        reason = "Tests that include lambdas are not supported by the test suite for remotes")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.OrderTest",
        method = "g_V_name_order_byXa1_b1X_byXb2_a2X",
        reason = "Tests that include lambdas are not supported by the test suite for remotes")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.OrderTest",
        method = "g_V_order_byXname_a1_b1X_byXname_b2_a2X_name",
        reason = "Tests that include lambdas are not supported by the test suite for remotes")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.SackTest",
        method = "g_withSackXmap__map_cloneX_V_out_out_sackXmap_a_nameX_sack",
        reason = "Tests that include lambdas are not supported by the test suite for remotes")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.SubgraphTest",
        method = "g_V_withSideEffectXsgX_outEXknowsX_subgraphXsgX_name_capXsgX",
        reason = "Tests that include lambdas are not supported by the test suite for remotes")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.SubgraphTest",
        method = "g_V_withSideEffectXsgX_repeatXbothEXcreatedX_subgraphXsgX_outVX_timesX5X_name_dedup",
        reason = "Tests that include lambdas are not supported by the test suite for remotes")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.SubgraphTest",
        method = "g_withSideEffectXsgX_V_hasXname_danielXout_capXsgX",
        reason = "Tests that include lambdas are not supported by the test suite for remotes")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.ProgramTest",
        method = "*",
        reason = "Reason requires investigation")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.TraversalInterruptionTest",
        method = "*",
        reason = "Reason requires investigation")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.TraversalInterruptionComputerTest",
        method = "*",
        reason = "Reason requires investigation")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.EventStrategyProcessTest",
        method = "*",
        reason = "Strategy not properly supported by Bytecode based traversals")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.ElementIdStrategyProcessTest",
        method = "*",
        reason = "Strategy not properly supported by Bytecode based traversals")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.CoreTraversalTest",
        method = "shouldNeverPropagateANoBulkTraverser",
        reason = "Reason requires investigation")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.ReadTest",
        method = "*",
        reason = "read and write tests don't translate locally well because of calling iterate() inside read()/write() add a none()")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.WriteTest",
        method = "*",
        reason = "read and write tests don't translate locally well because of calling iterate() inside read()/write() add a none()")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.OrderabilityTest",
        method = "g_inject_order_with_unknown_type",
        reason = "Remoting serializers only support known Gremlin types")
public abstract class AbstractTinkerGraphGraphSONTranslatorProvider extends TinkerGraphProvider {

    private final GraphSONVersion version;

    AbstractTinkerGraphGraphSONTranslatorProvider(final GraphSONVersion version) {
        this.version = version;
    }

    @Override
    public GraphTraversalSource traversal(final Graph graph) {
        final GraphTraversalSource g = graph.traversal();
        return g.withStrategies(new TranslationStrategy(g, new GraphSONTranslator<>(JavaTranslator.of(g), version), true));
    }

    @Graph.OptOut(
            test = "org.apache.tinkerpop.gremlin.process.traversal.step.OrderabilityTest",
            method = "g_inject_order",
            reason = "GraphSONv2 does not properly round trip Maps and Sets")
    @Graph.OptOut(
            test = "org.apache.tinkerpop.gremlin.process.traversal.step.OrderabilityTest",
            method = "g_inject_order_with_unknown_type",
            reason = "Remoting serializers only support known Gremlin types")
    @Graph.OptOut(
            test = "org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.SideEffectTest",
            method = "g_withSideEffectXx_setX_V_both_both_sideEffectXstoreXxX_byXnameXX_capXxX_unfold",
            reason = "GraphSONv2 does not properly round trip Maps and Sets")
    @Graph.OptOut(
            test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.MergeEdgeTest",
            method = "*",
            reason = "Remoting serializers only support known Gremlin types")
    @Graph.OptOut(
            test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.MergeVertexTest",
            method = "*",
            reason = "Remoting serializers only support known Gremlin types")
    public static class TinkerGraphGraphSONv2TranslatorProvider extends AbstractTinkerGraphGraphSONTranslatorProvider {
        public TinkerGraphGraphSONv2TranslatorProvider() {
            super(GraphSONVersion.V2_0);
        }
    }

    @Graph.OptOut(
            test = "org.apache.tinkerpop.gremlin.process.traversal.step.OrderabilityTest",
            method = "g_inject_order_with_unknown_type",
            reason = "Remoting serializers only support known Gremlin types")
    public static class TinkerGraphGraphSONv3TranslatorProvider extends AbstractTinkerGraphGraphSONTranslatorProvider {
        public TinkerGraphGraphSONv3TranslatorProvider() {
            super(GraphSONVersion.V3_0);
        }
    }

    @GraphProvider.Descriptor(computer = TinkerGraphComputer.class)
    @Graph.OptOut(
            test = "org.apache.tinkerpop.gremlin.process.computer.GraphComputerTest",
            method = "shouldSucceedWithProperTraverserRequirements",
            reason = "Reason requires investigation")
    public static class TinkerGraphGraphSONv2TranslatorComputerProvider extends TinkerGraphGraphSONv2TranslatorProvider {

        @Override
        public GraphTraversalSource traversal(final Graph graph) {
            return super.traversal(graph).withComputer();
        }
    }

    @GraphProvider.Descriptor(computer = TinkerGraphComputer.class)
    @Graph.OptOut(
            test = "org.apache.tinkerpop.gremlin.process.computer.GraphComputerTest",
            method = "shouldSucceedWithProperTraverserRequirements",
            reason = "Reason requires investigation")
    public static class TinkerGraphGraphSONv3TranslatorComputerProvider extends TinkerGraphGraphSONv3TranslatorProvider {

        @Override
        public GraphTraversalSource traversal(final Graph graph) {
            return super.traversal(graph).withComputer();
        }
    }
}