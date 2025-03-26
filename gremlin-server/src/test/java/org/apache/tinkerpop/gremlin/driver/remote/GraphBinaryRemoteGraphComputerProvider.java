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

package org.apache.tinkerpop.gremlin.driver.remote;

import org.apache.tinkerpop.gremlin.GraphProvider;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.tinkergraph.process.computer.TinkerGraphComputer;
import org.apache.tinkerpop.gremlin.util.ser.Serializers;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.AddEdgeTest",
        method = "*",
        reason = "The addEdge() step is not supported by GraphComputer")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.branch.ChooseTest",
        method = "g_injectX1X_chooseXisX1X__constantX10Xfold__foldX",
        reason = "The inject() step is not supported by GraphComputer")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.branch.ChooseTest",
        method = "g_injectX2X_chooseXisX1X__constantX10Xfold__foldX",
        reason = "The inject() step is not supported by GraphComputer")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.GraphTest",
        method = "g_V_hasLabelXpersonX_asXpX_VXsoftwareX_addInEXuses_pX",
        reason = "Mid-traversal V()/E() is currently not supported on GraphComputer")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.GraphTest",
        method = "g_V_hasXname_GarciaX_inXsungByX_asXsongX_V_hasXname_Willie_DixonX_inXwrittenByX_whereXeqXsongXX_name",
        reason = "Mid-traversal V()/E() is currently not supported on GraphComputer")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.GraphTest",
        method = "g_V_outXknowsX_V_name",
        reason = "Mid-traversal V()/E() is currently not supported on GraphComputer")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.GraphTest",
        method = "g_VX1X_V_valuesXnameX",
        reason = "Mid-traversal V()/E() is currently not supported on GraphComputer")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.EdgeTest",
        method = "*",
        reason = "Mid-traversal V()/E() is currently not supported on GraphComputer")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.InjectTest",
        method = "*",
        reason = "The inject() step is not supported by GraphComputer")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.MathTest",
        method = "g_withSackX1X_injectX1X_repeatXsackXsumX_byXconstantX1XXX_timesX5X_emit_mathXsin__X_byXsackX",
        reason = "The inject() step is not supported by GraphComputer")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.MinTest",
        method = "g_V_foo_injectX9999999999X_min",
        reason = "The inject() step is not supported by GraphComputer")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.branch.OptionalTest",
        method = "g_VX1X_optionalXaddVXdogXX_label",
        reason = "The addV() step is not supported on GraphComputer")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.filter.SimplePathTest",
        method = "g_V_asXaX_out_asXbX_out_asXcX_simplePath_byXlabelX_fromXbX_toXcX_path_byXnameX",
        reason = "It is not possible to access more than a path element's id on GraphComputer")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.VertexTest",
        method = "g_V_hasLabelXpersonX_V_hasLabelXsoftwareX_name",
        reason = "Mid-traversal V()/E() is currently not supported on GraphComputer")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.SackTest",
        method = "g_withBulkXfalseX_withSackX1_sumX_V_out_barrier_sack",
        reason = "One bulk is currently not supported on GraphComputer")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.SackTest",
        method = "g_withBulkXfalseX_withSackX1_sumX_VX1X_localXoutEXknowsX_barrierXnormSackX_inVX_inXknowsX_barrier_sack",
        reason = "One bulk is currently not supported on GraphComputer")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.PropertiesTest",
        method = "g_injectXg_VX1X_propertiesXnameX_nextX_value",
        reason = "The inject() step is not supported by GraphComputer")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.SumTest",
        method = "g_injectXnull_10_5_nullX_sum",
        reason = "The inject() step is not supported by GraphComputer")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.SumTest",
        method = "g_injectXlistXnull_10_5_nullXX_sumXlocalX",
        reason = "The inject() step is not supported by GraphComputer")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.MeanTest",
        method = "g_injectXlistXnull_10_20_nullXX_meanXlocalX",
        reason = "The inject() step is not supported by GraphComputer")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.SelectTest",
        method = "g_withSideEffectXk_nullX_injectXxX_selectXkX",
        reason = "The inject() step is not supported by GraphComputer")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.MeanTest",
        method = "g_injectXnull_10_20_nullX_mean",
        reason = "The inject() step is not supported by GraphComputer")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.ReadTest",
        method = "*",
        reason = "The io() step is not supported generally by GraphComputer")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.WriteTest",
        method = "*",
        reason = "The io() step is not supported generally by GraphComputer")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.filter.HasTest",
        method = "g_V_hasXk_withinXcXX_valuesXkX",
        reason = "Requires some test data initialization with addV()")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.filter.HasTest",
        method = "g_V_hasXname_regexXTinkerXX",
        reason = "Requires some test data initialization with addV()")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.filter.HasTest",
        method = "g_V_hasXname_regexXTinkerUnicodeXX",
        reason = "Requires some test data initialization with addV()")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.PathTest",
        method = "g_injectX1_null_nullX_path",
        reason = "The inject() step is not supported by GraphComputer")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.PathTest",
        method = "g_injectX1_null_nullX_path_dedup",
        reason = "The inject() step is not supported by GraphComputer")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.MatchTest$GreedyMatchTraversals",
        method = "*",
        reason = "MatchAlgorithmStrategy construction doesn't work for gremlin-groovy")
@GraphProvider.Descriptor(computer = TinkerGraphComputer.class)
public class GraphBinaryRemoteGraphComputerProvider extends AbstractRemoteGraphProvider {

    public GraphBinaryRemoteGraphComputerProvider() {
        super(createClusterBuilder(Serializers.GRAPHBINARY_V4).create(), true, "groovy-test");
    }
}
