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
import org.apache.tinkerpop.gremlin.driver.ser.Serializers;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.tinkergraph.process.computer.TinkerGraphComputer;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
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
        reason = "Needs investigation")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.ReadTest",
        method = "*",
        reason = "The io() step is not supported generally by GraphComputer")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.WriteTest",
        method = "*",
        reason = "The io() step is not supported generally by GraphComputer")
@GraphProvider.Descriptor(computer = TinkerGraphComputer.class)
public class GryoRemoteGraphComputerProvider extends AbstractRemoteGraphProvider {

    public GryoRemoteGraphComputerProvider() {
        super(createClusterBuilder(Serializers.GRYO_V3D0).create(), true);
    }
}
