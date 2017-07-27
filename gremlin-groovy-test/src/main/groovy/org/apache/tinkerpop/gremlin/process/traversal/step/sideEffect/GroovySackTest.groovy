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
package org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect

import org.apache.tinkerpop.gremlin.process.traversal.Traversal
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalScriptHelper
import org.apache.tinkerpop.gremlin.structure.Vertex

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public abstract class GroovySackTest {

    public static class Traversals extends SackTest {

        @Override
        public Traversal<Vertex, String> get_g_withSackXhellowX_V_outE_sackXassignX_byXlabelX_inV_sack() {
            TraversalScriptHelper.compute("g.withSack('hello').V.outE.sack(assign).by(label).inV.sack", g)
        }

        @Override
        public Traversal<Vertex, Double> get_g_withSackX0X_V_outE_sackXsumX_byXweightX_inV_sack_sum() {
            TraversalScriptHelper.compute("g.withSack(0.0f).V.outE.sack(sum).by('weight').inV.sack.sum()", g)
        }

        @Override
        public Traversal<Vertex, Double> get_g_withSackX0X_V_repeatXoutE_sackXsumX_byXweightX_inVX_timesX2X_sack() {
            TraversalScriptHelper.compute("g.withSack(0.0f).V.repeat(__.outE.sack(sum).by('weight').inV).times(2).sack()", g)
        }

        @Override
        public Traversal<Vertex, Double> get_g_withSackX0X_V_outE_sackXsum_weightX_inV_sack_sum() {
            TraversalScriptHelper.compute("g.withSack(0.0f).V().outE.sack(sum, 'weight').inV.sack.sum()", g)
        }

        @Override
        public Traversal<Vertex, Double> get_g_withSackX0X_V_repeatXoutE_sackXsum_weightX_inVX_timesX2X_sack() {
            TraversalScriptHelper.compute("g.withSack(0.0f).V.repeat(__.outE.sack(sum, 'weight').inV).times(2).sack", g)
        }

        @Override
        public Traversal<Vertex, Map> get_g_withSackXmap__map_cloneX_V_out_out_sackXmap_a_nameX_sack() {
            TraversalScriptHelper.compute("g.withSack{[:]}{ it.clone() }.V.out().out().sack { m, v -> m['a'] = v.name; m }.sack()", g)
        }

        @Override
        public Traversal<Vertex, Double> get_g_withSackX1_sumX_VX1X_localXoutXknowsX_barrierXnormSackXX_inXknowsX_barrier_sack(
                final Object v1Id) {
            TraversalScriptHelper.compute("g.withSack(1.0d,sum).V(v1Id).local(out('knows').barrier(normSack)).in('knows').barrier.sack", g, "v1Id", v1Id)
        }

        @Override
        public Traversal<Vertex, Integer> get_g_withBulkXfalseX_withSackX1_sumX_V_out_barrier_sack() {
            TraversalScriptHelper.compute("g.withBulk(false).withSack(1, sum).V.out.barrier.sack", g)
        }

        @Override
        Traversal<Vertex, BigDecimal> get_g_withSackXBigInteger_TEN_powX1000X_assignX_V_localXoutXknowsX_barrierXnormSackXX_inXknowsX_barrier_sack() {
            TraversalScriptHelper.compute("g.withSack(BigInteger.TEN.pow(1000), assign).V.local(out('knows').barrier(normSack)).in('knows').barrier.sack", g)
        }

        @Override
        Traversal<Vertex, Double> get_g_withSackX2X_V_sackXdivX_byXconstantX3_0XX_sack() {
            TraversalScriptHelper.compute("g.withSack(2).V.sack(div).by(constant(3.0)).sack", g)
        }
    }
}
