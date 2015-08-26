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
        public Traversal<Vertex, Double> get_g_withSackX0X_V_outE_sackXsumX_byXweightX_inV_sack_sum() {
            TraversalScriptHelper.compute("g.withSack(0.0f).V.outE.sack(sum).by('weight').inV.sack.sum()", g)
        }

        @Override
        public Traversal<Vertex, Float> get_g_withSackX0X_V_repeatXoutE_sackXsumX_byXweightX_inVX_timesX2X_sack() {
            TraversalScriptHelper.compute("g.withSack(0.0f).V.repeat(__.outE.sack(sum).by('weight').inV).times(2).sack()", g)
        }

        @Override
        public Traversal<Vertex, Double> get_g_withSackX0X_V_outE_sackXsum_weightX_inV_sack_sum() {
            TraversalScriptHelper.compute("g.withSack(0.0f).V().outE.sack(sum, 'weight').inV.sack.sum()", g);
        }

        @Override
        public Traversal<Vertex, Float> get_g_withSackX0X_V_repeatXoutE_sackXsum_weightX_inVX_timesX2X_sack() {
            TraversalScriptHelper.compute("g.withSack(0.0f).V.repeat(__.outE.sack(sum, 'weight').inV).times(2).sack", g)
        }

        @Override
        public Traversal<Vertex, Map> get_g_withSackXmap__map_cloneX_V_out_out_sackXmap_a_nameX_sack() {
            TraversalScriptHelper.compute("g.withSack{[:]}{ it.clone() }.V.out().out().sack { m, v -> m['a'] = v.name; m }.sack()", g);
        }
    }
}
