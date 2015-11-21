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
public abstract class GroovyGroupCountTest {

    public static class Traversals extends GroupCountTest {

        @Override
        public Traversal<Vertex, Map<String, Long>> get_g_V_outXcreatedX_groupCount_byXnameX() {
            TraversalScriptHelper.compute("g.V.out('created').groupCount.by('name')", g)
        }

        @Override
        public Traversal<Vertex, Map<String, Long>> get_g_V_outXcreatedX_groupCountXaX_byXnameX_capXaX() {
            TraversalScriptHelper.compute("g.V.out('created').groupCount('a').by('name').cap('a')", g)
        }

        @Override
        public Traversal<Vertex, Map<String, Long>> get_g_V_outXcreatedX_name_groupCount() {
            TraversalScriptHelper.compute("g.V.out('created').name.groupCount", g)
        }

        @Override
        public Traversal<Vertex, Map<Vertex, Long>> get_g_V_outXcreatedX_groupCountXxX_capXxX() {
            TraversalScriptHelper.compute("g.V.out('created').groupCount('x').cap('x')", g)
        }

        @Override
        public Traversal<Vertex, Map<String, Long>> get_g_V_outXcreatedX_name_groupCountXaX_capXaX() {
            TraversalScriptHelper.compute("g.V.out('created').name.groupCount('a').cap('a')", g)
        }

        @Override
        public Traversal<Vertex, Map<Object, Long>> get_g_V_hasXnoX_groupCount() {
            TraversalScriptHelper.compute("g.V.has('no').groupCount", g)
        }

        @Override
        public Traversal<Vertex, Map<Object, Long>> get_g_V_hasXnoX_groupCountXaX_capXaX() {
            TraversalScriptHelper.compute("g.V.has('no').groupCount('a').cap('a')", g)
        }

        @Override
        public Traversal<Vertex, Map<String, Long>> get_g_V_repeatXout_groupCountXaX_byXnameXX_timesX2X_capXaX() {
            TraversalScriptHelper.compute("g.V.repeat(__.out.groupCount('a').by('name')).times(2).cap('a')", g)
        }

        @Override
        public Traversal<Vertex, Map<String, Long>> get_g_V_unionXrepeatXoutX_timesX2X_groupCountXmX_byXlangXX__repeatXinX_timesX2X_groupCountXmX_byXnameXX_capXmX() {
            TraversalScriptHelper.compute("""
            g.V.union(
                    __.repeat(__.out).times(2).groupCount('m').by('lang'),
                    __.repeat(__.in).times(2).groupCount('m').by('name')).cap('m')
            """, g)
        }

        @Override
        public Traversal<Vertex, Map<Long, Long>> get_g_V_groupCount_byXbothE_countX() {
            TraversalScriptHelper.compute("g.V.groupCount.by(bothE().count)", g)
        }
    }
}
