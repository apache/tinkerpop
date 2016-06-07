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

package org.apache.tinkerpop.gremlin.process.traversal.step.map

import org.apache.tinkerpop.gremlin.process.traversal.Traversal
import org.apache.tinkerpop.gremlin.process.traversal.util.ScriptTraversal
import org.apache.tinkerpop.gremlin.structure.Vertex

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public abstract class GroovyProgramTest {

    public static class Traversals extends ProgramTest {

        @Override
        public Traversal<Vertex, Vertex> get_g_V_programXpageRankX() {
            new ScriptTraversal<>(g, "gremlin-groovy", "g.V.program(PageRankVertexProgram.build().create(graph))")
        }

        @Override
        public Traversal<Vertex, Map<String, List<Object>>> get_g_V_hasLabelXpersonX_programXpageRank_rankX_order_byXrank_incrX_valueMapXname_rankX() {
            new ScriptTraversal<>(g, "gremlin-groovy", "g.V.hasLabel('person').program(PageRankVertexProgram.build().property('rank').create(graph)).order.by('rank',incr).valueMap('name','rank')");
        }

        @Override
        public Traversal<Vertex, Map<String, Object>> get_g_V_outXcreatedX_aggregateXxX_byXlangX_groupCount_programXTestProgramX_asXaX_selectXa_xX() {
            new ScriptTraversal<>(g, "gremlin-groovy", "g.V.out('created').aggregate('x').by('lang').groupCount.program(new ${ProgramTest.TestProgram.class.getCanonicalName()}()).as('a').select('a', 'x')");
        }
    }
}


