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

import org.apache.tinkerpop.gremlin.process.traversal.Path
import org.apache.tinkerpop.gremlin.process.traversal.Traversal
import org.apache.tinkerpop.gremlin.process.traversal.util.ScriptTraversal
import org.apache.tinkerpop.gremlin.structure.Vertex

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public abstract class GroovyAggregateTest {

    public static class Traversals extends AggregateTest {

        @Override
        public Traversal<Vertex, List<String>> get_g_V_name_aggregateXxX_capXxX() {
            new ScriptTraversal<>(g, "gremlin-groovy", "g.V.name.aggregate('x').cap('x')")
        }

        @Override
        public Traversal<Vertex, List<String>> get_g_V_aggregateXxX_byXnameX_capXxX() {
            new ScriptTraversal<>(g, "gremlin-groovy", "g.V.aggregate('x').by('name').cap('x')")
        }

        @Override
        public Traversal<Vertex, Path> get_g_V_out_aggregateXaX_path() {
            new ScriptTraversal<>(g, "gremlin-groovy", "g.V.out.aggregate('a').path")
        }

        @Override
        public Traversal<Vertex, Collection<Integer>> get_g_V_hasLabelXpersonX_aggregateXxX_byXageX_capXxX_asXyX_selectXyX() {
            new ScriptTraversal<>(g, "gremlin-groovy", "g.V.hasLabel('person').aggregate('x').by('age').cap('x').as('y').select('y')")
        }
    }
}
