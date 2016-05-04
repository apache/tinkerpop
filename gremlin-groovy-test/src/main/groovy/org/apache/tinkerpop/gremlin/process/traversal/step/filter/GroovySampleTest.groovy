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
package org.apache.tinkerpop.gremlin.process.traversal.step.filter

import org.apache.tinkerpop.gremlin.process.traversal.Traversal
import org.apache.tinkerpop.gremlin.process.traversal.util.ScriptTraversal
import org.apache.tinkerpop.gremlin.structure.Edge
import org.apache.tinkerpop.gremlin.structure.Vertex

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public abstract class GroovySampleTest {

    public static class Traversals extends SampleTest {

        @Override
        public Traversal<Edge, Edge> get_g_E_sampleX1X() {
            new ScriptTraversal<>(g, "gremlin-groovy", "g.E.sample(1)")
        }

        @Override
        public Traversal<Edge, Edge> get_g_E_sampleX2X_byXweightX() {
            new ScriptTraversal<>(g, "gremlin-groovy", "g.E.sample(2).by('weight')")
        }

        @Override
        public Traversal<Vertex, Edge> get_g_V_localXoutE_sampleX1X_byXweightXX() {
            new ScriptTraversal<>(g, "gremlin-groovy", "g.V.local(__.outE.sample(1).by('weight'))")
        }

        @Override
        Traversal<Vertex, Map<String, Collection<Double>>> get_g_V_group_byXlabelX_byXbothE_weight_sampleX2X_foldX() {
            new ScriptTraversal<>(g, "gremlin-groovy", "g.V().group().by(T.label).by(bothE().weight.sample(2).fold)")
        }

        @Override
        Traversal<Vertex, Map<String, Collection<Double>>> get_g_V_group_byXlabelX_byXbothE_weight_fold_sampleXlocal_5XX() {
            new ScriptTraversal<>(g, "gremlin-groovy", "g.V().group().by(label).by(bothE().weight.fold().sample(local, 5))")
        }
    }
}
