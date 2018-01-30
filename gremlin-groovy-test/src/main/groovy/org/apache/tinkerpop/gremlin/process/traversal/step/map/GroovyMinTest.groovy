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
public abstract class GroovyMinTest {

    public static class Traversals extends MinTest {

        @Override
        public Traversal<Vertex, Integer> get_g_V_age_min() {
            new ScriptTraversal<>(g, "gremlin-groovy", "g.V.age.min")
        }

        @Override
        public Traversal<Vertex, Integer> get_g_V_repeatXbothX_timesX5X_age_min() {
            new ScriptTraversal<>(g, "gremlin-groovy", "g.V.repeat(__.both).times(5).age.min")
        }

        @Override
        public Traversal<Vertex, Map<String, Number>> get_g_V_hasLabelXsoftwareX_group_byXnameX_byXbothE_weight_minX() {
            new ScriptTraversal<>(g, "gremlin-groovy", "g.V.hasLabel('software').group().by('name').by(bothE().weight.min())")
        }

        @Override
        public Traversal<Vertex, Number> get_g_V_foo_injectX9999999999X_min() {
            new ScriptTraversal<>(g, "gremlin-groovy", "g.V.values('foo').inject(9999999999L).min")
        }
    }
}
