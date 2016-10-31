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
package org.apache.tinkerpop.gremlin.process.traversal.step.branch

import org.apache.tinkerpop.gremlin.process.traversal.Path
import org.apache.tinkerpop.gremlin.process.traversal.Traversal
import org.apache.tinkerpop.gremlin.process.traversal.util.ScriptTraversal
import org.apache.tinkerpop.gremlin.structure.Vertex

/**
 * @author Pieter Martin
 */
public abstract class GroovyOptionalTest {

    public static class Traversals extends OptionalTest {

        @Override
        public Traversal<Vertex, Vertex> get_g_VX2X_optionalXoutXknowsXX(Object v2Id) {
            new ScriptTraversal<>(g, "gremlin-groovy", "g.V(v2Id).optional(out('knows'))", "v2Id", v2Id)
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_VX2X_optionalXinXknowsXX(Object v2Id) {
            new ScriptTraversal<>(g, "gremlin-groovy", "g.V(v2Id).optional(__.in('knows'))", "v2Id", v2Id)
        }

        @Override
        public Traversal<Vertex, Path> get_g_V_hasLabelXpersonX_optionalXoutXknowsX_optionalXoutXcreatedXXX_path() {
            new ScriptTraversal<>(g, "gremlin-groovy", "g.V().hasLabel('person').optional(out('knows').optional(out('created'))).path()")
        }

        @Override
        public Traversal<Vertex, Path> get_g_V_optionalXout_optionalXoutXX_path() {
            new ScriptTraversal<>(g, "gremlin-groovy", "g.V.optional(out().optional(out())).path")
        }

        @Override
        public Traversal<Vertex, String> get_g_VX1X_optionalXaddVXdogXX_label(Object v1Id) {
            new ScriptTraversal<>(g, "gremlin-groovy", "g.V(v1Id).optional(addV('dog')).label", "v1Id", v1Id)
        }
    }

}
