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
import org.apache.tinkerpop.gremlin.process.traversal.util.ScriptTraversal
import org.apache.tinkerpop.gremlin.structure.Vertex

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public abstract class GroovySideEffectCapTest {

    public static class Traversals extends SideEffectCapTest {
        @Override
        public Traversal<Vertex, Map<String, Long>> get_g_V_hasXageX_groupCountXaX_byXnameX_out_capXaX() {
            new ScriptTraversal<>(g, "gremlin-groovy", "g.V.has('age').groupCount('a').by('name').out.cap('a')")
        }

        @Override
        public Traversal<Vertex, Map<String, Map<Object, Long>>> get_g_V_chooseXlabel_person__age_groupCountXaX__name_groupCountXbXX_capXa_bX() {
            new ScriptTraversal<>(g, "gremlin-groovy", """
            g.V.choose(__.has(T.label, 'person'),
                    __.age.groupCount('a'),
                    __.values("name").groupCount('b')).cap('a', 'b')
            """)
        }
    }
}
