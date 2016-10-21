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

import org.apache.tinkerpop.gremlin.process.traversal.Traversal
import org.apache.tinkerpop.gremlin.process.traversal.util.ScriptTraversal
import org.apache.tinkerpop.gremlin.structure.Vertex

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public abstract class GroovyBranchTest {

    public static class Traversals extends BranchTest {

        @Override
        public Traversal<Vertex, Object> get_g_V_branchXlabel_eq_person__a_bX_optionXa__ageX_optionXb__langX_optionXb__nameX() {
            new ScriptTraversal<>(g, "gremlin-groovy", "g.V.branch(__.label.is('person').count).option(1L, __.age).option(0L, __.lang).option(0L,__.name)");
        }

        @Override
        public Traversal<Vertex, Object> get_g_V_branchXlabel_isXpersonX_countX_optionX1__ageX_optionX0__langX_optionX0__nameX() {
            new ScriptTraversal<>(g, "gremlin-groovy", """
            g.V.branch{it.label == 'person' ? 'a' : 'b'}
                    .option('a', __.age)
                    .option('b', __.lang)
                    .option('b', __.name)
            """)
        }

        @Override
        public Traversal<Vertex, Object> get_g_V_branchXlabel_isXpersonX_countX_optionX1__ageX_optionX0__langX_optionX0__nameX_optionXany__labelX() {
            new ScriptTraversal<>(g, "gremlin-groovy", """
             g.V.branch(label().is("person").count)
                    .option(1L,__.age)
                    .option(0L,__.lang)
                    .option(0L,__.name)
                    .option(any,label())
            """)
        }
    }
}
