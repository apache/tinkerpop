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
 * @author Joshua Shinavier (http://fortytwo.net)
 */
public abstract class GroovyChooseTest {

    public static class Traversals extends ChooseTest {
        @Override
        public Traversal<Vertex, Object> get_g_V_chooseXout_countX_optionX2L__nameX_optionX3L__valueMapX() {
            new ScriptTraversal<>(g, "gremlin-groovy", "g.V.choose(__.out.count).option(2L, __.values('name')).option(3L, __.valueMap())")
        }

        @Override
        public Traversal<Vertex, String> get_g_V_chooseXlabel_eqXpersonX__outXknowsX__inXcreatedXX_name() {
            new ScriptTraversal<>(g, "gremlin-groovy", "g.V.choose({it.label() == 'person'}, out('knows'), __.in('created')).name")
        }

        @Override
        public Traversal<Vertex, String> get_g_V_chooseXhasLabelXpersonX_and_outXcreatedX__outXknowsX__identityX_name() {
            new ScriptTraversal<>(g, "gremlin-groovy", "g.V.choose(hasLabel('person').and().out('created'), out('knows'), identity()).name")
        }
    }
}
