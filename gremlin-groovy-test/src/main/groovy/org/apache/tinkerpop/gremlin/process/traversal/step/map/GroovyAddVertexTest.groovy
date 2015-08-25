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
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalScriptHelper
import org.apache.tinkerpop.gremlin.structure.Vertex

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public abstract class GroovyAddVertexTest {

    public static class Traversals extends AddVertexTest {

        @Override
        public Traversal<Vertex, Vertex> get_g_VX1X_addVXanimalX_propertyXage_selectXaX_byXageXX_propertyXname_puppyX(
                final Object v1Id) {
            TraversalScriptHelper.compute("g.V(${v1Id}).as('a').addV('animal').property('age', select('a').by('age')).property('name', 'puppy')", g, "v1Id", v1Id);
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_V_addVXanimalX_propertyXage_0X() {
            TraversalScriptHelper.compute("g.V.addV(label, 'animal', 'age', 0)", g)
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_addVXpersonX_propertyXname_stephenX() {
            TraversalScriptHelper.compute("g.addV(label, 'person', 'name', 'stephen')", g)
        }

        ///////// DEPRECATED BELOW

        @Override
        public Traversal<Vertex, Vertex> get_g_V_addVXlabel_animal_age_0X() {
            TraversalScriptHelper.compute("g.V.addV(label, 'animal', 'age', 0)", g)
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_addVXlabel_person_name_stephenX() {
            TraversalScriptHelper.compute("g.addV(label, 'person', 'name', 'stephen')", g);
        }
    }
}
