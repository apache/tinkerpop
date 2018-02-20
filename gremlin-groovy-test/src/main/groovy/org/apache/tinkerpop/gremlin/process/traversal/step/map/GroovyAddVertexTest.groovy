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
public abstract class GroovyAddVertexTest {

    public static class Traversals extends AddVertexTest {

        @Override
        public Traversal<Vertex, Vertex> get_g_VX1X_addVXanimalX_propertyXage_selectXaX_byXageXX_propertyXname_puppyX(
                final Object v1Id) {
            new ScriptTraversal<>(g, "gremlin-groovy", "g.V(v1Id).as('a').addV('animal').property('age', select('a').by('age')).property('name', 'puppy')", "v1Id", v1Id);
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_V_addVXanimalX_propertyXage_0X() {
            new ScriptTraversal<>(g, "gremlin-groovy", "g.V().addV('animal').property('age', 0)")
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_addVXpersonX_propertyXname_stephenX() {
            new ScriptTraversal<>(g, "gremlin-groovy", "g.addV(label, 'person', 'name', 'stephen')")
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_addVXpersonX_propertyXsingle_name_stephenX_propertyXsingle_name_stephenmX() {
            new ScriptTraversal<>(g, "gremlin-groovy", "g.addV('person').property(VertexProperty.Cardinality.single, 'name', 'stephen').property(VertexProperty.Cardinality.single, 'name', 'stephenm')")
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_addVXpersonX_propertyXsingle_name_stephenX_propertyXsingle_name_stephenm_since_2010X() {
            new ScriptTraversal<>(g, "gremlin-groovy", "g.addV('person').property(VertexProperty.Cardinality.single, 'name', 'stephen').property(VertexProperty.Cardinality.single, 'name', 'stephenm', 'since', 2010)")
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_V_hasXname_markoX_propertyXfriendWeight_outEXknowsX_weight_sum__acl_privateX() {
            new ScriptTraversal<>(g, "gremlin-groovy", "g.V.has('name', 'marko').property('friendWeight', outE('knows').weight.sum(), 'acl', 'private')")
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_addVXanimalX_propertyXname_mateoX_propertyXname_gateoX_propertyXname_cateoX_propertyXage_5X() {
            new ScriptTraversal<>(g, "gremlin-groovy", "g.addV('animal').property('name', 'mateo').property('name', 'gateo').property('name', 'cateo').property('age', 5)")
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_V_addVXanimalX_propertyXname_valuesXnameXX_propertyXname_an_animalX_propertyXvaluesXnameX_labelX() {
            new ScriptTraversal<>(g, "gremlin-groovy", "g.V.addV('animal').property('name', values('name')).property('name', 'an animal').property(values('name'), label())")
        }

        @Override
        public Traversal<Vertex, Map<String, List<String>>> get_g_withSideEffectXa_testX_V_hasLabelXsoftwareX_propertyXtemp_selectXaXX_valueMapXname_tempX() {
            new ScriptTraversal<>(g, "gremlin-groovy", "g.withSideEffect('a', 'test').V.hasLabel('software').property('temp', select('a')).valueMap('name', 'temp')")
        }

        @Override
        public Traversal<Vertex, String> get_g_withSideEffectXa_markoX_addV_propertyXname_selectXaXX_name() {
            new ScriptTraversal<>(g, "gremlin-groovy", "g.withSideEffect('a', 'marko').addV().property('name', select('a')).name")
        }

        ///////// DEPRECATED BELOW

        @Override
        public Traversal<Vertex, Vertex> get_g_V_addVXlabel_animal_age_0X() {
            new ScriptTraversal<>(g, "gremlin-groovy", "g.V.addV(label, 'animal', 'age', 0)")
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_addVXlabel_person_name_stephenX() {
            new ScriptTraversal<>(g, "gremlin-groovy", "g.addV(label, 'person', 'name', 'stephen')")
        }
    }
}
