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
package org.apache.tinkerpop.gremlin.process.traversal.step.map;

import org.apache.tinkerpop.gremlin.AbstractGremlinTest;
import org.apache.tinkerpop.gremlin.FeatureRequirement;
import org.apache.tinkerpop.gremlin.LoadGraphWith;
import org.apache.tinkerpop.gremlin.process.GremlinProcessRunner;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.apache.tinkerpop.gremlin.LoadGraphWith.GraphData.MODERN;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.select;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
@RunWith(GremlinProcessRunner.class)
public abstract class AddVertexTest extends AbstractGremlinTest {

    public abstract Traversal<Vertex, Vertex> get_g_VX1X_addVXanimalX_propertyXage_selectXaX_byXageXX_propertyXname_puppyX(final Object v1Id);

    public abstract Traversal<Vertex, Vertex> get_g_V_addVXanimalX_propertyXage_0X();

    public abstract Traversal<Vertex, Vertex> get_g_addVXpersonX_propertyXname_stephenX();

    public abstract Traversal<Vertex, Vertex> get_g_addVXpersonX_propertyXsingle_name_stephenX_propertyXsingle_name_stephenmX();

    public abstract Traversal<Vertex, Vertex> get_g_addVXpersonX_propertyXsingle_name_stephenX_propertyXsingle_name_stephenm_since_2010X();

    public abstract Traversal<Vertex, Vertex> get_g_V_hasXname_markoX_propertyXfriendWeight_outEXknowsX_weight_sum__acl_privateX();

    public abstract Traversal<Vertex, Vertex> get_g_addVXanimalX_propertyXname_mateoX_propertyXname_gateoX_propertyXname_cateoX_propertyXage_5X();

    public abstract Traversal<Vertex, Vertex> get_g_V_addVXanimalX_propertyXname_valuesXnameXX_propertyXname_an_animalX_propertyXvaluesXnameX_labelX();

    public abstract Traversal<Vertex, Map<String, List<String>>> get_g_withSideEffectXa_testX_V_hasLabelXsoftwareX_propertyXtemp_selectXaXX_valueMapXname_tempX();

    public abstract Traversal<Vertex, String> get_g_withSideEffectXa_markoX_addV_propertyXname_selectXaXX_name();

    public abstract Traversal<Vertex, String> get_g_withSideEffectXa_nameX_addV_propertyXselectXaX_markoX_name();

    // 3.0.0 DEPRECATIONS
    @Deprecated
    public abstract Traversal<Vertex, Vertex> get_g_V_addVXlabel_animal_age_0X();

    @Deprecated
    public abstract Traversal<Vertex, Vertex> get_g_addVXlabel_person_name_stephenX();

    @Test
    @LoadGraphWith(MODERN)
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_PROPERTY)
    public void g_VX1X_addVXanimalX_propertyXage_selectXaX_byXageXX_propertyXname_puppyX() {
        final Traversal<Vertex, Vertex> traversal = get_g_VX1X_addVXanimalX_propertyXage_selectXaX_byXageXX_propertyXname_puppyX(convertToVertexId(graph, "marko"));
        printTraversalForm(traversal);
        final Vertex vertex = traversal.next();
        assertEquals("animal", vertex.label());
        assertEquals(29, vertex.<Integer>value("age").intValue());
        assertEquals("puppy", vertex.<String>value("name"));
        assertFalse(traversal.hasNext());
        assertEquals(7, IteratorUtils.count(g.V()));
    }


    @Test
    @LoadGraphWith(MODERN)
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_PROPERTY)
    public void g_V_addVXanimalX_propertyXage_0X() {
        final Traversal<Vertex, Vertex> traversal = get_g_V_addVXanimalX_propertyXage_0X();
        printTraversalForm(traversal);
        int count = 0;
        while (traversal.hasNext()) {
            final Vertex vertex = traversal.next();
            assertEquals("animal", vertex.label());
            assertEquals(0, vertex.<Integer>value("age").intValue());
            count++;
        }
        assertEquals(6, count);
        assertEquals(12, IteratorUtils.count(g.V()));

    }

    @Test
    @LoadGraphWith(MODERN)
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_PROPERTY)
    public void g_addVXpersonX_propertyXname_stephenX() {
        final Traversal<Vertex, Vertex> traversal = get_g_addVXpersonX_propertyXname_stephenX();
        printTraversalForm(traversal);
        final Vertex stephen = traversal.next();
        assertFalse(traversal.hasNext());
        assertEquals("person", stephen.label());
        assertEquals("stephen", stephen.value("name"));
        assertEquals(1, IteratorUtils.count(stephen.properties()));
        assertEquals(7, IteratorUtils.count(g.V()));
    }

    @Test
    @LoadGraphWith(MODERN)
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_PROPERTY)
    public void g_addVXpersonX_propertyXsingle_name_stephenX_propertyXsingle_name_stephenmX() {
        final Traversal<Vertex, Vertex> traversal = get_g_addVXpersonX_propertyXsingle_name_stephenX_propertyXsingle_name_stephenmX();
        printTraversalForm(traversal);
        final Vertex stephen = traversal.next();
        assertFalse(traversal.hasNext());
        assertEquals("person", stephen.label());
        assertEquals("stephenm", stephen.value("name"));
        assertEquals(1, IteratorUtils.count(stephen.properties()));
        assertEquals(7, IteratorUtils.count(g.V()));
    }

    @Test
    @LoadGraphWith(MODERN)
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_PROPERTY)
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_MULTI_PROPERTIES)
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_META_PROPERTIES)
    public void g_addVXpersonX_propertyXsingle_name_stephenX_propertyXsingle_name_stephenm_since_2010X() {
        final Traversal<Vertex, Vertex> traversal = get_g_addVXpersonX_propertyXsingle_name_stephenX_propertyXsingle_name_stephenm_since_2010X();
        printTraversalForm(traversal);
        final Vertex stephen = traversal.next();
        assertFalse(traversal.hasNext());
        assertEquals("person", stephen.label());
        assertEquals("stephenm", stephen.value("name"));
        assertEquals(2010, Integer.parseInt(stephen.property("name").value("since").toString()));
        assertEquals(1, IteratorUtils.count(stephen.property("name").properties()));
        assertEquals(1, IteratorUtils.count(stephen.properties()));
        assertEquals(7, IteratorUtils.count(g.V()));
    }

    @Test
    @LoadGraphWith(MODERN)
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_PROPERTY)
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_META_PROPERTIES)
    public void g_V_hasXname_markoX_propertyXfriendWeight_outEXknowsX_weight_sum__acl_privateX() {
        final Traversal<Vertex, Vertex> traversal = get_g_V_hasXname_markoX_propertyXfriendWeight_outEXknowsX_weight_sum__acl_privateX();
        printTraversalForm(traversal);
        final Vertex marko = traversal.next();
        assertFalse(traversal.hasNext());
        assertEquals("person", marko.label());
        assertEquals("marko", marko.value("name"));
        assertEquals(1.5, marko.value("friendWeight"), 0.01);
        assertEquals("private", marko.property("friendWeight").value("acl"));
        assertEquals(3, IteratorUtils.count(marko.properties()));
        assertEquals(1, IteratorUtils.count(marko.property("friendWeight").properties()));
    }

    @Test
    @LoadGraphWith(MODERN)
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_PROPERTY)
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_MULTI_PROPERTIES)
    public void g_addVXanimalX_propertyXname_mateoX_propertyXname_gateoX_propertyXname_cateoX_propertyXage_5X() {
        final Traversal<Vertex, Vertex> traversal = get_g_addVXanimalX_propertyXname_mateoX_propertyXname_gateoX_propertyXname_cateoX_propertyXage_5X();
        printTraversalForm(traversal);
        final Vertex mateo = traversal.next();
        assertFalse(traversal.hasNext());
        assertEquals("animal", mateo.label());
        assertEquals(3, IteratorUtils.count(mateo.properties("name")));
        mateo.values("name").forEachRemaining(name -> {
            assertTrue(name.equals("mateo") || name.equals("cateo") || name.equals("gateo"));
        });
        assertEquals(5, ((Integer) mateo.value("age")).intValue());
    }

    @Test
    @LoadGraphWith(MODERN)
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_PROPERTY)
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_MULTI_PROPERTIES)
    public void g_V_addVXanimalX_propertyXname_valuesXnameXX_propertyXname_an_animalX_propertyXvaluesXnameX_labelX() {
        final Traversal<Vertex, Vertex> traversal = get_g_V_addVXanimalX_propertyXname_valuesXnameXX_propertyXname_an_animalX_propertyXvaluesXnameX_labelX();
        printTraversalForm(traversal);
        while (traversal.hasNext()) {
            final Vertex vertex = traversal.next();
            assertEquals("animal", vertex.label());
            assertEquals(2, IteratorUtils.count(vertex.properties("name")));
            List<String> names = IteratorUtils.asList(vertex.values("name"));
            assertEquals(2, names.size());
            assertTrue(names.contains("an animal"));
            assertTrue(names.contains("marko") || names.contains("vadas") || names.contains("josh") || names.contains("lop") || names.contains("ripple") || names.contains("peter"));
            if (names.contains("marko")) {
                assertEquals("person", vertex.value("marko"));
            } else if (names.contains("vadas")) {
                assertEquals("person", vertex.value("vadas"));
            } else if (names.contains("josh")) {
                assertEquals("person", vertex.value("josh"));
            } else if (names.contains("ripple")) {
                assertEquals("software", vertex.value("ripple"));
            } else if (names.contains("lop")) {
                assertEquals("software", vertex.value("lop"));
            } else if (names.contains("peter")) {
                assertEquals("person", vertex.value("peter"));
            } else {
                throw new IllegalStateException("This state should not have been reached");
            }
        }
    }

    /////

    @Test
    @LoadGraphWith(MODERN)
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_PROPERTY)
    public void g_V_addVXlabel_animal_age_0X() {
        final Traversal<Vertex, Vertex> traversal = get_g_V_addVXlabel_animal_age_0X();
        printTraversalForm(traversal);
        int count = 0;
        while (traversal.hasNext()) {
            final Vertex vertex = traversal.next();
            assertEquals("animal", vertex.label());
            assertEquals(0, vertex.<Integer>value("age").intValue());
            count++;
        }
        assertEquals(6, count);
        assertEquals(12, IteratorUtils.count(g.V()));

    }

    @Test
    @LoadGraphWith(MODERN)
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_PROPERTY)
    public void g_addVXlabel_person_name_stephenX() {
        final Traversal<Vertex, Vertex> traversal = get_g_addVXlabel_person_name_stephenX();
        printTraversalForm(traversal);
        final Vertex stephen = traversal.next();
        assertFalse(traversal.hasNext());
        assertEquals("person", stephen.label());
        assertEquals("stephen", stephen.value("name"));
        assertEquals(1, IteratorUtils.count(stephen.properties()));
        assertEquals(7, IteratorUtils.count(g.V()));
    }

    @Test
    @LoadGraphWith(MODERN)
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_PROPERTY)
    public void g_withSideEffectXa_testX_V_hasLabelXsoftwareX_propertyXtemp_selectXaXX_valueMapXname_tempX() {
        final Traversal<Vertex, Map<String, List<String>>> traversal = get_g_withSideEffectXa_testX_V_hasLabelXsoftwareX_propertyXtemp_selectXaXX_valueMapXname_tempX();
        printTraversalForm(traversal);
        int counter = 0;
        while (traversal.hasNext()) {
            counter++;
            final Map<String, List<String>> valueMap = traversal.next();
            assertEquals(2, valueMap.size());
            assertEquals(Collections.singletonList("test"), valueMap.get("temp"));
            assertTrue(valueMap.get("name").equals(Collections.singletonList("ripple")) || valueMap.get("name").equals(Collections.singletonList("lop")));
        }
        assertEquals(2, counter);
        assertFalse(traversal.hasNext());
    }

    @Test
    @LoadGraphWith(MODERN)
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_PROPERTY)
    public void g_withSideEffectXa_markoX_addV_propertyXname_selectXaXX_name() {
        final Traversal<Vertex, String> traversal = get_g_withSideEffectXa_markoX_addV_propertyXname_selectXaXX_name();
        printTraversalForm(traversal);
        assertEquals("marko", traversal.next());
        assertFalse(traversal.hasNext());
    }

    @Test
    @LoadGraphWith(MODERN)
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_PROPERTY)
    public void g_withSideEffectXa_nameX_addV_propertyXselectXaX_markoX_name() {
        final Traversal<Vertex, String> traversal = get_g_withSideEffectXa_nameX_addV_propertyXselectXaX_markoX_name();
        printTraversalForm(traversal);
        assertEquals("marko", traversal.next());
        assertFalse(traversal.hasNext());
    }


    public static class Traversals extends AddVertexTest {

        @Override
        public Traversal<Vertex, Vertex> get_g_VX1X_addVXanimalX_propertyXage_selectXaX_byXageXX_propertyXname_puppyX(final Object v1Id) {
            return g.V(v1Id).as("a").addV("animal").property("age", select("a").by("age")).property("name", "puppy");
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_V_addVXanimalX_propertyXage_0X() {
            return g.V().addV("animal").property("age", 0);
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_addVXpersonX_propertyXname_stephenX() {
            return g.addV("person").property("name", "stephen");
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_addVXpersonX_propertyXsingle_name_stephenX_propertyXsingle_name_stephenmX() {
            return g.addV("person").property(VertexProperty.Cardinality.single, "name", "stephen").property(VertexProperty.Cardinality.single, "name", "stephenm");
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_addVXpersonX_propertyXsingle_name_stephenX_propertyXsingle_name_stephenm_since_2010X() {
            return g.addV("person").property(VertexProperty.Cardinality.single, "name", "stephen").property(VertexProperty.Cardinality.single, "name", "stephenm", "since", 2010);
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_V_hasXname_markoX_propertyXfriendWeight_outEXknowsX_weight_sum__acl_privateX() {
            return g.V().has("name", "marko").property("friendWeight", __.outE("knows").values("weight").sum(), "acl", "private");
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_addVXanimalX_propertyXname_mateoX_propertyXname_gateoX_propertyXname_cateoX_propertyXage_5X() {
            return g.addV("animal").property("name", "mateo").property("name", "gateo").property("name", "cateo").property("age", 5);
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_V_addVXanimalX_propertyXname_valuesXnameXX_propertyXname_an_animalX_propertyXvaluesXnameX_labelX() {
            return g.V().addV("animal").property("name", __.values("name")).property("name", "an animal").property(__.values("name"), __.label());
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_V_addVXlabel_animal_age_0X() {
            return g.V().addV(T.label, "animal", "age", 0);
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_addVXlabel_person_name_stephenX() {
            return g.addV(T.label, "person", "name", "stephen");
        }

        @Override
        public Traversal<Vertex, Map<String, List<String>>> get_g_withSideEffectXa_testX_V_hasLabelXsoftwareX_propertyXtemp_selectXaXX_valueMapXname_tempX() {
            return g.withSideEffect("a", "test").V().hasLabel("software").property("temp", select("a")).valueMap("name", "temp");
        }

        @Override
        public Traversal<Vertex, String> get_g_withSideEffectXa_markoX_addV_propertyXname_selectXaXX_name() {
            return g.withSideEffect("a", "marko").addV().property("name", select("a")).values("name");
        }

        @Override
        public Traversal<Vertex, String> get_g_withSideEffectXa_nameX_addV_propertyXselectXaX_markoX_name() {
            return g.withSideEffect("a", "name").addV().property(select("a"),"marko").values("name");
        }
    }
}