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
import org.apache.tinkerpop.gremlin.process.traversal.Merge;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;
import org.junit.Test;
import org.junit.runner.RunWith;

import static org.apache.tinkerpop.gremlin.LoadGraphWith.GraphData.MODERN;
import static org.apache.tinkerpop.gremlin.util.tools.CollectionFactory.asMap;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

@RunWith(GremlinProcessRunner.class)
public abstract class MergeVertexTest extends AbstractGremlinTest {

    public abstract Traversal<Vertex, Vertex> get_g_mergeVXlabel_person_name_stephenX();

    public abstract Traversal<Integer, Vertex> get_g_injectX0X_mergeVXlabel_person_name_stephenX();

    public abstract Traversal<Vertex, Vertex> get_g_mergeVXlabel_person_name_markoX();

    public abstract Traversal<Integer, Vertex> get_g_injectX0X_mergeVXlabel_person_name_markoX();

    public abstract Traversal<Vertex, Vertex> get_g_mergeVXlabel_person_name_stephenX_optionXonCreate_label_person_name_stephen_age_19X_option();

    public abstract Traversal<Vertex, Vertex> get_g_mergeVXlabel_person_name_markoX_optionXonMatch_age_19X_option();

    public abstract Traversal<Object, Vertex> get_g_withSideEffectXc_label_person_name_stephenX_withSideEffectXm_label_person_name_stephen_age_19X_mergeVXselectXcXX_optionXonCreate_selectXmXX_option();

    public abstract Traversal<Object, Vertex> get_g_withSideEffectXc_label_person_name_markoX_withSideEffectXm_age_19X_mergeVXselectXcXX_optionXonMatch_selectXmXX_option();

    @Test
    @LoadGraphWith(MODERN)
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_PROPERTY)
    public void g_mergeVXlabel_person_name_stephenX() {
        final Traversal<Vertex, Vertex> traversal = get_g_mergeVXlabel_person_name_stephenX();
        printTraversalForm(traversal);
        final Vertex vertex = traversal.next();
        assertEquals("person", vertex.label());
        assertEquals("stephen", vertex.<String>value("name"));
        assertFalse(traversal.hasNext());
        assertEquals(7, IteratorUtils.count(g.V()));
    }

    @Test
    @LoadGraphWith(MODERN)
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_PROPERTY)
    public void g_injectX0X_mergeVXlabel_person_name_stephenX() {
        final Traversal<Integer, Vertex> traversal = get_g_injectX0X_mergeVXlabel_person_name_stephenX();
        printTraversalForm(traversal);
        final Vertex vertex = traversal.next();
        assertEquals("person", vertex.label());
        assertEquals("stephen", vertex.<String>value("name"));
        assertFalse(traversal.hasNext());
        assertEquals(7, IteratorUtils.count(g.V()));
    }

    @Test
    @LoadGraphWith(MODERN)
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_PROPERTY)
    public void g_mergeVXlabel_person_name_markoX() {
        final Traversal<Vertex, Vertex> traversal = get_g_mergeVXlabel_person_name_markoX();
        printTraversalForm(traversal);
        final Vertex vertex = traversal.next();
        assertEquals("person", vertex.label());
        assertEquals("marko", vertex.<String>value("name"));
        assertFalse(traversal.hasNext());
        assertEquals(6, IteratorUtils.count(g.V()));
    }


    @Test
    @LoadGraphWith(MODERN)
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_PROPERTY)
    public void g_injectX0X_mergeVXlabel_person_name_markoX() {
        final Traversal<Integer, Vertex> traversal = get_g_injectX0X_mergeVXlabel_person_name_markoX();
        printTraversalForm(traversal);
        final Vertex vertex = traversal.next();
        assertEquals("person", vertex.label());
        assertEquals("marko", vertex.<String>value("name"));
        assertFalse(traversal.hasNext());
        assertEquals(6, IteratorUtils.count(g.V()));
    }

    @Test
    @LoadGraphWith(MODERN)
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_PROPERTY)
    public void g_mergeVXlabel_person_name_stephenX_optionXonCreate_label_person_name_stephen_age_19X_option() {
        final Traversal<Vertex, Vertex> traversal = get_g_mergeVXlabel_person_name_stephenX_optionXonCreate_label_person_name_stephen_age_19X_option();
        printTraversalForm(traversal);
        final Vertex vertex = traversal.next();
        assertEquals("person", vertex.label());
        assertEquals("stephen", vertex.<String>value("name"));
        assertEquals(19, vertex.<Integer>value("age").intValue());
        assertFalse(traversal.hasNext());
        assertEquals(7, IteratorUtils.count(g.V()));
    }

    @Test
    @LoadGraphWith(MODERN)
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_PROPERTY)
    public void g_mergeVXlabel_person_name_markoX_optionXonMatch_age_19X_option() {
        final Traversal<Vertex, Vertex> traversal = get_g_mergeVXlabel_person_name_markoX_optionXonMatch_age_19X_option();
        printTraversalForm(traversal);
        final Vertex vertex = traversal.next();
        assertEquals("person", vertex.label());
        assertEquals("marko", vertex.<String>value("name"));
        assertEquals(19, vertex.<Integer>value("age").intValue());
        assertFalse(traversal.hasNext());
        assertEquals(6, IteratorUtils.count(g.V()));
    }

    @Test
    @LoadGraphWith(MODERN)
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_PROPERTY)
    public void g_withSideEffectXc_label_person_name_stephenX_withSideEffectXm_label_person_name_stephen_age_19X_mergeVXselectXcXX_optionXonCreate_selectXmXX_option() {
        final Traversal<Object, Vertex> traversal = get_g_withSideEffectXc_label_person_name_stephenX_withSideEffectXm_label_person_name_stephen_age_19X_mergeVXselectXcXX_optionXonCreate_selectXmXX_option();
        printTraversalForm(traversal);
        final Vertex vertex = traversal.next();
        assertEquals("person", vertex.label());
        assertEquals("stephen", vertex.<String>value("name"));
        assertEquals(19, vertex.<Integer>value("age").intValue());
        assertFalse(traversal.hasNext());
        assertEquals(7, IteratorUtils.count(g.V()));
    }

    @Test
    @LoadGraphWith(MODERN)
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_PROPERTY)
    public void g_withSideEffectXc_label_person_name_markoX_withSideEffectXm_age_19X_mergeVXselectXcXX_optionXonMatch_selectXmXX_option() {
        final Traversal<Object, Vertex> traversal = get_g_withSideEffectXc_label_person_name_markoX_withSideEffectXm_age_19X_mergeVXselectXcXX_optionXonMatch_selectXmXX_option();
        printTraversalForm(traversal);
        final Vertex vertex = traversal.next();
        assertEquals("person", vertex.label());
        assertEquals("marko", vertex.<String>value("name"));
        assertEquals(19, vertex.<Integer>value("age").intValue());
        assertFalse(traversal.hasNext());
        assertEquals(6, IteratorUtils.count(g.V()));
    }

    public static class Traversals extends MergeVertexTest {

        @Override
        public Traversal<Vertex, Vertex> get_g_mergeVXlabel_person_name_markoX() {
            return g.mergeV(asMap(T.label, "person", "name", "marko"));
        }

        @Override
        public Traversal<Integer, Vertex> get_g_injectX0X_mergeVXlabel_person_name_markoX() {
            return g.inject(0).mergeV(asMap(T.label, "person", "name", "marko"));
        }

        @Override
        public Traversal<Integer, Vertex> get_g_injectX0X_mergeVXlabel_person_name_stephenX() {
            return g.inject(0).mergeV(asMap(T.label, "person", "name", "stephen"));
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_mergeVXlabel_person_name_stephenX() {
            return g.mergeV(asMap(T.label, "person", "name", "stephen"));
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_mergeVXlabel_person_name_stephenX_optionXonCreate_label_person_name_stephen_age_19X_option() {
            return g.mergeV(asMap(T.label, "person", "name", "stephen")).option(Merge.onCreate, asMap(T.label, "person", "name", "stephen", "age", 19));
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_mergeVXlabel_person_name_markoX_optionXonMatch_age_19X_option() {
            return g.mergeV(asMap(T.label, "person", "name", "marko")).option(Merge.onMatch, asMap("age", 19));
        }

        @Override
        public Traversal<Object, Vertex> get_g_withSideEffectXc_label_person_name_stephenX_withSideEffectXm_label_person_name_stephen_age_19X_mergeVXselectXcXX_optionXonCreate_selectXmXX_option() {
            return g.withSideEffect("c", asMap(T.label, "person", "name", "stephen")).
                     withSideEffect("m", asMap(T.label, "person", "name", "stephen", "age", 19)).
                     mergeV(__.select("c")).option(Merge.onCreate, __.select("m"));
        }

        @Override
        public Traversal<Object, Vertex> get_g_withSideEffectXc_label_person_name_markoX_withSideEffectXm_age_19X_mergeVXselectXcXX_optionXonMatch_selectXmXX_option() {
            return g.withSideEffect("c", asMap(T.label, "person", "name", "marko")).
                    withSideEffect("m", asMap("age", 19)).
                    mergeV(__.select("c")).option(Merge.onMatch, __.select("m"));
        }
    }
}