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
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalEngine;
import org.apache.tinkerpop.gremlin.process.UseEngine;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;
import org.junit.Test;

import static org.apache.tinkerpop.gremlin.LoadGraphWith.GraphData.MODERN;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public abstract class AddVertexTest extends AbstractGremlinTest {

    public abstract Traversal<Vertex, Vertex> get_g_V_addVXlabel_animal_age_0X();

    public abstract Traversal<Vertex, Vertex> get_g_addVXlabel_person_name_stephenX();

    @Test
    @LoadGraphWith(MODERN)
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
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
        assertEquals(12, IteratorUtils.count(graph.vertices()));
        assertEquals(6, count);
    }

    @Test
    @LoadGraphWith(MODERN)
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
    public void g_addVXlabel_person_name_stephenX() {
        final Traversal<Vertex, Vertex> traversal = get_g_addVXlabel_person_name_stephenX();
        printTraversalForm(traversal);
        final Vertex stephen = traversal.next();
        assertFalse(traversal.hasNext());
        assertEquals("person", stephen.label());
        assertEquals("stephen", stephen.value("name"));
        assertEquals(1, IteratorUtils.count(stephen.properties()));
        assertEquals(7, IteratorUtils.count(graph.vertices()));
    }


    @UseEngine(TraversalEngine.Type.STANDARD)
    public static class Traversals extends AddVertexTest {

        @Override
        public Traversal<Vertex, Vertex> get_g_V_addVXlabel_animal_age_0X() {
            return g.V().addV(T.label, "animal", "age", 0);
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_addVXlabel_person_name_stephenX() {
            return g.addV(T.label, "person", "name", "stephen");
        }
    }
}