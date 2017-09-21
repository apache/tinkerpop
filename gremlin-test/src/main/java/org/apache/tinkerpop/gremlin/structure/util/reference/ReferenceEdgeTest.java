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
package org.apache.tinkerpop.gremlin.structure.util.reference;

import org.apache.tinkerpop.gremlin.AbstractGremlinTest;
import org.apache.tinkerpop.gremlin.FeatureRequirement;
import org.apache.tinkerpop.gremlin.FeatureRequirementSet;
import org.apache.tinkerpop.gremlin.LoadGraphWith;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.Attachable;
import org.apache.tinkerpop.gremlin.structure.util.detached.DetachedFactory;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;
import org.junit.Test;

import java.util.HashSet;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class ReferenceEdgeTest extends AbstractGremlinTest {

    @Test
    @FeatureRequirementSet(FeatureRequirementSet.Package.SIMPLE)
    public void shouldHashAndEqualCorrectly() {
        final Vertex v = graph.addVertex();
        final Edge e = v.addEdge("test", v);
        final Set<Edge> set = new HashSet<>();
        for (int i = 0; i < 100; i++) {
            set.add(ReferenceFactory.detach(e));
            set.add(e);
        }
        assertEquals(1, set.size());
    }

    @Test
    @FeatureRequirementSet(FeatureRequirementSet.Package.SIMPLE)
    public void shouldNotConstructNewWithSomethingAlreadyReferenced() {
        final Vertex v = graph.addVertex();
        final Edge e = v.addEdge("test", v);
        final ReferenceEdge re = ReferenceFactory.detach(e);
        assertEquals("test", e.label());
        assertSame(re, ReferenceFactory.detach(re));
    }

    @Test
    @LoadGraphWith(LoadGraphWith.GraphData.MODERN)
    @FeatureRequirementSet(FeatureRequirementSet.Package.SIMPLE)
    @FeatureRequirement(featureClass = Graph.Features.EdgePropertyFeatures.class, feature = Graph.Features.EdgePropertyFeatures.FEATURE_DOUBLE_VALUES)
    public void shouldConstructReferenceEdge() {
        g.E(convertToEdgeId("marko", "knows", "vadas")).next().property("year", 2002);
        final ReferenceEdge referenceEdge = ReferenceFactory.detach(g.E(convertToEdgeId("marko", "knows", "vadas")).next());
        assertEquals(convertToEdgeId("marko", "knows", "vadas"), referenceEdge.id());
        assertEquals(0, IteratorUtils.count(referenceEdge.properties()));
        assertEquals(2, IteratorUtils.count(referenceEdge.vertices(Direction.BOTH)));
    }

    @Test
    @LoadGraphWith(LoadGraphWith.GraphData.MODERN)
    public void shouldEvaluateToEqual() {
        assertTrue(ReferenceFactory.detach(g.E(convertToEdgeId("josh", "created", "lop")).next()).equals(ReferenceFactory.detach(g.E(convertToEdgeId("josh", "created", "lop")).next())));
        assertTrue(ReferenceFactory.detach(g.E(convertToEdgeId("josh", "created", "lop")).next()).equals(g.E(convertToEdgeId("josh", "created", "lop")).next()));
        assertTrue(ReferenceFactory.detach(g.E(convertToEdgeId("josh", "created", "lop")).next()).equals(DetachedFactory.detach(g.E(convertToEdgeId("josh", "created", "lop")).next(), true)));
        //
        assertTrue(ReferenceFactory.detach(g.E(convertToEdgeId("josh", "created", "lop")).next()).equals(ReferenceFactory.detach(g.E(convertToEdgeId("josh", "created", "lop")).next())));
        assertTrue(g.E(convertToEdgeId("josh", "created", "lop")).next().equals(ReferenceFactory.detach(g.E(convertToEdgeId("josh", "created", "lop")).next())));
        assertTrue(DetachedFactory.detach(g.E(convertToEdgeId("josh", "created", "lop")).next(), true).equals(ReferenceFactory.detach(g.E(convertToEdgeId("josh", "created", "lop")).next())));
    }

    @Test
    @LoadGraphWith(LoadGraphWith.GraphData.MODERN)
    public void shouldHaveSameHashCode() {
        assertEquals(ReferenceFactory.detach(g.E(convertToEdgeId("josh", "created", "lop")).next()).hashCode(), ReferenceFactory.detach(g.E(convertToEdgeId("josh", "created", "lop")).next()).hashCode());
        assertEquals(ReferenceFactory.detach(g.E(convertToEdgeId("josh", "created", "lop")).next()).hashCode(), g.E(convertToEdgeId("josh", "created", "lop")).next().hashCode());
        assertEquals(ReferenceFactory.detach(g.E(convertToEdgeId("josh", "created", "lop")).next()).hashCode(), DetachedFactory.detach(g.E(convertToEdgeId("josh", "created", "lop")).next(), false).hashCode());
    }

    @Test
    @LoadGraphWith(LoadGraphWith.GraphData.MODERN)
    public void shouldAttachToGraph() {
        final Edge toReference = g.E(convertToEdgeId("josh", "created", "lop")).next();
        final ReferenceEdge referenceEdge = ReferenceFactory.detach(toReference);
        final Edge referenced = referenceEdge.attach(Attachable.Method.get(graph));

        assertEquals(toReference, referenced);
        assertFalse(referenced instanceof ReferenceEdge);
    }

    @Test
    @LoadGraphWith(LoadGraphWith.GraphData.MODERN)
    public void shouldAttachToVertex() {
        final Edge toReference = g.E(convertToEdgeId("josh", "created", "lop")).next();
        final Vertex outV = toReference.vertices(Direction.OUT).next();
        final ReferenceEdge referenceEdge = ReferenceFactory.detach(toReference);
        final Edge attached = referenceEdge.attach(Attachable.Method.get(outV));

        assertEquals(toReference, attached);
        assertFalse(attached instanceof ReferenceEdge);
    }

    @Test
    @LoadGraphWith(LoadGraphWith.GraphData.MODERN)
    @FeatureRequirementSet(FeatureRequirementSet.Package.SIMPLE)
    @FeatureRequirement(featureClass = Graph.Features.EdgePropertyFeatures.class, feature = Graph.Features.EdgePropertyFeatures.FEATURE_DOUBLE_VALUES)
    public void shouldNotEvaluateToEqualDifferentId() {
        final Object joshCreatedLopEdgeId = convertToEdgeId("josh", "created", "lop");
        final Vertex vOut = g.V(convertToVertexId("josh")).next();
        final Vertex vIn = g.V(convertToVertexId("lop")).next();
        final Edge e = vOut.addEdge("created", vIn, "weight", 0.4d);
        assertFalse(ReferenceFactory.detach(g.E(joshCreatedLopEdgeId).next()).equals(ReferenceFactory.detach(e)));
    }

    @Test(expected = IllegalStateException.class)
    @FeatureRequirementSet(FeatureRequirementSet.Package.SIMPLE)
    public void shouldNotAllowSetProperty() {
        final Vertex v = graph.addVertex();
        final Edge e = v.addEdge("test", v);
        final ReferenceEdge re = ReferenceFactory.detach(e);
        re.property("test", "test");
    }

    @Test(expected = IllegalStateException.class)
    @FeatureRequirementSet(FeatureRequirementSet.Package.SIMPLE)
    public void shouldNotAllowRemove() {
        final Vertex v = graph.addVertex();
        final Edge e = v.addEdge("test", v);
        final ReferenceEdge re = ReferenceFactory.detach(e);
        re.remove();
    }
}
