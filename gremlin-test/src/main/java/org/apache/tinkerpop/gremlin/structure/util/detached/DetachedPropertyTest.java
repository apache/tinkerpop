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
package org.apache.tinkerpop.gremlin.structure.util.detached;

import org.apache.tinkerpop.gremlin.AbstractGremlinTest;
import org.apache.tinkerpop.gremlin.FeatureRequirement;
import org.apache.tinkerpop.gremlin.FeatureRequirementSet;
import org.apache.tinkerpop.gremlin.LoadGraphWith;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.Attachable;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class DetachedPropertyTest extends AbstractGremlinTest {

    @Test
    @FeatureRequirementSet(FeatureRequirementSet.Package.SIMPLE)
    public void shouldNotConstructNewWithSomethingAlreadyDetached() {
        final Vertex v = graph.addVertex();
        final Edge e = v.addEdge("test", v, "xxx", "yyy");
        final DetachedProperty dp = DetachedFactory.detach(e.property("xxx"));
        assertSame(dp, DetachedFactory.detach(dp));
    }

    @Test
    @LoadGraphWith(LoadGraphWith.GraphData.MODERN)
    public void shouldConstructDetachedPropertyWithPropertyFromEdge() {
        final DetachedProperty p = DetachedFactory.detach(g.E(convertToEdgeId("josh", "created", "lop")).next().property("weight"));
        assertEquals("weight", p.key());
        assertEquals(0.4d, (double) p.value(), 0.000001d);
        assertEquals(DetachedEdge.class, p.element().getClass());
    }

    @Test(expected = IllegalStateException.class)
    @LoadGraphWith(LoadGraphWith.GraphData.MODERN)
    public void shouldNotSupportRemove() {
        DetachedFactory.detach(g.E(convertToEdgeId("josh", "created", "lop")).next().property("weight")).remove();
    }

    @Test
    @LoadGraphWith(LoadGraphWith.GraphData.MODERN)
    public void shouldBeEqualProperties() {
        assertTrue(DetachedFactory.detach(g.E(convertToEdgeId("josh", "created", "lop")).next().property("weight")).equals(DetachedFactory.detach(g.E(convertToEdgeId("josh", "created", "lop")).next().property("weight"))));
    }

    @Test
    @LoadGraphWith(LoadGraphWith.GraphData.MODERN)
    public void shouldAttachToGraph() {
        final Edge e = g.E(convertToEdgeId("josh", "created", "lop")).next();
        final Property toDetach = e.properties("weight").next();
        final DetachedProperty<?> detachedProperty = DetachedFactory.detach(toDetach);
        final Property attached = detachedProperty.attach(Attachable.Method.get(graph));

        assertEquals(toDetach, attached);
        assertFalse(attached instanceof DetachedProperty);
    }

    @Test
    @LoadGraphWith(LoadGraphWith.GraphData.MODERN)
    public void shouldAttachToVertex() {
        final Edge e = g.E(convertToEdgeId("josh", "created", "lop")).next();
        final Property toDetach = e.property("weight");
        final DetachedProperty<?> detachedProperty = DetachedFactory.detach(toDetach);
        final Property attached = detachedProperty.attach(Attachable.Method.get(e.outVertex()));

        assertEquals(toDetach, attached);
        assertFalse(attached instanceof DetachedProperty);
    }

    @Test
    @LoadGraphWith(LoadGraphWith.GraphData.MODERN)
    public void shouldNotBeEqualPropertiesAsThereIsDifferentId() {
        assertFalse(DetachedFactory.detach(g.E(convertToEdgeId("marko", "created", "lop")).next().property("weight")).equals(DetachedFactory.detach(g.E(convertToEdgeId("josh", "created", "lop")).next().property("weight"))));
    }

    @Test
    @LoadGraphWith(LoadGraphWith.GraphData.MODERN)
    @FeatureRequirementSet(FeatureRequirementSet.Package.SIMPLE)
    @FeatureRequirement(featureClass = Graph.Features.EdgePropertyFeatures.class, feature = Graph.Features.EdgePropertyFeatures.FEATURE_DOUBLE_VALUES)
    public void shouldNotBeEqualPropertiesAsThereIsDifferentKey() {
        final Object joshCreatedLopEdgeId = convertToEdgeId("josh", "created", "lop");
        final Edge e = g.V(convertToVertexId("josh")).next().addEdge("created", g.V(convertToVertexId("lop")).next(), "weight", 0.4d);
        assertFalse(DetachedFactory.detach(e.property("weight")).equals(DetachedFactory.detach(g.E(joshCreatedLopEdgeId).next().property("weight"))));
    }
}
