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
import org.apache.tinkerpop.gremlin.FeatureRequirementSet;
import org.apache.tinkerpop.gremlin.LoadGraphWith;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.util.Attachable;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class DetachedVertexPropertyTest extends AbstractGremlinTest {

    @Test
    @FeatureRequirementSet(FeatureRequirementSet.Package.SIMPLE)
    public void shouldNotConstructNewWithSomethingAlreadyDetached() {
        final Vertex v = graph.addVertex();
        final VertexProperty vp = v.property(VertexProperty.Cardinality.single, "test", "this");
        final DetachedVertexProperty dvp = DetachedFactory.detach(vp, true);
        assertSame(dvp, DetachedFactory.detach(dvp, true));
    }

    @Test
    @FeatureRequirementSet(FeatureRequirementSet.Package.SIMPLE)
    public void shouldConstructDetachedPropertyWithPropertyFromVertex() {
        final Vertex v = graph.addVertex();
        final VertexProperty vp = v.property(VertexProperty.Cardinality.single, "test", "this");
        final DetachedVertexProperty mp = DetachedFactory.detach(vp, true);
        assertEquals("test", mp.key());
        assertEquals("this", mp.value());
        assertEquals(DetachedVertex.class, mp.element().getClass());
    }

    @Test
    @FeatureRequirementSet(FeatureRequirementSet.Package.SIMPLE)
    public void shouldConstructDetachedPropertyWithHiddenFromVertex() {
        final Vertex v = graph.addVertex();
        final VertexProperty vp = v.property(VertexProperty.Cardinality.single, "test", "this");
        final DetachedVertexProperty mp = DetachedFactory.detach(vp, true);
        assertEquals("test", mp.key());
        assertEquals("this", mp.value());
        assertEquals(DetachedVertex.class, mp.element().getClass());
    }

    @Test(expected = IllegalStateException.class)
    @FeatureRequirementSet(FeatureRequirementSet.Package.SIMPLE)
    public void shouldNotSupportRemove() {
        final Vertex v = graph.addVertex();
        final VertexProperty vp = v.property(VertexProperty.Cardinality.single, "test", "this");
        DetachedFactory.detach(vp, true).remove();
    }

    @Test
    @FeatureRequirementSet(FeatureRequirementSet.Package.SIMPLE)
    public void shouldBeEqualsPropertiesAsIdIsTheSame() {
        final Vertex v = graph.addVertex();
        final VertexProperty vp = v.property(VertexProperty.Cardinality.single, "test", "this");
        final DetachedVertexProperty mp1 = DetachedFactory.detach(vp, true);
        final DetachedVertexProperty mp2 = DetachedFactory.detach(vp, true);
        assertTrue(mp1.equals(mp2));
    }

    @Test
    @FeatureRequirementSet(FeatureRequirementSet.Package.SIMPLE)
    public void shouldNotBeEqualsPropertiesAsIdIsDifferent() {
        final Vertex v = graph.addVertex();
        final VertexProperty vp1 = v.property(VertexProperty.Cardinality.single, "test", "this");
        final DetachedVertexProperty mp1 = DetachedFactory.detach(vp1, true);
        final VertexProperty vp2 = v.property(VertexProperty.Cardinality.single, "testing", "this");
        final DetachedVertexProperty mp2 = DetachedFactory.detach(vp2, true);
        assertFalse(mp1.equals(mp2));
    }

    @Test
    @LoadGraphWith(LoadGraphWith.GraphData.CREW)
    public void shouldDetachMultiPropertiesAndMetaProperties() {
        final Vertex v1 = convertToVertex(graph, "marko");
        v1.properties("location").forEachRemaining(vp -> {
            final DetachedVertexProperty detached = DetachedFactory.detach(vp, true);
            if (detached.value().equals("san diego")) {
                assertEquals(1997, (int) detached.value("startTime"));
                assertEquals(2001, (int) detached.value("endTime"));
                assertEquals(2, (int) IteratorUtils.count(detached.properties()));
            } else if (vp.value().equals("santa cruz")) {
                assertEquals(2001, (int) detached.value("startTime"));
                assertEquals(2004, (int) detached.value("endTime"));
                assertEquals(2, (int) IteratorUtils.count(detached.properties()));
            } else if (detached.value().equals("brussels")) {
                assertEquals(2004, (int) vp.value("startTime"));
                assertEquals(2005, (int) vp.value("endTime"));
                assertEquals(2, (int) IteratorUtils.count(detached.properties()));
            } else if (detached.value().equals("santa fe")) {
                assertEquals(2005, (int) detached.value("startTime"));
                assertEquals(1, (int) IteratorUtils.count(detached.properties()));
            } else {
                fail("Found a value that should be there");
            }
        });
    }

    @Test
    @FeatureRequirementSet(FeatureRequirementSet.Package.SIMPLE)
    public void shouldAttachToGraph() {
        final Vertex v = graph.addVertex();
        final VertexProperty toDetach = v.property(VertexProperty.Cardinality.single, "test", "this");
        final DetachedVertexProperty<?> detached = DetachedFactory.detach(toDetach, true);
        final VertexProperty attached = detached.attach(Attachable.Method.get(graph));

        assertEquals(toDetach, attached);
        assertFalse(attached instanceof DetachedVertexProperty);
    }

    @Test
    @FeatureRequirementSet(FeatureRequirementSet.Package.SIMPLE)
    public void shouldAttachToVertex() {
        final Vertex v = graph.addVertex();
        final VertexProperty toDetach = v.property(VertexProperty.Cardinality.single, "test", "this");
        final DetachedVertexProperty<?> detached = DetachedFactory.detach(toDetach, true);
        final VertexProperty attached = detached.attach(Attachable.Method.get(v));

        assertEquals(toDetach, attached);
        assertEquals(toDetach.getClass(), attached.getClass());
        assertFalse(attached instanceof DetachedVertexProperty);
    }
}
