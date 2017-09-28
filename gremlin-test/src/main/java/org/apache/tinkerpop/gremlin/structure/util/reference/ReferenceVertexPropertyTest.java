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
import org.apache.tinkerpop.gremlin.FeatureRequirementSet;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.util.Attachable;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class ReferenceVertexPropertyTest extends AbstractGremlinTest {

    @Test
    @FeatureRequirementSet(FeatureRequirementSet.Package.SIMPLE)
    public void shouldNotConstructNewWithSomethingAlreadyReferenced() {
        final Vertex v = graph.addVertex();
        final VertexProperty vp = v.property(VertexProperty.Cardinality.single, "test", "this");
        final ReferenceVertexProperty dvp = ReferenceFactory.detach(vp);
        assertEquals("test", dvp.label());
        assertEquals("test", dvp.key());
        assertEquals("this", dvp.value());
        assertSame(dvp, ReferenceFactory.detach(dvp));
    }

    @Test(expected = IllegalStateException.class)
    @FeatureRequirementSet(FeatureRequirementSet.Package.SIMPLE)
    public void shouldNotSupportRemove() {
        final Vertex v = graph.addVertex();
        final VertexProperty vp = v.property(VertexProperty.Cardinality.single, "test", "this");
        ReferenceFactory.detach(vp).remove();
    }

    @Test
    @FeatureRequirementSet(FeatureRequirementSet.Package.SIMPLE)
    public void shouldBeEqualsPropertiesAsIdIsTheSame() {
        final Vertex v = graph.addVertex();
        final VertexProperty vp = v.property(VertexProperty.Cardinality.single, "test", "this");
        final ReferenceVertexProperty vp1 = ReferenceFactory.detach(vp);
        final ReferenceVertexProperty vp2 = ReferenceFactory.detach(vp);
        assertTrue(vp1.equals(vp2));
        assertTrue(vp1.equals(vp));
        assertTrue(vp.equals(vp2));
    }

    @Test
    @FeatureRequirementSet(FeatureRequirementSet.Package.SIMPLE)
    public void shouldNotBeEqualsPropertiesAsIdIsDifferent() {
        final Vertex v = graph.addVertex();
        final VertexProperty vp1 = v.property(VertexProperty.Cardinality.single, "test", "this");
        final ReferenceVertexProperty mp1 = ReferenceFactory.detach(vp1);
        final VertexProperty vp2 = v.property(VertexProperty.Cardinality.single, "testing", "this");
        final ReferenceVertexProperty mp2 = ReferenceFactory.detach(vp2);
        assertFalse(mp1.equals(mp2));
    }

    @Test
    @FeatureRequirementSet(FeatureRequirementSet.Package.SIMPLE)
    public void shouldAttachToGraph() {
        final Vertex v = graph.addVertex();
        final VertexProperty toReference = v.property(VertexProperty.Cardinality.single, "test", "this");
        final ReferenceVertexProperty<?> rvp = ReferenceFactory.detach(toReference);
        final VertexProperty referenced = rvp.attach(Attachable.Method.get(graph));

        assertEquals(toReference, referenced);
        assertFalse(referenced instanceof ReferenceVertexProperty);
    }

    @Test
    @FeatureRequirementSet(FeatureRequirementSet.Package.SIMPLE)
    public void shouldAttachToVertex() {
        final Vertex v = graph.addVertex();
        final VertexProperty toReference = v.property(VertexProperty.Cardinality.single, "test", "this");
        final ReferenceVertexProperty<?> rvp = ReferenceFactory.detach(toReference);
        final VertexProperty referenced = rvp.attach(Attachable.Method.get(v));

        assertEquals(toReference, referenced);
        assertEquals(toReference.getClass(), referenced.getClass());
        assertFalse(referenced instanceof ReferenceVertexProperty);
    }
}
