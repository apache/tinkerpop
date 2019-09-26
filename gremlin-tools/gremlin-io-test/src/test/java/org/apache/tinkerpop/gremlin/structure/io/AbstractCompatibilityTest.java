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
package org.apache.tinkerpop.gremlin.structure.io;

import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.io.graphbinary.GraphBinaryCompatibility;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

import java.util.Iterator;
import java.util.List;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assume.assumeThat;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public abstract class AbstractCompatibilityTest {
    protected final Model model = Model.instance();

    public abstract Compatibility getCompatibility();

    protected void assumeCompatibility(final String resource) {
        final Model.Entry e = model.find(resource).orElseThrow(() -> new IllegalStateException("Could not find model"));
        assumeThat("Test model is not compatible with IO - see comments in Model for details", e.isCompatibleWith(getCompatibility()), is(true));
    }

    protected <T> T findModelEntryObject(final String resourceName) {
        return model.find(resourceName).orElseThrow(() -> new IllegalStateException("Could not find requested model entry")).getObject();
    }

    protected void assertVertex(final Vertex expected, final Vertex actual) {
        assertEquals(expected.id(), actual.id());
        assertEquals(expected.label(), actual.label());

        if (!(getCompatibility() instanceof GraphBinaryCompatibility)) {
            assertEquals(IteratorUtils.count(expected.properties()), IteratorUtils.count(actual.properties()));
            for (String k : expected.keys()) {
                final Iterator<VertexProperty<Object>> expectedVps = expected.properties(k);
                final List<VertexProperty<Object>> actualVps = IteratorUtils.list(actual.properties(k));
                while (expectedVps.hasNext()) {
                    final VertexProperty expectedVp = expectedVps.next();
                    final VertexProperty<Object> found = actualVps.stream()
                            .filter(vp -> vp.id().equals(expectedVp.id()))
                            .findFirst()
                            .orElseThrow(() -> new RuntimeException("Could not find VertexProperty for " + expectedVp.id()));
                    assertVertexProperty(expectedVp, found);
                }
            }
        }
    }

    protected void assertEdge(final Edge expected, final Edge actual) {
        assertEquals(expected.id(), actual.id());
        assertEquals(expected.label(), actual.label());
        assertEquals(expected.inVertex().id(), actual.inVertex().id());
        assertEquals(expected.outVertex().id(), actual.outVertex().id());
        assertEquals(expected.inVertex().label(), actual.inVertex().label());
        assertEquals(expected.outVertex().label(), actual.outVertex().label());
        if (!(getCompatibility() instanceof GraphBinaryCompatibility)) {
            assertEquals(IteratorUtils.count(expected.properties()), IteratorUtils.count(actual.properties()));
            final Iterator<Property<Object>> itty = expected.properties();
            while(itty.hasNext()) {
                final Property p = itty.next();
                assertProperty(p, actual.property(p.key()));
            }
        }
    }

    protected void assertVertexProperty(final VertexProperty expected, final VertexProperty actual) {
        assertEquals(expected.id(), actual.id());
        assertEquals(expected.label(), actual.label());

        if (!(getCompatibility() instanceof GraphBinaryCompatibility)) {
            assertEquals(IteratorUtils.count(expected.properties()), IteratorUtils.count(actual.properties()));
            final Iterator<Property> itty = expected.properties();
            while (itty.hasNext()) {
                final Property p = itty.next();
                assertProperty(p, actual.property(p.key()));
            }
        }
    }

    protected void assertProperty(final Property expected, final Property actual) {
        assertEquals(expected.key(), actual.key());
        assertEquals(expected.value(), actual.value());
    }
}
