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
package org.apache.tinkerpop.gremlin.hadoop.structure.io;

import org.apache.tinkerpop.gremlin.FeatureRequirementSet;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.util.star.StarGraph;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.util.HashSet;
import java.util.Set;

import static org.junit.Assert.assertEquals;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class VertexWritableTest {

    private static VertexWritable byteClone(final VertexWritable vertexWritable) {
        try {
            final VertexWritable clone = new VertexWritable();
            final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
            vertexWritable.write(new DataOutputStream(outputStream));
            outputStream.flush();
            clone.readFields(new DataInputStream(new ByteArrayInputStream(outputStream.toByteArray())));
            return clone;
        } catch (final Exception e) {
            throw new IllegalStateException(e.getMessage(), e);
        }
    }

    @Test
    public void shouldHashAndEqualCorrectly() {
        final StarGraph graph = StarGraph.open();
        final Vertex v = graph.addVertex(T.id, 1, T.label, Vertex.DEFAULT_LABEL);
        final Set<VertexWritable> set = new HashSet<>();
        for (int i = 0; i < 100; i++) {
            set.add(new VertexWritable(v));
        }
        assertEquals(1, set.size());
    }

    @Test
    public void shouldHaveEqualVertexWritableAndInternalVertex() throws Exception {
        final StarGraph graph = StarGraph.open();
        final Vertex v = graph.addVertex(T.id, 1, T.label, Vertex.DEFAULT_LABEL);
        final VertexWritable vw = new VertexWritable(v);
        assertEquals(vw, vw);
        assertEquals(vw, byteClone(vw));
        assertEquals(v, byteClone(vw).get());
    }

    @Test
    @FeatureRequirementSet(FeatureRequirementSet.Package.VERTICES_ONLY)
    public void shouldConstructVertexWritableWithCorrectByteRepresentation() {
        final StarGraph graph = StarGraph.open();
        final Vertex v = graph.addVertex(T.id, 1, T.label, Vertex.DEFAULT_LABEL, "test", "123");
        v.property(VertexProperty.Cardinality.list, "name", "marko", "acl", "private");
        final VertexWritable vw = byteClone(new VertexWritable(v));

        assertEquals(v.id(), vw.get().id());
        assertEquals(v.label(), vw.get().label());
        assertEquals("123", vw.get().value("test"));
        assertEquals(2, IteratorUtils.count(vw.get().properties()));
        assertEquals("marko", vw.get().value("name"));
        assertEquals("private", vw.get().property("name").value("acl"));
    }
}
