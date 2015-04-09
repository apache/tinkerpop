/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one
 *  * or more contributor license agreements.  See the NOTICE file
 *  * distributed with this work for additional information
 *  * regarding copyright ownership.  The ASF licenses this file
 *  * to you under the Apache License, Version 2.0 (the
 *  * "License"); you may not use this file except in compliance
 *  * with the License.  You may obtain a copy of the License at
 *  *
 *  * http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing,
 *  * software distributed under the License is distributed on an
 *  * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  * KIND, either express or implied.  See the License for the
 *  * specific language governing permissions and limitations
 *  * under the License.
 *
 */

package org.apache.tinkerpop.gremlin.structure.util.star;

import org.apache.tinkerpop.gremlin.AbstractGremlinTest;
import org.apache.tinkerpop.gremlin.LoadGraphWith;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.reference.ReferenceFactory;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class StarGraphTest extends AbstractGremlinTest {

    @Test
    @LoadGraphWith(LoadGraphWith.GraphData.CREW)
    public void shouldHashAndEqualsCorrectly() {
        final Vertex gremlin = g.V(convertToVertexId("gremlin")).next();
        final StarGraph starGraph = StarGraph.of(gremlin);
        final StarGraph.StarVertex gremlinStar = starGraph.getStarVertex();
        final Set<Vertex> set = new HashSet<>();
        for (int i = 0; i < 100; i++) {
            set.add(gremlin);
            set.add(gremlinStar);
        }
        assertEquals(1, set.size());
    }


    @Test
    @LoadGraphWith(LoadGraphWith.GraphData.CREW)
    public void originalAndStarVerticesShouldHaveTheSameTopology() {
        final Vertex gremlin = g.V(convertToVertexId("gremlin")).next();
        final StarGraph starGraph = StarGraph.of(gremlin);
        final StarGraph.StarVertex gremlinStar = starGraph.getStarVertex();
        assertEquals(gremlin, gremlinStar);
        assertEquals(gremlinStar, gremlin);
        assertEquals(gremlinStar.id(), gremlin.id());
        assertEquals(gremlinStar.label(), gremlin.label());
        ///
        gremlin.properties().forEachRemaining(vp -> {
            assertEquals(vp, gremlinStar.property(vp.key()));
            assertEquals(vp.id(), gremlinStar.property(vp.key()).id());
            assertEquals(vp.value(), gremlinStar.property(vp.key()).value());

        });
        //
        List<Edge> originalEdges = new ArrayList<>(IteratorUtils.set(gremlin.edges(Direction.OUT)));
        List<Edge> starEdges = new ArrayList<>(IteratorUtils.set(gremlinStar.edges(Direction.OUT)));
        assertEquals(originalEdges.size(), starEdges.size());
        assertEquals(1,originalEdges.size());
        for (int i = 0; i < starEdges.size(); i++) {
            final Edge starEdge = starEdges.get(i);
            final Edge originalEdge = originalEdges.get(i);
            assertEquals(starEdge, originalEdge);
            assertEquals(starEdge.id(), originalEdge.id());
            assertEquals(starEdge.label(), originalEdge.label());
            assertEquals(starEdge.inVertex(), originalEdge.inVertex());
            assertEquals(starEdge.outVertex(), originalEdge.outVertex());
            originalEdge.properties().forEachRemaining(p -> assertEquals(p, starEdge.property(p.key())));
            starEdge.properties().forEachRemaining(p -> assertEquals(p, originalEdge.property(p.key())));
        }

        originalEdges = new ArrayList<>(IteratorUtils.set(gremlin.edges(Direction.IN)));
        starEdges = new ArrayList<>(IteratorUtils.set(gremlinStar.edges(Direction.IN)));
        assertEquals(7,starEdges.size());
        assertEquals(originalEdges.size(), starEdges.size());
        for (int i = 0; i < starEdges.size(); i++) {
            final Edge starEdge = starEdges.get(i);
            final Edge originalEdge = originalEdges.get(i);
            assertEquals(starEdge, originalEdge);
            assertEquals(starEdge.id(), originalEdge.id());
            assertEquals(starEdge.label(), originalEdge.label());
            assertEquals(starEdge.inVertex(), originalEdge.inVertex());
            assertEquals(starEdge.outVertex(), originalEdge.outVertex());
            originalEdge.properties().forEachRemaining(p -> assertEquals(p, starEdge.property(p.key())));
            starEdge.properties().forEachRemaining(p -> assertEquals(p, originalEdge.property(p.key())));
        }
    }

    @Test
    @LoadGraphWith(LoadGraphWith.GraphData.CREW)
    public void originalAndStarVerticesShouldHaveSameMetaProperties() {
        final Vertex matthias = g.V(convertToVertexId("matthias")).next();
        final StarGraph starGraph = StarGraph.of(matthias);
        final StarGraph.StarVertex matthiasStar = starGraph.getStarVertex();
        final AtomicInteger originalCounter = new AtomicInteger(0);
        final AtomicInteger originalPropertyCounter = new AtomicInteger(0);
        final AtomicInteger starCounter = new AtomicInteger(0);
        matthias.properties().forEachRemaining(vertexProperty -> {
            originalCounter.incrementAndGet();
            matthiasStar.properties(vertexProperty.label()).forEachRemaining(starVertexProperty -> {
                if (starVertexProperty.equals(vertexProperty)) {
                    assertEquals(starVertexProperty.id(), vertexProperty.id());
                    assertEquals(starVertexProperty.label(), vertexProperty.label());
                    assertEquals(starVertexProperty.value(), vertexProperty.value());
                    assertEquals(starVertexProperty.key(), vertexProperty.key());
                    assertEquals(starVertexProperty.element(), vertexProperty.element());
                    starCounter.incrementAndGet();
                    vertexProperty.properties().forEachRemaining(p -> {
                        originalPropertyCounter.incrementAndGet();
                        assertEquals(p.value(), starVertexProperty.property(p.key()).value());
                        assertEquals(p.key(), starVertexProperty.property(p.key()).key());
                        assertEquals(p.element(), starVertexProperty.property(p.key()).element());
                    });
                }
            });
        });
        assertEquals(5, originalCounter.get());
        assertEquals(5, starCounter.get());
        assertEquals(7, originalPropertyCounter.get());

        originalCounter.set(0);
        starCounter.set(0);
        originalPropertyCounter.set(0);

        matthiasStar.properties().forEachRemaining(starVertexProperty -> {
            starCounter.incrementAndGet();
            matthias.properties(starVertexProperty.label()).forEachRemaining(vertexProperty -> {
                if (starVertexProperty.equals(vertexProperty)) {
                    assertEquals(vertexProperty.id(), starVertexProperty.id());
                    assertEquals(vertexProperty.label(), starVertexProperty.label());
                    assertEquals(vertexProperty.value(), starVertexProperty.value());
                    assertEquals(vertexProperty.key(), starVertexProperty.key());
                    assertEquals(vertexProperty.element(), starVertexProperty.element());
                    originalCounter.incrementAndGet();
                    starVertexProperty.properties().forEachRemaining(p -> {
                        originalPropertyCounter.incrementAndGet();
                        assertEquals(p.value(), vertexProperty.property(p.key()).value());
                        assertEquals(p.key(), vertexProperty.property(p.key()).key());
                        assertEquals(p.element(), vertexProperty.property(p.key()).element());
                    });
                }
            });
        });
        assertEquals(5, originalCounter.get());
        assertEquals(5, starCounter.get());
        assertEquals(7, originalPropertyCounter.get());
    }
}
