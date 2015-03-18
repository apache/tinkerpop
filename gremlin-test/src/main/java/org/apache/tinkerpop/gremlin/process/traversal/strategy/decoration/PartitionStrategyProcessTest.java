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
package org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration;

import org.apache.tinkerpop.gremlin.FeatureRequirementSet;
import org.apache.tinkerpop.gremlin.process.AbstractGremlinProcessTest;
import org.apache.tinkerpop.gremlin.process.TraversalEngine;
import org.apache.tinkerpop.gremlin.process.UseEngine;
import org.apache.tinkerpop.gremlin.process.graph.traversal.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
@UseEngine(TraversalEngine.Type.STANDARD)
public class PartitionStrategyProcessTest extends AbstractGremlinProcessTest {
    private static final String partition = "gremlin.partitionGraphStrategy.partition";
    private final PartitionStrategy partitionStrategy = new PartitionStrategy(partition, "A");

    @Test
    @FeatureRequirementSet(FeatureRequirementSet.Package.VERTICES_ONLY)
    public void shouldAppendPartitionToVertex() {
        final Vertex v = create().addV("any", "thing").next();

        assertNotNull(v);
        assertEquals("thing", v.property("any").value());
        assertEquals("A", v.property(partition).value());
    }

    @Test
    @FeatureRequirementSet(FeatureRequirementSet.Package.SIMPLE)
    public void shouldAppendPartitionToEdge() {
        final GraphTraversalSource source = create();
        final Vertex v1 = source.addV("any", "thing").next();
        final Vertex v2 = source.addV("some", "thing").next();
        final Edge e = source.V(v1.id()).addInE("connectsTo", v2, "every", "thing").next();

        assertNotNull(v1);
        assertEquals("thing", v1.property("any").value());
        assertEquals("A", v2.property(partition).value());

        assertNotNull(v2);
        assertEquals("thing", v2.property("some").value());
        assertEquals("A", v2.property(partition).value());

        assertNotNull(e);
        assertEquals("thing", e.property("every").value());
        assertEquals("connectsTo", e.label());
        assertEquals("A", e.property(partition).value());
    }

    @Test
    @FeatureRequirementSet(FeatureRequirementSet.Package.VERTICES_ONLY)
    public void shouldWriteVerticesToMultiplePartitions() {
        final GraphTraversalSource source = create();
        final Vertex vA = source.addV("any", "a").next();
        partitionStrategy.setWritePartition("B");
        final Vertex vB = source.addV("any", "b").next();

        assertNotNull(vA);
        assertEquals("a", vA.property("any").value());
        assertEquals("A", vA.property(partition).value());

        assertNotNull(vB);
        assertEquals("b", vB.property("any").value());
        assertEquals("B", vB.property(partition).value());

        partitionStrategy.removeReadPartition("B");
        source.V().forEachRemaining(v -> assertEquals("a", v.property("any").value()));

        partitionStrategy.removeReadPartition("A");
        partitionStrategy.addReadPartition("B");

        source.V().forEachRemaining(v -> assertEquals("b", v.property("any").value()));

        partitionStrategy.addReadPartition("A");
        assertEquals(new Long(2), source.V().count().next());
    }

    @Test
    @FeatureRequirementSet(FeatureRequirementSet.Package.VERTICES_ONLY)
    public void shouldThrowExceptionOnVInDifferentPartition() {
        final GraphTraversalSource source = create();
        final Vertex vA = source.addV("any", "a").next();
        assertEquals(vA.id(), source.V(vA.id()).id().next());

        partitionStrategy.clearReadPartitions();

        try {
            g.V(vA.id());
        } catch (Exception ex) {
            final Exception expected = Graph.Exceptions.elementNotFound(Vertex.class, vA.id());
            assertEquals(expected.getClass(), ex.getClass());
            assertEquals(expected.getMessage(), ex.getMessage());
        }
    }

    @Test
    @FeatureRequirementSet(FeatureRequirementSet.Package.SIMPLE)
    public void shouldThrowExceptionOnEInDifferentPartition() {
        final GraphTraversalSource source = create();
        final Vertex vA = source.addV("any", "a").next();
        final Edge e = source.V(vA.id()).addOutE("knows", vA).next();
        assertEquals(e.id(), g.E(e.id()).id().next());

        partitionStrategy.clearReadPartitions();

        try {
            g.E(e.id());
        } catch (Exception ex) {
            final Exception expected = Graph.Exceptions.elementNotFound(Edge.class, e.id());
            assertEquals(expected.getClass(), ex.getClass());
            assertEquals(expected.getMessage(), ex.getMessage());
        }
    }

    @Test
    @FeatureRequirementSet(FeatureRequirementSet.Package.SIMPLE)
    public void shouldWriteToMultiplePartitions() {
        final GraphTraversalSource source = create();
        final Vertex vA = source.addV("any", "a").next();
        final Vertex vAA = source.addV("any", "aa").next();
        final Edge eAtoAA = source.V(vA.id()).addOutE("a->a", vAA).next();

        partitionStrategy.setWritePartition("B");
        final Vertex vB = source.addV("any", "b").next();
        source.V(vA.id()).addOutE("a->b", vB).next();

        partitionStrategy.addReadPartition("B");
        partitionStrategy.setWritePartition("C");
        final Vertex vC = source.addV("any", "c").next();
        final Edge eBtovC = source.V(vB.id()).addOutE("b->c", vC).next();
        final Edge eAtovC = source.V(vA.id()).addOutE("a->c", vC).next();

        partitionStrategy.clearReadPartitions();
        assertEquals(0, IteratorUtils.count(source.V()));
        assertEquals(0, IteratorUtils.count(source.E()));

        partitionStrategy.addReadPartition("A");
        assertEquals(new Long(2), source.V().count().next());
        assertEquals(new Long(1), source.E().count().next());
        assertEquals(new Long(1), source.V(vA.id()).outE().count().next());
        assertEquals(eAtoAA.id(), source.V(vA.id()).outE().next().id());
        assertEquals(new Long(1), source.V(vA.id()).out().count().next());
        assertEquals(vAA.id(), source.V(vA.id()).out().next().id());

        final Vertex vA1 = source.V(vA.id()).next();
        assertEquals(new Long(1), source.V(vA1).outE().count().next());
        assertEquals(eAtoAA.id(), source.V(vA1).outE().next().id());
        assertEquals(new Long(1), source.V(vA1).out().count().next());
        assertEquals(vAA.id(), source.V(vA1).out().next().id());

        partitionStrategy.addReadPartition("B");
        assertEquals(new Long(3), source.V().count().next());
        assertEquals(new Long(2), source.E().count().next());

        partitionStrategy.addReadPartition("C");
        assertEquals(new Long(4), source.V().count().next());
        assertEquals(new Long(4), source.E().count().next());

        partitionStrategy.removeReadPartition("A");
        partitionStrategy.removeReadPartition("B");

        assertEquals(1, IteratorUtils.count(source.V()));
        // two edges are in the "C" partition, but one each of their incident vertices are not
        assertEquals(2, IteratorUtils.count(source.E()));

        // two edges are in the "C" partition, but one each of their incident vertices are not
        assertEquals(new Long(2), source.V(vC.id()).inE().count().next());
        assertEquals(new Long(0), source.V(vC.id()).in().count().next());

        partitionStrategy.addReadPartition("B");

        // excluded vertices; vA is not in {B,C}
        assertEquals(new Long(2), source.V(vC.id()).inE().count().next());
        assertEquals(new Long(1), source.V(vC.id()).in().count().next());
        assertEquals(vC.id(), source.E(eBtovC.id()).inV().id().next());
        assertEquals(vB.id(), source.E(eBtovC.id()).outV().id().next());
        assertEquals(vC.id(), source.E(eAtovC.id()).inV().id().next());
        assertFalse(source.E(eAtovC.id()).outV().hasNext());
    }

    private GraphTraversalSource create() {
        resetPartitionStrategy();
        return graphProvider.traversal(graph, partitionStrategy);
    }

    private void resetPartitionStrategy() {
        partitionStrategy.clearReadPartitions();
        partitionStrategy.setWritePartition("A");
        partitionStrategy.addReadPartition("A");
    }
}
