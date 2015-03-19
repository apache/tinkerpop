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
import org.apache.tinkerpop.gremlin.process.traversal.TraversalEngine;
import org.apache.tinkerpop.gremlin.process.UseEngine;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
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

    @Test
    @FeatureRequirementSet(FeatureRequirementSet.Package.VERTICES_ONLY)
    public void shouldAppendPartitionToVertex() {
        final PartitionStrategy partitionStrategy = PartitionStrategy.build()
                .partitionKey(partition).writePartition("A").addReadPartition("A").create();
        final Vertex v = create(partitionStrategy).addV("any", "thing").next();

        assertNotNull(v);
        assertEquals("thing", v.property("any").value());
        assertEquals("A", v.property(partition).value());
    }

    @Test
    @FeatureRequirementSet(FeatureRequirementSet.Package.SIMPLE)
    public void shouldAppendPartitionToEdge() {
        final PartitionStrategy partitionStrategy = PartitionStrategy.build()
                .partitionKey(partition).writePartition("A").addReadPartition("A").create();
        final GraphTraversalSource source = create(partitionStrategy);
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
        final PartitionStrategy partitionStrategyAA = PartitionStrategy.build()
                .partitionKey(partition).writePartition("A").addReadPartition("A").create();
        final GraphTraversalSource sourceAA = create(partitionStrategyAA);

        final PartitionStrategy partitionStrategyBA = PartitionStrategy.build()
                .partitionKey(partition).writePartition("B").addReadPartition("A").create();
        final GraphTraversalSource sourceBA = create(partitionStrategyBA);

        final PartitionStrategy partitionStrategyBB = PartitionStrategy.build()
                .partitionKey(partition).writePartition("B").addReadPartition("B").create();
        final GraphTraversalSource sourceBB = create(partitionStrategyBB);

        final PartitionStrategy partitionStrategyBAB = PartitionStrategy.build()
                .partitionKey(partition).writePartition("B").addReadPartition("A").addReadPartition("B").create();
        final GraphTraversalSource sourceBAB = create(partitionStrategyBAB);


        final Vertex vA = sourceAA.addV("any", "a").next();
        final Vertex vB = sourceBA.addV("any", "b").next();

        assertNotNull(vA);
        assertEquals("a", vA.property("any").value());
        assertEquals("A", vA.property(partition).value());

        assertNotNull(vB);
        assertEquals("b", vB.property("any").value());
        assertEquals("B", vB.property(partition).value());

        sourceBA.V().forEachRemaining(v -> assertEquals("a", v.property("any").value()));
        sourceBB.V().forEachRemaining(v -> assertEquals("b", v.property("any").value()));

        assertEquals(new Long(2), sourceBAB.V().count().next());
    }

    @Test
    @FeatureRequirementSet(FeatureRequirementSet.Package.VERTICES_ONLY)
    public void shouldThrowExceptionOnVInDifferentPartition() {
        final PartitionStrategy partitionStrategyAA = PartitionStrategy.build()
                .partitionKey(partition).writePartition("A").addReadPartition("A").create();
        final GraphTraversalSource sourceAA = create(partitionStrategyAA);

        final PartitionStrategy partitionStrategyA = PartitionStrategy.build()
                .partitionKey(partition).writePartition("A").create();
        final GraphTraversalSource sourceA = create(partitionStrategyA);

        final Vertex vA = sourceAA.addV("any", "a").next();
        assertEquals(vA.id(), sourceAA.V(vA.id()).id().next());

        try {
            sourceA.V(vA.id());
        } catch (Exception ex) {
            final Exception expected = Graph.Exceptions.elementNotFound(Vertex.class, vA.id());
            assertEquals(expected.getClass(), ex.getClass());
            assertEquals(expected.getMessage(), ex.getMessage());
        }
    }

    @Test
    @FeatureRequirementSet(FeatureRequirementSet.Package.SIMPLE)
    public void shouldThrowExceptionOnEInDifferentPartition() {
        final PartitionStrategy partitionStrategyAA = PartitionStrategy.build()
                .partitionKey(partition).writePartition("A").addReadPartition("A").create();
        final GraphTraversalSource sourceAA = create(partitionStrategyAA);

        final PartitionStrategy partitionStrategyA = PartitionStrategy.build()
                .partitionKey(partition).writePartition("A").create();
        final GraphTraversalSource sourceA = create(partitionStrategyA);

        final Vertex vA = sourceAA.addV("any", "a").next();
        final Edge e = sourceAA.V(vA.id()).addOutE("knows", vA).next();
        assertEquals(e.id(), g.E(e.id()).id().next());

        try {
            sourceA.E(e.id());
        } catch (Exception ex) {
            final Exception expected = Graph.Exceptions.elementNotFound(Edge.class, e.id());
            assertEquals(expected.getClass(), ex.getClass());
            assertEquals(expected.getMessage(), ex.getMessage());
        }
    }

    @Test
    @FeatureRequirementSet(FeatureRequirementSet.Package.SIMPLE)
    public void shouldWriteToMultiplePartitions() {
        final PartitionStrategy partitionStrategyAA = PartitionStrategy.build()
                .partitionKey(partition).writePartition("A").addReadPartition("A").create();
        final GraphTraversalSource sourceAA = create(partitionStrategyAA);

        final PartitionStrategy partitionStrategyBA = PartitionStrategy.build()
                .partitionKey(partition).writePartition("B").addReadPartition("A").create();
        final GraphTraversalSource sourceBA = create(partitionStrategyBA);

        final PartitionStrategy partitionStrategyCAB = PartitionStrategy.build()
                .partitionKey(partition).writePartition("C").addReadPartition("A").addReadPartition("B").create();
        final GraphTraversalSource sourceCAB = create(partitionStrategyCAB);

        final PartitionStrategy partitionStrategyC = PartitionStrategy.build()
                .partitionKey(partition).writePartition("C").create();
        final GraphTraversalSource sourceC = create(partitionStrategyC);

        final PartitionStrategy partitionStrategyCA = PartitionStrategy.build()
                .partitionKey(partition).writePartition("C").addReadPartition("A").create();
        final GraphTraversalSource sourceCA = create(partitionStrategyCA);

        final PartitionStrategy partitionStrategyCABC = PartitionStrategy.build()
                .partitionKey(partition)
                .writePartition("C")
                .addReadPartition("A")
                .addReadPartition("B")
                .addReadPartition("C").create();
        final GraphTraversalSource sourceCABC = create(partitionStrategyCABC);

        final PartitionStrategy partitionStrategyCC = PartitionStrategy.build()
                .partitionKey(partition).writePartition("C").addReadPartition("C").create();
        final GraphTraversalSource sourceCC = create(partitionStrategyCC);

        final PartitionStrategy partitionStrategyCBC = PartitionStrategy.build()
                .partitionKey(partition).writePartition("C").addReadPartition("C").addReadPartition("B").create();
        final GraphTraversalSource sourceCBC = create(partitionStrategyCBC);

        final Vertex vA = sourceAA.addV("any", "a").next();
        final Vertex vAA = sourceAA.addV("any", "aa").next();
        final Edge eAtoAA = sourceAA.V(vA.id()).addOutE("a->a", vAA).next();

        final Vertex vB = sourceBA.addV("any", "b").next();
        sourceBA.V(vA.id()).addOutE("a->b", vB).next();

        final Vertex vC = sourceCAB.addV("any", "c").next();
        final Edge eBtovC = sourceCAB.V(vB.id()).addOutE("b->c", vC).next();
        final Edge eAtovC = sourceCAB.V(vA.id()).addOutE("a->c", vC).next();

        assertEquals(0, IteratorUtils.count(sourceC.V()));
        assertEquals(0, IteratorUtils.count(sourceC.E()));

        assertEquals(new Long(2), sourceCA.V().count().next());
        assertEquals(new Long(1), sourceCA.E().count().next());
        assertEquals(new Long(1), sourceCA.V(vA.id()).outE().count().next());
        assertEquals(eAtoAA.id(), sourceCA.V(vA.id()).outE().next().id());
        assertEquals(new Long(1), sourceCA.V(vA.id()).out().count().next());
        assertEquals(vAA.id(), sourceCA.V(vA.id()).out().next().id());

        final Vertex vA1 = sourceCA.V(vA.id()).next();
        assertEquals(new Long(1), sourceCA.V(vA1).outE().count().next());
        assertEquals(eAtoAA.id(), sourceCA.V(vA1).outE().next().id());
        assertEquals(new Long(1), sourceCA.V(vA1).out().count().next());
        assertEquals(vAA.id(), sourceCA.V(vA1).out().next().id());

        assertEquals(new Long(3), sourceCAB.V().count().next());
        assertEquals(new Long(2), sourceCAB.E().count().next());

        assertEquals(new Long(4), sourceCABC.V().count().next());
        assertEquals(new Long(4), sourceCABC.E().count().next());

        assertEquals(1, IteratorUtils.count(sourceCC.V()));
        // two edges are in the "C" partition, but one each of their incident vertices are not
        assertEquals(2, IteratorUtils.count(sourceCC.E()));

        // two edges are in the "C" partition, but one each of their incident vertices are not
        assertEquals(new Long(2), sourceCC.V(vC.id()).inE().count().next());
        assertEquals(new Long(0), sourceCC.V(vC.id()).in().count().next());

        // excluded vertices; vA is not in {B,C}
        assertEquals(new Long(2), sourceCBC.V(vC.id()).inE().count().next());
        assertEquals(new Long(1), sourceCBC.V(vC.id()).in().count().next());
        assertEquals(vC.id(), sourceCBC.E(eBtovC.id()).inV().id().next());
        assertEquals(vB.id(), sourceCBC.E(eBtovC.id()).outV().id().next());
        assertEquals(vC.id(), sourceCBC.E(eAtovC.id()).inV().id().next());
        assertFalse(sourceCBC.E(eAtovC.id()).outV().hasNext());
    }

    private GraphTraversalSource create(final PartitionStrategy strategy) {
        return graphProvider.traversal(graph, strategy);
    }
}
