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

import org.apache.tinkerpop.gremlin.FeatureRequirement;
import org.apache.tinkerpop.gremlin.FeatureRequirementSet;
import org.apache.tinkerpop.gremlin.process.AbstractGremlinProcessTest;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.util.FastNoSuchElementException;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;
import org.junit.Test;

import java.util.List;
import java.util.stream.Collectors;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.*;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class PartitionStrategyProcessTest extends AbstractGremlinProcessTest {
    private static final String partition = "gremlin.partitionGraphStrategy.partition";

    @Test
    @FeatureRequirementSet(FeatureRequirementSet.Package.VERTICES_ONLY)
    public void shouldAppendPartitionToVertex() {
        final PartitionStrategy partitionStrategy = PartitionStrategy.build()
                .partitionKey(partition).writePartition("A").addReadPartition("A").create();
        final Vertex v = create(partitionStrategy).addV().property("any", "thing").next();

        assertNotNull(v);
        assertEquals("thing", v.property("any").value());
        assertEquals("A", v.property(partition).value());
    }

    @Test
    @FeatureRequirementSet(FeatureRequirementSet.Package.VERTICES_ONLY)
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_META_PROPERTIES)
    public void shouldAppendPartitionToVertexProperty() {
        final PartitionStrategy partitionStrategy = PartitionStrategy.build()
                .includeMetaProperties(true)
                .partitionKey(partition).writePartition("A").addReadPartition("A").create();
        final Vertex v = create(partitionStrategy).addV().property("any", "thing").next();

        assertNotNull(v);
        assertEquals("thing", v.property("any").value());
        assertEquals("A", v.property(partition).value());
        assertEquals("A", v.property("any").value(partition));
    }

    @Test
    @FeatureRequirementSet(FeatureRequirementSet.Package.VERTICES_ONLY)
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_META_PROPERTIES)
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_MULTI_PROPERTIES)
    public void shouldAppendPartitionToVertexPropertyOverMultiProperty() {
        final PartitionStrategy partitionStrategy = PartitionStrategy.build()
                .includeMetaProperties(true)
                .partitionKey(partition).writePartition("A").addReadPartition("A").create();
        final Vertex v = create(partitionStrategy).addV().property(VertexProperty.Cardinality.list, "any", "thing")
                .property(VertexProperty.Cardinality.list, "any", "more").next();

        assertNotNull(v);
        assertThat((List<String>) IteratorUtils.asList(v.properties("any")).stream().map(p -> ((VertexProperty) p).value()).collect(Collectors.toList()), containsInAnyOrder("thing", "more"));
        assertEquals("A", v.property(partition).value());
        assertThat((List<String>) IteratorUtils.asList(v.properties("any")).stream().map(p -> ((VertexProperty) p).value(partition)).collect(Collectors.toList()), containsInAnyOrder("A", "A"));
    }

    @Test
    @FeatureRequirementSet(FeatureRequirementSet.Package.VERTICES_ONLY)
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_META_PROPERTIES)
    public void shouldNotAppendPartitionToVertexProperty() {
        final PartitionStrategy partitionStrategy = PartitionStrategy.build()
                .includeMetaProperties(false)
                .partitionKey(partition).writePartition("A").addReadPartition("A").create();
        final Vertex v = create(partitionStrategy).addV().property("any", "thing").next();

        assertNotNull(v);
        assertEquals("thing", v.property("any").value());
        assertEquals("A", v.property(partition).value());
        assertThat(v.property("any").properties().hasNext(), is(false));

    }

    @Test
    @FeatureRequirementSet(FeatureRequirementSet.Package.VERTICES_ONLY)
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_META_PROPERTIES)
    public void shouldAppendPartitionToAllVertexProperties() {
        final GraphTraversalSource gOverA = create(PartitionStrategy.build()
                .includeMetaProperties(true)
                .partitionKey(partition).writePartition("A").addReadPartition("A").create());

        final GraphTraversalSource gOverB = create(PartitionStrategy.build()
                .includeMetaProperties(true)
                .partitionKey(partition).writePartition("B").addReadPartition("B").create());

        final GraphTraversalSource gOverAB = create(PartitionStrategy.build()
                .includeMetaProperties(true)
                .partitionKey(partition).writePartition("B").addReadPartition("B").addReadPartition("A").create());

        final Vertex v = gOverA.addV().property("any", "thing").property("some","thing").next();

        assertNotNull(v);
        assertEquals("thing", v.property("any").value());
        assertEquals("A", v.property(partition).value());
        assertEquals("A", v.property("any").value(partition));
        assertEquals("thing", v.property("some").value());
        assertEquals("A", v.property("some").value(partition));

        gOverAB.V(v).property("that", "thing").iterate();
        assertEquals("thing", v.property("that").value());
        assertEquals("B", v.property("that").value(partition));

        assertThat(gOverAB.V(v).properties("any").hasNext(), is(true));
        assertThat(gOverAB.V(v).properties("that").hasNext(), is(true));
        assertThat(gOverA.V(v).properties("that").hasNext(), is(false));
        assertThat(gOverA.V(v).properties("any").hasNext(), is(true));
        assertThat(gOverB.V(v).properties("any").hasNext(), is(false));
        assertThat(gOverB.V(v).properties("that").hasNext(), is(false));
        assertThat(gOverAB.V(v).properties("partitionKey").hasNext(), is(false));

        assertThat(gOverAB.V(v).values("any").hasNext(), is(true));
        assertThat(gOverAB.V(v).values("that").hasNext(), is(true));
        assertThat(gOverA.V(v).values("that").hasNext(), is(false));
        assertThat(gOverA.V(v).values("any").hasNext(), is(true));
        assertThat(gOverB.V(v).values("any").hasNext(), is(false));
        assertThat(gOverB.V(v).values("that").hasNext(), is(false));
        assertThat(gOverAB.V(v).values("partitionKey").hasNext(), is(false));

        assertThat(gOverAB.V(v).propertyMap().next().containsKey("any"), is(true));
        assertThat(gOverAB.V(v).propertyMap().next().containsKey("that"), is(true));
        assertThat(gOverA.V(v).propertyMap().next().containsKey("that"), is(false));
        assertThat(gOverA.V(v).propertyMap().next().containsKey("any"), is(true));
        assertThat(gOverB.V(v).propertyMap().hasNext(), is(false));
        assertThat(gOverB.V(v).propertyMap().hasNext(), is(false));
        assertThat(gOverAB.V(v).propertyMap().next().containsKey(partition), is(false));

        assertThat(gOverAB.V(v).valueMap().next().containsKey("any"), is(true));
        assertThat(gOverAB.V(v).valueMap().next().containsKey("that"), is(true));
        assertThat(gOverA.V(v).valueMap().next().containsKey("that"), is(false));
        assertThat(gOverA.V(v).valueMap().next().containsKey("any"), is(true));
        assertThat(gOverB.V(v).valueMap().hasNext(), is(false));
        assertThat(gOverB.V(v).valueMap().hasNext(), is(false));
        assertThat(gOverAB.V(v).valueMap().next().containsKey(partition), is(false));
    }

    @Test(expected = IllegalStateException.class)
    @FeatureRequirementSet(FeatureRequirementSet.Package.VERTICES_ONLY)
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_META_PROPERTIES)
    public void shouldHidePartitionKeyForValues() {
        final GraphTraversalSource gOverA = create(PartitionStrategy.build()
                .includeMetaProperties(true)
                .partitionKey(partition).writePartition("A").addReadPartition("A").create());
        final Vertex v = gOverA.addV().property("any", "thing").next();

        gOverA.V(v).values(partition).next();
    }

    @Test
    @FeatureRequirementSet(FeatureRequirementSet.Package.VERTICES_ONLY)
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_META_PROPERTIES)
    public void shouldHidePartitionKeyForValuesWithEmptyKeys() {
        final GraphTraversalSource gOverA = create(PartitionStrategy.build()
                .includeMetaProperties(true)
                .partitionKey(partition).writePartition("A").addReadPartition("A").create());
        final Vertex v = gOverA.addV().property("any", "thing").next();

        assertEquals(1l, (long) gOverA.V(v).values().count().next());
        assertEquals("thing", gOverA.V(v).values().next());
    }

    @Test(expected = IllegalStateException.class)
    @FeatureRequirementSet(FeatureRequirementSet.Package.VERTICES_ONLY)
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_META_PROPERTIES)
    public void shouldHidePartitionKeyForProperties() {
        final GraphTraversalSource gOverA = create(PartitionStrategy.build()
                .includeMetaProperties(true)
                .partitionKey(partition).writePartition("A").addReadPartition("A").create());
        final Vertex v = gOverA.addV().property("any", "thing").next();

        gOverA.V(v).properties(partition).next();
    }

    @Test
    @FeatureRequirementSet(FeatureRequirementSet.Package.VERTICES_ONLY)
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_META_PROPERTIES)
    public void shouldHidePartitionKeyForPropertiesWithEmptyKeys() {
        final GraphTraversalSource gOverA = create(PartitionStrategy.build()
                .includeMetaProperties(true)
                .partitionKey(partition).writePartition("A").addReadPartition("A").create());
        final Vertex v = gOverA.addV().property("any", "thing").next();

        assertEquals(1l, (long) gOverA.V(v).properties().count().next());
        assertEquals("thing", gOverA.V(v).properties().value().next());
    }

    @Test(expected = IllegalStateException.class)
    @FeatureRequirementSet(FeatureRequirementSet.Package.VERTICES_ONLY)
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_META_PROPERTIES)
    public void shouldHidePartitionKeyForPropertyMap() {
        final GraphTraversalSource gOverA = create(PartitionStrategy.build()
                .includeMetaProperties(true)
                .partitionKey(partition).writePartition("A").addReadPartition("A").create());
        final Vertex v = gOverA.addV().property("any", "thing").next();

        gOverA.V(v).propertyMap(partition).next();
    }

    @Test
    @FeatureRequirementSet(FeatureRequirementSet.Package.VERTICES_ONLY)
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_META_PROPERTIES)
    public void shouldHidePartitionKeyForPropertyMapWithEmptyKeys() {
        final GraphTraversalSource gOverA = create(PartitionStrategy.build()
                .includeMetaProperties(true)
                .partitionKey(partition).writePartition("A").addReadPartition("A").create());
        final Vertex v = gOverA.addV().property("any", "thing").next();

        assertEquals(1l, (long) gOverA.V(v).propertyMap().count().next());
        assertEquals("thing", ((List<VertexProperty>) gOverA.V(v).propertyMap().next().get("any")).get(0).value());
    }

    @Test(expected = IllegalStateException.class)
    @FeatureRequirementSet(FeatureRequirementSet.Package.VERTICES_ONLY)
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_META_PROPERTIES)
    public void shouldHidePartitionKeyForValueMap() {
        final GraphTraversalSource gOverA = create(PartitionStrategy.build()
                .includeMetaProperties(true)
                .partitionKey(partition).writePartition("A").addReadPartition("A").create());
        final Vertex v = gOverA.addV().property("any", "thing").next();

        gOverA.V(v).valueMap(partition).next();
    }

    @Test
    @FeatureRequirementSet(FeatureRequirementSet.Package.VERTICES_ONLY)
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_META_PROPERTIES)
    public void shouldHidePartitionKeyForValueMapWithEmptyKeys() {
        final GraphTraversalSource gOverA = create(PartitionStrategy.build()
                .includeMetaProperties(true)
                .partitionKey(partition).writePartition("A").addReadPartition("A").create());
        final Vertex v = gOverA.addV().property("any", "thing").next();

        assertEquals(1l, (long) gOverA.V(v).valueMap().count().next());
        assertEquals("thing", ((List) gOverA.V(v).valueMap().next().get("any")).get(0));
    }

    @Test
    @FeatureRequirementSet(FeatureRequirementSet.Package.SIMPLE)
    public void shouldAppendPartitionToEdge() {
        final PartitionStrategy partitionStrategy = PartitionStrategy.build()
                .partitionKey(partition).writePartition("A").addReadPartition("A").create();
        final GraphTraversalSource source = create(partitionStrategy);
        final Vertex v1 = source.addV().property("any", "thing").next();
        final Vertex v2 = source.addV().property("some", "thing").next();
        final Edge e = source.withSideEffect("v2", v2).V(v1.id()).addE("connectsTo").from("v2").property("every", "thing").next();

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


        final Vertex vA = sourceAA.addV().property("any", "a").next();
        final Vertex vB = sourceBA.addV().property("any", "b").next();

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

        final Vertex vA = sourceAA.addV().property("any", "a").next();
        assertEquals(vA.id(), sourceAA.V(vA.id()).id().next());

        try {
            sourceA.V(vA.id()).next();
            fail("Vertex should not be in this partition");
        } catch (Exception ex) {
            final Exception expected = FastNoSuchElementException.instance();
            assertEquals(expected.getClass(), ex.getClass());
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

        final Vertex vA = sourceAA.addV().property("any", "a").next();
        final Edge e = sourceAA.withSideEffect("vA", vA).V(vA.id()).addE("knows").to("vA").next();
        assertEquals(e.id(), g.E(e.id()).id().next());

        try {
            sourceA.E(e.id()).next();
            fail("Edge should not be in this partition");
        } catch (Exception ex) {
            final Exception expected = FastNoSuchElementException.instance();
            assertEquals(expected.getClass(), ex.getClass());
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

        final Vertex vA = sourceAA.addV().property("any", "a").next();
        final Vertex vAA = sourceAA.addV().property("any", "aa").next();
        final Edge eAtoAA = sourceAA.withSideEffect("vAA", vAA).V(vA.id()).addE("a->a").to("vAA").next();

        final Vertex vB = sourceBA.addV().property("any", "b").next();
        sourceBA.withSideEffect("vB", vB).V(vA.id()).addE("a->b").to("vB").next();

        final Vertex vC = sourceCAB.addV().property("any", "c").next();
        final Edge eBtovC = sourceCAB.withSideEffect("vC", vC).V(vB.id()).addE("b->c").to("vC").next();
        final Edge eAtovC = sourceCAB.withSideEffect("vC", vC).V(vA.id()).addE("a->c").to("vC").next();

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
