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
import org.apache.tinkerpop.gremlin.process.traversal.step.util.event.MutationListener;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class EventStrategyProcessTest extends AbstractGremlinProcessTest {

    @Test
    @FeatureRequirementSet(FeatureRequirementSet.Package.VERTICES_ONLY)
    public void shouldTriggerAddVertex() {
        final StubMutationListener listener1 = new StubMutationListener();
        final StubMutationListener listener2 = new StubMutationListener();
        final EventStrategy.Builder builder = EventStrategy.build()
                .addListener(listener1)
                .addListener(listener2);

        if (graph.features().graph().supportsTransactions())
            builder.eventQueue(new EventStrategy.TransactionalEventQueue(graph));

        final EventStrategy eventStrategy = builder.create();

        graph.addVertex("some", "thing");
        final GraphTraversalSource gts = create(eventStrategy);
        gts.V().addV().property("any", "thing").next();

        tryCommit(graph, g -> assertEquals(1, IteratorUtils.count(gts.V().has("any", "thing"))));
        assertEquals(1, listener1.addVertexEventRecorded());
        assertEquals(1, listener2.addVertexEventRecorded());
    }

    @Test
    @FeatureRequirementSet(FeatureRequirementSet.Package.VERTICES_ONLY)
    public void shouldTriggerAddVertexFromStart() {
        final StubMutationListener listener1 = new StubMutationListener();
        final StubMutationListener listener2 = new StubMutationListener();
        final EventStrategy.Builder builder = EventStrategy.build()
                .addListener(listener1)
                .addListener(listener2);

        if (graph.features().graph().supportsTransactions())
            builder.eventQueue(new EventStrategy.TransactionalEventQueue(graph));

        final EventStrategy eventStrategy = builder.create();

        graph.addVertex("some", "thing");
        final GraphTraversalSource gts = create(eventStrategy);
        gts.addV().property("any", "thing").next();

        tryCommit(graph, g -> assertEquals(1, IteratorUtils.count(gts.V().has("any", "thing"))));
        assertEquals(1, listener1.addVertexEventRecorded());
        assertEquals(1, listener2.addVertexEventRecorded());
    }

    @Test
    @FeatureRequirementSet(FeatureRequirementSet.Package.SIMPLE)
    public void shouldTriggerAddEdge() {
        final StubMutationListener listener1 = new StubMutationListener();
        final StubMutationListener listener2 = new StubMutationListener();
        final EventStrategy.Builder builder = EventStrategy.build()
                .addListener(listener1)
                .addListener(listener2);

        if (graph.features().graph().supportsTransactions())
            builder.eventQueue(new EventStrategy.TransactionalEventQueue(graph));

        final EventStrategy eventStrategy = builder.create();

        final Vertex v = graph.addVertex();
        v.addEdge("self", v);

        final GraphTraversalSource gts = create(eventStrategy);
        gts.V(v).as("v").addE("self").to("v").next();

        tryCommit(graph, g -> assertEquals(2, IteratorUtils.count(gts.E())));

        assertEquals(0, listener1.addVertexEventRecorded());
        assertEquals(0, listener2.addVertexEventRecorded());

        assertEquals(1, listener1.addEdgeEventRecorded());
        assertEquals(1, listener2.addEdgeEventRecorded());
    }

    @Test
    @FeatureRequirementSet(FeatureRequirementSet.Package.SIMPLE)
    public void shouldTriggerAddEdgeByPath() {
        final StubMutationListener listener1 = new StubMutationListener();
        final StubMutationListener listener2 = new StubMutationListener();
        final EventStrategy.Builder builder = EventStrategy.build()
                .addListener(listener1)
                .addListener(listener2);

        if (graph.features().graph().supportsTransactions())
            builder.eventQueue(new EventStrategy.TransactionalEventQueue(graph));

        final EventStrategy eventStrategy = builder.create();

        final Vertex v = graph.addVertex();
        v.addEdge("self", v);

        final GraphTraversalSource gts = create(eventStrategy);
        gts.V(v).as("a").addE("self").to("a").next();

        tryCommit(graph, g -> assertEquals(2, IteratorUtils.count(gts.E())));

        assertEquals(0, listener1.addVertexEventRecorded());
        assertEquals(0, listener2.addVertexEventRecorded());

        assertEquals(1, listener1.addEdgeEventRecorded());
        assertEquals(1, listener2.addEdgeEventRecorded());
    }

    @Test
    @FeatureRequirementSet(FeatureRequirementSet.Package.VERTICES_ONLY)
    public void shouldTriggerAddVertexPropertyAdded() {
        final StubMutationListener listener1 = new StubMutationListener();
        final StubMutationListener listener2 = new StubMutationListener();
        final EventStrategy.Builder builder = EventStrategy.build()
                .addListener(listener1)
                .addListener(listener2);

        if (graph.features().graph().supportsTransactions())
            builder.eventQueue(new EventStrategy.TransactionalEventQueue(graph));

        final EventStrategy eventStrategy = builder.create();

        final Vertex vSome = graph.addVertex("some", "thing");
        vSome.property(VertexProperty.Cardinality.single, "that", "thing");
        final GraphTraversalSource gts = create(eventStrategy);
        gts.V().addV().property("this", "thing").next();

        tryCommit(graph, g -> assertEquals(1, IteratorUtils.count(gts.V().has("this", "thing"))));

        assertEquals(1, listener1.addVertexEventRecorded());
        assertEquals(1, listener2.addVertexEventRecorded());
        assertEquals(0, listener2.vertexPropertyChangedEventRecorded());
        assertEquals(0, listener1.vertexPropertyChangedEventRecorded());
    }

    @Test
    @FeatureRequirementSet(FeatureRequirementSet.Package.VERTICES_ONLY)
    public void shouldTriggerAddVertexWithPropertyThenPropertyAdded() {
        final StubMutationListener listener1 = new StubMutationListener();
        final StubMutationListener listener2 = new StubMutationListener();
        final EventStrategy.Builder builder = EventStrategy.build()
                .addListener(listener1)
                .addListener(listener2);

        if (graph.features().graph().supportsTransactions())
            builder.eventQueue(new EventStrategy.TransactionalEventQueue(graph));

        final EventStrategy eventStrategy = builder.create();

        final Vertex vSome = graph.addVertex("some", "thing");
        vSome.property(VertexProperty.Cardinality.single, "that", "thing");
        final GraphTraversalSource gts = create(eventStrategy);
        gts.V().addV().property("any", "thing").property(VertexProperty.Cardinality.single, "this", "thing").next();

        tryCommit(graph, g -> assertEquals(1, IteratorUtils.count(gts.V().has("this", "thing"))));

        assertEquals(1, listener1.addVertexEventRecorded());
        assertEquals(1, listener2.addVertexEventRecorded());
        assertEquals(1, listener2.vertexPropertyChangedEventRecorded());
        assertEquals(1, listener1.vertexPropertyChangedEventRecorded());
    }

    @Test
    @FeatureRequirementSet(FeatureRequirementSet.Package.VERTICES_ONLY)
    public void shouldTriggerAddVertexPropertyChanged() {
        final StubMutationListener listener1 = new StubMutationListener();
        final StubMutationListener listener2 = new StubMutationListener();
        final EventStrategy.Builder builder = EventStrategy.build()
                .addListener(listener1)
                .addListener(listener2);

        if (graph.features().graph().supportsTransactions())
            builder.eventQueue(new EventStrategy.TransactionalEventQueue(graph));

        final EventStrategy eventStrategy = builder.create();

        final Vertex vSome = graph.addVertex("some", "thing");
        vSome.property(VertexProperty.Cardinality.single, "that", "thing");
        final GraphTraversalSource gts = create(eventStrategy);
        final Vertex vAny = gts.V().addV().property("any", "thing").next();
        gts.V(vAny).property(VertexProperty.Cardinality.single, "any", "thing else").next();

        tryCommit(graph, g -> assertEquals(1, IteratorUtils.count(gts.V().has("any", "thing else"))));

        assertEquals(1, listener1.addVertexEventRecorded());
        assertEquals(1, listener2.addVertexEventRecorded());
        assertEquals(1, listener2.vertexPropertyChangedEventRecorded());
        assertEquals(1, listener1.vertexPropertyChangedEventRecorded());
    }

    @Test
    @FeatureRequirementSet(FeatureRequirementSet.Package.VERTICES_ONLY)
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_META_PROPERTIES)
    public void shouldTriggerAddVertexPropertyPropertyChanged() {
        final StubMutationListener listener1 = new StubMutationListener();
        final StubMutationListener listener2 = new StubMutationListener();
        final EventStrategy.Builder builder = EventStrategy.build()
                .addListener(listener1)
                .addListener(listener2);

        if (graph.features().graph().supportsTransactions())
            builder.eventQueue(new EventStrategy.TransactionalEventQueue(graph));

        final EventStrategy eventStrategy = builder.create();

        final Vertex vSome = graph.addVertex("some", "thing");
        vSome.property(VertexProperty.Cardinality.single, "that", "thing", "is", "good");
        final GraphTraversalSource gts = create(eventStrategy);
        final Vertex vAny = gts.V().addV().property("any", "thing").next();
        gts.V(vAny).properties("any").property("is", "bad").next();

        tryCommit(graph, g -> assertEquals(1, IteratorUtils.count(gts.V().has("any", "thing"))));

        assertEquals(1, listener1.addVertexEventRecorded());
        assertEquals(1, listener2.addVertexEventRecorded());
        assertEquals(1, listener2.vertexPropertyPropertyChangedEventRecorded());
        assertEquals(1, listener1.vertexPropertyPropertyChangedEventRecorded());
    }

    @Test
    @FeatureRequirementSet(FeatureRequirementSet.Package.SIMPLE)
    public void shouldTriggerAddEdgePropertyAdded() {
        final StubMutationListener listener1 = new StubMutationListener();
        final StubMutationListener listener2 = new StubMutationListener();
        final EventStrategy.Builder builder = EventStrategy.build()
                .addListener(listener1)
                .addListener(listener2);

        if (graph.features().graph().supportsTransactions())
            builder.eventQueue(new EventStrategy.TransactionalEventQueue(graph));

        final EventStrategy eventStrategy = builder.create();

        final Vertex v = graph.addVertex();
        v.addEdge("self", v);

        final GraphTraversalSource gts = create(eventStrategy);
        gts.V(v).as("v").addE("self").to("v").property("some", "thing").next();

        tryCommit(graph, g -> assertEquals(1, IteratorUtils.count(gts.E().has("some", "thing"))));

        assertEquals(0, listener1.addVertexEventRecorded());
        assertEquals(0, listener2.addVertexEventRecorded());

        assertEquals(1, listener1.addEdgeEventRecorded());
        assertEquals(1, listener2.addEdgeEventRecorded());

        assertEquals(0, listener2.edgePropertyChangedEventRecorded());
        assertEquals(0, listener1.edgePropertyChangedEventRecorded());
    }

    @Test
    @FeatureRequirementSet(FeatureRequirementSet.Package.SIMPLE)
    public void shouldTriggerEdgePropertyChanged() {
        final StubMutationListener listener1 = new StubMutationListener();
        final StubMutationListener listener2 = new StubMutationListener();
        final EventStrategy.Builder builder = EventStrategy.build()
                .addListener(listener1)
                .addListener(listener2);

        if (graph.features().graph().supportsTransactions())
            builder.eventQueue(new EventStrategy.TransactionalEventQueue(graph));

        final EventStrategy eventStrategy = builder.create();

        final Vertex v = graph.addVertex();
        final Edge e = v.addEdge("self", v);
        e.property("some", "thing");

        final GraphTraversalSource gts = create(eventStrategy);
        gts.E(e).property("some", "other thing").next();

        tryCommit(graph, g -> assertEquals(1, IteratorUtils.count(gts.E().has("some", "other thing"))));

        assertEquals(0, listener1.addVertexEventRecorded());
        assertEquals(0, listener2.addVertexEventRecorded());

        assertEquals(0, listener1.addEdgeEventRecorded());
        assertEquals(0, listener2.addEdgeEventRecorded());

        assertEquals(1, listener2.edgePropertyChangedEventRecorded());
        assertEquals(1, listener1.edgePropertyChangedEventRecorded());
    }

    @Test
    @FeatureRequirementSet(FeatureRequirementSet.Package.VERTICES_ONLY)
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_REMOVE_VERTICES)
    public void shouldTriggerRemoveVertex() {
        final StubMutationListener listener1 = new StubMutationListener();
        final StubMutationListener listener2 = new StubMutationListener();
        final EventStrategy.Builder builder = EventStrategy.build()
                .addListener(listener1)
                .addListener(listener2);

        if (graph.features().graph().supportsTransactions())
            builder.eventQueue(new EventStrategy.TransactionalEventQueue(graph));

        final EventStrategy eventStrategy = builder.create();

        graph.addVertex("some", "thing");
        final GraphTraversalSource gts = create(eventStrategy);
        gts.V().drop().iterate();

        tryCommit(graph, g -> assertEquals(0, IteratorUtils.count(gts.V())));

        assertEquals(1, listener1.vertexRemovedEventRecorded());
        assertEquals(1, listener2.vertexRemovedEventRecorded());
    }

    @Test
    @FeatureRequirementSet(FeatureRequirementSet.Package.VERTICES_ONLY)
    @FeatureRequirement(featureClass = Graph.Features.EdgeFeatures.class, feature = Graph.Features.EdgeFeatures.FEATURE_REMOVE_EDGES)
    public void shouldTriggerRemoveEdge() {
        final StubMutationListener listener1 = new StubMutationListener();
        final StubMutationListener listener2 = new StubMutationListener();
        final EventStrategy.Builder builder = EventStrategy.build()
                .addListener(listener1)
                .addListener(listener2);

        if (graph.features().graph().supportsTransactions())
            builder.eventQueue(new EventStrategy.TransactionalEventQueue(graph));

        final EventStrategy eventStrategy = builder.create();

        final Vertex v = graph.addVertex("some", "thing");
        v.addEdge("self", v);
        final GraphTraversalSource gts = create(eventStrategy);
        gts.E().drop().iterate();

        tryCommit(graph);

        assertEquals(1, listener1.edgeRemovedEventRecorded());
        assertEquals(1, listener2.edgeRemovedEventRecorded());
    }

    @Test
    @FeatureRequirementSet(FeatureRequirementSet.Package.VERTICES_ONLY)
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_REMOVE_PROPERTY)
    public void shouldTriggerRemoveVertexProperty() {
        final StubMutationListener listener1 = new StubMutationListener();
        final StubMutationListener listener2 = new StubMutationListener();
        final EventStrategy.Builder builder = EventStrategy.build()
                .addListener(listener1)
                .addListener(listener2);

        if (graph.features().graph().supportsTransactions())
            builder.eventQueue(new EventStrategy.TransactionalEventQueue(graph));

        final EventStrategy eventStrategy = builder.create();

        graph.addVertex("some", "thing");
        final GraphTraversalSource gts = create(eventStrategy);
        gts.V().properties().drop().iterate();

        tryCommit(graph, g -> assertEquals(0, IteratorUtils.count(gts.V().properties())));

        assertEquals(1, listener1.vertexPropertyRemovedEventRecorded());
        assertEquals(1, listener2.vertexPropertyRemovedEventRecorded());
    }

    @Test
    @FeatureRequirementSet(FeatureRequirementSet.Package.SIMPLE)
    @FeatureRequirement(featureClass = Graph.Features.EdgeFeatures.class, feature = Graph.Features.EdgeFeatures.FEATURE_REMOVE_PROPERTY)
    public void shouldTriggerRemoveEdgeProperty() {
        final StubMutationListener listener1 = new StubMutationListener();
        final StubMutationListener listener2 = new StubMutationListener();
        final EventStrategy.Builder builder = EventStrategy.build()
                .addListener(listener1)
                .addListener(listener2);

        if (graph.features().graph().supportsTransactions())
            builder.eventQueue(new EventStrategy.TransactionalEventQueue(graph));

        final EventStrategy eventStrategy = builder.create();

        final Vertex v = graph.addVertex();
        v.addEdge("self", v, "some", "thing");
        final GraphTraversalSource gts = create(eventStrategy);
        gts.E().properties().drop().iterate();

        tryCommit(graph, g -> assertEquals(0, IteratorUtils.count(gts.E().properties())));

        assertEquals(1, listener1.edgePropertyRemovedEventRecorded());
        assertEquals(1, listener2.edgePropertyRemovedEventRecorded());
    }

    @Test
    @FeatureRequirementSet(FeatureRequirementSet.Package.VERTICES_ONLY)
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_META_PROPERTIES)
    public void shouldTriggerAddVertexPropertyPropertyRemoved() {
        final StubMutationListener listener1 = new StubMutationListener();
        final StubMutationListener listener2 = new StubMutationListener();
        final EventStrategy.Builder builder = EventStrategy.build()
                .addListener(listener1)
                .addListener(listener2);

        if (graph.features().graph().supportsTransactions())
            builder.eventQueue(new EventStrategy.TransactionalEventQueue(graph));

        final EventStrategy eventStrategy = builder.create();

        final Vertex vSome = graph.addVertex("some", "thing");
        vSome.property(VertexProperty.Cardinality.single, "that", "thing", "is", "good");
        final GraphTraversalSource gts = create(eventStrategy);
        final Vertex vAny = gts.V().addV().property("any", "thing").next();
        gts.V(vAny).properties("any").property("is", "bad").next();
        gts.V(vAny).properties("any").properties("is").drop().iterate();

        tryCommit(graph, g -> assertEquals(1, IteratorUtils.count(gts.V().has("any", "thing"))));

        assertEquals(1, listener1.addVertexEventRecorded());
        assertEquals(1, listener2.addVertexEventRecorded());
        assertEquals(1, listener2.vertexPropertyPropertyChangedEventRecorded());
        assertEquals(1, listener1.vertexPropertyPropertyChangedEventRecorded());
        assertEquals(1, listener2.vertexPropertyPropertyRemovedEventRecorded());
        assertEquals(1, listener1.vertexPropertyPropertyRemovedEventRecorded());
    }

    @Test
    @FeatureRequirementSet(FeatureRequirementSet.Package.VERTICES_ONLY)
    @FeatureRequirement(featureClass = Graph.Features.GraphFeatures.class, feature = Graph.Features.GraphFeatures.FEATURE_TRANSACTIONS)
    public void shouldTriggerAfterCommit() {
        final StubMutationListener listener1 = new StubMutationListener();
        final StubMutationListener listener2 = new StubMutationListener();
        final EventStrategy.Builder builder = EventStrategy.build()
                .eventQueue(new EventStrategy.TransactionalEventQueue(graph))
                .addListener(listener1)
                .addListener(listener2);

        final EventStrategy eventStrategy = builder.create();

        graph.addVertex("some", "thing");
        final GraphTraversalSource gts = create(eventStrategy);
        gts.V().addV().property("any", "thing").next();
        gts.V().addV().property("any", "one").next();

        assertEquals(0, listener1.addVertexEventRecorded());
        assertEquals(0, listener2.addVertexEventRecorded());

        gts.tx().commit();
        assertEquals(2, IteratorUtils.count(gts.V().has("any")));

        assertEquals(2, listener1.addVertexEventRecorded());
        assertEquals(2, listener2.addVertexEventRecorded());
    }

    @Test
    @FeatureRequirementSet(FeatureRequirementSet.Package.VERTICES_ONLY)
    @FeatureRequirement(featureClass = Graph.Features.GraphFeatures.class, feature = Graph.Features.GraphFeatures.FEATURE_TRANSACTIONS)
    public void shouldResetAfterRollback() {
        final StubMutationListener listener1 = new StubMutationListener();
        final StubMutationListener listener2 = new StubMutationListener();
        final EventStrategy.Builder builder = EventStrategy.build()
                .eventQueue(new EventStrategy.TransactionalEventQueue(graph))
                .addListener(listener1)
                .addListener(listener2);

        final EventStrategy eventStrategy = builder.create();

        graph.addVertex("some", "thing");
        final GraphTraversalSource gts = create(eventStrategy);
        gts.V().addV().property("any", "thing").next();
        gts.V().addV().property("any", "one").next();

        assertEquals(0, listener1.addVertexEventRecorded());
        assertEquals(0, listener2.addVertexEventRecorded());

        gts.tx().rollback();
        assertThat(gts.V().has("any").hasNext(), is(false));

        assertEquals(0, listener1.addVertexEventRecorded());
        assertEquals(0, listener2.addVertexEventRecorded());

        graph.addVertex("some", "thing");
        gts.V().addV().property("any", "thing").next();
        gts.V().addV().property("any", "one").next();

        assertEquals(0, listener1.addVertexEventRecorded());
        assertEquals(0, listener2.addVertexEventRecorded());

        gts.tx().commit();
        assertEquals(2, IteratorUtils.count(gts.V().has("any")));

        assertEquals(2, listener1.addVertexEventRecorded());
        assertEquals(2, listener2.addVertexEventRecorded());
    }

    @Test
    @FeatureRequirementSet(FeatureRequirementSet.Package.VERTICES_ONLY)
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_META_PROPERTIES)
    public void shouldDetachPropertyOfVertexPropertyWhenRemoved() {
        final AtomicBoolean triggered = new AtomicBoolean(false);
        final Vertex v = graph.addVertex();
        final VertexProperty vp = v.property("xxx","blah");
        final String label = vp.label();
        final Object value = vp.value();
        final Property p = vp.property("to-drop", "dah");
        vp.property("not-dropped", "yay!");

        final MutationListener listener = new AbstractMutationListener() {
            @Override
            public void vertexPropertyPropertyRemoved(final VertexProperty element, final Property property) {
                assertEquals(label, element.label());
                assertEquals(value, element.value());
                assertEquals("dah", property.value());
                assertEquals("to-drop", property.key());
                triggered.set(true);
            }
        };
        final EventStrategy.Builder builder = EventStrategy.build().addListener(listener);

        if (graph.features().graph().supportsTransactions())
            builder.eventQueue(new EventStrategy.TransactionalEventQueue(graph));

        final EventStrategy eventStrategy = builder.create();
        final GraphTraversalSource gts = create(eventStrategy);

        gts.V(v).properties("xxx").properties("to-drop").drop().iterate();
        tryCommit(graph);

        assertEquals(1, IteratorUtils.count(g.V(v).properties()));
        assertEquals(1, IteratorUtils.count(g.V(v).properties().properties()));
        assertEquals("yay!", g.V(vp.element()).properties("xxx").values("not-dropped").next());
        assertThat(triggered.get(), is(true));
    }

    @Test
    @FeatureRequirementSet(FeatureRequirementSet.Package.VERTICES_ONLY)
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_META_PROPERTIES)
    public void shouldDetachPropertyOfVertexPropertyWhenChanged() {
        final AtomicBoolean triggered = new AtomicBoolean(false);
        final Vertex v = graph.addVertex();
        final VertexProperty vp = v.property("xxx","blah");
        final String label = vp.label();
        final Object value = vp.value();
        vp.property("to-change", "dah");

        final MutationListener listener = new AbstractMutationListener() {
            @Override
            public void vertexPropertyPropertyChanged(final VertexProperty element, final Property oldValue, final Object setValue) {
                assertEquals(label, element.label());
                assertEquals(value, element.value());
                assertEquals("dah", oldValue.value());
                assertEquals("to-change", oldValue.key());
                assertEquals("bah", setValue);
                triggered.set(true);
            }
        };
        final EventStrategy.Builder builder = EventStrategy.build().addListener(listener);

        if (graph.features().graph().supportsTransactions())
            builder.eventQueue(new EventStrategy.TransactionalEventQueue(graph));

        final EventStrategy eventStrategy = builder.create();
        final GraphTraversalSource gts = create(eventStrategy);

        gts.V(v).properties("xxx").property("to-change","bah").iterate();
        tryCommit(graph);

        assertEquals(1, IteratorUtils.count(g.V(v).properties()));
        assertEquals(1, IteratorUtils.count(g.V(v).properties().properties()));
        assertThat(triggered.get(), is(true));
    }

    @Test
    @FeatureRequirementSet(FeatureRequirementSet.Package.VERTICES_ONLY)
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_META_PROPERTIES)
    public void shouldDetachPropertyOfVertexPropertyWhenNew() {
        final AtomicBoolean triggered = new AtomicBoolean(false);
        final Vertex v = graph.addVertex();
        final VertexProperty vp = v.property("xxx","blah");
        final String label = vp.label();
        final Object value = vp.value();
        vp.property("to-change", "dah");

        final MutationListener listener = new AbstractMutationListener() {
            @Override
            public void vertexPropertyPropertyChanged(final VertexProperty element, final Property oldValue, final Object setValue) {
                assertEquals(label, element.label());
                assertEquals(value, element.value());
                assertEquals(null, oldValue.value());
                assertEquals("new", oldValue.key());
                assertEquals("yay!", setValue);
                triggered.set(true);
            }
        };
        final EventStrategy.Builder builder = EventStrategy.build().addListener(listener);

        if (graph.features().graph().supportsTransactions())
            builder.eventQueue(new EventStrategy.TransactionalEventQueue(graph));

        final EventStrategy eventStrategy = builder.create();
        final GraphTraversalSource gts = create(eventStrategy);

        gts.V(v).properties("xxx").property("new","yay!").iterate();
        tryCommit(graph);

        assertEquals(1, IteratorUtils.count(g.V(v).properties()));
        assertEquals(2, IteratorUtils.count(g.V(v).properties().properties()));
        assertThat(triggered.get(), is(true));
    }

    @Test
    @FeatureRequirementSet(FeatureRequirementSet.Package.SIMPLE)
    public void shouldDetachPropertyOfEdgeWhenRemoved() {
        final AtomicBoolean triggered = new AtomicBoolean(false);
        final Vertex v = graph.addVertex();
        final Edge e = v.addEdge("self", v, "not-dropped", "yay!");
        final String label = e.label();
        final Object inId = v.id();
        final Object outId = v.id();
        e.property("to-drop", "dah");

        final MutationListener listener = new AbstractMutationListener() {
            @Override
            public void edgePropertyRemoved(final Edge element, final Property property) {
                assertEquals(label, element.label());
                assertEquals(inId, element.inVertex().id());
                assertEquals(outId, element.outVertex().id());
                assertEquals("dah", property.value());
                assertEquals("to-drop", property.key());
                triggered.set(true);
            }
        };
        final EventStrategy.Builder builder = EventStrategy.build().addListener(listener);

        if (graph.features().graph().supportsTransactions())
            builder.eventQueue(new EventStrategy.TransactionalEventQueue(graph));

        final EventStrategy eventStrategy = builder.create();
        final GraphTraversalSource gts = create(eventStrategy);

        gts.E(e).properties("to-drop").drop().iterate();
        tryCommit(graph);

        assertEquals(1, IteratorUtils.count(g.E(e).properties()));
        assertEquals("yay!", e.value("not-dropped"));
        assertThat(triggered.get(), is(true));
    }

    @Test
    @FeatureRequirementSet(FeatureRequirementSet.Package.SIMPLE)
    public void shouldDetachPropertyOfEdgeWhenChanged() {
        final AtomicBoolean triggered = new AtomicBoolean(false);
        final Vertex v = graph.addVertex();
        final Edge e = v.addEdge("self", v);
        final String label = e.label();
        final Object inId = v.id();
        final Object outId = v.id();
        e.property("to-change", "no!");

        final MutationListener listener = new AbstractMutationListener() {
            @Override
            public void edgePropertyChanged(final Edge element, final Property oldValue, final Object setValue) {
                assertEquals(label, element.label());
                assertEquals(inId, element.inVertex().id());
                assertEquals(outId, element.outVertex().id());
                assertEquals("no!", oldValue.value());
                assertEquals("to-change", oldValue.key());
                assertEquals("yay!", setValue);
                triggered.set(true);
            }
        };
        final EventStrategy.Builder builder = EventStrategy.build().addListener(listener);

        if (graph.features().graph().supportsTransactions())
            builder.eventQueue(new EventStrategy.TransactionalEventQueue(graph));

        final EventStrategy eventStrategy = builder.create();
        final GraphTraversalSource gts = create(eventStrategy);

        gts.E(e).property("to-change","yay!").iterate();
        tryCommit(graph);

        assertEquals(1, IteratorUtils.count(g.E(e).properties()));
        assertThat(triggered.get(), is(true));
    }

    @Test
    @FeatureRequirementSet(FeatureRequirementSet.Package.SIMPLE)
    public void shouldDetachPropertyOfEdgeWhenNew() {
        final AtomicBoolean triggered = new AtomicBoolean(false);
        final Vertex v = graph.addVertex();
        final Edge e = v.addEdge("self", v);
        final String label = e.label();
        final Object inId = v.id();
        final Object outId = v.id();
        e.property("to-change", "no!");

        final MutationListener listener = new AbstractMutationListener() {
            @Override
            public void edgePropertyChanged(final Edge element, final Property oldValue, final Object setValue) {
                assertEquals(label, element.label());
                assertEquals(inId, element.inVertex().id());
                assertEquals(outId, element.outVertex().id());
                assertEquals(null, oldValue.value());
                assertEquals("new", oldValue.key());
                assertEquals("yay!", setValue);
                triggered.set(true);
            }
        };
        final EventStrategy.Builder builder = EventStrategy.build().addListener(listener);

        if (graph.features().graph().supportsTransactions())
            builder.eventQueue(new EventStrategy.TransactionalEventQueue(graph));

        final EventStrategy eventStrategy = builder.create();
        final GraphTraversalSource gts = create(eventStrategy);

        gts.E(e).property("new","yay!").iterate();
        tryCommit(graph);

        assertEquals(2, IteratorUtils.count(g.E(e).properties()));
        assertThat(triggered.get(), is(true));
    }


    @Test
    @FeatureRequirementSet(FeatureRequirementSet.Package.SIMPLE)
    public void shouldDetachEdgeWhenRemoved() {
        final AtomicBoolean triggered = new AtomicBoolean(false);
        final Vertex v = graph.addVertex();
        final Edge e = v.addEdge("self", v, "dropped", "yay!");
        final String label = e.label();
        final Object inId = v.id();
        final Object outId = v.id();

        final MutationListener listener = new AbstractMutationListener() {
            @Override
            public void edgeRemoved(final Edge element) {
                assertEquals(label, element.label());
                assertEquals(inId, element.inVertex().id());
                assertEquals(outId, element.outVertex().id());
                triggered.set(true);
            }
        };
        final EventStrategy.Builder builder = EventStrategy.build().addListener(listener);

        if (graph.features().graph().supportsTransactions())
            builder.eventQueue(new EventStrategy.TransactionalEventQueue(graph));

        final EventStrategy eventStrategy = builder.create();
        final GraphTraversalSource gts = create(eventStrategy);

        gts.E(e).drop().iterate();
        tryCommit(graph);

        assertVertexEdgeCounts(graph, 1, 0);
        assertThat(triggered.get(), is(true));
    }

    @Test
    @FeatureRequirementSet(FeatureRequirementSet.Package.SIMPLE)
    public void shouldDetachEdgeWhenAdded() {
        final AtomicBoolean triggered = new AtomicBoolean(false);
        final Vertex v = graph.addVertex();
        final Object id = v.id();

        final MutationListener listener = new AbstractMutationListener() {
            @Override
            public void edgeAdded(final Edge element) {
                assertEquals("self", element.label());
                assertEquals(id, element.inVertex().id());
                assertEquals(id, element.outVertex().id());
                assertEquals("there", element.value("here"));
                triggered.set(true);
            }
        };
        final EventStrategy.Builder builder = EventStrategy.build().addListener(listener);

        if (graph.features().graph().supportsTransactions())
            builder.eventQueue(new EventStrategy.TransactionalEventQueue(graph));

        final EventStrategy eventStrategy = builder.create();
        final GraphTraversalSource gts = create(eventStrategy);

        gts.V(v).as("a").addE("self").property("here", "there").from("a").to("a").iterate();
        tryCommit(graph);

        assertVertexEdgeCounts(graph, 1, 1);
        assertThat(triggered.get(), is(true));
    }

    @Test
    @FeatureRequirementSet(FeatureRequirementSet.Package.VERTICES_ONLY)
    public void shouldDetachVertexPropertyWhenRemoved() {
        final AtomicBoolean triggered = new AtomicBoolean(false);
        final Vertex v = graph.addVertex();
        final VertexProperty vp = v.property("to-remove","blah");
        final String label = vp.label();
        final Object value = vp.value();
        final VertexProperty vpToKeep = v.property("to-keep","dah");

        final MutationListener listener = new AbstractMutationListener() {
            @Override
            public void vertexPropertyRemoved(final VertexProperty element) {
                assertEquals(label, element.label());
                assertEquals(value, element.value());
                triggered.set(true);
            }
        };
        final EventStrategy.Builder builder = EventStrategy.build().addListener(listener);

        if (graph.features().graph().supportsTransactions())
            builder.eventQueue(new EventStrategy.TransactionalEventQueue(graph));

        final EventStrategy eventStrategy = builder.create();
        final GraphTraversalSource gts = create(eventStrategy);

        gts.V(v).properties("to-remove").drop().iterate();
        tryCommit(graph);

        assertEquals(1, IteratorUtils.count(g.V(v).properties()));
        assertEquals(vpToKeep.value(), g.V(v).values("to-keep").next());
        assertThat(triggered.get(), is(true));
    }

    @Test
    @FeatureRequirementSet(FeatureRequirementSet.Package.VERTICES_ONLY)
    public void shouldDetachVertexPropertyWhenChanged() {
        final AtomicBoolean triggered = new AtomicBoolean(false);
        final Vertex v = graph.addVertex();
        final String label = v.label();
        final Object id = v.id();
        v.property("to-change", "blah");

        final MutationListener listener = new AbstractMutationListener() {
            @Override
            public void vertexPropertyChanged(final Vertex element, final Property oldValue, final Object setValue, final Object... vertexPropertyKeyValues) {
                assertEquals(label, element.label());
                assertEquals(id, element.id());
                assertEquals("to-change", oldValue.key());
                assertEquals("blah", oldValue.value());
                assertEquals("dah", setValue);
                triggered.set(true);
            }
        };
        final EventStrategy.Builder builder = EventStrategy.build().addListener(listener);

        if (graph.features().graph().supportsTransactions())
            builder.eventQueue(new EventStrategy.TransactionalEventQueue(graph));

        final EventStrategy eventStrategy = builder.create();
        final GraphTraversalSource gts = create(eventStrategy);

        gts.V(v).property(VertexProperty.Cardinality.single, "to-change", "dah").iterate();
        tryCommit(graph);

        assertEquals(1, IteratorUtils.count(g.V(v).properties()));
        assertThat(triggered.get(), is(true));
    }

    @Test
    @FeatureRequirementSet(FeatureRequirementSet.Package.VERTICES_ONLY)
    public void shouldDetachVertexPropertyWhenNew() {
        final AtomicBoolean triggered = new AtomicBoolean(false);
        final Vertex v = graph.addVertex();
        final String label = v.label();
        final Object id = v.id();
        v.property("old","blah");

        final MutationListener listener = new AbstractMutationListener() {
            @Override
            public void vertexPropertyChanged(final Vertex element, final Property oldValue, final Object setValue, final Object... vertexPropertyKeyValues) {
                assertEquals(label, element.label());
                assertEquals(id, element.id());
                assertEquals("new", oldValue.key());
                assertEquals(null, oldValue.value());
                assertEquals("dah", setValue);
                triggered.set(true);
            }
        };
        final EventStrategy.Builder builder = EventStrategy.build().addListener(listener);

        if (graph.features().graph().supportsTransactions())
            builder.eventQueue(new EventStrategy.TransactionalEventQueue(graph));

        final EventStrategy eventStrategy = builder.create();
        final GraphTraversalSource gts = create(eventStrategy);

        gts.V(v).property(VertexProperty.Cardinality.single, "new", "dah").iterate();
        tryCommit(graph);

        assertEquals(2, IteratorUtils.count(g.V(v).properties()));
        assertThat(triggered.get(), is(true));
    }

    @Test
    @FeatureRequirementSet(FeatureRequirementSet.Package.VERTICES_ONLY)
    public void shouldDetachVertexWhenRemoved() {
        final AtomicBoolean triggered = new AtomicBoolean(false);
        final Vertex v = graph.addVertex();
        final String label = v.label();
        final Object id = v.id();

        final MutationListener listener = new AbstractMutationListener() {
            @Override
            public void vertexRemoved(final Vertex element) {
                assertEquals(id, element.id());
                assertEquals(label, element.label());
                triggered.set(true);
            }
        };
        final EventStrategy.Builder builder = EventStrategy.build().addListener(listener);

        if (graph.features().graph().supportsTransactions())
            builder.eventQueue(new EventStrategy.TransactionalEventQueue(graph));

        final EventStrategy eventStrategy = builder.create();
        final GraphTraversalSource gts = create(eventStrategy);

        gts.V(v).drop().iterate();
        tryCommit(graph);

        assertVertexEdgeCounts(graph, 0, 0);
        assertThat(triggered.get(), is(true));
    }

    @Test
    @FeatureRequirementSet(FeatureRequirementSet.Package.VERTICES_ONLY)
    public void shouldDetachVertexWhenAdded() {
        final AtomicBoolean triggered = new AtomicBoolean(false);

        final MutationListener listener = new AbstractMutationListener() {
            @Override
            public void vertexAdded(final Vertex element) {
                assertEquals("thing", element.label());
                assertEquals("there", element.value("here"));
                triggered.set(true);
            }
        };
        final EventStrategy.Builder builder = EventStrategy.build().addListener(listener);

        if (graph.features().graph().supportsTransactions())
            builder.eventQueue(new EventStrategy.TransactionalEventQueue(graph));

        final EventStrategy eventStrategy = builder.create();
        final GraphTraversalSource gts = create(eventStrategy);

        gts.addV("thing").property("here", "there").iterate();
        tryCommit(graph);

        assertVertexEdgeCounts(graph, 1, 0);
        assertThat(triggered.get(), is(true));
    }

    private GraphTraversalSource create(final EventStrategy strategy) {
        return graphProvider.traversal(graph, strategy);
    }

    static abstract class AbstractMutationListener implements MutationListener {
        @Override
        public void vertexAdded(final Vertex vertex) {

        }

        @Override
        public void vertexRemoved(final Vertex vertex) {

        }

        @Override
        public void vertexPropertyChanged(final Vertex element, final Property oldValue, final Object setValue, final Object... vertexPropertyKeyValues) {

        }

        @Override
        public void vertexPropertyRemoved(final VertexProperty vertexProperty) {

        }

        @Override
        public void edgeAdded(final Edge edge) {

        }

        @Override
        public void edgeRemoved(final Edge edge) {

        }

        @Override
        public void edgePropertyChanged(final Edge element, final Property oldValue, final Object setValue) {

        }

        @Override
        public void edgePropertyRemoved(final Edge element, final Property property) {

        }

        @Override
        public void vertexPropertyPropertyChanged(final VertexProperty element, final Property oldValue, final Object setValue) {

        }

        @Override
        public void vertexPropertyPropertyRemoved(final VertexProperty element, final Property property) {

        }
    }

    static class StubMutationListener implements MutationListener {
        private final AtomicLong addEdgeEvent = new AtomicLong(0);
        private final AtomicLong addVertexEvent = new AtomicLong(0);
        private final AtomicLong vertexRemovedEvent = new AtomicLong(0);
        private final AtomicLong edgePropertyChangedEvent = new AtomicLong(0);
        private final AtomicLong vertexPropertyChangedEvent = new AtomicLong(0);
        private final AtomicLong vertexPropertyPropertyChangedEvent = new AtomicLong(0);
        private final AtomicLong edgePropertyRemovedEvent = new AtomicLong(0);
        private final AtomicLong vertexPropertyPropertyRemovedEvent = new AtomicLong(0);
        private final AtomicLong edgeRemovedEvent = new AtomicLong(0);
        private final AtomicLong vertexPropertyRemovedEvent = new AtomicLong(0);

        private final ConcurrentLinkedQueue<String> order = new ConcurrentLinkedQueue<>();

        public void reset() {
            addEdgeEvent.set(0);
            addVertexEvent.set(0);
            vertexRemovedEvent.set(0);
            edgePropertyChangedEvent.set(0);
            vertexPropertyChangedEvent.set(0);
            vertexPropertyPropertyChangedEvent.set(0);
            vertexPropertyPropertyRemovedEvent.set(0);
            edgePropertyRemovedEvent.set(0);
            edgeRemovedEvent.set(0);
            vertexPropertyRemovedEvent.set(0);

            order.clear();
        }

        public List<String> getOrder() {
            return new ArrayList<>(this.order);
        }

        @Override
        public void vertexAdded(final Vertex vertex) {
            addVertexEvent.incrementAndGet();
            order.add("v-added-" + vertex.id());
        }

        @Override
        public void vertexRemoved(final Vertex vertex) {
            vertexRemovedEvent.incrementAndGet();
            order.add("v-removed-" + vertex.id());
        }

        @Override
        public void edgeAdded(final Edge edge) {
            addEdgeEvent.incrementAndGet();
            order.add("e-added-" + edge.id());
        }

        @Override
        public void edgePropertyRemoved(final Edge element, final Property o) {
            edgePropertyRemovedEvent.incrementAndGet();
            order.add("e-property-removed-" + element.id() + "-" + o);
        }

        @Override
        public void vertexPropertyPropertyRemoved(final VertexProperty element, final Property o) {
            vertexPropertyPropertyRemovedEvent.incrementAndGet();
            order.add("vp-property-removed-" + element.id() + "-" + o);
        }

        @Override
        public void edgeRemoved(final Edge edge) {
            edgeRemovedEvent.incrementAndGet();
            order.add("e-removed-" + edge.id());
        }

        @Override
        public void vertexPropertyRemoved(final VertexProperty vertexProperty) {
            vertexPropertyRemovedEvent.incrementAndGet();
            order.add("vp-property-removed-" + vertexProperty.id());
        }

        @Override
        public void edgePropertyChanged(final Edge element, final Property oldValue, final Object setValue) {
            edgePropertyChangedEvent.incrementAndGet();
            order.add("e-property-chanaged-" + element.id());
        }

        @Override
        public void vertexPropertyPropertyChanged(final VertexProperty element, final Property oldValue, final Object setValue) {
            vertexPropertyPropertyChangedEvent.incrementAndGet();
            order.add("vp-property-changed-" + element.id());
        }

        @Override
        public void vertexPropertyChanged(final Vertex element, final Property oldValue, final Object setValue, final Object... vertexPropertyKeyValues) {
            vertexPropertyChangedEvent.incrementAndGet();
            order.add("v-property-changed-" + element.id());
        }

        public long addEdgeEventRecorded() {
            return addEdgeEvent.get();
        }

        public long addVertexEventRecorded() {
            return addVertexEvent.get();
        }

        public long vertexRemovedEventRecorded() {
            return vertexRemovedEvent.get();
        }

        public long edgeRemovedEventRecorded() {
            return edgeRemovedEvent.get();
        }

        public long edgePropertyRemovedEventRecorded() {
            return edgePropertyRemovedEvent.get();
        }

        public long vertexPropertyRemovedEventRecorded() {
            return vertexPropertyRemovedEvent.get();
        }

        public long vertexPropertyPropertyRemovedEventRecorded() {
            return vertexPropertyPropertyRemovedEvent.get();
        }

        public long edgePropertyChangedEventRecorded() {
            return edgePropertyChangedEvent.get();
        }

        public long vertexPropertyChangedEventRecorded() {
            return vertexPropertyChangedEvent.get();
        }

        public long vertexPropertyPropertyChangedEventRecorded() {
            return vertexPropertyPropertyChangedEvent.get();
        }
    }

}
