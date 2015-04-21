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
import org.apache.tinkerpop.gremlin.process.UseEngine;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalEngine;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;
import org.junit.Test;

import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
@UseEngine(TraversalEngine.Type.STANDARD)
public class ElementIdStrategyProcessTest extends AbstractGremlinProcessTest {

    @Test
    @FeatureRequirementSet(FeatureRequirementSet.Package.VERTICES_ONLY)
    public void shouldGenerateDefaultIdOnGraphAddVWithGeneratedDefaultId() throws Exception {
        final ElementIdStrategy strategy = ElementIdStrategy.build().create();
        final GraphTraversalSource sg = create(strategy);
        final Vertex v = sg.addV("name", "stephen").next();
        assertEquals("stephen", v.value("name"));

        final Traversal t1 = graph.traversal().V(v);
        t1.asAdmin().applyStrategies();
        System.out.println(t1);

        final Traversal t2 = sg.V(v);
        t2.asAdmin().applyStrategies();
        System.out.println(t2);

        assertNotNull(UUID.fromString(sg.V(v).id().next().toString()));
    }

    @Test
    @FeatureRequirementSet(FeatureRequirementSet.Package.VERTICES_ONLY)
    public void shouldGenerateDefaultIdOnGraphAddVWithGeneratedCustomId() throws Exception {
        final ElementIdStrategy strategy = ElementIdStrategy.build().idMaker(() -> "xxx").create();
        final GraphTraversalSource sg = create(strategy);
        final Vertex v = sg.addV("name", "stephen").next();
        assertEquals("stephen", v.value("name"));
        assertEquals("xxx", sg.V(v).id().next());
        assertEquals("xxx", sg.V("xxx").id().next());
    }

    @Test
    @FeatureRequirementSet(FeatureRequirementSet.Package.VERTICES_ONLY)
    public void shouldGenerateDefaultIdOnGraphAddVWithSpecifiedId() throws Exception {
        final ElementIdStrategy strategy = ElementIdStrategy.build().create();
        final GraphTraversalSource sg = create(strategy);
        final Vertex v = sg.addV(T.id, "STEPHEN", "name", "stephen").next();
        assertEquals("stephen", v.value("name"));
        assertEquals("STEPHEN", sg.V(v).id().next());
        assertEquals("STEPHEN", sg.V("STEPHEN").id().next());
    }

    @Test
    @FeatureRequirementSet(FeatureRequirementSet.Package.VERTICES_ONLY)
    public void shouldGenerateDefaultIdOnAddVWithGeneratedDefaultId() throws Exception {
        final ElementIdStrategy strategy = ElementIdStrategy.build().create();
        final GraphTraversalSource sg = create(strategy);
        sg.addV().next();
        assertEquals(1, IteratorUtils.count(sg.V()));

        final Vertex v = sg.V().addV("name", "stephen").next();
        assertEquals("stephen", v.value("name"));
        assertNotNull(UUID.fromString(sg.V(v).id().next().toString()));
    }

    @Test
    @FeatureRequirementSet(FeatureRequirementSet.Package.VERTICES_ONLY)
    public void shouldGenerateDefaultIdOnAddVWithGeneratedCustomId() throws Exception {
        final AtomicBoolean first = new AtomicBoolean(false);
        final ElementIdStrategy strategy = ElementIdStrategy.build().idMaker(() -> {
            final String key = first.get() ? "xxx" : "yyy";
            first.set(true);
            return key;
        }).create();
        final GraphTraversalSource sg = create(strategy);
        sg.addV().next();
        assertEquals(1, IteratorUtils.count(sg.V()));

        final Vertex v = sg.V().addV("name", "stephen").next();
        assertEquals("stephen", v.value("name"));
        assertEquals("xxx", sg.V(v).id().next());
        assertEquals("xxx", sg.V("xxx").id().next());
        assertEquals("yyy", sg.V("yyy").id().next());
    }

    @Test
    @FeatureRequirementSet(FeatureRequirementSet.Package.VERTICES_ONLY)
    public void shouldGenerateDefaultIdOnAddVWithSpecifiedId() throws Exception {
        final ElementIdStrategy strategy = ElementIdStrategy.build().create();
        final GraphTraversalSource sg = create(strategy);
        sg.addV().next();
        assertEquals(1, IteratorUtils.count(sg.V()));

        final Vertex v = sg.V().addV(T.id, "STEPHEN", "name", "stephen").next();
        assertEquals("stephen", v.value("name"));
        assertEquals("STEPHEN", sg.V(v).id().next());
        assertEquals("STEPHEN", sg.V("STEPHEN").id().next());
    }

    @Test
    @FeatureRequirementSet(FeatureRequirementSet.Package.SIMPLE)
    public void shouldGenerateDefaultIdOnAddEWithSpecifiedId() throws Exception {
        final ElementIdStrategy strategy = ElementIdStrategy.build().create();
        final GraphTraversalSource sg = create(strategy);
        final Vertex v = sg.addV().next();
        final Edge e = sg.V(v).addE(Direction.OUT, "self", v, "test", "value", T.id, "some-id").next();
        assertEquals("value", e.value("test"));
        assertEquals("some-id", sg.E(e).id().next());
        assertEquals("some-id", sg.E("some-id").id().next());
    }

    @Test
    @FeatureRequirementSet(FeatureRequirementSet.Package.SIMPLE)
    public void shouldGenerateDefaultIdOnAddEWithGeneratedId() throws Exception {
        final ElementIdStrategy strategy = ElementIdStrategy.build().create();
        final GraphTraversalSource sg = create(strategy);
        final Vertex v = sg.addV().next();
        final Edge e = sg.V(v).addE(Direction.OUT, "self", v, "test", "value").next();
        assertEquals("value", e.value("test"));
        assertNotNull(UUID.fromString(sg.E(e).id().next().toString()));
    }

    private GraphTraversalSource create(final ElementIdStrategy strategy) {
        return graphProvider.traversal(graph, strategy);
    }
}
