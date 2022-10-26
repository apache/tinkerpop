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
package org.apache.tinkerpop.gremlin.process.traversal.strategy.finalization;

import org.apache.tinkerpop.gremlin.LoadGraphWith;
import org.apache.tinkerpop.gremlin.process.AbstractGremlinProcessTest;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;
import org.junit.Test;

import static org.apache.tinkerpop.gremlin.LoadGraphWith.GraphData.MODERN;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * @author Valentyn Kahamlyk
 */
public class DetachStrategyProcessTest extends AbstractGremlinProcessTest {

    @Test
    @LoadGraphWith(MODERN)
    public void shouldReturnOnlyRequestedFields() {

        final Vertex v = g.V(1).next();
        final Vertex vWithAllProps = g
                .withStrategies(new DetachStrategy(DetachStrategy.DetachMode.ALL, new String[] { "name" }))
                .V(1).next();
        final Vertex vWithName = g
                .withStrategies(new DetachStrategy(DetachStrategy.DetachMode.CUSTOM, new String[] { "name" }))
                .V(1).next();
        final Vertex vWithoutProps = g
                .withStrategies(new DetachStrategy(DetachStrategy.DetachMode.NONE, new String[] { "name" }))
                .V(1).next();

        assertNotNull(v);
        assertEquals(2, IteratorUtils.count(v.properties()));
        assertEquals(2, IteratorUtils.count(vWithAllProps.properties()));
        assertEquals(1, IteratorUtils.count(vWithName.properties()));
        assertEquals(1, IteratorUtils.count(vWithName.properties("name")));
        assertEquals(0, IteratorUtils.count(vWithoutProps.properties()));
    }
}
