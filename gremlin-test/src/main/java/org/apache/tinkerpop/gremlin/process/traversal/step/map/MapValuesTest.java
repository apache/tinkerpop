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
package org.apache.tinkerpop.gremlin.process.traversal.step.map;

import org.apache.tinkerpop.gremlin.LoadGraphWith;
import org.apache.tinkerpop.gremlin.process.AbstractGremlinProcessTest;
import org.apache.tinkerpop.gremlin.process.GremlinProcessRunner;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.Arrays;

import static org.apache.tinkerpop.gremlin.LoadGraphWith.GraphData.MODERN;

/**
 * @author Daniel Kuppitz (http://gremlin.guru)
 * @deprecated As of release 3.1.0-incubating
 */
@Deprecated
@RunWith(GremlinProcessRunner.class)
public abstract class MapValuesTest extends AbstractGremlinProcessTest {

    public abstract Traversal<Vertex, Long> get_g_V_outE_valuesXweightX_groupCount_mapValues();

    public abstract Traversal<Vertex, Long> get_g_V_outE_valuesXweightX_groupCount_unfold_mapValues();

    public abstract Traversal<Vertex, Long> get_g_V_outE_valuesXweightX_groupCount_mapValues_groupCount_mapValues();

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_outE_valuesXweightX_groupCount_mapValues() {
        final Traversal<Vertex, Long> traversal = get_g_V_outE_valuesXweightX_groupCount_mapValues();
        printTraversalForm(traversal);
        checkResults(Arrays.asList(1l, 1l, 2l, 2l), traversal);
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_outE_valuesXweightX_groupCount_unfold_mapValues() {
        final Traversal<Vertex, Long> traversal = get_g_V_outE_valuesXweightX_groupCount_unfold_mapValues();
        printTraversalForm(traversal);
        checkResults(Arrays.asList(1l, 1l, 2l, 2l), traversal);
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_outE_valuesXweightX_groupCount_mapValues_groupCount_mapValues() {
        final Traversal<Vertex, Long> traversal = get_g_V_outE_valuesXweightX_groupCount_mapValues_groupCount_mapValues();
        printTraversalForm(traversal);
        checkResults(Arrays.asList(2l, 2l), traversal);
    }

    /**
     * @deprecated As of release 3.1.0-incubating
     */
    @Deprecated
    public static class Traversals extends MapValuesTest {

        @Override
        public Traversal<Vertex, Long> get_g_V_outE_valuesXweightX_groupCount_mapValues() {
            return g.V().outE().values("weight").groupCount().mapValues();
        }

        @Override
        public Traversal<Vertex, Long> get_g_V_outE_valuesXweightX_groupCount_unfold_mapValues() {
            return g.V().outE().values("weight").groupCount().unfold().mapValues();
        }

        @Override
        public Traversal<Vertex, Long> get_g_V_outE_valuesXweightX_groupCount_mapValues_groupCount_mapValues() {
            return g.V().outE().values("weight").groupCount().mapValues().groupCount().mapValues();
        }
    }
}
