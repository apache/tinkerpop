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
package org.apache.tinkerpop.gremlin.process.graph.traversal.step.filter;

import org.apache.tinkerpop.gremlin.LoadGraphWith;
import org.apache.tinkerpop.gremlin.process.AbstractGremlinProcessTest;
import org.apache.tinkerpop.gremlin.process.T;
import org.apache.tinkerpop.gremlin.process.Traversal;
import org.apache.tinkerpop.gremlin.structure.Compare;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Test;

import java.util.Arrays;

import static org.apache.tinkerpop.gremlin.LoadGraphWith.GraphData.MODERN;
import static org.apache.tinkerpop.gremlin.process.graph.traversal.__.has;
import static org.apache.tinkerpop.gremlin.process.graph.traversal.__.outE;
import static org.apache.tinkerpop.gremlin.structure.Compare.gt;
import static org.apache.tinkerpop.gremlin.structure.Compare.gte;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public abstract class OrTest extends AbstractGremlinProcessTest {

    public abstract Traversal<Vertex, String> get_g_V_orXhasXage_gt_27X__outE_count_gte_2X_name();

    public abstract Traversal<Vertex, String> get_g_V_orXoutEXknowsX__hasXlabel_softwareX_or_hasXage_gte_35XX_name();

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_orXhasXage_gt_27X__outE_count_gte_2X_name() {
        final Traversal<Vertex, String> traversal = get_g_V_orXhasXage_gt_27X__outE_count_gte_2X_name();
        printTraversalForm(traversal);
        checkResults(Arrays.asList("marko", "josh", "peter"), traversal);
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_orXoutEXknowsX__hasXlabel_softwareX_or_hasXage_gte_35XX_name() {
        final Traversal<Vertex, String> traversal = get_g_V_orXoutEXknowsX__hasXlabel_softwareX_or_hasXage_gte_35XX_name();
        printTraversalForm(traversal);
        checkResults(Arrays.asList("marko", "ripple", "lop", "peter"), traversal);
    }

    public static class StandardTest extends OrTest {

        public StandardTest() {
            requiresGraphComputer = false;
        }

        @Override
        public Traversal<Vertex, String> get_g_V_orXhasXage_gt_27X__outE_count_gte_2X_name() {
            return g.V().or(has("age", gt, 27), outE().count().is(gte, 2l)).values("name");
        }

        @Override
        public Traversal<Vertex, String> get_g_V_orXoutEXknowsX__hasXlabel_softwareX_or_hasXage_gte_35XX_name() {
            return g.V().or(outE("knows"), has(T.label, "software").or().has("age", Compare.gte, 35)).values("name");
        }
    }

    public static class ComputerTest extends OrTest {
        public ComputerTest() {
            requiresGraphComputer = true;
        }

        @Override
        public Traversal<Vertex, String> get_g_V_orXhasXage_gt_27X__outE_count_gte_2X_name() {
            return g.V().or(has("age", gt, 27), outE().count().is(gte, 2l)).<String>values("name");
        }

        @Override
        public Traversal<Vertex, String> get_g_V_orXoutEXknowsX__hasXlabel_softwareX_or_hasXage_gte_35XX_name() {
            return g.V().or(outE("knows"), has(T.label, "software").or().has("age", Compare.gte, 35)).<String>values("name");
        }
    }
}
