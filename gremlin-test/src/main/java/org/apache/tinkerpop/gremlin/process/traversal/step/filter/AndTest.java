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
package org.apache.tinkerpop.gremlin.process.traversal.step.filter;

import org.apache.tinkerpop.gremlin.LoadGraphWith;
import org.apache.tinkerpop.gremlin.process.AbstractGremlinProcessTest;
import org.apache.tinkerpop.gremlin.process.UseEngine;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalEngine;
import org.apache.tinkerpop.gremlin.structure.P;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Test;

import java.util.Arrays;

import static org.apache.tinkerpop.gremlin.LoadGraphWith.GraphData.MODERN;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.has;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.outE;


/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public abstract class AndTest extends AbstractGremlinProcessTest {

    public abstract Traversal<Vertex, String> get_g_V_andXhasXage_gt_27X__outE_count_gt_2X_name();

    public abstract Traversal<Vertex, String> get_g_V_andXoutE__hasXlabel_personX_and_hasXage_gte_32XX_name();

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_andXhasXage_gt_27X__outE_count_gt_2X_name() {
        final Traversal<Vertex, String> traversal = get_g_V_andXhasXage_gt_27X__outE_count_gt_2X_name();
        printTraversalForm(traversal);
        checkResults(Arrays.asList("marko", "josh"), traversal);
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_andXout__hasXlabel_personX_and_hasXage_gte_32XX_name() {
        final Traversal<Vertex, String> traversal = get_g_V_andXoutE__hasXlabel_personX_and_hasXage_gte_32XX_name();
        printTraversalForm(traversal);
        checkResults(Arrays.asList("josh", "peter"), traversal);
    }


    @UseEngine(TraversalEngine.Type.STANDARD)
    @UseEngine(TraversalEngine.Type.COMPUTER)
    public static class Traversals extends AndTest {

        @Override
        public Traversal<Vertex, String> get_g_V_andXhasXage_gt_27X__outE_count_gt_2X_name() {
            return g.V().and(has("age", P.gt(27)), outE().count().is(P.gte(2l))).values("name");
        }

        @Override
        public Traversal<Vertex, String> get_g_V_andXoutE__hasXlabel_personX_and_hasXage_gte_32XX_name() {
            return g.V().and(outE(), has(T.label, "person").and().has("age", P.gte(32))).values("name");
        }
    }
}
