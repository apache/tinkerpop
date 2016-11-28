/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */

package org.apache.tinkerpop.gremlin.process.traversal.step;

import org.apache.tinkerpop.gremlin.LoadGraphWith;
import org.apache.tinkerpop.gremlin.process.AbstractGremlinProcessTest;
import org.apache.tinkerpop.gremlin.process.GremlinProcessRunner;
import org.apache.tinkerpop.gremlin.process.traversal.Order;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.BulkSet;
import org.apache.tinkerpop.gremlin.structure.Column;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.select;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
@RunWith(GremlinProcessRunner.class)
public abstract class ComplexTest extends AbstractGremlinProcessTest {

    public abstract Traversal<Vertex, String> getClassicRecommendation();

    @Test
    @LoadGraphWith(LoadGraphWith.GraphData.GRATEFUL)
    public void classicRecommendation() {
        final Traversal<Vertex, String> traversal = getClassicRecommendation();
        printTraversalForm(traversal);
        checkResults(Arrays.asList("LET IT GROW", "UNCLE JOHNS BAND", "I KNOW YOU RIDER", "SHIP OF FOOLS", "GOOD LOVING"), traversal);
        final List<Map<String, Object>> list = new ArrayList<>(traversal.asAdmin().getSideEffects().<BulkSet<Map<String, Object>>>get("m"));
        assertEquals(5, list.size());
        assertFalse(traversal.hasNext());
        assertEquals("LET IT GROW", list.get(0).get("x"));
        assertEquals(276, list.get(0).get("y"));
        assertEquals(21L, list.get(0).get("z"));
        //
        assertEquals("UNCLE JOHNS BAND", list.get(1).get("x"));
        assertEquals(332, list.get(1).get("y"));
        assertEquals(20L, list.get(1).get("z"));
        //
        assertEquals("I KNOW YOU RIDER", list.get(2).get("x"));
        assertEquals(550, list.get(2).get("y"));
        assertEquals(20L, list.get(2).get("z"));
        //
        assertEquals("SHIP OF FOOLS", list.get(3).get("x"));
        assertEquals(225, list.get(3).get("y"));
        assertEquals(18L, list.get(3).get("z"));
        //
        assertEquals("GOOD LOVING", list.get(4).get("x"));
        assertEquals(428, list.get(4).get("y"));
        assertEquals(18L, list.get(4).get("z"));
        //
        checkSideEffects(traversal.asAdmin().getSideEffects(), "m", BulkSet.class, "stash", BulkSet.class);

    }

    public static class Traversals extends ComplexTest {

        @Override
        public Traversal<Vertex, String> getClassicRecommendation() {
            return g.V().has("name", "DARK STAR").as("a").out("followedBy").aggregate("stash").
                    in("followedBy").where(P.neq("a").and(P.not(P.within("stash")))).
                    groupCount().
                    unfold().
                    project("x", "y", "z").
                    by(select(Column.keys).values("name")).
                    by(select(Column.keys).values("performances")).
                    by(select(Column.values)).
                    order().
                    by(select("z"), Order.decr).
                    by(select("y"), Order.incr).
                    limit(5).store("m").select("x");
        }

    }
}

