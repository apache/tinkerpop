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
package org.apache.tinkerpop.gremlin.process.traversal.step.branch;

import org.apache.tinkerpop.gremlin.LoadGraphWith;
import org.apache.tinkerpop.gremlin.process.AbstractGremlinProcessTest;
import org.apache.tinkerpop.gremlin.process.GremlinProcessRunner;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.process.traversal.step.TraversalOptionParent;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.MapHelper;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.tinkerpop.gremlin.LoadGraphWith.GraphData.MODERN;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.hasLabel;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.identity;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.in;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.label;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.out;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.outE;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.valueMap;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.values;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 * @author Joshua Shinavier (http://fortytwo.net)
 */
@RunWith(GremlinProcessRunner.class)
public abstract class ChooseTest extends AbstractGremlinProcessTest {

    public abstract Traversal<Vertex, Object> get_g_V_chooseXout_countX_optionX2L__nameX_optionX3L__valueMapX();

    public abstract Traversal<Vertex, String> get_g_V_chooseXlabel_eqXpersonX__outXknowsX__inXcreatedXX_name();

    public abstract Traversal<Vertex, String> get_g_V_chooseXhasLabelXpersonX_and_outXcreatedX__outXknowsX__identityX_name();

    public abstract Traversal<Vertex, String> get_g_V_chooseXlabelX_optionXblah__outXknowsXX_optionXbleep__outXcreatedXX_optionXnone__identityX_name();

    public abstract Traversal<Vertex, String> get_g_V_chooseXoutXknowsX_count_isXgtX0XX__outXknowsXX_name();

    public abstract Traversal<Vertex, Map<String, String>> get_g_V_hasLabelXpersonX_asXp1X_chooseXoutEXknowsX__outXknowsXX_asXp2X_selectXp1_p2X_byXnameX();

    public abstract Traversal<Integer, List<Integer>> get_g_injectX1X_chooseXisX1X__constantX10Xfold__foldX();

    public abstract Traversal<Integer, List<Integer>> get_g_injectX2X_chooseXisX1X__constantX10Xfold__foldX();

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_chooseXout_countX_optionX2L__nameX_optionX3L__valueMapX() {
        final Traversal<Vertex, Object> traversal = get_g_V_chooseXout_countX_optionX2L__nameX_optionX3L__valueMapX();
        printTraversalForm(traversal);
        final Map<String, Long> counts = new HashMap<>();
        int counter = 0;
        while (traversal.hasNext()) {
            MapHelper.incr(counts, traversal.next().toString(), 1l);
            counter++;
        }
        assertFalse(traversal.hasNext());
        assertEquals(2, counter);
        assertEquals(2, counts.size());
        assertEquals(Long.valueOf(1), counts.get("{name=[marko], age=[29]}"));
        assertEquals(Long.valueOf(1), counts.get("josh"));
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_chooseXlabel_eqXpersonX__outXknowsX__inXcreatedXX_name() {
        final Traversal<Vertex, String> traversal = get_g_V_chooseXlabel_eqXpersonX__outXknowsX__inXcreatedXX_name();
        printTraversalForm(traversal);
        checkResults(Arrays.asList("josh", "vadas", "josh", "josh", "marko", "peter"), traversal);
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_chooseXhasLabelXpersonX_and_outXcreatedX__outXknowsX__identityX_name() {
        final Traversal<Vertex, String> traversal = get_g_V_chooseXhasLabelXpersonX_and_outXcreatedX__outXknowsX__identityX_name();
        printTraversalForm(traversal);
        checkResults(Arrays.asList("lop", "ripple", "josh", "vadas", "vadas"), traversal);
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_chooseXlabelX_optionXblah__outXknowsXX_optionXbleep__outXcreatedXX_optionXnone__identityX_name() {
        final Traversal<Vertex, String> traversal = get_g_V_chooseXlabelX_optionXblah__outXknowsXX_optionXbleep__outXcreatedXX_optionXnone__identityX_name();
        printTraversalForm(traversal);
        checkResults(Arrays.asList("marko", "vadas", "peter", "josh", "lop", "ripple"), traversal);
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_chooseXoutXknowsX_count_isXgtX0XX__outXknowsXX_name() {
        final Traversal<Vertex, String> traversal = get_g_V_chooseXoutXknowsX_count_isXgtX0XX__outXknowsXX_name();
        printTraversalForm(traversal);
        checkResults(Arrays.asList("vadas", "josh", "vadas", "josh", "peter", "lop", "ripple"), traversal);
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_hasLabelXpersonX_asXp1X_chooseXoutEXknowsX__outXknowsXX_asXp2X_selectXp1_p2X_byXnameX() {
        final Traversal<Vertex, Map<String, String>> traversal = get_g_V_hasLabelXpersonX_asXp1X_chooseXoutEXknowsX__outXknowsXX_asXp2X_selectXp1_p2X_byXnameX();
        printTraversalForm(traversal);
        checkResults(makeMapList(2,
                "p1", "marko", "p2", "vadas",
                "p1", "marko", "p2", "josh",
                "p1", "vadas", "p2", "vadas",
                "p1", "josh", "p2", "josh",
                "p1", "peter", "p2", "peter"
        ), traversal);
    }

    @Test
    public void g_injectX1X_chooseXisX1X__constantX10Xfold__foldX() {
        final Traversal<Integer, List<Integer>> traversal = get_g_injectX1X_chooseXisX1X__constantX10Xfold__foldX();
        printTraversalForm(traversal);
        assertEquals(Collections.singletonList(10), traversal.next());
        assertThat(traversal.hasNext(), is(false));
    }

    @Test
    public void g_injectX2X_chooseXisX1X__constantX10Xfold__foldX() {
        final Traversal<Integer, List<Integer>> traversal = get_g_injectX2X_chooseXisX1X__constantX10Xfold__foldX();
        printTraversalForm(traversal);
        assertEquals(Collections.singletonList(2), traversal.next());
        assertThat(traversal.hasNext(), is(false));
    }

    public static class Traversals extends ChooseTest {

        @Override
        public Traversal<Vertex, Object> get_g_V_chooseXout_countX_optionX2L__nameX_optionX3L__valueMapX() {
            return g.V().choose(out().count())
                    .option(2L, values("name"))
                    .option(3L, valueMap());
        }

        @Override
        public Traversal<Vertex, String> get_g_V_chooseXlabel_eqXpersonX__outXknowsX__inXcreatedXX_name() {
            return g.V().choose(v -> v.label().equals("person"), out("knows"), in("created")).values("name");
        }

        @Override
        public Traversal<Vertex, String> get_g_V_chooseXhasLabelXpersonX_and_outXcreatedX__outXknowsX__identityX_name() {
            return g.V().choose(hasLabel("person").and().out("created"), out("knows"), identity()).values("name");
        }

        @Override
        public Traversal<Vertex, String> get_g_V_chooseXlabelX_optionXblah__outXknowsXX_optionXbleep__outXcreatedXX_optionXnone__identityX_name() {
            return g.V().choose(label())
                    .option("blah", out("knows"))
                    .option("bleep", out("created"))
                    .option(TraversalOptionParent.Pick.none, identity()).values("name");
        }

        @Override
        public Traversal<Vertex, String> get_g_V_chooseXoutXknowsX_count_isXgtX0XX__outXknowsXX_name() {
            return g.V().choose(out("knows").count().is(P.gt(0)), out("knows")).values("name");
        }

        @Override
        public Traversal<Vertex, Map<String, String>> get_g_V_hasLabelXpersonX_asXp1X_chooseXoutEXknowsX__outXknowsXX_asXp2X_selectXp1_p2X_byXnameX() {
            return g.V().hasLabel("person").as("p1").choose(outE("knows"), out("knows")).as("p2").<String>select("p1", "p2").by("name");
        }

        @Override
        public Traversal<Integer, List<Integer>> get_g_injectX1X_chooseXisX1X__constantX10Xfold__foldX() {
            return g.inject(1).choose(__.is(1), __.constant(10).fold(), __.fold());
        }

        @Override
        public Traversal<Integer, List<Integer>> get_g_injectX2X_chooseXisX1X__constantX10Xfold__foldX() {
            return g.inject(2).choose(__.is(1), __.constant(10).fold(), __.fold());
        }
    }
}