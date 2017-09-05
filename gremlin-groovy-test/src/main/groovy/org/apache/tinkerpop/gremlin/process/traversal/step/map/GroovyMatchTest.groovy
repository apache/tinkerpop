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
package org.apache.tinkerpop.gremlin.process.traversal.step.map

import org.apache.tinkerpop.gremlin.process.traversal.Traversal
import org.apache.tinkerpop.gremlin.process.traversal.strategy.finalization.MatchAlgorithmStrategy
import org.apache.tinkerpop.gremlin.process.traversal.util.ScriptTraversal
import org.apache.tinkerpop.gremlin.structure.Vertex
import org.junit.Before

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public abstract class GroovyMatchTest {

    public static class GreedyMatchTraversals extends Traversals {
        @Before
        public void setupTest() {
            super.setupTest();
            g = graphProvider.traversal(graph, MatchAlgorithmStrategy.build().algorithm(MatchStep.GreedyMatchAlgorithm.class).create());
        }
    }

    public static class CountMatchTraversals extends Traversals {
        @Before
        public void setupTest() {
            super.setupTest();
            g = graphProvider.traversal(graph, MatchAlgorithmStrategy.build().algorithm(MatchStep.CountMatchAlgorithm.class).create());
        }
    }

    public abstract static class Traversals extends MatchTest {

        @Override
        public Traversal<Vertex, Map<String, Object>> get_g_V_valueMap_matchXa_selectXnameX_bX() {
            new ScriptTraversal<>(g, "gremlin-groovy", "g.V.valueMap.match(__.as('a').select('name').as('b'))")
        }

        @Override
        public Traversal<Vertex, Map<String, Vertex>> get_g_V_matchXa_out_bX() {
            new ScriptTraversal<>(g, "gremlin-groovy", "g.V.match(__.as('a').out.as('b'))")
        }

        @Override
        public Traversal<Vertex, Object> get_g_V_matchXa_out_bX_selectXb_idX() {
            new ScriptTraversal<>(g, "gremlin-groovy", "g.V.match( __.as('a').out.as('b')).select('b').by(id)")
        }

        @Override
        public Traversal<Vertex, Map<String, Vertex>> get_g_V_matchXa_knows_b__b_created_cX() {
            new ScriptTraversal<>(g, "gremlin-groovy", """
                g.V.match(
                    __.as('a').out('knows').as('b'),
                    __.as('b').out('created').as('c'))
            """)
        }

        @Override
        public Traversal<Vertex, Map<String, Vertex>> get_g_V_matchXa_knows_b__a_created_cX() {
            new ScriptTraversal<>(g, "gremlin-groovy", """
                g.V.match(
                    __.as('a').out('knows').as('b'),
                    __.as('a').out('created').as('c'))
            """)
        }

        @Override
        public Traversal<Vertex, Map<String, Vertex>> get_g_V_matchXd_0knows_a__d_hasXname_vadasX__a_knows_b__b_created_cX() {
            new ScriptTraversal<>(g, "gremlin-groovy", """
                g.V.match(
                    __.as('d').in('knows').as('a'),
                    __.as('d').has('name', 'vadas'),
                    __.as('a').out('knows').as('b'),
                    __.as('b').out('created').as('c'))
            """)
        }

        @Override
        public Traversal<Vertex, Map<String, Vertex>> get_g_V_matchXa_created_b__a_repeatXoutX_timesX2XX_selectXa_bX() {
            new ScriptTraversal<>(g, "gremlin-groovy", """
                g.V.match(
                    __.as('a').out('created').as('b'),
                    __.as('a').repeat(__.out).times(2).as('b')).select('a', 'b')
            """)
        }

        @Override
        public Traversal<Vertex, Map<String, String>> get_g_V_matchXa_created_lop_b__b_0created_29_c__c_whereXrepeatXoutX_timesX2XXX() {
            new ScriptTraversal<>(g, "gremlin-groovy", """
                g.V.match(
                    __.as('a').out('created').has('name', 'lop').as('b'),
                    __.as('b').in('created').has('age', 29).as('c'),
                    __.as('c').where(repeat(__.out).times(2)))
            """)
        }

        @Override
        public Traversal<Vertex, String> get_g_V_out_out_matchXa_0created_b__b_0knows_cX_selectXcX_outXcreatedX_name() {
            new ScriptTraversal<>(g, "gremlin-groovy", """
                g.V.out.out.match(
                    __.as('a').in('created').as('b'),
                    __.as('b').in('knows').as('c')).select('c').out('created').name
            """)
        }

        @Override
        public Traversal<Vertex, Map<String, Vertex>> get_g_V_matchXa_created_b__b_0created_aX() {
            new ScriptTraversal<>(g, "gremlin-groovy", """
                g.V.match(
                    __.as('a').out('created').as('b'),
                    __.as('b').in('created').as('a'))
            """)
        }

        @Override
        public Traversal<Vertex, Map<String, Vertex>> get_g_V_matchXa_knows_b__c_knows_bX() {
            new ScriptTraversal<>(g, "gremlin-groovy", """
                g.V().match(
                    __.as('a').out('knows').as('b'),
                    __.as('c').out('knows').as('b'))
            """)
        }

        @Override
        public Traversal<Vertex, Map<String, Vertex>> get_g_V_matchXa_knows_b__b_created_lop__b_matchXb_created_d__d_0created_cX_selectXcX_cX_selectXa_b_cX() {
            new ScriptTraversal<>(g, "gremlin-groovy", """
                g.V.match(
                    __.as("a").out("knows").as("b"),
                    __.as("b").out("created").has("name", "lop"),
                    __.as("b").match(
                            __.as("b").out("created").as("d"),
                            __.as("d").in("created").as("c")).select("c").as("c")).select('a','b','c')
            """)
        }

        @Override
        public Traversal<Vertex, Map<String, Vertex>> get_g_V_matchXa_hasXname_GarciaX__a_0writtenBy_b__a_0sungBy_bX() {
            new ScriptTraversal<>(g, "gremlin-groovy", """
                g.V.match(
                    __.as('a').has('name', 'Garcia'),
                    __.as('a').in('writtenBy').as('b'),
                    __.as('a').in('sungBy').as('b'));
            """)
        }

        @Override
        public Traversal<Vertex, Map<String, Vertex>> get_g_V_matchXa_0sungBy_b__a_0sungBy_c__b_writtenBy_d__c_writtenBy_e__d_hasXname_George_HarisonX__e_hasXname_Bob_MarleyXX() {
            new ScriptTraversal<>(g, "gremlin-groovy", """
                g.V.match(
                    __.as('a').in('sungBy').as('b'),
                    __.as('a').in('sungBy').as('c'),
                    __.as('b').out('writtenBy').as('d'),
                    __.as('c').out('writtenBy').as('e'),
                    __.as('d').has('name', 'George_Harrison'),
                    __.as('e').has('name', 'Bob_Marley'))
            """)
        }

        @Override
        public Traversal<Vertex, Map<String, Vertex>> get_g_V_matchXa_0sungBy_b__a_0writtenBy_c__b_writtenBy_d__c_sungBy_d__d_hasXname_GarciaXX() {
            new ScriptTraversal<>(g, "gremlin-groovy", """
                g.V.match(
                    __.as('a').in('sungBy').as('b'),
                    __.as('a').in('writtenBy').as('c'),
                    __.as('b').out('writtenBy').as('d'),
                    __.as('c').out('sungBy').as('d'),
                    __.as('d').has('name', 'Garcia'))
            """)
        }

        @Override
        public Traversal<Vertex, Map<String, Vertex>> get_g_V_matchXa_0sungBy_b__a_0writtenBy_c__b_writtenBy_dX_whereXc_sungBy_dX_whereXd_hasXname_GarciaXX() {
            new ScriptTraversal<>(g, "gremlin-groovy", """
                g.V.match(
                    __.as('a').in('sungBy').as('b'),
                    __.as('a').in('writtenBy').as('c'),
                    __.as('b').out('writtenBy').as('d'))
                    .where(__.as('c').out('sungBy').as('d'))
                    .where(__.as('d').has('name', 'Garcia'));
            """)
        }

        @Override
        public Traversal<Vertex, Map<String, String>> get_g_V_matchXa_created_lop_b__b_0created_29_cX_whereXc_repeatXoutX_timesX2XX_selectXa_b_cX() {
            new ScriptTraversal<>(g, "gremlin-groovy", """
                g.V.match(
                    __.as("a").out("created").has("name", "lop").as("b"),
                    __.as("b").in("created").has("age", 29).as("c"))
                    .where(__.as("c").repeat(__.out).times(2))
                    .select('a','b','c')
            """)
        }

        @Override
        public Traversal<Vertex, Map<String, Vertex>> get_g_V_matchXa_created_b__b_0created_cX_whereXa_neq_cX_selectXa_cX() {
            new ScriptTraversal<>(g, "gremlin-groovy", """
                g.V.match(
                    __.as('a').out('created').as('b'),
                    __.as('b').in('created').as('c'))
                    .where('a', neq('c'))
                    .select('a', 'c')
            """)
        }

        @Override
        public Traversal<Vertex, Map<String, String>> get_g_V_matchXa_created_b__c_created_bX_selectXa_b_cX_byXnameX() {
            new ScriptTraversal<>(g, "gremlin-groovy", """
                g.V.match(
                    __.as('a').out('created').as('b'),
                    __.as('c').out('created').as('b')).select('a','b','c').by('name')
            """)
        }

        @Override
        public Traversal<Vertex, String> get_g_V_out_asXcX_matchXb_knows_a__c_created_eX_selectXcX() {
            new ScriptTraversal<>(g, "gremlin-groovy", """
                g.V().out().as("c").match(
                    __.as("b").out("knows").as("a"),
                    __.as("c").out("created").as("e")).select("c")
            """)
        }

        @Override
        public Traversal<Vertex, Map<String, Object>> get_g_V_matchXa_whereXa_neqXcXX__a_created_b__orXa_knows_vadas__a_0knows_and_a_hasXlabel_personXX__b_0created_c__b_0created_count_isXgtX1XXX_selectXa_b_cX_byXidX() {
            new ScriptTraversal<>(g, "gremlin-groovy", """
                g.V.match(
                    where('a', neq('c')),
                    __.as('a').out('created').as('b'),
                    or(
                        __.as('a').out('knows').has('name', 'vadas'),
                        __.as('a').in('knows').and.as('a').has(label, 'person')
                    ),
                    __.as('b').in('created').as('c'),
                    __.as('b').in('created').count.is(gt(1)))
                    .select('a','b','c').by(id);
            """)
        }

        @Override
        public Traversal<Vertex, Map<String, Object>> get_g_V_asXaX_out_asXbX_matchXa_out_count_c__b_in_count_cX() {
            new ScriptTraversal<>(g, "gremlin-groovy", """
                g.V.as('a').out.as('b').match(__.as('a').out.count.as('c'), __.as('b').in.count.as('c'))
            """)
        }

        @Override
        public Traversal<Vertex, Map<String, Vertex>> get_g_V_matchXa_hasXname_GarciaX__a_0writtenBy_b__b_followedBy_c__c_writtenBy_d__whereXd_neqXaXXX() {
            new ScriptTraversal<>(g, "gremlin-groovy", """
                g.V.match(
                    __.as('a').has('name', 'Garcia'),
                    __.as('a').in('writtenBy').as('b'),
                    __.as('b').out('followedBy').as('c'),
                    __.as('c').out('writtenBy').as('d'),
                    where('d', neq('a')))
            """)
        }

        @Override
        public Traversal<Vertex, Map<String, Object>> get_g_V_matchXa_knows_b__andXa_created_c__b_created_c__andXb_created_count_d__a_knows_count_dXXX() {
            new ScriptTraversal<>(g, "gremlin-groovy", """
                g.V.match(
                    __.as('a').out('knows').as('b'),
                    and(
                            __.as('a').out('created').as('c'),
                            __.as('b').out('created').as('c'),
                            and(
                                    __.as('b').out('created').count.as('d'),
                                    __.as('a').out('knows').count.as('d')
                            )
                    ))
            """)
        }

        public Traversal<Vertex, Map<String, Object>> get_g_V_asXaX_out_asXbX_matchXa_out_count_c__orXa_knows_b__b_in_count_c__and__c_isXgtX2XXXX() {
            new ScriptTraversal<>(g, "gremlin-groovy", """
            g.V.as('a').out.as('b').
                    match(
                            __.as('a').out.count.as('c'),
                            or(
                                    __.as('a').out('knows').as('b'),
                                    __.as('b').in.count.as('c').and.as('c').is(gt(2))
                            )
                    )
            """)
        }

        @Override
        public Traversal<Vertex, Map<String, Object>> get_g_V_matchXa__a_out_b__notXa_created_bXX() {
            new ScriptTraversal<>(g, "gremlin-groovy", """
            g.V.match(
                    __.as('a').out.as('b'),
                    __.not(__.as('a').out('created').as('b')));
            """)
        }

        @Override
        public Traversal<Vertex, Map<String, Object>> get_g_V_matchXwhereXandXa_created_b__b_0created_count_isXeqX3XXXX__a_both_b__whereXb_inXX() {
            new ScriptTraversal<>(g, "gremlin-groovy", """
             g.V.match(
                    where(and(
                            __.as('a').out('created').as('b'),
                            __.as('b').in('created').count.is(eq(3)))),
                    __.as('a').both.as('b'),
                    where(__.as('b').in()));
            """)
        }

        @Override
        public Traversal<Vertex, Map<String, Vertex>> get_g_V_matchXa__a_both_b__b_both_cX_dedupXa_bX() {
            new ScriptTraversal<>(g, "gremlin-groovy", """
             g.V.match(
                    __.as('a').both.as('b'),
                    __.as('b').both.as('c')).dedup('a','b')
            """)
        }

        @Override
        public Traversal<Vertex, Map<String, Vertex>> get_g_V_matchXa_both_b__b_both_cX_dedupXa_bX_byXlabelX() {
            new ScriptTraversal<>(g, "gremlin-groovy", """
             g.V.match(
                    __.as('a').both.as('b'),
                    __.as('b').both.as('c')).dedup('a','b').by(label)
            """)
        }

        @Override
        public Traversal<Vertex, Long> get_g_V_hasLabelXsongsX_matchXa_name_b__a_performances_cX_selectXb_cX_count() {
            new ScriptTraversal<>(g, "gremlin-groovy", """
             g.V.hasLabel('song').match(
                    __.as('a').values('name').as('b'),
                    __.as('a').values('performances').as('c')
            ).select('b', 'c').count();
            """)
        }

        @Override
        public Traversal<Vertex, Long> get_g_V_matchXa_knows_count_bX_selectXbX() {
            new ScriptTraversal<>(g, "gremlin-groovy", "g.V.match(__.as('a').out('knows').count.as('b')).select('b')")
        }

        @Override
        public Traversal<Vertex, String> get_g_V_matchXa_knows_b__b_created_c__a_created_cX_dedupXa_b_cX_selectXaX_byXnameX() {
            new ScriptTraversal<>(g, "gremlin-groovy", "g.V.match(__.as('a').out('knows').as('b'), __.as('b').out('created').as('c'), __.as('a').out('created').as('c')).dedup('a', 'b', 'c').select('a').by('name')")
        }

        @Override
        public Traversal<Vertex, String> get_g_V_notXmatchXa_age_b__a_name_cX_whereXb_eqXcXX_selectXaXX_name() {
            new ScriptTraversal<>(g, "gremlin-groovy", """
                g.V.not(match(
                            __.as('a').age.as('b'),
                            __.as('a').name.as('c')).
                        where('b', eq('c')).select('a')).name
            """)
        }

        @Override
        public Traversal<Vertex, Long> get_g_V_matchXa_followedBy_count_isXgtX10XX_b__a_0followedBy_count_isXgtX10XX_bX_count() {
            new ScriptTraversal<>(g, "gremlin-groovy", """
             g.V.match(
                    __.as("a").out("followedBy").count.is(gt(10)).as("b"),
                    __.as("a").in("followedBy").count().is(gt(10)).as("b")).count;
            """)
        }

        @Override
        public Traversal<Vertex, String> get_g_V_matchXa_hasXsong_name_sunshineX__a_mapX0followedBy_weight_meanX_b__a_0followedBy_c__c_filterXweight_whereXgteXbXXX_outV_dX_selectXdX_byXnameX() {
            new ScriptTraversal<>(g, "gremlin-groovy", """
              g.V().match(
                  __.as("a").has("song", "name", "HERE COMES SUNSHINE"),
                  __.as("a").map(inE("followedBy").weight.mean()).as("b"),
                  __.as("a").inE("followedBy").as("c"),
                  __.as("c").filter(values("weight").where(gte("b"))).outV.as("d")).
                select("d").by("name");
                """)
        }

        @Override
        public Traversal<Vertex, Map<String, String>> get_g_V_matchXa_outEXcreatedX_order_byXweight_decrX_limitX1X_inV_b__b_hasXlang_javaXX_selectXa_bX_byXnameX() {
            new ScriptTraversal<>(g, "gremlin-groovy", """
             g.V.match(
                    __.as("a").outE("created").order.by("weight", decr).limit(1).inV.as("b"),
                    __.as("b").has("lang", "java")).
                select("a", "b").by("name")
            """)
        }
    }
}