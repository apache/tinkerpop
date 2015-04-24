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

import org.apache.tinkerpop.gremlin.structure.T
import org.apache.tinkerpop.gremlin.process.traversal.Traversal
import org.apache.tinkerpop.gremlin.process.traversal.TraversalEngine
import org.apache.tinkerpop.gremlin.process.UseEngine
import static org.apache.tinkerpop.gremlin.structure.P.*;
import org.apache.tinkerpop.gremlin.structure.Vertex
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__
/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public abstract class GroovyMatchTest {

    @UseEngine(TraversalEngine.Type.STANDARD)
    public static class StandardTraversals extends MatchTest {

        @Override
        public Traversal<Vertex, Map<String, Vertex>> get_g_V_matchXa_out_bX() {
            g.V().match('a', __.as('a').out().as('b'));
        }

        @Override
        public Traversal<Vertex, Object> get_g_V_matchXa_out_bX_selectXb_idX() {
            g.V().match('a', __.as('a').out().as('b')).select('b').by(T.id)
        }

        @Override
        public Traversal<Vertex, Map<String, Vertex>> get_g_V_matchXa_knows_b__b_created_cX() {
            g.V().match('a',
                    __.as('a').out('knows').as('b'),
                    __.as('b').out('created').as('c'))
        }

        @Override
        public Traversal<Vertex, Map<String, Vertex>> get_g_V_matchXa_knows_b__a_created_cX() {
            return g.V().match('a',
                    __.as('a').out('knows').as('b'),
                    __.as('a').out('created').as('c'));
        }

        @Override
        public Traversal<Vertex, Map<String, Vertex>> get_g_V_matchXd_0knows_a__d_hasXname_vadasX__a_knows_b__b_created_cX() {
            return g.V().match('d',
                    __.as('d').in('knows').as('a'),
                    __.as('d').has('name', 'vadas'),
                    __.as('a').out('knows').as('b'),
                    __.as('b').out('created').as('c'));
        }

        @Override
        public Traversal<Vertex, Map<String, String>> get_g_V_matchXa_created_b__a_repeatXoutX_timesX2XX_selectXab_nameX() {
            g.V().match('a',
                    __.as('a').out('created').as('b'),
                    __.as('a').repeat(__.out).times(2).as('b')).select('a', 'b').by('name')
        }

        @Override
        public Traversal<Vertex, Map<String, String>> get_g_V_matchXa_created_lop_b__b_0created_29_c__c_repeatXoutX_timesX2XX_selectXnameX() {
            g.V().match('a',
                    __.as('a').out('created').has('name', 'lop').as('b'),
                    __.as('b').in('created').has('age', 29).as('c'),
                    __.as('c').repeat(__.out).times(2)).select.by('name')
        }

        @Override
        public Traversal<Vertex, String> get_g_V_out_out_matchXa_0created_b__b_0knows_cX_selectXcX_outXcreatedX_name() {
            g.V().out().out().match('a',
                    __.as('a').in('created').as('b'),
                    __.as('b').in('knows').as('c')).select('c').out('created').name
        }

        @Override
        public Traversal<Vertex, Map<String, Object>> get_g_V_matchXa_created_b__b_0created_aX() {
            g.V().match('a',
                    __.as('a').out('created').as('b'),
                    __.as('b').in('created').as('a'))
        }

        @Override
        public Traversal<Vertex, Map<String, Object>> get_g_V_matchXa_knows_b__c_knows_bX() {
            return g.V().match('a', __.as('a').out('knows').as('b'),
                    __.as('c').out('knows').as('b'));
        }

        @Override
        public Traversal<Vertex, Map<String, String>> get_g_V_matchXa_knows_b__b_created_lop__b_matchXa1_created_b1__b1_0created_c1X_selectXc1X_cX_selectXnameX() {
            g.V().match('a',
                    __.as('a').out('knows').as('b'),
                    __.as('b').out('created').has('name', 'lop'),
                    __.as('b').match('a1',
                            __.as('a1').out('created').as('b1'),
                            __.as('b1').in('created').as('c1')).select('c1').as('c')).select.by('name')
        }

        @Override
        public Traversal<Vertex, Map<String, Vertex>> get_g_V_matchXa_hasXname_GarciaX__a_0writtenBy_b__a_0sungBy_bX() {
            g.V().match('a',
                    __.as('a').has('name', 'Garcia'),
                    __.as('a').in('writtenBy').as('b'),
                    __.as('a').in('sungBy').as('b'));
        }

        @Override
        public Traversal<Vertex, Map<String, Vertex>> get_g_V_matchXa_0sungBy_b__a_0sungBy_c__b_writtenBy_d__c_writtenBy_e__d_hasXname_George_HarisonX__e_hasXname_Bob_MarleyXX() {
            g.V().match('a',
                    __.as('a').in('sungBy').as('b'),
                    __.as('a').in('sungBy').as('c'),
                    __.as('b').out('writtenBy').as('d'),
                    __.as('c').out('writtenBy').as('e'),
                    __.as('d').has('name', 'George_Harrison'),
                    __.as('e').has('name', 'Bob_Marley'));
        }

        @Override
        public Traversal<Vertex, Map<String, Vertex>> get_g_V_matchXa_0sungBy_b__a_0writtenBy_c__b_writtenBy_d__c_sungBy_d__d_hasXname_GarciaXX() {
            g.V().match('a',
                    __.as('a').in('sungBy').as('b'),
                    __.as('a').in('writtenBy').as('c'),
                    __.as('b').out('writtenBy').as('d'),
                    __.as('c').out('sungBy').as('d'),
                    __.as('d').has('name', 'Garcia'));
        }

        @Override
        public Traversal<Vertex, Map<String, Vertex>> get_g_V_matchXa_0sungBy_b__a_0writtenBy_c__b_writtenBy_dX_whereXc_sungBy_dX_whereXd_hasXname_GarciaXX() {
            return g.V().match('a',
                    __.as('a').in('sungBy').as('b'),
                    __.as('a').in('writtenBy').as('c'),
                    __.as('b').out('writtenBy').as('d'))
                    .where(__.as('c').out('sungBy').as('d'))
                    .where(__.as('d').has('name', 'Garcia'));
        }

        @Override
        public Traversal<Vertex, Map<String, String>> get_g_V_matchXa_created_lop_b__b_0created_29_cX_whereXc_repeatXoutX_timesX2XX_selectXnameX() {
            g.V().match("a",
                    __.as("a").out("created").has("name", "lop").as("b"),
                    __.as("b").in("created").has("age", 29).as("c"))
                    .where(__.as("c").repeat(__.out()).times(2))
                    .select.by('name')
        }

        @Override
        public Traversal<Vertex, Map<String, String>> get_g_V_matchXa_created_b__b_0created_cX_whereXa_neq_cX_selectXa_c_nameX() {
            g.V().match('a',
                    __.as('a').out('created').as('b'),
                    __.as('b').in('created').as('c'))
                    .where('a', neq('c'))
                    .select('a', 'c').by('name')
        }

        /*@Override
        public Traversal<Vertex, Map<String, String>> get_g_V_matchXa_created_b__c_created_bX_selectXnameX() {
            g.V.match('a',
                    __.as('a').out('created').as('b'),
                    __.as('c').out('created').as('b')).select { it.value('name') }
        }

        @Override
        public Traversal<Vertex, String> get_g_V_out_out_hasXname_rippleX_matchXb_created_a__c_knows_bX_selectXcX_outXknowsX_name() {
            g.V.out.out.match('a',
                    __.as('b').out('created').as('a'),
                    __.as('c').out('knows').as('b')).select('c').out('knows').name
        }*/
    }

}