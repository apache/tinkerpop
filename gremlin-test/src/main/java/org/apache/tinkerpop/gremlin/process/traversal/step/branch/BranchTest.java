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
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.Arrays;

import static org.apache.tinkerpop.gremlin.LoadGraphWith.GraphData.MODERN;
import static org.apache.tinkerpop.gremlin.process.traversal.P.gt;
import static org.apache.tinkerpop.gremlin.process.traversal.P.lt;
import static org.apache.tinkerpop.gremlin.process.traversal.P.neq;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.bothE;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.constant;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.has;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.hasLabel;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.identity;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.in;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.label;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.values;
import static org.apache.tinkerpop.gremlin.process.traversal.Pick.any;
import static org.apache.tinkerpop.gremlin.process.traversal.Pick.none;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
@RunWith(GremlinProcessRunner.class)
public abstract class BranchTest extends AbstractGremlinProcessTest {


    public abstract Traversal<Vertex, Object> get_g_V_branchXlabel_isXpersonX_countX_optionX1__ageX_optionX0__langX_optionX0__nameX();

    public abstract Traversal<Vertex, Object> get_g_V_branchXlabel_isXpersonX_countX_optionX1__ageX_optionX0__langX_optionX0__nameX_optionXany__labelX();

    public abstract Traversal<Vertex, Object> get_g_V_branchXageX_optionXltX30X__youngX_optionXgtX30X__oldX_optionXnone__on_the_edgeX();

    public abstract Traversal<Vertex, Object> get_g_V_branchXidentityX_optionXhasLabelXsoftwareX__inXcreatedX_name_order_foldX_optionXhasXname_vadasX__ageX_optionXneqX123X__bothE_countX();

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_branchXlabel_isXpersonX_countX_optionX1__ageX_optionX0__langX_optionX0__nameX() {
        final Traversal<Vertex, Object> traversal = get_g_V_branchXlabel_isXpersonX_countX_optionX1__ageX_optionX0__langX_optionX0__nameX();
        printTraversalForm(traversal);
        checkResults(Arrays.asList("java", "java", "lop", "ripple", 29, 27, 32, 35), traversal);
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_branchXlabel_isXpersonX_countX_optionX1__ageX_optionX0__langX_optionX0__nameX_optionXany__labelX() {
        final Traversal<Vertex, Object> traversal = get_g_V_branchXlabel_isXpersonX_countX_optionX1__ageX_optionX0__langX_optionX0__nameX_optionXany__labelX();
        printTraversalForm(traversal);
        checkResults(Arrays.asList("java", "java", "lop", "ripple", 29, 27, 32, 35, "person", "person", "person", "person", "software", "software"), traversal);
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_branchXageX_optionXltX30X__youngX_optionXgtX30X__oldX_optionXnone__on_the_edgeX() {
        final Traversal<Vertex, Object> traversal = get_g_V_branchXageX_optionXltX30X__youngX_optionXgtX30X__oldX_optionXnone__on_the_edgeX();
        printTraversalForm(traversal);
        checkResults(Arrays.asList("young", "young", "old", "old"), traversal);
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_branchXidentityX_optionXhasLabelXsoftwareX__inXcreatedX_name_order_foldX_optionXhasXname_vadasX__ageX_optionXneqX123X__bothE_countX() {
        final Traversal<Vertex, Object> traversal = get_g_V_branchXidentityX_optionXhasLabelXsoftwareX__inXcreatedX_name_order_foldX_optionXhasXname_vadasX__ageX_optionXneqX123X__bothE_countX();
        printTraversalForm(traversal);
        checkResults(Arrays.asList(Arrays.asList("josh", "josh", "marko", "peter"), 27, 12L), traversal);
    }

    public static class Traversals extends BranchTest {

        @Override
        public Traversal<Vertex, Object> get_g_V_branchXlabel_isXpersonX_countX_optionX1__ageX_optionX0__langX_optionX0__nameX() {
            return g.V().branch(label().is("person").count())
                    .option(1L, values("age"))
                    .option(0L, values("lang"))
                    .option(0L, values("name"));
        }

        @Override
        public Traversal<Vertex, Object> get_g_V_branchXlabel_isXpersonX_countX_optionX1__ageX_optionX0__langX_optionX0__nameX_optionXany__labelX() {
            return g.V().branch(label().is("person").count())
                    .option(1L, values("age"))
                    .option(0L, values("lang"))
                    .option(0L, values("name"))
                    .option(any, label());
        }

        @Override
        public Traversal<Vertex, Object> get_g_V_branchXageX_optionXltX30X__youngX_optionXgtX30X__oldX_optionXnone__on_the_edgeX() {
            return g.V().hasLabel("person")
                    .branch(values("age"))
                    .option(lt(30), constant("young"))
                    .option(gt(30), constant("old"))
                    .option(none, constant("on the edge"));
        }

        @Override
        public Traversal<Vertex, Object> get_g_V_branchXidentityX_optionXhasLabelXsoftwareX__inXcreatedX_name_order_foldX_optionXhasXname_vadasX__ageX_optionXneqX123X__bothE_countX() {
            return g.V().branch(identity())
                    .option(hasLabel("software"), in("created").values("name").order().fold())
                    .option(has("name","vadas"), values("age"))
                    .option(neq(123), bothE().count());
        }
    }
}