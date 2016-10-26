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
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.label;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.values;
import static org.apache.tinkerpop.gremlin.process.traversal.step.TraversalOptionParent.Pick.any;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
@RunWith(GremlinProcessRunner.class)
public abstract class BranchTest extends AbstractGremlinProcessTest {

    public abstract Traversal<Vertex, Object> get_g_V_branchXlabel_eq_person__a_bX_optionXa__ageX_optionXb__langX_optionXb__nameX();

    public abstract Traversal<Vertex, Object> get_g_V_branchXlabel_isXpersonX_countX_optionX1__ageX_optionX0__langX_optionX0__nameX();

    public abstract Traversal<Vertex, Object> get_g_V_branchXlabel_isXpersonX_countX_optionX1__ageX_optionX0__langX_optionX0__nameX_optionXany__labelX();

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_branchXlabel_eq_person__a_bX_optionXa__ageX_optionXb__langX_optionXb__nameX() {
        final Traversal<Vertex, Object> traversal = get_g_V_branchXlabel_eq_person__a_bX_optionXa__ageX_optionXb__langX_optionXb__nameX();
        printTraversalForm(traversal);
        checkResults(Arrays.asList("java", "java", "lop", "ripple", 29, 27, 32, 35), traversal);
    }

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

    public static class Traversals extends BranchTest {

        @Override
        public Traversal<Vertex, Object> get_g_V_branchXlabel_eq_person__a_bX_optionXa__ageX_optionXb__langX_optionXb__nameX() {
            return g.V().branch(v -> v.get().label().equals("person") ? "a" : "b")
                    .option("a", values("age"))
                    .option("b", values("lang"))
                    .option("b", values("name"));
        }

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
    }
}