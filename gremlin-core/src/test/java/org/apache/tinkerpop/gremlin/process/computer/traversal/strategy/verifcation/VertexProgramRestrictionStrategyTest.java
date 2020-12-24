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
package org.apache.tinkerpop.gremlin.process.computer.traversal.strategy.verifcation;

import org.apache.tinkerpop.gremlin.process.computer.traversal.strategy.verification.VertexProgramRestrictionStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.structure.util.empty.EmptyGraph;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;

import static org.junit.Assert.fail;

/**
 * @author Marc de Lignie
 */
@RunWith(Parameterized.class)
public class VertexProgramRestrictionStrategyTest {

    @Parameterized.Parameters(name = "{0}")
    public static Iterable<Object[]> data() {
        return Arrays.asList(new Object[][]{
                // illegal
                {"withComputer().withStrategies(VertexProgramRestrictionStrategy.instance()).V()",
                        EmptyGraph.instance().traversal().withComputer().withStrategies(VertexProgramRestrictionStrategy.instance()).V(), false},
                {"withStrategies(VertexProgramRestrictionStrategy.instance()).withComputer().V()",
                        EmptyGraph.instance().traversal().withStrategies(VertexProgramRestrictionStrategy.instance()).withComputer().V(), false},
                {"withStrategies(VertexProgramRestrictionStrategy.instance()).withComputer().V().connectedComponent()",
                        EmptyGraph.instance().traversal().withStrategies(VertexProgramRestrictionStrategy.instance()).withComputer().V().connectedComponent(), false},
                {"withStrategies(VertexProgramRestrictionStrategy.instance()).withComputer().V().pageRank()",
                        EmptyGraph.instance().traversal().withStrategies(VertexProgramRestrictionStrategy.instance()).withComputer().V().pageRank(), false},
                // legal
                {"withStrategies(VertexProgramRestrictionStrategy.instance()).V()", EmptyGraph.instance().traversal().withStrategies(VertexProgramRestrictionStrategy.instance()).V(), true},
        });
    }

    @Parameterized.Parameter(value = 0)
    public String name;

    @Parameterized.Parameter(value = 1)
    public Traversal<?, ?> traversal;

    @Parameterized.Parameter(value = 2)
    public boolean legal;

    @Test
    public void shouldBeVerifiedIllegal() {
        try {
            this.traversal.asAdmin().applyStrategies();
            if (!this.legal)
                fail("The traversal should not be allowed: " + this.traversal);
        } catch (final IllegalStateException ise) {
            if (this.legal)
                fail("The traversal should be allowed: " + this.traversal);
        }
    }
}
