/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one
 *  * or more contributor license agreements.  See the NOTICE file
 *  * distributed with this work for additional information
 *  * regarding copyright ownership.  The ASF licenses this file
 *  * to you under the Apache License, Version 2.0 (the
 *  * "License"); you may not use this file except in compliance
 *  * with the License.  You may obtain a copy of the License at
 *  *
 *  * http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing,
 *  * software distributed under the License is distributed on an
 *  * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  * KIND, either express or implied.  See the License for the
 *  * specific language governing permissions and limitations
 *  * under the License.
 *
 */

package org.apache.tinkerpop.gremlin.process.traversal.step.filter;

import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategies;
import org.apache.tinkerpop.gremlin.process.traversal.step.StepTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.SelectOneStep;
import org.apache.tinkerpop.gremlin.process.traversal.util.AndP;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalP;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.as;
import static org.junit.Assert.assertEquals;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 * @author Daniel Kuppitz (http://gremlin.guru)
 */
public class WhereStepTest extends StepTest {

    @Test
    public void shouldHaveProperPredicate() {
        Traversal<?, ?> traversal = as("a").out().as("b").where(as("a").out());
        WhereStep<?> whereStep = (WhereStep) traversal.asAdmin().getEndStep();
        assertEquals(TraversalP.class, whereStep.predicate.getClass());

        traversal = as("a", "b").out().as("b", "c").where(as("a").out().as("b").and().as("c").out().as("d"));
        traversal.asAdmin().setStrategies(TraversalStrategies.GlobalCache.getStrategies(Graph.class));
        traversal.iterate();
        whereStep = (WhereStep) traversal.asAdmin().getEndStep();
        //System.out.println(traversal);
        //System.out.println(whereStep.predicate);
        // TODO: do something here
    }

    @Override
    public List<Traversal> getTraversals() {
        return Arrays.asList(
                as("a").out().as("b").where(as("a").out()),
                as("a").out().as("b").where(as("a").out().as("b"))
        );
    }
}
