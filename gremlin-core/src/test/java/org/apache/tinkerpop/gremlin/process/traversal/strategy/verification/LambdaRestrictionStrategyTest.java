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
package org.apache.tinkerpop.gremlin.process.traversal.strategy.verification;

import org.apache.tinkerpop.gremlin.process.traversal.Scope;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategies;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.process.traversal.engine.StandardTraversalEngine;
import org.apache.tinkerpop.gremlin.process.traversal.util.DefaultTraversalStrategies;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
@RunWith(Parameterized.class)
public class LambdaRestrictionStrategyTest {

    @Parameterized.Parameters(name = "{0}")
    public static Iterable<Object[]> data() {
        return Arrays.asList(new Object[][]{
                {"filter(x->true)", __.filter(x -> true)},
                {"map(Traverser::get)", __.map(Traverser::get)},
                {"sideEffect(x -> {int i = 1+1;})", __.sideEffect(x -> {
                    int i = 1 + 1;
                })},
                {"select('a','b').by(Object::toString)", __.select("a","b").by(Object::toString)},
                {"order().by((a,b)->a.compareTo(b))", __.order().by((a, b) -> ((Integer) a).compareTo((Integer) b))},
                {"order(local).by((a,b)->a.compareTo(b))", __.order(Scope.local).by((a, b) -> ((Integer) a).compareTo((Integer) b))},
                {"__.choose(v->v.toString().equals(\"marko\"),__.out(),__.in())", __.choose(v -> v.toString().equals("marko"), __.out(), __.in())},
        });
    }

    @Parameterized.Parameter(value = 0)
    public String name;

    @Parameterized.Parameter(value = 1)
    public Traversal traversal;

    @Test
    public void shouldBeVerifiedIllegal() {
        try {
            final TraversalStrategies strategies = new DefaultTraversalStrategies();
            strategies.addStrategies(LambdaRestrictionStrategy.instance());
            traversal.asAdmin().setStrategies(strategies);
            traversal.asAdmin().setEngine(StandardTraversalEngine.instance());
            traversal.asAdmin().applyStrategies();
            fail("The strategy should not allow lambdas: " + this.traversal);
        } catch (VerificationException ise) {
            assertTrue(ise.getMessage().contains("lambda"));
        }
    }
}
