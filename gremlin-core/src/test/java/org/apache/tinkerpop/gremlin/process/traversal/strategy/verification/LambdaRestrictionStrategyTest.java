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

import org.apache.tinkerpop.gremlin.process.traversal.Order;
import org.apache.tinkerpop.gremlin.process.traversal.Scope;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategies;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.finalization.ProfileStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.util.DefaultTraversalStrategies;
import org.apache.tinkerpop.gremlin.structure.Column;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.util.function.Lambda;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;

import static org.apache.tinkerpop.gremlin.process.traversal.Operator.sum;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.outE;
import static org.apache.tinkerpop.gremlin.structure.Column.values;
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
                {"map(Lambda.function('true')}", __.map(Lambda.function("true")), false},
                {"filter(Lambda.predicate('true')}", __.filter(Lambda.predicate("true")), false},
                {"filter(x->true)", __.filter(x -> true), false},
                {"map(Traverser::get)", __.map(Traverser::get), false},
                {"sideEffect(x -> {int i = 1+1;})", __.sideEffect(x -> {
                    int i = 1 + 1;
                }), false},
                {"select('a').by(values)", __.select("a").by(values), true},
                {"select('a','b').by(Object::toString)", __.select("a", "b").by(Object::toString), false},
                {"order().by((a,b)->a.compareTo(b))", __.order().by((a, b) -> ((Integer) a).compareTo((Integer) b)), false},
                {"order(local).by((a,b)->a.compareTo(b))", __.order(Scope.local).by((a, b) -> ((Integer) a).compareTo((Integer) b)), false},
                {"__.choose(v->v.toString().equals(\"marko\"),__.out(),__.in())", __.choose(v -> v.toString().equals("marko"), __.out(), __.in()), false},
                {"order().by(label,desc)", __.order().by(T.label, Order.desc), true},
                {"order(local).by(values)", __.order(Scope.local).by(values), true},
                {"order(local).by(values,desc)", __.order(Scope.local).by(values,Order.desc), true},
                {"order(local).by(values,(a,b) -> a.compareTo(b))", __.order(Scope.local).by(values, (a, b) -> ((Double) a).compareTo((Double) b)), false},
                //
                {"groupCount().by(label)", __.groupCount().by(T.label), true},
                //
                {"sack(sum).by('age')", __.sack(sum).by("age"), true},
                {"sack{a,b -> a+b}.by('age')", __.sack((a, b) -> (int) a + (int) b).by("age"), false},
                //
                {"order().by(outE('rating').values('stars').mean()).profile()", __.order().by(outE("ratings").values("stars").mean()).profile(), true}
        });
    }

    @Parameterized.Parameter(value = 0)
    public String name;

    @Parameterized.Parameter(value = 1)
    public Traversal traversal;

    @Parameterized.Parameter(value = 2)
    public boolean allow;

    @Test
    public void shouldBeVerifiedIllegal() {
        final TraversalStrategies strategies = new DefaultTraversalStrategies();
        strategies.addStrategies(ProfileStrategy.instance());
        strategies.addStrategies(LambdaRestrictionStrategy.instance());
        traversal.asAdmin().setStrategies(strategies);
        if (allow) {
            traversal.asAdmin().applyStrategies();
        } else {
            try {
                traversal.asAdmin().applyStrategies();
                fail("The strategy should not allow lambdas: " + this.traversal);
            } catch (VerificationException ise) {
                assertTrue(ise.getMessage().contains("lambda"));
            }
        }
    }
}
