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
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.util.function.Lambda;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;

import static org.apache.tinkerpop.gremlin.process.traversal.Operator.sum;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.outE;
import static org.apache.tinkerpop.gremlin.structure.Column.values;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.StringContains.containsString;
import static org.junit.Assert.fail;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
@RunWith(Parameterized.class)
public class LambdaRestrictionStrategyTest {

    @Parameterized.Parameters(name = "{0}")
    public static Iterable<Object[]> data() {
        return Arrays.asList(new Object[][]{
                {__.map(Lambda.function("true")), false},
                {__.filter(Lambda.predicate("true")), false},
                {__.filter(x -> true), false},
                {__.map(Traverser::get), false},
                {__.sideEffect(x -> {
                    int i = 1 + 1;
                }), false},
                {__.select("a").by(values), true},
                {__.select("a", "b").by(Object::toString), false},
                {__.order().by((a, b) -> ((Integer) a).compareTo((Integer) b)), false},
                {__.order(Scope.local).by((a, b) -> ((Integer) a).compareTo((Integer) b)), false},
                {__.choose(v -> v.toString().equals("marko"), __.out(), __.in()), false},
                {__.order().by(T.label, Order.desc), true},
                {__.order(Scope.local).by(values), true},
                {__.order(Scope.local).by(values,Order.desc), true},
                {__.order(Scope.local).by(values, (a, b) -> ((Double) a).compareTo((Double) b)), false},
                //
                {__.groupCount().by(T.label), true},
                //
                {__.sack(sum).by("age"), true},
                {__.sack((a, b) -> (int) a + (int) b).by("age"), false},
                //
                {__.order().by(outE("ratings").values("stars").mean()).profile(), true}
        });
    }

    @Parameterized.Parameter(value = 0)
    public Traversal.Admin traversal;

    @Parameterized.Parameter(value = 1)
    public boolean allow;

    @Test
    public void shouldBeVerifiedIllegal() {
        final String repr = traversal.getGremlinLang().getGremlin();
        final TraversalStrategies strategies = new DefaultTraversalStrategies();
        strategies.addStrategies(ProfileStrategy.instance());
        strategies.addStrategies(LambdaRestrictionStrategy.instance());
        traversal.asAdmin().setStrategies(strategies);
        if (allow) {
            traversal.asAdmin().applyStrategies();
        } else {
            try {
                traversal.asAdmin().applyStrategies();
                fail("The strategy should not allow lambdas: " + repr);
            } catch (VerificationException ise) {
                assertThat(ise.getMessage(), containsString("lambda"));
            }
        }
    }
}
