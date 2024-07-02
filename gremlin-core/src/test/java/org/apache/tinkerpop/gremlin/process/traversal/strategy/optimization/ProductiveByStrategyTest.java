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
package org.apache.tinkerpop.gremlin.process.traversal.strategy.optimization;

import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.Scope;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategies;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.process.traversal.lambda.ConstantTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.lambda.ValueTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.util.DefaultTraversalStrategies;
import org.apache.tinkerpop.gremlin.structure.T;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import static org.apache.tinkerpop.gremlin.process.traversal.Operator.assign;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.bothE;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.coalesce;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.constant;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.values;
import static org.junit.Assert.assertEquals;

@RunWith(Parameterized.class)
public class ProductiveByStrategyTest {
    private static final Traversal<Object,Object> ageValueTraversal = new ValueTraversal<>("age");
    private static final Traversal<Object,Object> nameValueTraversal = new ValueTraversal<>("name");
    private static final Traversal<Object,Object> nullTraversal = new ConstantTraversal<>(null);

    @Parameterized.Parameter(value = 0)
    public Traversal.Admin original;

    @Parameterized.Parameter(value = 1)
    public Traversal optimized;

    @Parameterized.Parameter(value = 2)
    public Set<String> productiveKeys;

    void applyProductiveByStrategy(final Traversal<?,?> traversal) {
        final TraversalStrategies strategies = new DefaultTraversalStrategies();
        strategies.addStrategies(ProductiveByStrategy.build().productiveKeys(productiveKeys).create());
        traversal.asAdmin().setStrategies(strategies);
        traversal.asAdmin().applyStrategies();
    }

    @Test
    public void doTest() {
        final String repr = original.getGremlinLang().getGremlin("__");
        applyProductiveByStrategy(original);
        assertEquals(repr, optimized, original);
    }

    @Parameterized.Parameters(name = "{0}")
    public static Iterable<Object[]> generateTestParameters() {

        // purposefully skipping value/propertyMap() as it lacks relevance here and currently doesn't support local
        // traversal replacement
        return Arrays.asList(new Object[][]{
                {__.aggregate("a").by("name"),
                        __.aggregate("a").by(new ValueTraversal<>("name", coalesce(nameValueTraversal, nullTraversal).asAdmin())),
                        Collections.emptySet()},
                {__.aggregate(Scope.local,"a").by("name"),
                        __.aggregate(Scope.local, "a").by(new ValueTraversal<>("name", coalesce(nameValueTraversal, nullTraversal).asAdmin())),
                        Collections.emptySet()},
                {__.aggregate(Scope.local,"a").by("name"),
                        __.aggregate(Scope.local,"a").by("name"),
                        keys("name")},
                {__.aggregate("a").by(values("age").is(P.gt(29))),
                        __.aggregate("a").by(coalesce(values("age").is(P.gt(29)), nullTraversal)),
                        Collections.emptySet()},
                {__.aggregate("a").by(values("age").fold()),
                        __.aggregate("a").by(values("age").fold()),
                        Collections.emptySet()},
                {__.aggregate("a").by(values("age").sum()),
                        __.aggregate("a").by(values("age").sum()),
                        Collections.emptySet()},
                {__.cyclicPath().by("name"),
                        __.cyclicPath().by(new ValueTraversal<>("name", coalesce(nameValueTraversal, nullTraversal).asAdmin())),
                        Collections.emptySet()},
                {__.cyclicPath().by("name").by("age"),
                        __.cyclicPath().by(new ValueTraversal<>("name", coalesce(nameValueTraversal, nullTraversal).asAdmin())).by(new ValueTraversal<>("age", coalesce(ageValueTraversal, nullTraversal).asAdmin())),
                        Collections.emptySet()},
                {__.cyclicPath().by("name").by("age"),
                        __.cyclicPath().by("name").by(new ValueTraversal<>("age", coalesce(ageValueTraversal, nullTraversal).asAdmin())),
                        keys("name")},
                {__.cyclicPath().by("name").by("age"),
                        __.cyclicPath().by(new ValueTraversal<>("name", coalesce(nameValueTraversal, nullTraversal).asAdmin())).by("age"),
                        keys("age")},
                {__.cyclicPath().by("name").by("age"),
                        __.cyclicPath().by("name").by("age"),
                        keys("age","name")},
                {__.dedup().by("name"),
                        __.dedup().by(new ValueTraversal<>("name", coalesce(nameValueTraversal, nullTraversal).asAdmin())),
                        Collections.emptySet()},
                {__.dedup().by("name"),
                        __.dedup().by("name"),
                        keys("name")},
                {__.group().by("name"),
                        __.group().by(new ValueTraversal<>("name", coalesce(nameValueTraversal, nullTraversal).asAdmin())),
                        Collections.emptySet()},
                {__.group().by("name").by("age"),
                        __.group().by(new ValueTraversal<>("name", coalesce(nameValueTraversal, nullTraversal).asAdmin())).by(__.map(ageValueTraversal).fold()),
                        Collections.emptySet()},
                {__.group().by("name").by("age"),
                        __.group().by("name").by(__.map(ageValueTraversal).fold()),
                        keys("name")},
                {__.group().by("name").by("age"),
                        __.group().by("name").by("age"),
                        keys("name","age")},
                {__.group().by(T.label).by(bothE().values("weight").sample(2).fold()),
                        __.group().by(T.label).by(bothE().values("weight").sample(2).fold()),
                        Collections.emptySet()},
                // this double wrapping is unnecessary - guess there is an improvement that could be made here but
                // there would need to be a good way to generally detect a surely productive coalesce(). something for
                // later perhaps - this strategy likely won't be used for most production cases in new Gremlin and is
                // mostly present to bridge functionality back to 3.5.x so perhaps it doesn't need a lot of
                // optimization at this point
                {__.group().by(coalesce(values("age"), constant(null))),
                        __.group().by(coalesce((Traversal.Admin) coalesce(values("age"), constant(null)).fold(), (Traversal.Admin) nullTraversal).fold()),
                        Collections.emptySet()},
                {__.groupCount().by("name"),
                        __.groupCount().by(new ValueTraversal<>("name", coalesce(nameValueTraversal, nullTraversal).asAdmin())),
                        Collections.emptySet()},
                {__.groupCount().by("name"),
                        __.groupCount().by("name"),
                        keys("name")},
                {__.order().by("name"),
                        __.order().by(new ValueTraversal<>("name", coalesce(nameValueTraversal, nullTraversal).asAdmin())),
                        Collections.emptySet()},
                {__.order().by("name").by("age"),
                        __.order().by(new ValueTraversal<>("name", coalesce(nameValueTraversal, nullTraversal).asAdmin())).by(new ValueTraversal<>("age", coalesce(ageValueTraversal, nullTraversal).asAdmin())),
                        Collections.emptySet()},
                {__.order().by("name").by("age"),
                        __.order().by("name").by(new ValueTraversal<>("age", coalesce(ageValueTraversal, nullTraversal).asAdmin())),
                        keys("name")},
                {__.order().by("name").by("age"),
                        __.order().by(new ValueTraversal<>("name", coalesce(nameValueTraversal, nullTraversal).asAdmin())).by("age"),
                        keys("age")},
                {__.path().by("name"),
                        __.path().by(new ValueTraversal<>("name", coalesce(nameValueTraversal, nullTraversal).asAdmin())),
                        Collections.emptySet()},
                {__.path().by("name").by("age"),
                        __.path().by(new ValueTraversal<>("name", coalesce(nameValueTraversal, nullTraversal).asAdmin())).by(new ValueTraversal<>("age", coalesce(ageValueTraversal, nullTraversal).asAdmin())),
                        Collections.emptySet()},
                {__.path().by("name").by("age"),
                        __.path().by(new ValueTraversal<>("name", coalesce(nameValueTraversal, nullTraversal).asAdmin())).by("age"),
                        keys("age")},
                {__.project("name").by("name"),
                        __.project("name").by(new ValueTraversal<>("name", coalesce(nameValueTraversal, nullTraversal).asAdmin())),
                        Collections.emptySet()},
                {__.project("name","age").by("name").by("age"),
                        __.project("name","age").by(new ValueTraversal<>("name", coalesce(nameValueTraversal, nullTraversal).asAdmin())).by(new ValueTraversal<>("age", coalesce(ageValueTraversal, nullTraversal).asAdmin())),
                        Collections.emptySet()},
                {__.project("name","age", "count").by("name").by("age").by(__.count()),
                        __.project("name","age", "count").by(new ValueTraversal<>("name", coalesce(nameValueTraversal, nullTraversal).asAdmin())).by(new ValueTraversal<>("age", coalesce(ageValueTraversal, nullTraversal).asAdmin())).by(__.count()),
                        Collections.emptySet()},
                {__.project("name","age", "count").by("name").by("age").by(__.count()),
                        __.project("name","age", "count").by("name").by("age").by(__.count()),
                        keys("name", "age")},
                {__.sack(assign).by("age"),
                        __.sack(assign).by(new ValueTraversal<>("age", coalesce(ageValueTraversal, nullTraversal).asAdmin())),
                        Collections.emptySet()},
                {__.sample(10).by("name"),
                        __.sample(10).by(new ValueTraversal<>("name", coalesce(nameValueTraversal, nullTraversal).asAdmin())),
                        Collections.emptySet()},
                {__.select("name").by("name"),
                        __.select("name").by(new ValueTraversal<>("name", coalesce(nameValueTraversal, nullTraversal).asAdmin())),
                        Collections.emptySet()},
                {__.select("name","age").by("name").by("age"),
                        __.select("name","age").by(new ValueTraversal<>("name", coalesce(nameValueTraversal, nullTraversal).asAdmin())).by(new ValueTraversal<>("age", coalesce(ageValueTraversal, nullTraversal).asAdmin())),
                        Collections.emptySet()},
                {__.select("name","age", "count").by("name").by("age").by(__.count()),
                        __.select("name","age", "count").by(new ValueTraversal<>("name", coalesce(nameValueTraversal, nullTraversal).asAdmin())).by(new ValueTraversal<>("age", coalesce(ageValueTraversal, nullTraversal).asAdmin())).by(__.count()),
                        Collections.emptySet()},
                {__.select("name","age", "count").by("name").by("age").by(__.count()),
                        __.select("name","age", "count").by("name").by("age").by(__.count()),
                        keys("name", "age")},
                {__.simplePath().by("name"),
                        __.simplePath().by(new ValueTraversal<>("name", coalesce(nameValueTraversal, nullTraversal).asAdmin())),
                        Collections.emptySet()},
                {__.simplePath().by("name").by("age"),
                        __.simplePath().by(new ValueTraversal<>("name", coalesce(nameValueTraversal, nullTraversal).asAdmin())).by(new ValueTraversal<>("age", coalesce(ageValueTraversal, nullTraversal).asAdmin())),
                        Collections.emptySet()},
                {__.simplePath().by("name").by("age"),
                        __.simplePath().by("name").by(new ValueTraversal<>("age", coalesce(ageValueTraversal, nullTraversal).asAdmin())),
                        keys("name")},
                {__.simplePath().by("name").by().by("age"),
                        __.simplePath().by("name").by().by(new ValueTraversal<>("age", coalesce(ageValueTraversal, nullTraversal).asAdmin())),
                        keys("name")},
                {__.tree().by("name"),
                        __.tree().by(new ValueTraversal<>("name", coalesce(nameValueTraversal, nullTraversal).asAdmin())),
                        Collections.emptySet()},
                {__.tree().by("name").by("age"),
                        __.tree().by(new ValueTraversal<>("name", coalesce(nameValueTraversal, nullTraversal).asAdmin())).by(new ValueTraversal<>("age", coalesce(ageValueTraversal, nullTraversal).asAdmin())),
                        Collections.emptySet()},
                {__.where("a", P.eq("b")).by("name"),
                        __.where("a", P.eq("b")).by(new ValueTraversal<>("name", coalesce(nameValueTraversal, nullTraversal).asAdmin())),
                        Collections.emptySet()},
                {__.where("a", P.eq("b")).by("name").by("age"),
                        __.where("a", P.eq("b")).by(new ValueTraversal<>("name", coalesce(nameValueTraversal, nullTraversal).asAdmin())).by(new ValueTraversal<>("age", coalesce(ageValueTraversal, nullTraversal).asAdmin())),
                        Collections.emptySet()},
                {__.where("a", P.eq("b")).by("name").by("age"),
                        __.where("a", P.eq("b")).by("name").by(new ValueTraversal<>("age", coalesce(ageValueTraversal, nullTraversal).asAdmin())),
                        keys("name")},
        });
    }

    private static Set<String> keys(final String... keys) {
        return new HashSet<>(Arrays.asList(keys));
    }
}