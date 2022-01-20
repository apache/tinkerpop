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
package org.apache.tinkerpop.gremlin.process.traversal.step;

import org.apache.tinkerpop.gremlin.FeatureRequirement;
import org.apache.tinkerpop.gremlin.process.AbstractGremlinProcessTest;
import org.apache.tinkerpop.gremlin.process.GremlinProcessRunner;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.function.BiFunction;

import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.and;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.is;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.not;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.or;
import static org.apache.tinkerpop.gremlin.structure.Graph.Features.GraphFeatures;

/**
 * @author Mike Personick (http://github.com/mikepersonick)
 */
@RunWith(GremlinProcessRunner.class)
public class TernaryBooleanLogicsTest extends AbstractGremlinProcessTest {

    /**
     * NaN comparisons always produce UNDEF comparison, reducing to FALSE in ternary->binary reduction.
     */
    @Test
    @FeatureRequirement(featureClass = GraphFeatures.class, feature = GraphFeatures.FEATURE_ORDERABILITY_SEMANTICS)
    public void testCompareNaN() {
        // NaN vs. NaN
        // P.eq
        checkHasNext(false, g.inject(Double.NaN).is(P.eq(Double.NaN)));
        // P.neq
        checkHasNext(true, g.inject(Double.NaN).is(P.neq(Double.NaN)));
        // P.lt
        checkHasNext(false, g.inject(Double.NaN).is(P.lt(Double.NaN)));
        // P.lte
        checkHasNext(false, g.inject(Double.NaN).is(P.lte(Double.NaN)));
        // P.gt
        checkHasNext(false, g.inject(Double.NaN).is(P.gt(Double.NaN)));
        // P.gte
        checkHasNext(false, g.inject(Double.NaN).is(P.gte(Double.NaN)));

        // non-NaN vs. NaN
        // P.eq
        checkHasNext(false, g.inject(1.0d).is(P.eq(Double.NaN)));
        checkHasNext(false, g.inject(Double.NaN).is(P.eq(1.0d)));
        // P.neq
        checkHasNext(true, g.inject(1.0d).is(P.neq(Double.NaN)));
        checkHasNext(true, g.inject(Double.NaN).is(P.neq(1.0d)));
        // P.lt
        checkHasNext(false, g.inject(1.0d).is(P.lt(Double.NaN)));
        checkHasNext(false, g.inject(Double.NaN).is(P.lt(1.0d)));
        // P.lte
        checkHasNext(false, g.inject(1.0d).is(P.lte(Double.NaN)));
        checkHasNext(false, g.inject(Double.NaN).is(P.lte(1.0d)));
        // P.gt
        checkHasNext(false, g.inject(1.0d).is(P.gt(Double.NaN)));
        checkHasNext(false, g.inject(Double.NaN).is(P.gt(1.0d)));
        // P.gte
        checkHasNext(false, g.inject(1.0d).is(P.gte(Double.NaN)));
        checkHasNext(false, g.inject(Double.NaN).is(P.gte(1.0d)));
    }

    /**
     * Null comparisons. Null is considered to be a distinct type, thus will produce ERROR for comparison against
     * non-null, just like any cross-type comparison. Null is == null and is != to any non-null (except for NaN, which
     * is always UNDEF/ERROR).
     */
    @Test
    @FeatureRequirement(featureClass = GraphFeatures.class, feature = GraphFeatures.FEATURE_ORDERABILITY_SEMANTICS)
    public void testCompareNull() {
        // null vs. null
        // P.eq
        checkHasNext(true, g.inject(null).is(P.eq(null)));
        // P.neq
        checkHasNext(false, g.inject(null).is(P.neq(null)));
        // P.lt
        checkHasNext(false, g.inject(null).is(P.lt(null)));
        // P.lte
        checkHasNext(true, g.inject(null).is(P.lte(null)));
        // P.gt
        checkHasNext(false, g.inject(null).is(P.gt(null)));
        // P.gte
        checkHasNext(true, g.inject(null).is(P.gte(null)));

        // non-null vs. null
        // P.eq
        checkHasNext(false, g.inject(1.0d).is(P.eq(null)));
        checkHasNext(false, g.inject(null).is(P.eq(1.0d)));
        // P.neq
        checkHasNext(true, g.inject(1.0d).is(P.neq(null)));
        checkHasNext(true, g.inject(null).is(P.neq(1.0d)));
        // P.lt
        checkHasNext(false, g.inject(1.0d).is(P.lt(null)));
        checkHasNext(false, g.inject(null).is(P.lt(1.0d)));
        // P.lte
        checkHasNext(false, g.inject(1.0d).is(P.lte(null)));
        checkHasNext(false, g.inject(null).is(P.lte(1.0d)));
        // P.gt
        checkHasNext(false, g.inject(1.0d).is(P.gt(null)));
        checkHasNext(false, g.inject(null).is(P.gt(1.0d)));
        // P.gte
        checkHasNext(false, g.inject(1.0d).is(P.gte(null)));
        checkHasNext(false, g.inject(null).is(P.gte(1.0d)));

        // NaN vs null
        // P.eq
        checkHasNext(false, g.inject(null).is(P.eq(Double.NaN)));
        checkHasNext(false, g.inject(Double.NaN).is(P.eq(null)));
        // P.neq
        checkHasNext(true, g.inject(null).is(P.neq(Double.NaN)));
        checkHasNext(true, g.inject(Double.NaN).is(P.neq(null)));
        // P.lt
        checkHasNext(false, g.inject(null).is(P.lt(Double.NaN)));
        checkHasNext(false, g.inject(Double.NaN).is(P.lt(null)));
        // P.lte
        checkHasNext(false, g.inject(null).is(P.lte(Double.NaN)));
        checkHasNext(false, g.inject(Double.NaN).is(P.lte(null)));
        // P.gt
        checkHasNext(false, g.inject(null).is(P.gt(Double.NaN)));
        checkHasNext(false, g.inject(Double.NaN).is(P.gt(null)));
        // P.gte
        checkHasNext(false, g.inject(null).is(P.gte(Double.NaN)));
        checkHasNext(false, g.inject(Double.NaN).is(P.gte(null)));
    }

    /**
     * Comparisons across type families. FALSE for P.eq, TRUE for P.neq, and UNDEF with binary
     * reduction to FALSE for P.lt/lte/gt/gte.
     */
    @Test
    @FeatureRequirement(featureClass = GraphFeatures.class, feature = GraphFeatures.FEATURE_ORDERABILITY_SEMANTICS)
    public void testCompareAcrossTypes() {
        // P.eq
        checkHasNext(false, g.inject("foo").is(P.eq(1.0d)));
        checkHasNext(false, g.inject(1.0d).is(P.eq("foo")));
        // P.neq
        checkHasNext(true, g.inject("foo").is(P.neq(1.0d)));
        checkHasNext(true, g.inject(1.0d).is(P.neq("foo")));
        // P.lt
        checkHasNext(false, g.inject("foo").is(P.lt(1.0d)));
        checkHasNext(false, g.inject(1.0d).is(P.lt("foo")));
        // P.lte
        checkHasNext(false, g.inject("foo").is(P.lte(1.0d)));
        checkHasNext(false, g.inject(1.0d).is(P.lte("foo")));
        // P.gt
        checkHasNext(false, g.inject("foo").is(P.gt(1.0d)));
        checkHasNext(false, g.inject(1.0d).is(P.gt("foo")));
        // P.gte
        checkHasNext(false, g.inject("foo").is(P.gte(1.0d)));
        checkHasNext(false, g.inject(1.0d).is(P.gte("foo")));
    }

    /**
     * Over a ternary binary semantics, the AND predicate evaluates to TRUE only if both arguments are TRUE. AND will
     * propogate ERROR values unless the other argument evalutes to FALSE, since FALSE && X => FALSE.
     */
    @Test
    @FeatureRequirement(featureClass = GraphFeatures.class, feature = GraphFeatures.FEATURE_ORDERABILITY_SEMANTICS)
    public void testAnd() {
        final P TRUE = P.eq(1);
        final P FALSE = P.gt(1);
        final P ERROR = P.lt(Double.NaN);

        // TRUE, TRUE -> TRUE
        checkHasNext(true, g.inject(1).and(is(TRUE),is(TRUE)));
        checkHasNext(true, g.inject(1).is(TRUE.and(TRUE)));
        // TRUE, FALSE -> FALSE
        checkHasNext(false, g.inject(1).and(is(TRUE),is(FALSE)));
        checkHasNext(false, g.inject(1).is(TRUE.and(FALSE)));
        // TRUE, ERROR -> ERROR -> FALSE
        checkHasNext(false, g.inject(1).and(is(TRUE),is(ERROR)));
        checkHasNext(false, g.inject(1).is(TRUE.and(ERROR)));

        // FALSE, TRUE -> FALSE
        checkHasNext(false, g.inject(1).and(is(FALSE),is(TRUE)));
        checkHasNext(false, g.inject(1).is(FALSE.and(TRUE)));
        // FALSE, FALSE -> FALSE
        checkHasNext(false, g.inject(1).and(is(FALSE),is(FALSE)));
        checkHasNext(false, g.inject(1).is(FALSE.and(FALSE)));
        // FALSE, ERROR -> FALSE
        checkHasNext(false, g.inject(1).and(is(FALSE),is(ERROR)));
        checkHasNext(false, g.inject(1).is(FALSE.and(ERROR)));

        // ERROR, TRUE -> ERROR -> FALSE
        checkHasNext(false, g.inject(1).and(is(ERROR),is(TRUE)));
        checkHasNext(false, g.inject(1).is(ERROR.and(TRUE)));
        // ERROR, FALSE -> FALSE
        checkHasNext(false, g.inject(1).and(is(ERROR),is(FALSE)));
        checkHasNext(false, g.inject(1).is(ERROR.and(FALSE)));
        // ERROR, ERROR -> ERROR -> FALSE
        checkHasNext(false, g.inject(1).and(is(ERROR),is(ERROR)));
        checkHasNext(false, g.inject(1).is(ERROR.and(ERROR)));
    }

    /**
     * Being symetrically defined to AND, OR only returns FALSE in the case that both arguments evalute to FALSE.
     * In cases where one argument is FALSE and the other ERROR, OR propogates the ERROR. Whenever one argument
     * is TRUE, OR returns TRUE since TRUE || X => TRUE.
     */
    @Test
    @FeatureRequirement(featureClass = GraphFeatures.class, feature = GraphFeatures.FEATURE_ORDERABILITY_SEMANTICS)
    public void testOr() {
        final P TRUE = P.eq(1);
        final P FALSE = P.gt(1);
        final P ERROR = P.lt(Double.NaN);

        // TRUE, TRUE -> TRUE
        checkHasNext(true, g.inject(1).or(is(TRUE),is(TRUE)));
        checkHasNext(true, g.inject(1).is(TRUE.or(TRUE)));
        // TRUE, FALSE -> TRUE
        checkHasNext(true, g.inject(1).or(is(TRUE),is(FALSE)));
        checkHasNext(true, g.inject(1).is(TRUE.or(FALSE)));
        // TRUE, ERROR -> TRUE
        checkHasNext(true, g.inject(1).or(is(TRUE),is(ERROR)));
        checkHasNext(true, g.inject(1).is(TRUE.or(ERROR)));

        // FALSE, TRUE -> TRUE
        checkHasNext(true, g.inject(1).or(is(FALSE),is(TRUE)));
        checkHasNext(true, g.inject(1).is(FALSE.or(TRUE)));
        // FALSE, FALSE -> FALSE
        checkHasNext(false, g.inject(1).or(is(FALSE),is(FALSE)));
        checkHasNext(false, g.inject(1).is(FALSE.or(FALSE)));
        // FALSE, ERROR -> ERROR -> FALSE
        checkHasNext(false, g.inject(1).or(is(FALSE),is(ERROR)));
        checkHasNext(false, g.inject(1).is(FALSE.or(ERROR)));

        // ERROR, TRUE -> TRUE
        checkHasNext(true, g.inject(1).or(is(ERROR),is(TRUE)));
        checkHasNext(true, g.inject(1).is(ERROR.or(TRUE)));
        // ERROR, FALSE -> ERROR -> FALSE
        checkHasNext(false, g.inject(1).or(is(ERROR),is(FALSE)));
        checkHasNext(false, g.inject(1).is(ERROR.or(FALSE)));
        // ERROR, ERROR -> ERROR -> FALSE
        checkHasNext(false, g.inject(1).or(is(ERROR),is(ERROR)));
        checkHasNext(false, g.inject(1).is(ERROR.or(ERROR)));
    }

    /**
     * The NOT predicate inverts TRUE and FALSE but maintains ERROR values. For ERROR, we can neither prove
     * nor disprove the value expression and hence stick with ERROR.
     */
    @Test
    @FeatureRequirement(featureClass = GraphFeatures.class, feature = GraphFeatures.FEATURE_ORDERABILITY_SEMANTICS)
    public void testNot() {
        final P TRUE = P.eq(1);
        final P FALSE = P.gt(1);
        final P ERROR = P.lt(Double.NaN);

        // TRUE -> FALSE
        checkHasNext(false, g.inject(1).not(is(TRUE)));
        // FALSE -> TRUE
        checkHasNext(true, g.inject(1).not(is(FALSE)));
        // ERROR -> ERROR -> FALSE
        checkHasNext(false, g.inject(1).not(is(ERROR)));

        // Binary reduction with NaN
        checkHasNext(false, g.inject(1).is(P.eq(Double.NaN)));
        checkHasNext(true, g.inject(1).is(P.neq(Double.NaN)));
        checkHasNext(true, g.inject(1).not(is(P.eq(Double.NaN))));
        checkHasNext(false, g.inject(1).not(not(is(P.eq(Double.NaN)))));
        checkHasNext(false, g.inject(1).where(__.inject(1).not(is(ERROR))));
    }

    /**
     * Ternary XOR semantics.
     */
    @Test
    @FeatureRequirement(featureClass = GraphFeatures.class, feature = GraphFeatures.FEATURE_ORDERABILITY_SEMANTICS)
    public void testXor() {
        final P TRUE = P.eq(1);
        final P FALSE = P.gt(1);
        final P ERROR = P.lt(Double.NaN);

        final BiFunction<P,P, GraphTraversal> xor = (A, B) -> or(and(is(A),not(is(B))),
                                                                 and(is(B),not(is(A))));

        // TRUE, TRUE -> FALSE
        checkHasNext(false, g.inject(1).filter(xor.apply(TRUE, TRUE)));
        // TRUE, FALSE -> TRUE
        checkHasNext(true, g.inject(1).filter(xor.apply(TRUE, FALSE)));
        // TRUE, ERROR -> ERROR -> FALSE
        checkHasNext(false, g.inject(1).filter(xor.apply(TRUE, ERROR)));

        // FALSE, TRUE -> TRUE
        checkHasNext(true, g.inject(1).filter(xor.apply(FALSE, TRUE)));
        // FALSE, FALSE -> FALSE
        checkHasNext(false, g.inject(1).filter(xor.apply(FALSE, FALSE)));
        // FALSE, ERROR -> ERROR -> FALSE
        checkHasNext(false, g.inject(1).filter(xor.apply(FALSE, ERROR)));

        // ERROR, TRUE -> ERROR -> FALSE
        checkHasNext(false, g.inject(1).filter(xor.apply(ERROR, TRUE)));
        // ERROR, FALSE -> ERROR -> FALSE
        checkHasNext(false, g.inject(1).filter(xor.apply(ERROR, FALSE)));
        // ERROR, ERROR -> ERROR -> FALSE
        checkHasNext(false, g.inject(1).filter(xor.apply(ERROR, ERROR)));
    }

}
