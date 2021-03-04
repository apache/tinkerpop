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
package org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration;

import org.apache.tinkerpop.gremlin.process.traversal.Operator;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategies;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.process.traversal.util.DefaultTraversalStrategies;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;

import java.util.stream.IntStream;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.MatcherAssert.assertThat;

/**
 * @author Daniel Kuppitz (http://gremlin.guru)
 */
@RunWith(Enclosed.class)
public class SackStrategyTest {

    public static class SanityCheck {

        @Test
        public void traversersWithUnmergeableSacksShouldGenerateDiversifiedHashCodes() {

            final TraversalStrategies strategies = new DefaultTraversalStrategies();
            strategies.addStrategies(SackStrategy.build().initialValue(() -> 0).create());

            final Integer[] starts = IntStream.range(0, 8192).boxed().toArray(Integer[]::new);
            final GraphTraversal<Integer, Long> traversal = __.inject(starts).sack(Operator.mult).sack().
                    map(Traverser::hashCode).dedup().count();

            traversal.asAdmin().setStrategies(strategies);
            traversal.asAdmin().applyStrategies();
            assertThat(traversal.next(), is(greaterThan(8100L))); // allow a few hash collisions
        }
    }
}
