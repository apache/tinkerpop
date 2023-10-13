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

package org.apache.tinkerpop.gremlin.tinkergraph.structure;

import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.optimization.RepeatUnrollStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.util.TraverserSet;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

public class TinkerGraphRepeatBarrierTest {

    @Test
    public void test() {
        final TinkerGraph tg = TinkerGraph.open();
        final GraphTraversalSource g = tg.traversal();

        // Create structure:
        // root-branch1->[leaf_1, leaf_2, leaf_3]
        // leaf_1-branch2->[leaf2_1, leaf2_2, leaf2_3]
        // leaf_2-branch2->[leaf2_4, leaf2_5, leaf2_6]
        // leaf_3-branch2->[leaf2_7, leaf2_8, leaf2_9]
        g.addV("root").as("root").
                addE("branch1").to(__.addV("leaf1_1").as("leaf1_1").
                        sideEffect(__.addE("branch2").to(__.addV("leaf2_1"))).
                        sideEffect(__.addE("branch2").to(__.addV("leaf2_2"))).
                        sideEffect(__.addE("branch2").to(__.addV("leaf2_3"))).select("leaf1_1")).
                select("root").
                addE("branch1x").to(__.addV("leaf1_2").as("leaf1_2").
                        sideEffect(__.addE("branch2").to(__.addV("leaf2_4"))).
                        sideEffect(__.addE("branch2").to(__.addV("leaf2_5"))).
                        sideEffect(__.addE("branch2").to(__.addV("leaf2_6"))).select("leaf1_2")).
                select("root").
                addE("branch1xx").to(__.addV("leaf1_3").as("leaf1_3").
                        sideEffect(__.addE("branch2").to(__.addV("leaf2_7"))).
                        sideEffect(__.addE("branch2").to(__.addV("leaf2_8"))).
                        sideEffect(__.addE("branch2").to(__.addV("leaf2_9"))).select("leaf1_3")).iterate();

        final BarrierConsumerInputCounter untilBlockBarrierConsumer = new BarrierConsumerInputCounter("until");
        final BarrierConsumerInputCounter repeatBlockBarrierConsumer = new BarrierConsumerInputCounter("repeat");

        // If the barrier is respected by the repeat step, we will get 3 then 9.
        final List<?> data1 = g.
                withoutStrategies(RepeatUnrollStrategy.class)
                .V().hasLabel("root").
                repeat(
                        __.barrier(repeatBlockBarrierConsumer).out()).
                until(
                        __.barrier(untilBlockBarrierConsumer).out().count().is(P.eq(0))).
                toList();
        // Should also try barrier of 1 < barrierSize < count per stage
        System.out.println("data1: " + data1);
        System.out.println("untilBlockBarrierConsumer.barrierInputs: " + untilBlockBarrierConsumer.barrierInputs);
        System.out.println("repeatBlockBarrierConsumer.barrierInputs: " + repeatBlockBarrierConsumer.barrierInputs);
    }

    static class BarrierConsumerInputCounter implements Consumer<TraverserSet<Object>> {
        public final List<Integer> barrierInputs = new ArrayList<>();
        public final String label;

        public BarrierConsumerInputCounter(final String label) {
            this.label = label;
        }

        @Override
        public void accept(TraverserSet<Object> admins) {
            barrierInputs.add(admins.size());
            System.out.println(label + "-" + admins.size());
            // Pass through.
        }
    }
}
