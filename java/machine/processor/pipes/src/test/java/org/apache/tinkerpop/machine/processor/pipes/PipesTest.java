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
package org.apache.tinkerpop.machine.processor.pipes;

import org.apache.tinkerpop.language.gremlin.Gremlin;
import org.apache.tinkerpop.language.gremlin.P;
import org.apache.tinkerpop.language.gremlin.Traversal;
import org.apache.tinkerpop.language.gremlin.TraversalSource;
import org.apache.tinkerpop.language.gremlin.TraversalUtil;
import org.apache.tinkerpop.language.gremlin.common.__;
import org.apache.tinkerpop.machine.Machine;
import org.apache.tinkerpop.machine.coefficient.LongCoefficient;
import org.apache.tinkerpop.machine.species.LocalMachine;
import org.apache.tinkerpop.machine.species.remote.MachineServer;
import org.apache.tinkerpop.machine.species.remote.RemoteMachine;
import org.apache.tinkerpop.machine.strategy.optimization.IdentityStrategy;
import org.apache.tinkerpop.machine.structure.blueprints.BlueprintsStructure;
import org.apache.tinkerpop.machine.structure.data.JMap;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.apache.tinkerpop.language.gremlin.common.__.incr;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class PipesTest {

    @Test
    public void doStuff() {
        final Machine machine = LocalMachine.open();
        final TraversalSource<Long> g = Gremlin.<Long>traversal(machine)
                .withCoefficient(LongCoefficient.class)
                .withProcessor(PipesProcessor.class)
                .withStructure(BlueprintsStructure.class)
                .withStrategy(IdentityStrategy.class);

        Traversal<Long, ?, ?> traversal = g.V().identity().union(__.count(), __.count()).map(__.<Long, Long>count().identity()).explain();
        System.out.println(TraversalUtil.getBytecode(traversal));
        System.out.println(traversal);
        System.out.println(TraversalUtil.getBytecode(traversal));
        System.out.println(traversal.next());
        System.out.println("\n----------\n");
    }

    @Test
    public void shouldWork() {
        final MachineServer server = new MachineServer(7777);
        final Machine machine = RemoteMachine.open(6666, "localhost", 7777);
        final TraversalSource<Long> g = Gremlin.<Long>traversal(machine)
                .withCoefficient(LongCoefficient.class)
                .withProcessor(PipesProcessor.class)
                .withStrategy(IdentityStrategy.class);

        Traversal<Long, ?, ?> traversal = g.inject(List.of(1L, 1L)).<Long>unfold().map(incr()).c(4L).repeat(incr()).until(__.is(__.constant(8L).incr().incr())).sum();
        System.out.println(TraversalUtil.getBytecode(traversal));
        System.out.println(traversal);
        System.out.println(traversal.toList());
        System.out.println("\n----------\n");
        traversal = g.inject(List.of(1L, 2L)).unfold().is(P.lt(__.constant(2L))).groupCount().by(__.incr());
        System.out.println(TraversalUtil.getBytecode(traversal));
        System.out.println(traversal);
        System.out.println(traversal.toList());
        System.out.println("\n----------\n");
        traversal = g.inject(1L).times(10).repeat(__.incr()).emit();
        System.out.println(TraversalUtil.getBytecode(traversal));
        System.out.println(traversal);
        System.out.println(traversal.toList());
        System.out.println("\n----------\n");
        /*traversal = g.inject(1L).repeat(incr()).emit(__.constant(true)).until(__.<Long, Long>loops().is(P.gt(5)));
        System.out.println(TraversalUtil.getBytecode(traversal));
        System.out.println(traversal);
        System.out.println(traversal.toList());
        System.out.println("\n----------\n");*/
        traversal = g.inject(1L).emit(__.constant(true)).until(__.is(5L)).repeat(incr());
        System.out.println(TraversalUtil.getBytecode(traversal));
        System.out.println(traversal);
        System.out.println(traversal.toList());
        System.out.println("\n----------\n");
        traversal = g.inject(1L).until(__.is(5L)).repeat(incr()).emit(__.constant(true));
        System.out.println(TraversalUtil.getBytecode(traversal));
        System.out.println(traversal);
        System.out.println(traversal.toList());
        System.out.println("\n----------\n");
        traversal = g.inject(7L).union(__.incr(), __.<Long>incr().incr().union(__.incr(), __.incr()));
        System.out.println(TraversalUtil.getBytecode(traversal));
        System.out.println(traversal);
        System.out.println(traversal.toList());
        System.out.println("\n----------\n");
        traversal = g.inject(7L).choose(__.is(7L), __.incr()).sum();
        System.out.println(TraversalUtil.getBytecode(traversal));
        System.out.println(traversal);
        System.out.println(traversal.toList());
        System.out.println("\n----------\n");
        traversal = g.inject(List.of(7L, 8L, 9L)).<Long>unfold().choose(__.is(7L), __.incr(), __.<Long>incr().incr());
        System.out.println(TraversalUtil.getBytecode(traversal));
        System.out.println(traversal);
        System.out.println(traversal.toList());
        System.out.println("\n----------\n");
        traversal = g.inject(7L).as("a").union(__.<Long>incr().as("b"), __.<Long>incr().incr().as("b"), __.<Long>incr().incr().incr().as("b")).path("a", "b").by(__.incr());
        System.out.println(TraversalUtil.getBytecode(traversal));
        System.out.println(traversal);
        System.out.println(traversal.toList());
        System.out.println("\n----------\n");
        traversal = g.inject(7L, 7L, 7L, 2L).incr().barrier();
        System.out.println(TraversalUtil.getBytecode(traversal));
        System.out.println(traversal);
        System.out.println(traversal.hasNext());
        System.out.println(traversal.nextTraverser());
        System.out.println(traversal.hasNext());
        System.out.println(traversal.nextTraverser());
        System.out.println(traversal.hasNext());
        ///
        g.close();
        server.close();

    }

    @Test
    public void shouldWork2() {
        final Machine machine = LocalMachine.open();
        final TraversalSource<Long> g = Gremlin.<Long>traversal(machine)
                .withCoefficient(LongCoefficient.class)
                .withProcessor(PipesProcessor.class)
                .withStrategy(IdentityStrategy.class);

        List<JMap<String, Object>> listA = List.of(
                JMap.create(Map.of("name", "marko", "age", 29)),
                JMap.create(Map.of("name", "josh", "age", 32)),
                JMap.create(Map.of("name", "peter", "age", 35)),
                JMap.create(Map.of("name", "vadas", "age", 27)));

        List<JMap<String, Object>> listB = List.of(
                JMap.create(Map.of("name", "marko", "city", "santa fe")),
                JMap.create(Map.of("name", "marko", "city", "santa cruz")),
                JMap.create(Map.of("name", "josh", "city", "san jose")),
                JMap.create(Map.of("name", "peter", "city", "malmo")),
                JMap.create(Map.of("name", "vadas", "city", "durham")));


        Traversal<Long, ?, ?> traversal = g.inject(listA).unfold().hasKey(P.regex(__.constant("[a].*[e]"))); //.join(inner, __.constant(listB).unfold()).by("name");
        System.out.println(TraversalUtil.getBytecode(traversal));
        System.out.println(traversal);
        System.out.println(traversal.toList());
        System.out.println("\n----------\n");
        traversal = g.inject(listA).unfold().has(__.<Map, String>constant("name"), "marko").value("age");
        System.out.println(TraversalUtil.getBytecode(traversal));
        System.out.println(traversal);
        System.out.println(traversal.toList());
        System.out.println("\n----------\n");
        traversal = g.inject(JMap.create(Map.of("name", "marko", "age", 29)), JMap.create(Map.of("name", "josh", "age", 32))).hasKey(P.regex(__.constant("[a].*[e]"))).has("name", "marko").value("age");
        System.out.println(TraversalUtil.getBytecode(traversal));
        System.out.println(traversal);
        System.out.println(traversal.toList());
        System.out.println("\n----------\n");
    }
}
