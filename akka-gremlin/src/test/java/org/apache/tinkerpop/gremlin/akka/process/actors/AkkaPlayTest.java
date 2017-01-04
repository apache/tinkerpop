/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */

package org.apache.tinkerpop.gremlin.akka.process.actors;

import org.apache.tinkerpop.gremlin.akka.process.actors.AkkaGraphActors;
import org.apache.tinkerpop.gremlin.process.actors.GraphActors;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.io.gryo.GryoIo;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;
import org.junit.Ignore;
import org.junit.Test;

import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.in;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.out;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class AkkaPlayTest {

    @Test
    @Ignore
    public void testPlay1() throws Exception {
        final Graph graph = TinkerGraph.open();
        graph.io(GryoIo.build()).readGraph("../data/tinkerpop-modern.kryo");
        GraphTraversalSource g = graph.traversal().withProcessor(GraphActors.open(AkkaGraphActors.class).workers(3));
        // System.out.println(g.V().group().by("name").by(outE().values("weight").fold()).toList());

        for (int i = 0; i < 1000; i++) {
            if (12l != g.V().union(out(), in()).values("name").count().next())
                System.out.println(i);
        }

        //3, 1.9, 1
        /*for (int i = 0; i < 10000; i++) {
            final Graph graph = TinkerGraph.open();
            graph.io(GryoIo.build()).readGraph("data/tinkerpop-modern.kryo");
            final GraphTraversalSource g = graph.traversal().withComputer();
            final List<Pair<Integer, Traversal.Admin<?, ?>>> traversals = Arrays.asList(
                    // match() works
                    Pair.with(6, g.V().match(
                            as("a").out("created").as("b"),
                            as("b").in("created").as("c"),
                            as("b").has("name", P.eq("lop"))).where("a", P.neq("c")).select("a", "b", "c").by("name").asAdmin()),
                    // side-effects work
                    Pair.with(3, g.V().repeat(both()).times(2).
                            groupCount("a").by("name").
                            cap("a").unfold().order().by(Column.values, Order.decr).limit(3).asAdmin()),
                    // barriers work and beyond the local star graph works
                    Pair.with(1, g.V().repeat(both()).times(2).hasLabel("person").
                            group().
                            by("name").
                            by(out("created").values("name").dedup().fold()).asAdmin()),
                    // no results works
                    Pair.with(0, g.V().out("blah").asAdmin())
            );
            for (final Pair<Integer,Traversal.Admin<?, ?>> pair : traversals) {
                final Integer count = pair.getValue0();
                final Traversal.Admin<?,?> traversal = pair.getValue1();
                System.out.println("EXECUTING: " + traversal.getBytecode());
                final TinkerActorSystem<?,?> actors = new TinkerActorSystem<>(traversal.clone(),new HashPartitioner(graph.partitioner(), 3));
                System.out.println(IteratorUtils.asList(actors.getResults().get()));
                if(IteratorUtils.count(actors.getResults().get()) != count)
                    throw new IllegalStateException();
                System.out.println("//////////////////////////////////\n");
            }
        }
    }*/

    }
}
