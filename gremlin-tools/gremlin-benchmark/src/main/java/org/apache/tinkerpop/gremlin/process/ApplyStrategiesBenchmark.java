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
package org.apache.tinkerpop.gremlin.process;

import org.apache.tinkerpop.benchmark.util.AbstractBenchmarkBase;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.util.empty.EmptyGraph;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;

import java.util.Arrays;
import java.util.HashSet;
import java.util.concurrent.TimeUnit;

import static org.apache.tinkerpop.gremlin.process.traversal.AnonymousTraversalSource.traversal;

/**
 * Benchmark for measuring the performance of applying strategies to a complex traversal.
 */
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Thread)
public class ApplyStrategiesBenchmark extends AbstractBenchmarkBase {

    private Graph graph = EmptyGraph.instance();
    private GraphTraversalSource g = traversal().withEmbedded(graph);
    private Traversal<?, ?> traversal;

    /**
     * Setup method that constructs the complex traversal before benchmarking on each invocation of the test.
     */
    @Setup(Level.Invocation)
    public void setup() {
        traversal = g.V().or(
                __.has("name", P.within(new HashSet<>(Arrays.asList("DARK STAR", "ST. STEPHEN", "CHINA CAT SUNFLOWER")))),
                __.has("songType", P.eq("cover"))).where(
                __.coalesce(
                        __.where(
                                __.union(
                                        __.as("a").inE("sungBy").choose(
                                                __.has("weight"), __.has("weight", P.gt(1)), __.identity()
                                        ).outV().filter(__.has("weight", P.lt(1))),
                                        __.as("a").outE("followedBy").choose(
                                                __.has("weight"), __.has("weight", P.gt(1)), __.identity()
                                        ).inV().where(
                                                __.coalesce(
                                                        __.where(
                                                                __.union(
                                                                        __.as("a").outE("followedBy").choose(
                                                                                __.has("weight"), __.has("weight", P.gt(1)), __.identity()
                                                                        ).inV().has("songType", P.neq("cover")).where(
                                                                                __.coalesce(
                                                                                        __.where(
                                                                                                __.union(
                                                                                                        __.as("a").outE("followedBy").choose(
                                                                                                                __.has("weight"), __.has("weight", P.gt(1)), __.identity()
                                                                                                        ).inV().where(
                                                                                                                __.coalesce(
                                                                                                                        __.where(
                                                                                                                                __.union(
                                                                                                                                        __.as("a").outE("followedBy").choose(
                                                                                                                                                __.has("weight"), __.has("weight", P.gt(1)), __.identity()
                                                                                                                                        ).inV().where(
                                                                                                                                                __.coalesce(
                                                                                                                                                        __.where(
                                                                                                                                                                __.union(
                                                                                                                                                                        __.as("a").outE("followedBy").choose(
                                                                                                                                                                                __.has("weight"), __.has("weight", P.gt(1)), __.identity()
                                                                                                                                                                        ).inV().has("songType", P.within("original"))
                                                                                                                                                        )
                                                                                                                                                )
                                                                                                                                        )
                                                                                                                                )
                                                                                                                        )
                                                                                                                )
                                                                                                        )
                                                                                                )
                                                                                        )
                                                                                )
                                                                        )
                                                                )
                                                        )
                                                )
                                        )
                                )
                        ).dedup().select("a")
                )
        ));
    }

    @Override
    protected int getWarmupIterations() {
        return 1;
    }

    @Override
    protected int getForks() {
        return 1;
    }

    /**
     * Benchmark that measures only the time it takes to apply strategies to a complex traversal.
     */
    @Benchmark
    public Traversal testLockTraversal() {
        if (traversal.asAdmin().isLocked())
            throw new RuntimeException("Traversal is locked - that shouldn't be possible if the traversal is new");
        traversal.asAdmin().applyStrategies();
        return traversal;
    }
}
