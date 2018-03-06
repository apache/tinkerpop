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
package org.apache.tinkerpop.jsr223;

import org.apache.tinkerpop.benchmark.util.AbstractBenchmarkBase;
import org.apache.tinkerpop.gremlin.jsr223.JavaTranslator;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.SubgraphStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.verification.ReadOnlyStrategy;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.empty.EmptyGraph;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;

import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.as;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.hasLabel;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.inE;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.values;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
@State(Scope.Thread)
public class JavaTranslatorBenchmark extends AbstractBenchmarkBase {

    private final Graph graph = EmptyGraph.instance();
    private final GraphTraversalSource g = graph.traversal();
    private final JavaTranslator<GraphTraversalSource, GraphTraversal.Admin<Vertex,Vertex>> translator = JavaTranslator.of(g);

    @Benchmark
    public GraphTraversal.Admin<Vertex,Vertex> testTranslationShort() {
        return translator.translate(g.V().asAdmin().getBytecode());
    }

    @Benchmark
    public GraphTraversal.Admin<Vertex,Vertex> testTranslationMedium() {
        return translator.translate(g.V().out().in("link").out().in("link").asAdmin().getBytecode());
    }

    @Benchmark
    public GraphTraversal.Admin<Vertex,Vertex> testTranslationLong() {
        return translator.translate(g.V().match(
                as("a").has("song", "name", "HERE COMES SUNSHINE"),
                as("a").map(inE("followedBy").values("weight").mean()).as("b"),
                as("a").inE("followedBy").as("c"),
                as("c").filter(values("weight").where(P.gte("b"))).outV().as("d")).
                <String>select("d").by("name").asAdmin().getBytecode());
    }

    @Benchmark
    public GraphTraversal.Admin<Vertex,Vertex> testTranslationWithStrategy() {
        return translator.translate(g.withStrategies(ReadOnlyStrategy.instance())
                .withStrategies(SubgraphStrategy.build().vertices(hasLabel("person")).create())
                .V().out().in("link").out().in("link").asAdmin().getBytecode());
    }
}
