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
package org.apache.tinkerpop.benchmark.util;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;

import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;

/**
 * Graph write and update benchmarks extend {@code AbstractGraphMutateBenchmark}.  {@code AbstractGraphMutateBenchmark}
 * runs setup once per invocation so that benchmark measurements are made on an empty {@link TinkerGraph}.  This approach
 * was taken to isolate the tested method from the performance side effects of unbounded graph growth.
 *
 * @author Ted Wilmes (http://twilmes.org)
 */
@State(Scope.Thread)
public abstract class AbstractGraphMutateBenchmark extends AbstractBenchmarkBase {

    protected Graph graph;
    protected GraphTraversalSource g;

    @Setup(Level.Invocation)
    public void prepare() {
        graph = TinkerGraph.open();
        g = graph.traversal();
    }
}
