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
import org.apache.tinkerpop.gremlin.groovy.jsr223.GremlinGroovyScriptEngine;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.empty.EmptyGraph;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;

import javax.script.Bindings;
import javax.script.SimpleBindings;

import java.util.HashMap;
import java.util.Random;
import java.util.stream.IntStream;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
@State(Scope.Thread)
public class GremlinGroovyScriptEngineBenchmark extends AbstractBenchmarkBase {
    private final static GremlinGroovyScriptEngine scriptEngine = new GremlinGroovyScriptEngine();
    private final static Graph graph = EmptyGraph.instance();
    private final static GraphTraversalSource g = graph.traversal();
    private final static Bindings globalBindings = new SimpleBindings(new HashMap<String,Object>() {{ put("g", g); }} );
    private final static Random rand = new Random(989023843L);

    private Bindings localBindings;
    private String generatedGremlinCached;
    private String generatedGremlinNoBindings;
    private String generatedGremlinWithBindings;

    @Setup(Level.Trial)
    public void setupTrial() {
        generatedGremlinCached = generateScript(100, false);
    }

    @Setup(Level.Invocation)
    public void setupInvocation() {
        localBindings = new SimpleBindings();
        localBindings.put("g", g);
        localBindings.put("x", rand.nextInt());

        generatedGremlinNoBindings = generateScript(rand.nextInt(), false);
        generatedGremlinWithBindings = generateScript(null, true);
    }

    @Benchmark
    public GraphTraversal.Admin<Vertex,Vertex> testEvalShortCached() throws Exception {
        return (GraphTraversal.Admin<Vertex,Vertex>) scriptEngine.eval("g.V(1)", globalBindings);
    }

    @Benchmark
    public GraphTraversal.Admin<Vertex,Vertex> testEvalShortBindings() throws Exception {
        return (GraphTraversal.Admin<Vertex,Vertex>) scriptEngine.eval("g.V(x)", localBindings);
    }

    @Benchmark
    public GraphTraversal.Admin<Vertex,Vertex> testEvalShortNoCache() throws Exception {
        return (GraphTraversal.Admin<Vertex,Vertex>) scriptEngine.eval(String.format("g.V(%s)", rand.nextInt()), globalBindings);
    }

    @Benchmark
    public GraphTraversal.Admin<Vertex,Edge> testEvalLongCached() throws Exception {
        return (GraphTraversal.Admin<Vertex, Edge>) scriptEngine.eval(generatedGremlinCached, globalBindings);
    }

    @Benchmark
    public GraphTraversal.Admin<Vertex,Edge> testEvalLongBindings() throws Exception {
        return (GraphTraversal.Admin<Vertex, Edge>) scriptEngine.eval(generatedGremlinWithBindings, localBindings);
    }

    @Benchmark
    public GraphTraversal.Admin<Vertex,Edge> testEvalLongNoCache() throws Exception {
        return (GraphTraversal.Admin<Vertex, Edge>) scriptEngine.eval(generatedGremlinNoBindings, globalBindings);
    }

    private String generateScript(final Integer id, final boolean binding) {
        if (binding && id != null) throw new IllegalArgumentException("If binding is true then the id should be null");

        final StringBuilder b = new StringBuilder();
        b.append("g.with('evaluationTimeout', 1000L).addV('person').property(id,");

        if (!binding) b.append("'");
        b.append(null == id ? "x" : id);
        if (!binding) b.append("'");
        b.append(").");

        IntStream.range(0, 256).forEach(i -> b.append("addV('person').property(id,").append(i).append(")."));

        b.deleteCharAt(b.length() - 1);

        return b.toString();
    }
}
