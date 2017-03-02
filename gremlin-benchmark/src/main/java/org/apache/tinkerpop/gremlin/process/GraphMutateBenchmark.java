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

import org.apache.tinkerpop.benchmark.util.AbstractGraphMutateBenchmark;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Setup;

import java.util.Random;

/**
 * {@code GraphMutateBenchmark} benchmarks {@link org.apache.tinkerpop.gremlin.process.traversal.Traversal} and
 * {@link org.apache.tinkerpop.gremlin.structure.Graph} mutation methods.
 *
 * @author Ted Wilmes (http://twilmes.org)
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class GraphMutateBenchmark extends AbstractGraphMutateBenchmark {

    private Vertex a;
    private Vertex b;
    private Vertex c;
    private Edge e;

    @Setup
    @Override
    public void prepare() {
        super.prepare();
        a = g.addV().next();
        b = g.addV().next();
        c = g.addV().next();
        e = b.addEdge("knows", c);
    }

    @Benchmark
    public Vertex testAddVertex() {
        return graph.addVertex("test");
    }

    @Benchmark
    public Vertex testAddVertexWithProps() {
        final Vertex v = graph.addVertex("test");
        for (int iy = 0; iy < 32; iy++) {
            if (iy % 2 == 0)
                v.property("x" + String.valueOf(iy), iy);
            else
                v.property("x" + String.valueOf(iy), String.valueOf(iy));
        }

        return v;
    }

    @Benchmark
    public VertexProperty testVertexProperty() {
        return a.property("name", "Susan");
    }

    @Benchmark
    public Edge testAddEdge() {
        return a.addEdge("knows", b);
    }

    @Benchmark
    public Property testEdgeProperty() {
        return e.property("met", 1967);
    }

    @Benchmark
    public Vertex testAddV() {
        return g.addV("test").next();
    }

    @Benchmark
    public Vertex testAddVWithProps() {
        GraphTraversal<Vertex, Vertex> t = g.addV("test");
        for (int iy = 0; iy < 32; iy++) {
            if (iy % 2 == 0)
                t = t.property("x" + String.valueOf(iy), iy);
            else
                t = t.property("x" + String.valueOf(iy), String.valueOf(iy));
        }
        return t.next();
    }

    @Benchmark
    public Vertex testVertexPropertyStep() {
        return g.V(a).property("name", "Susan").next();
    }

    @Benchmark
    public Vertex testAddVWithPropsChained() {
        // construct a traversal that adds 100 vertices with 32 properties each
        GraphTraversal<Vertex, Vertex> t = null;
        for (int ix = 0; ix < 100; ix++) {
            if (null == t)
                t = g.addV("person");
            else
                t = t.addV("person");

            for (int iy = 0; iy < 32; iy++) {
                if (iy % 2 == 0)
                    t = t.property("x" + String.valueOf(iy), iy * ix);
                else
                    t = t.property("x" + String.valueOf(iy), String.valueOf(iy + ix));
            }
        }

        return t.next();
    }

    @Benchmark
    public Edge testAddVAddEWithPropsChained() {
        // construct a traversal that adds 100 vertices with 32 properties each as well as 300 edges with 8
        // properties each
        final Random rand = new Random(584545454L);

        GraphTraversal<Vertex, ?> t = null;
        for (int ix = 0; ix < 10; ix++) {
            if (null == t)
                t = g.addV("person");
            else
                t = t.addV("person");

            for (int iy = 0; iy < 32; iy++) {
                if (iy % 2 == 0)
                    t = t.property("x" + String.valueOf(iy), iy * ix);
                else
                    t = t.property("x" + String.valueOf(iy), String.valueOf(iy + ix));
            }

            t = t.as("person" + ix);

            if (ix > 0) {
                int edgeCount = ix == 9 ? 6 : 3;
                for (int ie = 0; ie < edgeCount; ie++) {
                    t = t.addE("knows").from("person" + ix).to("person" + rand.nextInt(ix));

                    for (int iy = 0; iy < 8; iy++) {
                        if (iy % 2 == 0)
                            t = t.property("x" + String.valueOf(iy), iy * ie);
                        else
                            t = t.property("x" + String.valueOf(iy), String.valueOf(iy + ie));
                    }
                }
            }
        }

        final Edge e = (Edge) t.next();
        return e;
    }

    @Benchmark
    public Edge testAddE() {
        return g.V(a).as("a").V(b).as("b").addE("knows").from("a").to("b").next();
    }

    @Benchmark
    public Edge testEdgePropertyStep() {
        return g.E(e).property("met", 1967).next();
    }
}
