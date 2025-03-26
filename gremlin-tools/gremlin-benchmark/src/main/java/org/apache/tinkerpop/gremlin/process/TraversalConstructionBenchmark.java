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

import org.apache.tinkerpop.benchmark.util.AbstractGraphBenchmark;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.step.GValue;
import org.openjdk.jmh.annotations.Benchmark;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.as;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.inE;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.out;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.project;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.values;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class TraversalConstructionBenchmark  extends AbstractGraphBenchmark {

    private static final Map<String,Object> m = new HashMap<String,Object>() {{
        put("k0", "v0");
        put("k1", "v1");
        put("k2", "v2");
        put("k3", "v3");
        put("k4", "v4");
        put("k5", "v5");
        put("k6", "v6");
        put("k7", "v7");
        put("k8", "v8");
        put("k9", "v9");
    }};

    @Benchmark
    public GraphTraversal constructShort() throws Exception {
        return g.V().out("knows");
    }

    @Benchmark
    public GraphTraversal constructShortWithMapArgument() throws Exception {
        return g.withSideEffect("m", m).V().out("knows");
    }

    @Benchmark
    public GraphTraversal constructMedium() throws Exception {
        return g.V().has("person","name","marko").
                project("user","knows","created").
                by(project("name","age").by("name").by("age")).
                by(out("knows").project("name","age").by("name").by("age")).
                by(out("created").project("name","lang").by("name").by("lang"));
    }

    @Benchmark
    public GraphTraversal constructMediumWithBindings() {
        return g.V().has("person","name", GValue.of("x","marko")).
                project("user","knows","created").
                by(project("name","age").by("name").by("age")).
                by(out("knows").project("name","age").by("name").by("age")).
                by(out("created").project("name","lang").by("name").by("lang"));
    }

    @Benchmark
    public GraphTraversal constructLong() throws Exception {
        return g.V().
                match(as("a").has("song", "name", "HERE COMES SUNSHINE"),
                      as("a").map(inE("followedBy").values("weight").mean()).as("b"),
                      as("a").inE("followedBy").as("c"),
                      as("c").filter(values("weight").where(P.gte("b"))).outV().as("d")).
                select("d").by("name");
    }

    @Benchmark
    public GraphTraversal testAddVWithPropsChained() {
        // construct a traversal that adds 100 vertices with 32 properties each
        GraphTraversal t = null;
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

        return t;
    }

    @Benchmark
    public GraphTraversal testAddVAddEWithPropsChained() {
        // construct a traversal that adds 100 vertices with 32 properties each as well as 300 edges with 8
        // properties each
        final Random rand = new Random(584545454L);

        GraphTraversal t = null;
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

        return t;
    }
}
