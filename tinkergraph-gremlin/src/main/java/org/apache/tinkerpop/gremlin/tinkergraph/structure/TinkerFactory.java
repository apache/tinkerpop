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

import org.apache.commons.configuration2.BaseConfiguration;
import org.apache.commons.configuration2.Configuration;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;

import java.io.File;
import java.io.InputStream;
import java.util.Arrays;
import java.util.List;

import static org.apache.tinkerpop.gremlin.structure.io.IoCore.gryo;

/**
 * Helps create a variety of different toy graphs for testing and learning purposes.
 *
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public final class TinkerFactory {

    private TinkerFactory() {}

    /**
     * Create the "classic" graph which was the original toy graph from TinkerPop 2.x.
     */
    public static TinkerGraph createClassic() {
        final Configuration conf = new BaseConfiguration();
        conf.setProperty(TinkerGraph.GREMLIN_TINKERGRAPH_VERTEX_ID_MANAGER, TinkerGraph.DefaultIdManager.INTEGER.name());
        conf.setProperty(TinkerGraph.GREMLIN_TINKERGRAPH_EDGE_ID_MANAGER, TinkerGraph.DefaultIdManager.INTEGER.name());
        conf.setProperty(TinkerGraph.GREMLIN_TINKERGRAPH_VERTEX_PROPERTY_ID_MANAGER, TinkerGraph.DefaultIdManager.INTEGER.name());
        final TinkerGraph g = TinkerGraph.open(conf);
        generateClassic(g);
        return g;
    }

    /**
     * Generate the graph in {@link #createClassic()} into an existing graph.
     */
    public static void generateClassic(final TinkerGraph g) {
        final Vertex marko = g.addVertex(T.id, 1, "name", "marko", "age", 29);
        final Vertex vadas = g.addVertex(T.id, 2, "name", "vadas", "age", 27);
        final Vertex lop = g.addVertex(T.id, 3, "name", "lop", "lang", "java");
        final Vertex josh = g.addVertex(T.id, 4, "name", "josh", "age", 32);
        final Vertex ripple = g.addVertex(T.id, 5, "name", "ripple", "lang", "java");
        final Vertex peter = g.addVertex(T.id, 6, "name", "peter", "age", 35);
        marko.addEdge("knows", vadas, T.id, 7, "weight", 0.5f);
        marko.addEdge("knows", josh, T.id, 8, "weight", 1.0f);
        marko.addEdge("created", lop, T.id, 9, "weight", 0.4f);
        josh.addEdge("created", ripple, T.id, 10, "weight", 1.0f);
        josh.addEdge("created", lop, T.id, 11, "weight", 0.4f);
        peter.addEdge("created", lop, T.id, 12, "weight", 0.2f);
    }

    /**
     * Create the "modern" graph which has the same structure as the "classic" graph from TinkerPop 2.x but includes
     * 3.x features like vertex labels.
     */
    public static TinkerGraph createModern() {
        final TinkerGraph g = getTinkerGraphWithNumberManager();
        generateModern(g);
        return g;
    }

    /**
     * Generate the graph in {@link #createModern()} into an existing graph.
     */
    public static void generateModern(final TinkerGraph g) {
        final Vertex marko = g.addVertex(T.id, 1, T.label, "person", "name", "marko", "age", 29);
        final Vertex vadas = g.addVertex(T.id, 2, T.label, "person", "name", "vadas", "age", 27);
        final Vertex lop = g.addVertex(T.id, 3, T.label, "software", "name", "lop", "lang", "java");
        final Vertex josh = g.addVertex(T.id, 4, T.label, "person", "name", "josh", "age", 32);
        final Vertex ripple = g.addVertex(T.id, 5, T.label, "software", "name", "ripple", "lang", "java");
        final Vertex peter = g.addVertex(T.id, 6, T.label, "person", "name", "peter", "age", 35);
        marko.addEdge("knows", vadas, T.id, 7, "weight", 0.5d);
        marko.addEdge("knows", josh, T.id, 8, "weight", 1.0d);
        marko.addEdge("created", lop, T.id, 9, "weight", 0.4d);
        josh.addEdge("created", ripple, T.id, 10, "weight", 1.0d);
        josh.addEdge("created", lop, T.id, 11, "weight", 0.4d);
        peter.addEdge("created", lop, T.id, 12, "weight", 0.2d);
    }

    /**
     * Create the "the crew" graph which is a TinkerPop 3.x toy graph showcasing many 3.x features like meta-properties,
     * multi-properties and graph variables.
     */
    public static TinkerGraph createTheCrew() {
        final Configuration conf = getNumberIdManagerConfiguration();
        conf.setProperty(TinkerGraph.GREMLIN_TINKERGRAPH_DEFAULT_VERTEX_PROPERTY_CARDINALITY, VertexProperty.Cardinality.list.name());
        final TinkerGraph g = TinkerGraph.open(conf);
        generateTheCrew(g);
        return g;
    }

    /**
     * Generate the graph in {@link #createTheCrew()} into an existing graph.
     */
    public static void generateTheCrew(final TinkerGraph g) {
        final Vertex marko = g.addVertex(T.id, 1, T.label, "person", "name", "marko");
        final Vertex stephen = g.addVertex(T.id, 7, T.label, "person", "name", "stephen");
        final Vertex matthias = g.addVertex(T.id, 8, T.label, "person", "name", "matthias");
        final Vertex daniel = g.addVertex(T.id, 9, T.label, "person", "name", "daniel");
        final Vertex gremlin = g.addVertex(T.id, 10, T.label, "software", "name", "gremlin");
        final Vertex tinkergraph = g.addVertex(T.id, 11, T.label, "software", "name", "tinkergraph");

        marko.property(VertexProperty.Cardinality.list, "location", "san diego", "startTime", 1997, "endTime", 2001);
        marko.property(VertexProperty.Cardinality.list, "location", "santa cruz", "startTime", 2001, "endTime", 2004);
        marko.property(VertexProperty.Cardinality.list, "location", "brussels", "startTime", 2004, "endTime", 2005);
        marko.property(VertexProperty.Cardinality.list, "location", "santa fe", "startTime", 2005);

        stephen.property(VertexProperty.Cardinality.list, "location", "centreville", "startTime", 1990, "endTime", 2000);
        stephen.property(VertexProperty.Cardinality.list, "location", "dulles", "startTime", 2000, "endTime", 2006);
        stephen.property(VertexProperty.Cardinality.list, "location", "purcellville", "startTime", 2006);

        matthias.property(VertexProperty.Cardinality.list, "location", "bremen", "startTime", 2004, "endTime", 2007);
        matthias.property(VertexProperty.Cardinality.list, "location", "baltimore", "startTime", 2007, "endTime", 2011);
        matthias.property(VertexProperty.Cardinality.list, "location", "oakland", "startTime", 2011, "endTime", 2014);
        matthias.property(VertexProperty.Cardinality.list, "location", "seattle", "startTime", 2014);

        daniel.property(VertexProperty.Cardinality.list, "location", "spremberg", "startTime", 1982, "endTime", 2005);
        daniel.property(VertexProperty.Cardinality.list, "location", "kaiserslautern", "startTime", 2005, "endTime", 2009);
        daniel.property(VertexProperty.Cardinality.list, "location", "aachen", "startTime", 2009);

        marko.addEdge("develops", gremlin, T.id, 13, "since", 2009);
        marko.addEdge("develops", tinkergraph, T.id, 14, "since", 2010);
        marko.addEdge("uses", gremlin, T.id, 15, "skill", 4);
        marko.addEdge("uses", tinkergraph, T.id, 16, "skill", 5);

        stephen.addEdge("develops", gremlin, T.id, 17, "since", 2010);
        stephen.addEdge("develops", tinkergraph, T.id, 18, "since", 2011);
        stephen.addEdge("uses", gremlin, T.id, 19, "skill", 5);
        stephen.addEdge("uses", tinkergraph, T.id, 20, "skill", 4);

        matthias.addEdge("develops", gremlin, T.id, 21, "since", 2012);
        matthias.addEdge("uses", gremlin, T.id, 22, "skill", 3);
        matthias.addEdge("uses", tinkergraph, T.id, 23, "skill", 3);

        daniel.addEdge("uses", gremlin, T.id, 24, "skill", 5);
        daniel.addEdge("uses", tinkergraph, T.id, 25, "skill", 3);

        gremlin.addEdge("traverses", tinkergraph, T.id, 26);

        g.variables().set("creator", "marko");
        g.variables().set("lastModified", 2014);
        g.variables().set("comment", "this graph was created to provide examples and test coverage for tinkerpop3 api advances");
    }

    /**
     * Creates the "kitchen sink" graph which is a collection of structures (e.g. self-loops) that aren't represented
     * in other graphs and are useful for various testing scenarios.
     */
    public static TinkerGraph createKitchenSink() {
        final TinkerGraph g = getTinkerGraphWithNumberManager();
        generateKitchenSink(g);
        return g;
    }

    /**
     * Generate the graph in {@link #createKitchenSink()} into an existing graph.
     */
    public static void generateKitchenSink(final TinkerGraph graph) {
        final GraphTraversalSource g = graph.traversal();
        g.addV("loops").property(T.id, 1000).property("name", "loop").as("me").
          addE("self").to("me").property(T.id, 1001).
          iterate();
        g.addV("message").property(T.id, 2000).property("name", "a").as("a").
          addV("message").property(T.id, 2001).property("name", "b").as("b").
          addE("link").from("a").to("b").property(T.id, 2002).
          addE("link").from("a").to("a").property(T.id, 2003).iterate();
    }

    /**
     * Creates the "grateful dead" graph which is a larger graph than most of the toy graphs but has real-world
     * structure and application and is therefore useful for demonstrating more complex traversals.
     */
    public static TinkerGraph createGratefulDead() {
        final TinkerGraph g = getTinkerGraphWithNumberManager();
        generateGratefulDead(g);
        return g;
    }

    /**
     * Generate the graph in {@link #createGratefulDead()} into an existing graph.
     */
    public static void generateGratefulDead(final TinkerGraph graph) {
        final InputStream stream = TinkerFactory.class.getResourceAsStream("grateful-dead.kryo");
        try {
            graph.io(gryo()).reader().create().readGraph(stream, graph);
        } catch (Exception ex) {
            throw new IllegalStateException(ex);
        }
    }

    private static TinkerGraph getTinkerGraphWithNumberManager() {
        final Configuration conf = getNumberIdManagerConfiguration();
        return TinkerGraph.open(conf);
    }

    private static Configuration getNumberIdManagerConfiguration() {
        final Configuration conf = new BaseConfiguration();
        conf.setProperty(TinkerGraph.GREMLIN_TINKERGRAPH_VERTEX_ID_MANAGER, TinkerGraph.DefaultIdManager.INTEGER.name());
        conf.setProperty(TinkerGraph.GREMLIN_TINKERGRAPH_EDGE_ID_MANAGER, TinkerGraph.DefaultIdManager.INTEGER.name());
        conf.setProperty(TinkerGraph.GREMLIN_TINKERGRAPH_VERTEX_PROPERTY_ID_MANAGER, TinkerGraph.DefaultIdManager.LONG.name());
        return conf;
    }
}
