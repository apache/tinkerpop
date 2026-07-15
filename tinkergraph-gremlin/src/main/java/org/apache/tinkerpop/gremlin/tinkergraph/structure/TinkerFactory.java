/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
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
import org.apache.tinkerpop.gremlin.structure.LabelCardinality;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;

import java.io.InputStream;

import static org.apache.tinkerpop.gremlin.structure.io.IoCore.gryo;
import static org.apache.tinkerpop.gremlin.tinkergraph.structure.AbstractTinkerGraph.DefaultIdManager;
import static org.apache.tinkerpop.gremlin.tinkergraph.structure.AbstractTinkerGraph.DefaultIdManager.INTEGER;
import static org.apache.tinkerpop.gremlin.tinkergraph.structure.AbstractTinkerGraph.DefaultIdManager.LONG;

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
        final TinkerGraph g = getTinkerGraphWithClassicNumberManager();
        generateClassic(g);
        return g;
    }

    /**
     * Generate the graph in {@link #createClassic()} into an existing graph.
     */
    public static void generateClassic(final AbstractTinkerGraph g) {
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
        final TinkerGraph g = getTinkerGraphWithCurrentNumberManager();
        generateModern(g);
        return g;
    }

    /**
     * Generate the graph in {@link #createModern()} into an existing graph.
     */
    public static void generateModern(final AbstractTinkerGraph g) {
        final Vertex marko = g.addVertex(T.id, 1, T.label, "person");
        marko.property("name", "marko", T.id, 0l);
        marko.property("age", 29, T.id, 1l);
        final Vertex vadas = g.addVertex(T.id, 2, T.label, "person");
        vadas.property("name", "vadas", T.id, 2l);
        vadas.property("age", 27, T.id, 3l);
        final Vertex lop = g.addVertex(T.id, 3, T.label, "software");
        lop.property("name", "lop", T.id, 4l);
        lop.property("lang", "java", T.id, 5l);
        final Vertex josh = g.addVertex(T.id, 4, T.label, "person");
        josh.property("name", "josh", T.id, 6l);
        josh.property("age", 32, T.id, 7l);
        final Vertex ripple = g.addVertex(T.id, 5, T.label, "software");
        ripple.property("name", "ripple", T.id, 8l);
        ripple.property("lang", "java", T.id, 9l);
        final Vertex peter = g.addVertex(T.id, 6, T.label, "person");
        peter.property("name", "peter", T.id, 10l);
        peter.property("age", 35, T.id, 11l);

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
        final Configuration conf = getConfigurationWithCurrentNumberManager();
        conf.setProperty(TinkerGraph.GREMLIN_TINKERGRAPH_DEFAULT_VERTEX_PROPERTY_CARDINALITY, VertexProperty.Cardinality.list.name());
        final TinkerGraph g = TinkerGraph.open(conf);
        generateTheCrew(g);
        return g;
    }

    /**
     * Generate the graph in {@link #createTheCrew()} into an existing graph.
     */
    public static void generateTheCrew(final AbstractTinkerGraph g) {
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
        final TinkerGraph g = getTinkerGraphWithCurrentNumberManager();
        generateKitchenSink(g);
        return g;
    }

    /**
     * Generate the graph in {@link #createKitchenSink()} into an existing graph.
     */
    public static void generateKitchenSink(final AbstractTinkerGraph graph) {
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
        final TinkerGraph g = getTinkerGraphWithCurrentNumberManager();
        generateGratefulDead(g);
        return g;
    }

    /**
     * Generate the graph in {@link #createGratefulDead()} into an existing graph.
     */
    public static void generateGratefulDead(final AbstractTinkerGraph graph) {
        final InputStream stream = TinkerFactory.class.getResourceAsStream("grateful-dead.kryo");
        if (null == stream) {
            throw new IllegalStateException("The dataset is not accessible - ensure that you are not using tinkergraph-gremlin with the 'min' classifier");
        }

        try {
            graph.io(gryo()).reader().create().readGraph(stream, graph);
        } catch (Exception ex) {
            throw new IllegalStateException(ex);
        }
    }

    /**
     * Creates the "air-routes" graph which is a larger graph than most of the toy graphs but has real-world
     * structure and application and is therefore useful for demonstrating more complex traversals.
     */
    public static TinkerGraph createAirRoutes() {
        final TinkerGraph g = getTinkerGraphWithCurrentNumberManager();
        generateAirRoutes(g);
        return g;
    }

    /**
     * Generate the graph in {@link #createAirRoutes()} into an existing graph.
     */
    public static void generateAirRoutes(final AbstractTinkerGraph graph) {
        final InputStream stream = TinkerFactory.class.getResourceAsStream("air-routes.kryo");

        if (null == stream) {
            throw new IllegalStateException("The dataset is not accessible - ensure that you are not using tinkergraph-gremlin with the 'min' classifier");
        }

        try {
            graph.io(gryo()).reader().create().readGraph(stream, graph);
        } catch (Exception ex) {
            throw new IllegalStateException(ex);
        }
    }

    /**
     * Create "the zoo" graph — a TinkerPop 4.x toy graph showcasing multi-label vertex support
     * and diverse property types. Contains 13 vertices, 20 edges with animals classified across
     * taxonomy, behavior, and conservation axes.
     */
    public static TinkerGraph createTheZoo() {
        final Configuration conf = getConfigurationWithCurrentNumberManager();
        conf.setProperty(AbstractTinkerGraph.GREMLIN_TINKERGRAPH_VERTEX_LABEL_CARDINALITY, LabelCardinality.ZERO_OR_MORE.name());
        final TinkerGraph g = TinkerGraph.open(conf);
        generateTheZoo(g);
        return g;
    }

    /**
     * Generate the graph in {@link #createTheZoo()} into an existing graph. The graph must support
     * multi-label vertices (LabelCardinality of ONE_OR_MORE or ZERO_OR_MORE).
     */
    public static void generateTheZoo(final AbstractTinkerGraph g) {
        // Animals
        final Vertex tux = g.addVertex(T.id, 1, T.label, "animal");
        tux.addLabel("bird", "aquatic", "endangered");
        tux.property("name", "tux");
        tux.property("species", "african penguin");
        tux.property("weight", 3.7d);
        tux.property("age", 5);
        tux.property("captiveBorn", true);
        tux.property(VertexProperty.Cardinality.list, "diet", "fish");
        tux.property(VertexProperty.Cardinality.list, "diet", "krill");
        tux.property(VertexProperty.Cardinality.list, "diet", "squid");

        final Vertex atlas = g.addVertex(T.id, 2, T.label, "animal");
        atlas.addLabel("reptile", "aquatic", "endangered");
        atlas.property("name", "atlas");
        atlas.property("species", "green sea turtle");
        atlas.property("weight", 180.5d);
        atlas.property("age", 30);
        atlas.property("captiveBorn", false);
        atlas.property(VertexProperty.Cardinality.list, "diet", "seagrass");
        atlas.property(VertexProperty.Cardinality.list, "diet", "algae");

        final Vertex ripple = g.addVertex(T.id, 3, T.label, "animal");
        ripple.addLabel("mammal", "aquatic");
        ripple.property("name", "ripple");
        ripple.property("species", "bottlenose dolphin");
        ripple.property("weight", 220.0d);
        ripple.property("age", 12);
        ripple.property("captiveBorn", true);
        ripple.property(VertexProperty.Cardinality.list, "diet", "fish");
        ripple.property(VertexProperty.Cardinality.list, "diet", "squid");

        final Vertex monty = g.addVertex(T.id, 4, T.label, "animal");
        monty.addLabel("reptile", "nocturnal");
        monty.property("name", "monty");
        monty.property("species", "ball python");
        monty.property("weight", 1.8d);
        monty.property("age", 8);
        monty.property("captiveBorn", true);
        monty.property("venomous", false);
        monty.property(VertexProperty.Cardinality.list, "diet", "mice");
        monty.property(VertexProperty.Cardinality.list, "diet", "rats");

        final Vertex echo = g.addVertex(T.id, 5, T.label, "animal");
        echo.addLabel("mammal", "flying", "nocturnal");
        echo.property("name", "echo");
        echo.property("species", "fruit bat");
        echo.property("weight", 0.3d);
        echo.property("age", 3);
        echo.property("captiveBorn", true);
        echo.property(VertexProperty.Cardinality.list, "diet", "fruit");
        echo.property(VertexProperty.Cardinality.list, "diet", "nectar");

        final Vertex blaze = g.addVertex(T.id, 6, T.label, "animal");
        blaze.addLabel("mammal", "endangered", "nocturnal");
        blaze.property("name", "blaze");
        blaze.property("species", "red panda");
        blaze.property("weight", 5.4d);
        blaze.property("age", 4);
        blaze.property("captiveBorn", false);
        blaze.property(VertexProperty.Cardinality.list, "diet", "bamboo");
        blaze.property(VertexProperty.Cardinality.list, "diet", "fruit");
        blaze.property(VertexProperty.Cardinality.list, "diet", "insects");

        final Vertex titan = g.addVertex(T.id, 7, T.label, "animal");
        titan.addLabel("mammal", "endangered");
        titan.property("name", "titan");
        titan.property("species", "african elephant");
        titan.property("weight", 4000.0d);
        titan.property("age", 15);
        titan.property("captiveBorn", false);
        titan.property(VertexProperty.Cardinality.list, "diet", "grass");
        titan.property(VertexProperty.Cardinality.list, "diet", "leaves");
        titan.property(VertexProperty.Cardinality.list, "diet", "bark");

        final Vertex bitsy = g.addVertex(T.id, 8, T.label, "animal");
        bitsy.addLabel("mammal", "nocturnal");
        bitsy.property("name", "bitsy");
        bitsy.property("species", "harvest mouse");
        bitsy.property("weight", 0.006d);
        bitsy.property("age", 1);
        bitsy.property("captiveBorn", true);
        bitsy.property(VertexProperty.Cardinality.list, "diet", "seeds");
        bitsy.property(VertexProperty.Cardinality.list, "diet", "insects");

        final Vertex splash = g.addVertex(T.id, 9, T.label, "animal");
        splash.addLabel("mammal", "aquatic", "nocturnal", "endangered");
        splash.property("name", "splash");
        splash.property("species", "fishing cat");
        splash.property("weight", 8.2d);
        splash.property("age", 2);
        splash.property("captiveBorn", false);
        splash.property(VertexProperty.Cardinality.list, "diet", "fish");
        splash.property(VertexProperty.Cardinality.list, "diet", "frogs");
        splash.property(VertexProperty.Cardinality.list, "diet", "crustaceans");

        final Vertex tinker = g.addVertex(T.id, 10, T.label, "animal");
        tinker.addLabel("mammal", "nocturnal", "endangered");
        tinker.property("name", "tinker");
        tinker.property("species", "bengal tiger");
        tinker.property("weight", 220.0d);
        tinker.property("age", 5);
        tinker.property("captiveBorn", false);
        tinker.property(VertexProperty.Cardinality.list, "diet", "deer");
        tinker.property(VertexProperty.Cardinality.list, "diet", "boar");
        tinker.property(VertexProperty.Cardinality.list, "diet", "snakes");

        // Habitats
        final Vertex lagoon = g.addVertex(T.id, 11, T.label, "habitat");
        lagoon.addLabel("aquatic");
        lagoon.property("name", "lagoon");
        lagoon.property("biome", "marine");
        lagoon.property("capacity", 8);
        lagoon.property("openAir", true);

        final Vertex canopy = g.addVertex(T.id, 12, T.label, "habitat");
        canopy.property("name", "canopy");
        canopy.property("biome", "tropical");
        canopy.property("capacity", 8);
        canopy.property("openAir", false);

        // People
        final Vertex drGremlin = g.addVertex(T.id, 13, T.label, "person");
        drGremlin.addLabel("veterinarian", "keeper");
        drGremlin.property("name", "dr_gremlin");
        drGremlin.property("since", 2015);
        drGremlin.property(VertexProperty.Cardinality.list, "specialties", "conservation");
        drGremlin.property(VertexProperty.Cardinality.list, "specialties", "surgery");
        drGremlin.property(VertexProperty.Cardinality.list, "specialties", "nutrition");

        // Edges: livesIn
        tux.addEdge("livesIn", lagoon, T.id, 14, "since", 2020);
        atlas.addEdge("livesIn", lagoon, T.id, 15, "since", 2018);
        ripple.addEdge("livesIn", lagoon, T.id, 16, "since", 2019);
        splash.addEdge("livesIn", lagoon, T.id, 17, "since", 2023);
        monty.addEdge("livesIn", canopy, T.id, 18, "since", 2021);
        echo.addEdge("livesIn", canopy, T.id, 19, "since", 2022);
        blaze.addEdge("livesIn", canopy, T.id, 20, "since", 2023);
        bitsy.addEdge("livesIn", canopy, T.id, 21, "since", 2024);
        tinker.addEdge("livesIn", canopy, T.id, 22, "since", 2020);
        titan.addEdge("livesIn", canopy, T.id, 23, "since", 2019);

        // Edges: careFor
        drGremlin.addEdge("careFor", atlas, T.id, 24, "specialty", "conservation");
        drGremlin.addEdge("careFor", blaze, T.id, 25, "specialty", "conservation");
        drGremlin.addEdge("careFor", splash, T.id, 26, "specialty", "conservation");
        drGremlin.addEdge("careFor", tinker, T.id, 27, "specialty", "conservation");

        // Edges: friendsWith
        tux.addEdge("friendsWith", atlas, T.id, 28, "since", 2020);
        ripple.addEdge("friendsWith", tux, T.id, 29, "since", 2020);
        titan.addEdge("friendsWith", blaze, T.id, 30, "since", 2022);

        // Edges: eats (food chain: tinker → monty → bitsy)
        tinker.addEdge("eats", monty, T.id, 31);
        monty.addEdge("eats", bitsy, T.id, 32);

        // Edges: avoids
        echo.addEdge("avoids", bitsy, T.id, 33);

        g.variables().set("name", "the-zoo");
        g.variables().set("creator", "tinkerpop");
        g.variables().set("comment",
                "this graph showcases multi-label vertex support and diverse " +
                "property types introduced in tinkerpop 4.x");
    }

    private static TinkerGraph getTinkerGraphWithCurrentNumberManager() {
        return TinkerGraph.open(getConfigurationWithCurrentNumberManager());
    }

    private static Configuration getConfigurationWithCurrentNumberManager() {
        return getNumberIdManagerConfiguration(INTEGER, INTEGER, LONG);
    }

    private static TinkerGraph getTinkerGraphWithClassicNumberManager() {
        return TinkerGraph.open(getConfigurationHWithClassicNumberManager());
    }

    private static Configuration getConfigurationHWithClassicNumberManager() {
        return getNumberIdManagerConfiguration(INTEGER, INTEGER, INTEGER);
    }

    private static Configuration getNumberIdManagerConfiguration(final DefaultIdManager v, final DefaultIdManager e,
                                                                 final DefaultIdManager vp) {
        final Configuration conf = new BaseConfiguration();
        conf.setProperty(TinkerGraph.GREMLIN_TINKERGRAPH_VERTEX_ID_MANAGER, v.name());
        conf.setProperty(TinkerGraph.GREMLIN_TINKERGRAPH_EDGE_ID_MANAGER, e.name());
        conf.setProperty(TinkerGraph.GREMLIN_TINKERGRAPH_VERTEX_PROPERTY_ID_MANAGER, vp.name());
        return conf;
    }
}
