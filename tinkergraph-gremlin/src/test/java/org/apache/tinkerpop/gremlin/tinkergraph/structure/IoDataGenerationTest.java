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

import org.apache.commons.io.FileUtils;
import org.apache.tinkerpop.gremlin.AbstractGremlinTest;
import org.apache.tinkerpop.gremlin.TestHelper;
import org.apache.tinkerpop.gremlin.algorithm.generator.DistributionGenerator;
import org.apache.tinkerpop.gremlin.algorithm.generator.PowerLawDistribution;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.io.GraphReader;
import org.apache.tinkerpop.gremlin.structure.io.graphml.GraphMLWriter;
import org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONMapper;
import org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONVersion;
import org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONWriter;
import org.apache.tinkerpop.gremlin.structure.io.graphson.TypeInfo;
import org.apache.tinkerpop.gremlin.structure.io.gryo.GryoMapper;
import org.apache.tinkerpop.gremlin.structure.io.gryo.GryoReader;
import org.apache.tinkerpop.gremlin.structure.io.gryo.GryoVersion;
import org.apache.tinkerpop.gremlin.structure.io.gryo.GryoWriter;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.stream.IntStream;

/**
 * Less of a test of functionality and more of a tool to help generate data files for TinkerPop.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class IoDataGenerationTest {
    private File tempPath;

    @Before
    public void before() throws IOException {
        tempPath = TestHelper.makeTestDataPath(TinkerGraphTest.class, "tinkerpop-io");

        FileUtils.deleteDirectory(tempPath);
        if (!tempPath.mkdirs()) throw new IOException(String.format("Could not create %s", tempPath));
    }

    /**
     * No assertions.  Just write out the graph for convenience.
     */
    @Test
    public void shouldWriteClassicGraphAsGryoV1() throws IOException {
        final OutputStream os = new FileOutputStream(new File(tempPath, "tinkerpop-classic-v1.kryo"));
        GryoWriter.build().mapper(GryoMapper.build().version(GryoVersion.V1_0).create()).create().writeGraph(os, TinkerFactory.createClassic());
        os.close();
    }

    /**
     * No assertions.  Just write out the graph for convenience.
     */
    @Test
    public void shouldWriteModernGraphAsGryoV1() throws IOException {
        final OutputStream os = new FileOutputStream(new File(tempPath, "tinkerpop-modern-v1.kryo"));
        GryoWriter.build().mapper(GryoMapper.build().version(GryoVersion.V1_0).create()).create().writeGraph(os, TinkerFactory.createModern());
        os.close();
    }

    /**
     * No assertions.  Just write out the graph for convenience.
     */
    @Test
    public void shouldWriteCrewGraphAsGryoV1() throws IOException {
        final OutputStream os = new FileOutputStream(new File(tempPath, "tinkerpop-crew-v1.kryo"));
        GryoWriter.build().mapper(GryoMapper.build().version(GryoVersion.V1_0).create()).create().writeGraph(os, TinkerFactory.createTheCrew());
        os.close();
    }

    /**
     * No assertions.  Just write out the graph for convenience.
     */
    @Test
    public void shouldWriteSinkGraphAsGryoV1() throws IOException {
        final OutputStream os = new FileOutputStream(new File(tempPath, "tinkerpop-sink-v1.kryo"));
        GryoWriter.build().mapper(GryoMapper.build().version(GryoVersion.V1_0).create()).create().writeGraph(os, TinkerFactory.createKitchenSink());
        os.close();
    }

    /**
     * No assertions.  Just write out the graph for convenience.
     */
    @Test
    public void shouldWriteClassicGraphAsGryoV3() throws IOException {
        final OutputStream os = new FileOutputStream(new File(tempPath, "tinkerpop-classic-v3.kryo"));
        GryoWriter.build().mapper(GryoMapper.build().version(GryoVersion.V3_0).create()).create().writeGraph(os, TinkerFactory.createClassic());
        os.close();
    }

    /**
     * No assertions.  Just write out the graph for convenience.
     */
    @Test
    public void shouldWriteModernGraphAsGryoV3() throws IOException {
        final OutputStream os = new FileOutputStream(new File(tempPath, "tinkerpop-modern-v3.kryo"));
        GryoWriter.build().mapper(GryoMapper.build().version(GryoVersion.V3_0).create()).create().writeGraph(os, TinkerFactory.createModern());
        os.close();
    }

    /**
     * No assertions.  Just write out the graph for convenience.
     */
    @Test
    public void shouldWriteCrewGraphAsGryoV3() throws IOException {
        final OutputStream os = new FileOutputStream(new File(tempPath, "tinkerpop-crew-v3.kryo"));
        GryoWriter.build().mapper(GryoMapper.build().version(GryoVersion.V3_0).create()).create().writeGraph(os, TinkerFactory.createTheCrew());
        os.close();
    }

    /**
     * No assertions.  Just write out the graph for convenience.
     */
    @Test
    public void shouldWriteSinkGraphAsGryoV3() throws IOException {
        final OutputStream os = new FileOutputStream(new File(tempPath, "tinkerpop-sink-v3.kryo"));
        GryoWriter.build().mapper(GryoMapper.build().version(GryoVersion.V3_0).create()).create().writeGraph(os, TinkerFactory.createKitchenSink());
        os.close();
    }

    /**
     * No assertions.  Just write out the graph for convenience.
     */
    @Test
    public void shouldWriteDEFAULTClassicGraphAsGryoV3() throws IOException {
        final OutputStream os = new FileOutputStream(new File(tempPath, "tinkerpop-classic.kryo"));
        GryoWriter.build().mapper(GryoMapper.build().version(GryoVersion.V3_0).create()).create().writeGraph(os, TinkerFactory.createClassic());
        os.close();
    }

    /**
     * No assertions.  Just write out the graph for convenience.
     */
    @Test
    public void shouldWriteDEFAULTModernGraphAsGryoV3() throws IOException {
        final OutputStream os = new FileOutputStream(new File(tempPath, "tinkerpop-modern.kryo"));
        GryoWriter.build().mapper(GryoMapper.build().version(GryoVersion.V3_0).create()).create().writeGraph(os, TinkerFactory.createModern());
        os.close();
    }

    /**
     * No assertions.  Just write out the graph for convenience.
     */
    @Test
    public void shouldWriteDEFAULTCrewGraphAsGryoV3() throws IOException {
        final OutputStream os = new FileOutputStream(new File(tempPath, "tinkerpop-crew.kryo"));
        GryoWriter.build().mapper(GryoMapper.build().version(GryoVersion.V3_0).create()).create().writeGraph(os, TinkerFactory.createTheCrew());
        os.close();
    }

    /**
     * No assertions.  Just write out the graph for convenience.
     */
    @Test
    public void shouldWriteDEFAULTSinkGraphAsGryoV3() throws IOException {
        final OutputStream os = new FileOutputStream(new File(tempPath, "tinkerpop-sink.kryo"));
        GryoWriter.build().mapper(GryoMapper.build().version(GryoVersion.V3_0).create()).create().writeGraph(os, TinkerFactory.createKitchenSink());
        os.close();
    }

    /**
     * No assertions.  Just write out the graph for convenience.
     */
    @Test
    public void shouldWriteClassicGraphAsGraphML() throws IOException {
        try (final OutputStream os = new FileOutputStream(new File(tempPath, "tinkerpop-classic.xml"))) {
            GraphMLWriter.build().create().writeGraph(os, TinkerFactory.createClassic());
        }
    }

    /**
     * No assertions.  Just write out the graph for convenience.
     */
    @Test
    public void shouldWriteModernGraphAsGraphML() throws IOException {
        try (final OutputStream os = new FileOutputStream(new File(tempPath, "tinkerpop-modern.xml"))) {
            GraphMLWriter.build().create().writeGraph(os, TinkerFactory.createModern());
        }
    }

    /**
     * No assertions.  Just write out the graph for convenience.
     */
    @Test
    public void shouldWriteClassicGraphAsGraphSONV1NoTypes() throws IOException {
        final OutputStream os = new FileOutputStream(new File(tempPath, "tinkerpop-classic-v1.json"));
        GraphSONWriter.build().mapper(GraphSONMapper.build().version(GraphSONVersion.V1_0).typeInfo(TypeInfo.NO_TYPES).create()).create().writeGraph(os, TinkerFactory.createClassic());
        os.close();
    }

    /**
     * No assertions.  Just write out the graph for convenience.
     */
    @Test
    public void shouldWriteModernGraphAsGraphSONV1NoTypes() throws IOException {
        final OutputStream os = new FileOutputStream(new File(tempPath, "tinkerpop-modern-v1.json"));
        GraphSONWriter.build().mapper(GraphSONMapper.build().version(GraphSONVersion.V1_0).typeInfo(TypeInfo.NO_TYPES).create()).create().writeGraph(os, TinkerFactory.createModern());
        os.close();
    }

    /**
     * No assertions.  Just write out the graph for convenience.
     */
    @Test
    public void shouldWriteCrewGraphAsGraphSONV1NoTypes() throws IOException {
        final OutputStream os = new FileOutputStream(new File(tempPath, "tinkerpop-crew-v1.json"));
        GraphSONWriter.build().mapper(GraphSONMapper.build().version(GraphSONVersion.V1_0).typeInfo(TypeInfo.NO_TYPES).create()).create().writeGraph(os, TinkerFactory.createTheCrew());
        os.close();
    }

    /**
     * No assertions. Just write out the graph for convenience
     */
    @Test
    public void shouldWriteKitchenSinkAsGraphSONNoTypes() throws IOException {
        final OutputStream os = new FileOutputStream(new File(tempPath, "tinkerpop-sink-v1.json"));
        GraphSONWriter.build().mapper(GraphSONMapper.build().version(GraphSONVersion.V1_0).typeInfo(TypeInfo.NO_TYPES).create()).create().writeGraph(os, TinkerFactory.createKitchenSink());
        os.close();
    }

    /**
     * No assertions.  Just write out the graph for convenience.
     */
    @Test
    public void shouldWriteClassicGraphNormalizedAsGraphSONV1() throws IOException {
        final OutputStream os = new FileOutputStream(new File(tempPath, "tinkerpop-classic-normalized-v1.json"));
        GraphSONWriter.build().mapper(GraphSONMapper.build().normalize(true).version(GraphSONVersion.V1_0).typeInfo(TypeInfo.NO_TYPES).create()).create().writeGraph(os, TinkerFactory.createClassic());
        os.close();
    }

    /**
     * No assertions.  Just write out the graph for convenience.
     */
    @Test
    public void shouldWriteModernGraphNormalizedAsGraphSONV1() throws IOException {
        final OutputStream os = new FileOutputStream(new File(tempPath, "tinkerpop-modern-normalized-v1.json"));
        GraphSONWriter.build().mapper(GraphSONMapper.build().normalize(true).version(GraphSONVersion.V1_0).typeInfo(TypeInfo.NO_TYPES).create()).create().writeGraph(os, TinkerFactory.createModern());
        os.close();
    }

    /**
     * No assertions.  Just write out the graph for convenience.
     */
    @Test
    public void shouldWriteClassicGraphAsGraphSONV1WithTypes() throws IOException {
        final OutputStream os = new FileOutputStream(new File(tempPath, "tinkerpop-classic-typed-v1.json"));
        GraphSONWriter.build().mapper(GraphSONMapper.build().version(GraphSONVersion.V1_0).typeInfo(TypeInfo.PARTIAL_TYPES).create())
                .create().writeGraph(os, TinkerFactory.createClassic());
        os.close();
    }

    /**
     * No assertions.  Just write out the graph for convenience.
     */
    @Test
    public void shouldWriteModernGraphAsGraphSONV1WithTypes() throws IOException {
        final OutputStream os = new FileOutputStream(new File(tempPath, "tinkerpop-modern-typed-v1.json"));
        GraphSONWriter.build().mapper(GraphSONMapper.build().version(GraphSONVersion.V1_0).typeInfo(TypeInfo.PARTIAL_TYPES).create())
                .create().writeGraph(os, TinkerFactory.createModern());
        os.close();
    }

    /**
     * No assertions.  Just write out the graph for convenience.
     */
    @Test
    public void shouldWriteCrewGraphAsGraphSONV1WithTypes() throws IOException {
        final OutputStream os = new FileOutputStream(new File(tempPath, "tinkerpop-crew-typed-v1.json"));
        GraphSONWriter.build().mapper(GraphSONMapper.build().version(GraphSONVersion.V1_0).typeInfo(TypeInfo.PARTIAL_TYPES).create())
                .create().writeGraph(os, TinkerFactory.createTheCrew());
        os.close();
    }

    /**
     * No assertions. Just write out the graph for convenience
     */
    @Test
    public void shouldWriteKitchenSinkAsGraphSONWithTypes() throws IOException {
        final OutputStream os = new FileOutputStream(new File(tempPath, "tinkerpop-sink-typed-v1.json"));
        GraphSONWriter.build().mapper(GraphSONMapper.build().version(GraphSONVersion.V1_0).typeInfo(TypeInfo.PARTIAL_TYPES).create())
                .create().writeGraph(os, TinkerFactory.createKitchenSink());
        os.close();
    }

    /**
     * No assertions.  Just write out the graph for convenience.
     */
    @Test
    public void shouldWriteClassicGraphAsGraphSONV2NoTypes() throws IOException {
        final OutputStream os = new FileOutputStream(new File(tempPath, "tinkerpop-classic-v2.json"));
        GraphSONWriter.build().mapper(GraphSONMapper.build().version(GraphSONVersion.V2_0).typeInfo(TypeInfo.NO_TYPES).create()).create()
                .writeGraph(os, TinkerFactory.createClassic());
        os.close();
    }

    /**
     * No assertions.  Just write out the graph for convenience.
     */
    @Test
    public void shouldWriteModernGraphAsGraphSOV2NNoTypes() throws IOException {
        final OutputStream os = new FileOutputStream(new File(tempPath, "tinkerpop-modern-v2.json"));
        GraphSONWriter.build().mapper(GraphSONMapper.build().version(GraphSONVersion.V2_0).typeInfo(TypeInfo.NO_TYPES).create()).create()
                .writeGraph(os, TinkerFactory.createModern());
        os.close();
    }

    /**
     * No assertions.  Just write out the graph for convenience.
     */
    @Test
    public void shouldWriteCrewGraphAsGraphSONV2NoTypes() throws IOException {
        final OutputStream os = new FileOutputStream(new File(tempPath, "tinkerpop-crew-v2.json"));
        GraphSONWriter.build().mapper(GraphSONMapper.build().version(GraphSONVersion.V2_0).typeInfo(TypeInfo.NO_TYPES).create()).create()
                .writeGraph(os, TinkerFactory.createTheCrew());
        os.close();
    }

    /**
     * No assertions. Just write out the graph for convenience
     */
    @Test
    public void shouldWriteKitchenSinkAsGraphSONV2NoTypes() throws IOException {
        final OutputStream os = new FileOutputStream(new File(tempPath, "tinkerpop-sink-v2.json"));
        GraphSONWriter.build().mapper(GraphSONMapper.build().version(GraphSONVersion.V2_0).typeInfo(TypeInfo.NO_TYPES).create()).create()
                .writeGraph(os, TinkerFactory.createKitchenSink());
        os.close();
    }

    /**
     * No assertions.  Just write out the graph for convenience.
     */
    @Test
    public void shouldWriteClassicGraphNormalizedAsGraphSONV2() throws IOException {
        final OutputStream os = new FileOutputStream(new File(tempPath, "tinkerpop-classic-normalized-v2.json"));
        GraphSONWriter.build().mapper(GraphSONMapper.build().version(GraphSONVersion.V2_0).typeInfo(TypeInfo.NO_TYPES).normalize(true).create()).create()
                .writeGraph(os, TinkerFactory.createClassic());
        os.close();
    }

    /**
     * No assertions.  Just write out the graph for convenience.
     */
    @Test
    public void shouldWriteModernGraphNormalizedAsGraphSONV2() throws IOException {
        final OutputStream os = new FileOutputStream(new File(tempPath, "tinkerpop-modern-normalized-v2.json"));
        GraphSONWriter.build().mapper(GraphSONMapper.build().version(GraphSONVersion.V2_0).typeInfo(TypeInfo.NO_TYPES).normalize(true).create()).create()
                .writeGraph(os, TinkerFactory.createModern());
        os.close();
    }

    /**
     * No assertions.  Just write out the graph for convenience.
     */
    @Test
    public void shouldWriteClassicGraphAsGraphSONV2WithTypes() throws IOException {
        final OutputStream os = new FileOutputStream(new File(tempPath, "tinkerpop-classic-typed-v2.json"));
        GraphSONWriter.build().mapper(GraphSONMapper.build().version(GraphSONVersion.V2_0).typeInfo(TypeInfo.PARTIAL_TYPES).create()).create()
                .writeGraph(os, TinkerFactory.createClassic());
        os.close();
    }

    /**
     * No assertions.  Just write out the graph for convenience.
     */
    @Test
    public void shouldWriteModernGraphAsGraphSONV2WithTypes() throws IOException {
        final OutputStream os = new FileOutputStream(new File(tempPath, "tinkerpop-modern-typed-v2.json"));
        GraphSONWriter.build().mapper(GraphSONMapper.build().version(GraphSONVersion.V2_0).typeInfo(TypeInfo.PARTIAL_TYPES).create()).create()
                .writeGraph(os, TinkerFactory.createModern());
        os.close();
    }

    /**
     * No assertions.  Just write out the graph for convenience.
     */
    @Test
    public void shouldWriteCrewGraphAsGraphSONV2WithTypes() throws IOException {
        final OutputStream os = new FileOutputStream(new File(tempPath, "tinkerpop-crew-typed-v2.json"));
        GraphSONWriter.build().mapper(GraphSONMapper.build().version(GraphSONVersion.V2_0).typeInfo(TypeInfo.PARTIAL_TYPES).create()).create()
                .writeGraph(os, TinkerFactory.createTheCrew());
        os.close();
    }

    /**
     * No assertions. Just write out the graph for convenience
     */
    @Test
    public void shouldWriteKitchenSinkAsGraphSONV2WithTypes() throws IOException {
        final OutputStream os = new FileOutputStream(new File(tempPath, "tinkerpop-sink-typed-v2.json"));
        GraphSONWriter.build().mapper(GraphSONMapper.build().version(GraphSONVersion.V2_0).typeInfo(TypeInfo.PARTIAL_TYPES).create()).create()
                .writeGraph(os, TinkerFactory.createKitchenSink());
        os.close();
    }

    /**
     * No assertions.  Just write out the graph for convenience.
     */
    @Test
    public void shouldWriteClassicGraphAsGraphSONV3() throws IOException {
        final OutputStream os = new FileOutputStream(new File(tempPath, "tinkerpop-classic-v3.json"));
        GraphSONWriter.build().mapper(GraphSONMapper.build().version(GraphSONVersion.V3_0).create()).create()
                .writeGraph(os, TinkerFactory.createClassic());
        os.close();
    }

    /**
     * No assertions.  Just write out the graph for convenience.
     */
    @Test
    public void shouldWriteModernGraphAsGraphSONV3() throws IOException {
        final OutputStream os = new FileOutputStream(new File(tempPath, "tinkerpop-modern-v3.json"));
        GraphSONWriter.build().mapper(GraphSONMapper.build().version(GraphSONVersion.V3_0).create()).create()
                .writeGraph(os, TinkerFactory.createModern());
        os.close();
    }

    /**
     * No assertions.  Just write out the graph for convenience.
     */
    @Test
    public void shouldWriteCrewGraphAsGraphSONV3() throws IOException {
        final OutputStream os = new FileOutputStream(new File(tempPath, "tinkerpop-crew-v3.json"));
        GraphSONWriter.build().mapper(GraphSONMapper.build().version(GraphSONVersion.V3_0).create()).create()
                .writeGraph(os, TinkerFactory.createTheCrew());
        os.close();
    }

    /**
     * No assertions.  Just write out the graph for convenience.
     */
    @Test
    public void shouldWriteSinkGraphAsGraphSONV3() throws IOException {
        final OutputStream os = new FileOutputStream(new File(tempPath, "tinkerpop-sink-v3.json"));
        GraphSONWriter.build().mapper(GraphSONMapper.build().version(GraphSONVersion.V3_0).create()).create()
                .writeGraph(os, TinkerFactory.createKitchenSink());
        os.close();
    }

    /**
     * No assertions.  Just write out the graph for convenience.
     */
    @Test
    public void shouldWriteDEFAULTClassicGraphAsGraphSONV3() throws IOException {
        final OutputStream os = new FileOutputStream(new File(tempPath, "tinkerpop-classic.json"));
        GraphSONWriter.build().mapper(GraphSONMapper.build().version(GraphSONVersion.V3_0).create()).create()
                .writeGraph(os, TinkerFactory.createClassic());
        os.close();
    }

    /**
     * No assertions.  Just write out the graph for convenience.
     */
    @Test
    public void shouldWriteDEFAULTModernGraphAsGraphSONV3() throws IOException {
        final OutputStream os = new FileOutputStream(new File(tempPath, "tinkerpop-modern.json"));
        GraphSONWriter.build().mapper(GraphSONMapper.build().version(GraphSONVersion.V3_0).create()).create()
                .writeGraph(os, TinkerFactory.createModern());
        os.close();
    }

    /**
     * No assertions.  Just write out the graph for convenience.
     */
    @Test
    public void shouldWriteDEFAULTCrewGraphAsGraphSONV3() throws IOException {
        final OutputStream os = new FileOutputStream(new File(tempPath, "tinkerpop-crew.json"));
        GraphSONWriter.build().mapper(GraphSONMapper.build().version(GraphSONVersion.V3_0).create()).create()
                .writeGraph(os, TinkerFactory.createTheCrew());
        os.close();
    }

    /**
     * No assertions.  Just write out the graph for convenience.
     */
    @Test
    public void shouldWriteDEFAULTSinkGraphAsGraphSONV3() throws IOException {
        final OutputStream os = new FileOutputStream(new File(tempPath, "tinkerpop-sink.json"));
        GraphSONWriter.build().mapper(GraphSONMapper.build().version(GraphSONVersion.V3_0).create()).create()
                .writeGraph(os, TinkerFactory.createKitchenSink());
        os.close();
    }

    /**
     * No assertions.  Just write out the graph for convenience.
     */
    @Test
    public void shouldWriteClassicGraphNormalizedAsGraphSONV3() throws IOException {
        final OutputStream os = new FileOutputStream(new File(tempPath, "tinkerpop-classic-normalized-v3.json"));
        GraphSONWriter.build().mapper(GraphSONMapper.build().version(GraphSONVersion.V3_0).normalize(true).create()).create()
                .writeGraph(os, TinkerFactory.createClassic());
        os.close();
    }

    /**
     * No assertions.  Just write out the graph for convenience.
     */
    @Test
    public void shouldWriteModernGraphNormalizedAsGraphSONV3() throws IOException {
        final OutputStream os = new FileOutputStream(new File(tempPath, "tinkerpop-modern-normalized-v3.json"));
        GraphSONWriter.build().mapper(GraphSONMapper.build().version(GraphSONVersion.V3_0).normalize(true).create()).create()
                .writeGraph(os, TinkerFactory.createModern());
        os.close();
    }

    @Test
    public void shouldWriteSampleForGremlinServer() throws IOException {
        final Graph g = TinkerGraph.open();
        IntStream.range(0, 10000).forEach(i -> g.addVertex("oid", i));
        DistributionGenerator.build(g)
                .label("knows")
                .seedGenerator(() -> 987654321L)
                .outDistribution(new PowerLawDistribution(2.1))
                .inDistribution(new PowerLawDistribution(2.1))
                .expectedNumEdges(100000).create().generate();

        final OutputStream os = new FileOutputStream(new File(tempPath, "sample.kryo"));
        GryoWriter.build().mapper(GryoMapper.build().version(GryoVersion.V3_0).create()).create().writeGraph(os, g);
        os.close();
    }

    /**
     * This test helps with data conversions on Grateful Dead.  No Assertions...run as needed. Never read from the
     * GraphML source as it will always use a String identifier.
     */
    @Test
    public void shouldWriteGratefulDead() throws IOException {
        final Graph g = TinkerGraph.open();

        // read from a Gryo 3.0 file for now
        final GraphReader reader = GryoReader.build().mapper(GryoMapper.build().version(GryoVersion.V3_0).create()).create();
        try (final InputStream stream = AbstractGremlinTest.class.getResourceAsStream("/org/apache/tinkerpop/gremlin/structure/io/gryo/grateful-dead-v3.kryo")) {
            reader.readGraph(stream, g);
        }

        /* keep this hanging around because changes to gryo format will need grateful dead generated from json so you can generate the gio
        final GraphSONMapper mapper = GraphSONMapper.build().typeInfo(TypeInfo.PARTIAL_TYPES).create();
        final GraphReader reader = org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONReader.build().mapper(mapper).create();
        try (final InputStream stream = AbstractGremlinTest.class.getResourceAsStream("/org/apache/tinkerpop/gremlin/structure/io/graphson/grateful-dead-typed.json")) {
            reader.readGraph(stream, g);
        }
        */

        final Graph ng = TinkerGraph.open();
        g.traversal().V().sideEffect(ov -> {
            final Vertex v = ov.get();
            if (v.label().equals("song"))
                ng.addVertex(T.id, Integer.parseInt(v.id().toString()), T.label, "song", "name", v.value("name"), "performances", v.property("performances").orElse(0), "songType", v.property("songType").orElse(""));
            else if (v.label().equals("artist"))
                ng.addVertex(T.id, Integer.parseInt(v.id().toString()), T.label, "artist", "name", v.value("name"));
            else
                throw new RuntimeException("damn");
        }).iterate();

        g.traversal().E().sideEffect(oe -> {
            final Edge e = oe.get();
            final Vertex v2 = ng.traversal().V(Integer.parseInt(e.inVertex().id().toString())).next();
            final Vertex v1 = ng.traversal().V(Integer.parseInt(e.outVertex().id().toString())).next();

            if (e.label().equals("followedBy"))
                v1.addEdge("followedBy", v2, T.id, Integer.parseInt(e.id().toString()), "weight", e.value("weight"));
            else if (e.label().equals("sungBy"))
                v1.addEdge("sungBy", v2, T.id, Integer.parseInt(e.id().toString()));
            else if (e.label().equals("writtenBy"))
                v1.addEdge("writtenBy", v2, T.id, Integer.parseInt(e.id().toString()));
            else
                throw new RuntimeException("bah");

        }).iterate();

        final OutputStream os = new FileOutputStream(new File(tempPath, "grateful-dead-v1.kryo"));
        GryoWriter.build().mapper(GryoMapper.build().version(GryoVersion.V1_0).create()).create().writeGraph(os, ng);
        os.close();

        final OutputStream os8 = new FileOutputStream(new File(tempPath, "grateful-dead-v3.kryo"));
        GryoWriter.build().mapper(GryoMapper.build().version(GryoVersion.V3_0).create()).create().writeGraph(os8, ng);
        os8.close();

        // ****DEFAULT Grateful Dead Gryo****
        final OutputStream os9 = new FileOutputStream(new File(tempPath, "grateful-dead.kryo"));
        GryoWriter.build().mapper(GryoMapper.build().version(GryoVersion.V3_0).create()).create().writeGraph(os9, ng);
        os9.close();

        final OutputStream os2 = new FileOutputStream(new File(tempPath, "grateful-dead-v1.json"));
        GraphSONWriter.build().mapper(GraphSONMapper.build().version(GraphSONVersion.V1_0).typeInfo(TypeInfo.NO_TYPES).create()).create().writeGraph(os2, g);
        os2.close();

        final OutputStream os3 = new FileOutputStream(new File(tempPath, "grateful-dead-v2.json"));
        GraphSONWriter.build().mapper(GraphSONMapper.build().version(GraphSONVersion.V2_0)
                .typeInfo(TypeInfo.NO_TYPES).create())
                .create()
                .writeGraph(os3, g);
        os3.close();

        final OutputStream os4 = new FileOutputStream(new File(tempPath, "grateful-dead.xml"));
        GraphMLWriter.build().create().writeGraph(os4, g);
        os4.close();

        final OutputStream os5 = new FileOutputStream(new File(tempPath, "grateful-dead-typed-v1.json"));
        GraphSONWriter.build().mapper(GraphSONMapper.build().version(GraphSONVersion.V1_0).typeInfo(TypeInfo.PARTIAL_TYPES).create()).create().writeGraph(os5, g);
        os5.close();

        final OutputStream os6 = new FileOutputStream(new File(tempPath, "grateful-dead-typed-v2.json"));
        GraphSONWriter.build().mapper(GraphSONMapper.build().version(GraphSONVersion.V2_0)
                .typeInfo(TypeInfo.PARTIAL_TYPES).create())
                .create()
                .writeGraph(os6, g);
        os6.close();

        final OutputStream os7 = new FileOutputStream(new File(tempPath, "grateful-dead-v3.json"));
        GraphSONWriter.build().mapper(GraphSONMapper.build().version(GraphSONVersion.V3_0)
                .typeInfo(TypeInfo.PARTIAL_TYPES).create())
                .create()
                .writeGraph(os7, g);
        os7.close();

        // ****DEFAULT Grateful Dead GraphSON****
        final OutputStream os10 = new FileOutputStream(new File(tempPath, "grateful-dead.json"));
        GraphSONWriter.build().mapper(GraphSONMapper.build().version(GraphSONVersion.V3_0)
                .typeInfo(TypeInfo.PARTIAL_TYPES).create())
                .create()
                .writeGraph(os10, g);
        os10.close();

    }
}
