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
package org.apache.tinkerpop.gremlin.structure.io;

import org.apache.commons.configuration.Configuration;
import org.apache.tinkerpop.gremlin.AbstractGremlinTest;
import org.apache.tinkerpop.gremlin.FeatureRequirement;
import org.apache.tinkerpop.gremlin.LoadGraphWith;
import org.apache.tinkerpop.gremlin.TestHelper;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.util.Arrays;

import static org.hamcrest.core.IsNot.not;
import static org.junit.Assume.assumeThat;

/**
 * Tests for all IO implementations that are specific to reading and writing of a {@link Graph}.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
@RunWith(Parameterized.class)
public class IoGraphTest extends AbstractGremlinTest {

    @Parameterized.Parameters(name = "{0}")
    public static Iterable<Object[]> data() {
        return Arrays.asList(new Object[][]{
                {"graphml", IoCore.graphml(), false, true, ".xml"},
                {"graphson", IoCore.graphson(), true, true, ".json"},
                {"gryo", IoCore.gryo(), false, false, ".kryo"}
        });
    }

    @Parameterized.Parameter(value = 0)
    public String ioType;

    @Parameterized.Parameter(value = 1)
    public Io.Builder ioBuilderToTest;

    @Parameterized.Parameter(value = 2)
    public boolean assertDouble;

    @Parameterized.Parameter(value = 3)
    public boolean lossyForId;

    @Parameterized.Parameter(value = 4)
    public String fileExtension;

    @Test
    @LoadGraphWith(LoadGraphWith.GraphData.CLASSIC)
    @FeatureRequirement(featureClass = Graph.Features.EdgeFeatures.class, feature = Graph.Features.EdgeFeatures.FEATURE_ADD_EDGES)
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
    public void shouldReadWriteClassic() throws Exception {
        try (final ByteArrayOutputStream os = new ByteArrayOutputStream()) {
            final GraphWriter writer = graph.io(ioBuilderToTest).writer().create();
            writer.writeGraph(os, graph);

            final Configuration configuration = graphProvider.newGraphConfiguration("readGraph", this.getClass(), name.getMethodName(), LoadGraphWith.GraphData.CLASSIC);
            graphProvider.clear(configuration);
            final Graph g1 = graphProvider.openTestGraph(configuration);
            final GraphReader reader = graph.io(ioBuilderToTest).reader().create();
            try (final ByteArrayInputStream bais = new ByteArrayInputStream(os.toByteArray())) {
                reader.readGraph(bais, g1);
            }

            IoTest.assertClassicGraph(g1, assertDouble, lossyForId);

            graphProvider.clear(g1, configuration);
        }
    }

    @Test
    @LoadGraphWith(LoadGraphWith.GraphData.CLASSIC)
    @FeatureRequirement(featureClass = Graph.Features.EdgeFeatures.class, feature = Graph.Features.EdgeFeatures.FEATURE_ADD_EDGES)
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
    public void shouldReadWriteClassicToFileWithHelpers() throws Exception {
        final File f = TestHelper.generateTempFile(this.getClass(), name.getMethodName(), fileExtension);
        try {
            graph.io(ioBuilderToTest).writeGraph(f.getAbsolutePath());

            final Configuration configuration = graphProvider.newGraphConfiguration("readGraph", this.getClass(), name.getMethodName(), LoadGraphWith.GraphData.CLASSIC);
            final Graph g1 = graphProvider.openTestGraph(configuration);
            g1.io(ioBuilderToTest).readGraph(f.getAbsolutePath());

            IoTest.assertClassicGraph(g1, assertDouble, lossyForId);

            graphProvider.clear(g1, configuration);
        } catch (Exception ex) {
            f.delete();
            throw ex;
        }
    }

    @Test
    @LoadGraphWith(LoadGraphWith.GraphData.CLASSIC)
    @FeatureRequirement(featureClass = Graph.Features.EdgeFeatures.class, feature = Graph.Features.EdgeFeatures.FEATURE_ADD_EDGES)
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
    public void shouldMigrateClassicGraph() throws Exception {
        final Configuration configuration = graphProvider.newGraphConfiguration("readGraph", this.getClass(), name.getMethodName(), LoadGraphWith.GraphData.CLASSIC);
        graphProvider.clear(configuration);
        final Graph g1 = graphProvider.openTestGraph(configuration);

        final GraphReader reader = graph.io(ioBuilderToTest).reader().create();
        final GraphWriter writer = graph.io(ioBuilderToTest).writer().create();

        GraphMigrator.migrateGraph(graph, g1, reader, writer);

        IoTest.assertClassicGraph(g1, assertDouble, lossyForId);

        graphProvider.clear(g1, configuration);
    }

    @Test
    @LoadGraphWith(LoadGraphWith.GraphData.MODERN)
    @FeatureRequirement(featureClass = Graph.Features.EdgeFeatures.class, feature = Graph.Features.EdgeFeatures.FEATURE_ADD_EDGES)
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
    public void shouldReadWriteModern() throws Exception {
        try (final ByteArrayOutputStream os = new ByteArrayOutputStream()) {
            final GraphWriter writer = graph.io(ioBuilderToTest).writer().create();
            writer.writeGraph(os, graph);

            final Configuration configuration = graphProvider.newGraphConfiguration("readGraph", this.getClass(), name.getMethodName(), LoadGraphWith.GraphData.MODERN);
            graphProvider.clear(configuration);
            final Graph g1 = graphProvider.openTestGraph(configuration);
            final GraphReader reader = graph.io(ioBuilderToTest).reader().create();
            try (final ByteArrayInputStream bais = new ByteArrayInputStream(os.toByteArray())) {
                reader.readGraph(bais, g1);
            }

            // modern uses double natively so always assert as such
            IoTest.assertModernGraph(g1, true, lossyForId);

            graphProvider.clear(g1, configuration);
        }
    }

    @Test
    @LoadGraphWith(LoadGraphWith.GraphData.MODERN)
    @FeatureRequirement(featureClass = Graph.Features.EdgeFeatures.class, feature = Graph.Features.EdgeFeatures.FEATURE_ADD_EDGES)
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
    public void shouldReadWriteModernToFileWithHelpers() throws Exception {
        final File f = TestHelper.generateTempFile(this.getClass(), name.getMethodName(), fileExtension);
        try {
            graph.io(ioBuilderToTest).writeGraph(f.getAbsolutePath());

            final Configuration configuration = graphProvider.newGraphConfiguration("readGraph", this.getClass(), name.getMethodName(), LoadGraphWith.GraphData.MODERN);
            final Graph g1 = graphProvider.openTestGraph(configuration);
            g1.io(ioBuilderToTest).readGraph(f.getAbsolutePath());

            // modern uses double natively so always assert as such
            IoTest.assertModernGraph(g1, true, lossyForId);

            graphProvider.clear(g1, configuration);
        } catch (Exception ex) {
            f.delete();
            throw ex;
        }
    }

    @Test
    @LoadGraphWith(LoadGraphWith.GraphData.MODERN)
    @FeatureRequirement(featureClass = Graph.Features.EdgeFeatures.class, feature = Graph.Features.EdgeFeatures.FEATURE_ADD_EDGES)
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
    public void shouldMigrateModernGraph() throws Exception {
        final Configuration configuration = graphProvider.newGraphConfiguration("readGraph", this.getClass(), name.getMethodName(), LoadGraphWith.GraphData.MODERN);
        graphProvider.clear(configuration);
        final Graph g1 = graphProvider.openTestGraph(configuration);

        final GraphReader reader = graph.io(ioBuilderToTest).reader().create();
        final GraphWriter writer = graph.io(ioBuilderToTest).writer().create();

        GraphMigrator.migrateGraph(graph, g1, reader, writer);

        IoTest.assertModernGraph(g1, true, lossyForId);

        graphProvider.clear(g1, configuration);
    }

    @Test
    @LoadGraphWith(LoadGraphWith.GraphData.CREW)
    @FeatureRequirement(featureClass = Graph.Features.EdgeFeatures.class, feature = Graph.Features.EdgeFeatures.FEATURE_ADD_EDGES)
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
    public void shouldReadWriteCrewToGryo() throws Exception {
        assumeThat("GraphML does not suppport multi/metaproperties", ioType, not("graphml"));
        try (final ByteArrayOutputStream os = new ByteArrayOutputStream()) {

            final GraphWriter writer = graph.io(ioBuilderToTest).writer().create();
            writer.writeGraph(os, graph);

            final Configuration configuration = graphProvider.newGraphConfiguration("readGraph", this.getClass(), name.getMethodName(), LoadGraphWith.GraphData.CREW);
            graphProvider.clear(configuration);
            final Graph g1 = graphProvider.openTestGraph(configuration);

            final GraphReader reader = graph.io(ioBuilderToTest).reader().create();
            try (final ByteArrayInputStream bais = new ByteArrayInputStream(os.toByteArray())) {
                reader.readGraph(bais, g1);
            }

            IoTest.assertCrewGraph(g1, lossyForId);

            graphProvider.clear(g1, configuration);
        }
    }
}
