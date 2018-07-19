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
package org.apache.tinkerpop.gremlin.process.traversal.step.map;

import org.apache.tinkerpop.gremlin.FeatureRequirement;
import org.apache.tinkerpop.gremlin.FeatureRequirementSet;
import org.apache.tinkerpop.gremlin.TestHelper;
import org.apache.tinkerpop.gremlin.process.AbstractGremlinProcessTest;
import org.apache.tinkerpop.gremlin.process.GremlinProcessRunner;
import org.apache.tinkerpop.gremlin.process.remote.RemoteGraph;
import org.apache.tinkerpop.gremlin.process.traversal.IO;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.io.IoTest;
import org.apache.tinkerpop.gremlin.structure.io.graphml.GraphMLResourceAccess;
import org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONResourceAccess;
import org.apache.tinkerpop.gremlin.structure.io.gryo.GryoResourceAccess;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.util.function.Supplier;

import static org.apache.tinkerpop.gremlin.structure.Graph.Features.GraphFeatures.FEATURE_IO_READ;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
@RunWith(GremlinProcessRunner.class)
public abstract class ReadTest extends AbstractGremlinProcessTest {

    public abstract Traversal<Object,Object> get_g_io_readXkryoX(final String fileToRead)  throws IOException;

    public abstract Traversal<Object,Object> get_g_io_read_withXreader_gryoX(final String fileToRead)  throws IOException;

    public abstract Traversal<Object,Object> get_g_io_readXjsonX(final String fileToRead)  throws IOException;

    public abstract Traversal<Object,Object> get_g_io_read_withXreader_graphsonX(final String fileToRead)  throws IOException;

    public abstract Traversal<Object,Object> get_g_io_readXxmlX(final String fileToRead)  throws IOException;

    public abstract Traversal<Object,Object> get_g_io_read_withXreader_graphmlX(final String fileToRead)  throws IOException;

    @Test
    @FeatureRequirement(featureClass = Graph.Features.GraphFeatures.class, feature = Graph.Features.GraphFeatures.FEATURE_IO_READ)
    public void g_io_readXkryoX() throws IOException {
        final String fileToRead = TestHelper.generateTempFileFromResource(ReadTest.class, GryoResourceAccess.class, "tinkerpop-modern-v3d0.kryo", "").getAbsolutePath().replace('\\', '/');
        final Traversal<Object,Object> traversal = get_g_io_readXkryoX(fileToRead);
        printTraversalForm(traversal);
        traversal.iterate();

        if (graph instanceof RemoteGraph) {
            assertEquals(6L, g.V().count().next().longValue());
            assertEquals(6L, g.E().count().next().longValue());
        } else {
            IoTest.assertModernGraph(graph, false, true);
        }
    }

    @Test
    @FeatureRequirement(featureClass = Graph.Features.GraphFeatures.class, feature = Graph.Features.GraphFeatures.FEATURE_IO_READ)
    public void g_io_read_withXreader_gryoX() throws IOException {
        final String fileToRead = TestHelper.generateTempFileFromResource(ReadTest.class, GryoResourceAccess.class, "tinkerpop-modern-v3d0.kryo", "").getAbsolutePath().replace('\\', '/');
        final Traversal<Object,Object> traversal = get_g_io_read_withXreader_gryoX(fileToRead);
        printTraversalForm(traversal);
        traversal.iterate();

        if (graph instanceof RemoteGraph) {
            assertEquals(6L, g.V().count().next().longValue());
            assertEquals(6L, g.E().count().next().longValue());
        } else {
            IoTest.assertModernGraph(graph, false, true);
        }
    }

    @Test
    @FeatureRequirement(featureClass = Graph.Features.GraphFeatures.class, feature = Graph.Features.GraphFeatures.FEATURE_IO_READ)
    public void g_io_readXjsonX() throws IOException {
        final String fileToRead = TestHelper.generateTempFileFromResource(ReadTest.class, GraphSONResourceAccess.class, "tinkerpop-modern-v3d0.json", "").getAbsolutePath().replace('\\', '/');
        final Traversal<Object,Object> traversal = get_g_io_readXjsonX(fileToRead);
        printTraversalForm(traversal);
        traversal.iterate();

        if (graph instanceof RemoteGraph) {
            assertEquals(6L, g.V().count().next().longValue());
            assertEquals(6L, g.E().count().next().longValue());
        } else {
            IoTest.assertModernGraph(graph, false, true);
        }
    }

    @Test
    @FeatureRequirement(featureClass = Graph.Features.GraphFeatures.class, feature = Graph.Features.GraphFeatures.FEATURE_IO_READ)
    public void g_io_read_withXreader_graphsonX() throws IOException {
        final String fileToRead = TestHelper.generateTempFileFromResource(ReadTest.class, GraphSONResourceAccess.class, "tinkerpop-modern-v3d0.json", "").getAbsolutePath().replace('\\', '/');
        final Traversal<Object,Object> traversal = get_g_io_read_withXreader_graphsonX(fileToRead);
        printTraversalForm(traversal);
        traversal.iterate();

        if (graph instanceof RemoteGraph) {
            assertEquals(6L, g.V().count().next().longValue());
            assertEquals(6L, g.E().count().next().longValue());
        } else {
            IoTest.assertModernGraph(graph, false, true);
        }
    }

    @Test
    @FeatureRequirement(featureClass = Graph.Features.GraphFeatures.class, feature = Graph.Features.GraphFeatures.FEATURE_IO_READ)
    public void g_io_readXxmlX() throws IOException {
        final String fileToRead = TestHelper.generateTempFileFromResource(ReadTest.class, GraphMLResourceAccess.class, "tinkerpop-modern.xml", "").getAbsolutePath().replace('\\', '/');
        final Traversal<Object,Object> traversal = get_g_io_readXxmlX(fileToRead);
        printTraversalForm(traversal);
        traversal.iterate();

        if (graph instanceof RemoteGraph) {
            assertEquals(6L, g.V().count().next().longValue());
            assertEquals(6L, g.E().count().next().longValue());
        } else {
            IoTest.assertModernGraph(graph, false, true);
        }
    }

    @Test
    @FeatureRequirement(featureClass = Graph.Features.GraphFeatures.class, feature = Graph.Features.GraphFeatures.FEATURE_IO_READ)
    public void g_io_read_withXreader_graphmlX() throws IOException {
        final String fileToRead = TestHelper.generateTempFileFromResource(ReadTest.class, GraphMLResourceAccess.class, "tinkerpop-modern.xml", "").getAbsolutePath().replace('\\', '/');
        final Traversal<Object,Object> traversal = get_g_io_read_withXreader_graphmlX(fileToRead);
        printTraversalForm(traversal);
        traversal.iterate();

        if (graph instanceof RemoteGraph) {
            assertEquals(6L, g.V().count().next().longValue());
            assertEquals(6L, g.E().count().next().longValue());
        } else {
            IoTest.assertModernGraph(graph, false, true);
        }
    }

    public static class Traversals extends ReadTest {
        @Override
        public Traversal<Object,Object> get_g_io_readXkryoX(final String fileToRead) throws IOException {
            return g.io(fileToRead).read();
        }

        @Override
        public Traversal<Object,Object> get_g_io_read_withXreader_gryoX(final String fileToRead) throws IOException {
            return g.io(fileToRead).with(IO.reader, IO.gryo).read();
        }

        @Override
        public Traversal<Object,Object> get_g_io_readXjsonX(final String fileToRead) throws IOException {
            return g.io(fileToRead).read();
        }

        @Override
        public Traversal<Object,Object> get_g_io_read_withXreader_graphsonX(final String fileToRead) throws IOException {
            return g.io(fileToRead).with(IO.reader, IO.graphson).read();
        }
        @Override
        public Traversal<Object,Object> get_g_io_readXxmlX(final String fileToRead) throws IOException {
            return g.io(fileToRead).read();
        }

        @Override
        public Traversal<Object,Object> get_g_io_read_withXreader_graphmlX(final String fileToRead) throws IOException {
            return g.io(fileToRead).with(IO.reader, IO.graphml).read();
        }
    }
}
