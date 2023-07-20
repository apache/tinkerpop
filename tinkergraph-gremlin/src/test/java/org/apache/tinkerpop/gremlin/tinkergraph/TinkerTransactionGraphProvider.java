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
package org.apache.tinkerpop.gremlin.tinkergraph;

import org.apache.commons.configuration2.Configuration;
import org.apache.tinkerpop.gremlin.AbstractGraphProvider;
import org.apache.tinkerpop.gremlin.LoadGraphWith;
import org.apache.tinkerpop.gremlin.TestHelper;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.GraphTest;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.io.IoEdgeTest;
import org.apache.tinkerpop.gremlin.structure.io.IoVertexTest;
import org.apache.tinkerpop.gremlin.structure.util.detached.DetachedGraphTest;
import org.apache.tinkerpop.gremlin.structure.util.star.StarGraphTest;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.AbstractTinkerGraph.DefaultIdManager;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerEdge;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerElement;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraphVariables;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerProperty;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerTransactionGraph;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerVertex;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerVertexProperty;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class TinkerTransactionGraphProvider extends AbstractGraphProvider {

    private static final Set<Class> IMPLEMENTATION = new HashSet<Class>() {{
        add(TinkerEdge.class);
        add(TinkerElement.class);
        add(TinkerTransactionGraph.class);
        add(TinkerGraphVariables.class);
        add(TinkerProperty.class);
        add(TinkerVertex.class);
        add(TinkerVertexProperty.class);
    }};

    @Override
    public Map<String, Object> getBaseConfiguration(final String graphName, final Class<?> test, final String testMethodName,
                                                    final LoadGraphWith.GraphData loadGraphWith) {
        final DefaultIdManager idManager = selectIdMakerFromGraphData(loadGraphWith);
        final String idMaker = (idManager.equals(DefaultIdManager.ANY) ? selectIdMakerFromTest(test, testMethodName) : idManager).name();
        return new HashMap<String, Object>() {{
            put(Graph.GRAPH, TinkerTransactionGraph.class.getName());
            put(TinkerTransactionGraph.GREMLIN_TINKERGRAPH_VERTEX_ID_MANAGER, idMaker);
            put(TinkerTransactionGraph.GREMLIN_TINKERGRAPH_EDGE_ID_MANAGER, idMaker);
            put(TinkerTransactionGraph.GREMLIN_TINKERGRAPH_VERTEX_PROPERTY_ID_MANAGER, idMaker);
            if (requiresListCardinalityAsDefault(loadGraphWith, test, testMethodName))
                put(TinkerTransactionGraph.GREMLIN_TINKERGRAPH_DEFAULT_VERTEX_PROPERTY_CARDINALITY, VertexProperty.Cardinality.list.name());
            if (requiresPersistence(test, testMethodName)) {
                put(TinkerTransactionGraph.GREMLIN_TINKERGRAPH_GRAPH_FORMAT, "gryo");
                put(TinkerTransactionGraph.GREMLIN_TINKERGRAPH_GRAPH_LOCATION,TestHelper.makeTestDataFile(test, "temp", testMethodName + ".kryo"));
            }
        }};
    }

    @Override
    public void clear(final Graph graph, final Configuration configuration) throws Exception {
        if (graph != null)
            graph.close();

        // in the even the graph is persisted we need to clean up
        final String graphLocation = null != configuration ? configuration.getString(TinkerTransactionGraph.GREMLIN_TINKERGRAPH_GRAPH_LOCATION, null) : null;
        if (graphLocation != null) {
            final File f = new File(graphLocation);
            f.delete();
        }
    }

    @Override
    public Set<Class> getImplementations() {
        return IMPLEMENTATION;
    }

    /**
     * Determines if a test requires TinkerGraph persistence to be configured with graph location and format.
     */
    protected static boolean requiresPersistence(final Class<?> test, final String testMethodName) {
        return test == GraphTest.class && testMethodName.equals("shouldPersistDataOnClose");
    }

    /**
     * Determines if a test requires a different cardinality as the default or not.
     */
    protected static boolean requiresListCardinalityAsDefault(final LoadGraphWith.GraphData loadGraphWith,
                                                            final Class<?> test, final String testMethodName) {
        return loadGraphWith == LoadGraphWith.GraphData.CREW
                || (test == StarGraphTest.class && testMethodName.equals("shouldAttachWithCreateMethod"))
                || (test == DetachedGraphTest.class && testMethodName.equals("testAttachableCreateMethod"));
    }

    /**
     * Some tests require special configuration for TinkerGraph to properly configure the id manager.
     */
    protected DefaultIdManager selectIdMakerFromTest(final Class<?> test, final String testMethodName) {
        if (test.equals(GraphTest.class)) {
            final Set<String> testsThatNeedLongIdManager = new HashSet<String>(){{
                add("shouldIterateVerticesWithNumericIdSupportUsingDoubleRepresentation");
                add("shouldIterateVerticesWithNumericIdSupportUsingDoubleRepresentations");
                add("shouldIterateVerticesWithNumericIdSupportUsingIntegerRepresentation");
                add("shouldIterateVerticesWithNumericIdSupportUsingIntegerRepresentations");
                add("shouldIterateVerticesWithNumericIdSupportUsingFloatRepresentation");
                add("shouldIterateVerticesWithNumericIdSupportUsingFloatRepresentations");
                add("shouldIterateVerticesWithNumericIdSupportUsingStringRepresentation");
                add("shouldIterateVerticesWithNumericIdSupportUsingStringRepresentations");
                add("shouldIterateEdgesWithNumericIdSupportUsingDoubleRepresentation");
                add("shouldIterateEdgesWithNumericIdSupportUsingDoubleRepresentations");
                add("shouldIterateEdgesWithNumericIdSupportUsingIntegerRepresentation");
                add("shouldIterateEdgesWithNumericIdSupportUsingIntegerRepresentations");
                add("shouldIterateEdgesWithNumericIdSupportUsingFloatRepresentation");
                add("shouldIterateEdgesWithNumericIdSupportUsingFloatRepresentations");
                add("shouldIterateEdgesWithNumericIdSupportUsingStringRepresentation");
                add("shouldIterateEdgesWithNumericIdSupportUsingStringRepresentations");
            }};

            final Set<String> testsThatNeedUuidIdManager = new HashSet<String>(){{
                add("shouldIterateVerticesWithUuidIdSupportUsingStringRepresentation");
                add("shouldIterateVerticesWithUuidIdSupportUsingStringRepresentations");
                add("shouldIterateEdgesWithUuidIdSupportUsingStringRepresentation");
                add("shouldIterateEdgesWithUuidIdSupportUsingStringRepresentations");
            }};

            if (testsThatNeedLongIdManager.contains(testMethodName))
                return DefaultIdManager.LONG;
            else if (testsThatNeedUuidIdManager.contains(testMethodName))
                return DefaultIdManager.UUID;
        }  else if (test.equals(IoEdgeTest.class)) {
            final Set<String> testsThatNeedLongIdManager = new HashSet<String>(){{
                add("shouldReadWriteEdge[graphson-v1]");
                add("shouldReadWriteDetachedEdgeAsReference[graphson-v1]");
                add("shouldReadWriteDetachedEdge[graphson-v1]");
                add("shouldReadWriteEdge[graphson-v2]");
                add("shouldReadWriteDetachedEdgeAsReference[graphson-v2]");
                add("shouldReadWriteDetachedEdge[graphson-v2]");
            }};

            if (testsThatNeedLongIdManager.contains(testMethodName))
                return DefaultIdManager.LONG;
        } else if (test.equals(IoVertexTest.class)) {
            final Set<String> testsThatNeedLongIdManager = new HashSet<String>(){{
                add("shouldReadWriteVertexWithBOTHEdges[graphson-v1]");
                add("shouldReadWriteVertexWithINEdges[graphson-v1]");
                add("shouldReadWriteVertexWithOUTEdges[graphson-v1]");
                add("shouldReadWriteVertexNoEdges[graphson-v1]");
                add("shouldReadWriteDetachedVertexNoEdges[graphson-v1]");
                add("shouldReadWriteDetachedVertexAsReferenceNoEdges[graphson-v1]");
                add("shouldReadWriteVertexMultiPropsNoEdges[graphson-v1]");
                add("shouldReadWriteVertexWithBOTHEdges[graphson-v2]");
                add("shouldReadWriteVertexWithINEdges[graphson-v2]");
                add("shouldReadWriteVertexWithOUTEdges[graphson-v2]");
                add("shouldReadWriteVertexNoEdges[graphson-v2]");
                add("shouldReadWriteDetachedVertexNoEdges[graphson-v2]");
                add("shouldReadWriteDetachedVertexAsReferenceNoEdges[graphson-v2]");
                add("shouldReadWriteVertexMultiPropsNoEdges[graphson-v2]");
            }};

            if (testsThatNeedLongIdManager.contains(testMethodName))
                return DefaultIdManager.LONG;
        }

        return DefaultIdManager.ANY;
    }

    /**
     * Test that load with specific graph data can be configured with a specific id manager as the data type to
     * be used in the test for that graph is known.
     */
    protected DefaultIdManager selectIdMakerFromGraphData(final LoadGraphWith.GraphData loadGraphWith) {
        if (null == loadGraphWith) return DefaultIdManager.ANY;
        if (loadGraphWith.equals(LoadGraphWith.GraphData.CLASSIC))
            return DefaultIdManager.INTEGER;
        else if (loadGraphWith.equals(LoadGraphWith.GraphData.MODERN))
            return DefaultIdManager.INTEGER;
        else if (loadGraphWith.equals(LoadGraphWith.GraphData.CREW))
            return DefaultIdManager.INTEGER;
        else if (loadGraphWith.equals(LoadGraphWith.GraphData.GRATEFUL))
            return DefaultIdManager.INTEGER;
        else if (loadGraphWith.equals(LoadGraphWith.GraphData.SINK))
            return DefaultIdManager.INTEGER;
        else
            throw new IllegalStateException(String.format("Need to define a new %s for %s", TinkerTransactionGraph.IdManager.class.getName(), loadGraphWith.name()));
    }

    @Override
    protected void readIntoGraph(final Graph graph, final String path) throws IOException {
        super.readIntoGraph(graph, path);

        graph.tx().commit();
    }
}
