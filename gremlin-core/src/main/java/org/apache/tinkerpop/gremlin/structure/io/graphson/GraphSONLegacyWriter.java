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
package org.apache.tinkerpop.gremlin.structure.io.graphson;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.MappingJsonFactory;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.io.graphson.TP2GraphSONUtility.GraphSONMode;
import org.codehaus.jettison.json.JSONException;

import java.io.*;
import java.util.*;

/**
 * GraphSONWriter writes a Graph to a TinkerPop JSON OutputStream.
 *
 * @author Edi Bice
 * @author Stephen Mallette
 */
public class GraphSONLegacyWriter {

    /**
     * Elements are sorted in lexicographical order of IDs.
     */
    public class LexicographicalElementComparator implements Comparator<Element> {

        @Override
        public int compare(final Element a, final Element b) {
            return a.id().toString().compareTo(b.id().toString());
        }
    }

    private static final JsonFactory jsonFactory = new MappingJsonFactory();
    private final Graph graph;

    /**
     * @param graph the Graph to pull the data from
     */
    public GraphSONLegacyWriter(final Graph graph) {
        this.graph = graph;
    }

    /**
     * Write the data in a Graph to a JSON OutputStream.
     *
     * @param filename           the JSON file to write the Graph data to
     * @param vertexPropertyKeys the keys of the vertex elements to write to JSON
     * @param edgePropertyKeys   the keys of the edge elements to write to JSON
     * @param mode               determines the format of the GraphSON
     * @param adjacencyListFormat whether to output in adjacency list or vertices/edges format
     * @throws IOException thrown if there is an error generating the JSON data
     */
    public void outputGraph(final String filename, final Set<String> vertexPropertyKeys,
                            final Set<String> edgePropertyKeys, final GraphSONMode mode,
                            final boolean adjacencyListFormat) throws IOException {
        final FileOutputStream fos = new FileOutputStream(filename);
        outputGraph(fos, vertexPropertyKeys, edgePropertyKeys, mode, adjacencyListFormat);
        fos.close();
    }

    /**
     * Write the data in a Graph to a JSON OutputStream.
     *
     * @param jsonOutputStream   the JSON OutputStream to write the Graph data to
     * @param vertexPropertyKeys the keys of the vertex elements to write to JSON
     * @param edgePropertyKeys   the keys of the edge elements to write to JSON
     * @param mode               determines the format of the GraphSON
     * @param adjacencyListFormat whether to output in adjacency list or vertices/edges format
     * @throws IOException thrown if there is an error generating the JSON data
     */
    public void outputGraph(final OutputStream jsonOutputStream, final Set<String> vertexPropertyKeys,
                            final Set<String> edgePropertyKeys, final GraphSONMode mode,
                            final boolean adjacencyListFormat) throws IOException {
        outputGraph(jsonOutputStream, vertexPropertyKeys, edgePropertyKeys, mode, false, adjacencyListFormat);
    }

    private void outputVerticesEdgesGraph(final OutputStream jsonOutputStream, final Set<String> vertexPropertyKeys,
                                          final Set<String> edgePropertyKeys, final GraphSONMode mode, final boolean normalize) throws IOException {
        final JsonGenerator jg = jsonFactory.createGenerator(jsonOutputStream);

        // don't let the JsonGenerator close the underlying stream...leave that to the client passing in the stream
        jg.configure(JsonGenerator.Feature.AUTO_CLOSE_TARGET, false);

        final TP2GraphSONUtility graphson = new TP2GraphSONUtility(mode, null,
                ElementPropertyConfig.includeProperties(vertexPropertyKeys, edgePropertyKeys, normalize));

        jg.writeStartObject();

        jg.writeStringField(GraphSONTokensTP2.MODE, mode.toString());

        jg.writeArrayFieldStart(GraphSONTokensTP2.VERTICES);

        final Iterable<Vertex> vertices = vertices(normalize);
        for (Vertex v : vertices) {
            jg.writeTree(graphson.objectNodeFromElement(v));
        }

        jg.writeEndArray();

        jg.writeArrayFieldStart(GraphSONTokensTP2.EDGES);

        final Iterable<Edge> edges = edges(normalize);
        for (Edge e : edges) {
            jg.writeTree(graphson.objectNodeFromElement(e));
        }
        jg.writeEndArray();

        jg.writeEndObject();

        jg.flush();
        jg.close();

    }

    private void outputAdjListGraph(final OutputStream jsonOutputStream, final Set<String> vertexPropertyKeys,
                                    final Set<String> edgePropertyKeys, final GraphSONMode mode, final boolean normalize) throws IOException {
        final BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(jsonOutputStream));
        final Iterable<Vertex> vertices = vertices(normalize);
        for (Vertex v : vertices) {
            try {
                FaunusGraphSONUtility.toJSON(v).write(writer);
            } catch (JSONException e) {
                e.printStackTrace();
                throw new IOException(e);
            }
            writer.newLine();
        }
        writer.flush();
        writer.close();
    }

    public void outputGraph(final OutputStream jsonOutputStream, final Set<String> vertexPropertyKeys,
                            final Set<String> edgePropertyKeys, final GraphSONMode mode, final boolean normalize,
                            final boolean adjacencyListFormat) throws IOException {
        if (adjacencyListFormat) {
            outputAdjListGraph(jsonOutputStream, vertexPropertyKeys, edgePropertyKeys, mode, normalize);
        } else {
            outputVerticesEdgesGraph(jsonOutputStream, vertexPropertyKeys, edgePropertyKeys, mode, normalize);
        }
    }

    private Iterable<Vertex> vertices(boolean normalize) {
        Iterable<Vertex> vertices;
        if (normalize) {
            vertices = new ArrayList<Vertex>();
            Iterator<Vertex> viter = graph.vertices();
            while (viter.hasNext()) {
                ((Collection<Vertex>) vertices).add(viter.next());
            }
            Collections.sort((List<Vertex>) vertices, new LexicographicalElementComparator());
        } else {
            vertices = new Iterable<Vertex>() {
                @Override
                public Iterator<Vertex> iterator() {
                    return graph.vertices();
                }
            };
        }
        return vertices;
    }

    private Iterable<Edge> edges(boolean normalize) {
        Iterable<Edge> edges;
        if (normalize) {
            edges = new ArrayList<Edge>();
            Iterator<Edge> eiter = graph.edges();
            while (eiter.hasNext()) {
                ((Collection<Edge>) edges).add(eiter.next());
            }
            Collections.sort((List<Edge>) edges, new LexicographicalElementComparator());
        } else {
            edges = new Iterable<Edge>() {
                @Override
                public Iterator<Edge> iterator() {
                    return graph.edges();
                }
            };
        }
        return edges;
    }

    /**
     * Write the data in a Graph to a JSON OutputStream. All keys are written to JSON. Utilizing
     * GraphSONMode.NORMAL.
     *
     * @param graph            the graph to serialize to JSON
     * @param jsonOutputStream the JSON OutputStream to write the Graph data to
     * @param adjacencyListFormat whether to output in adjacency list or vertices/edges format
     * @throws IOException thrown if there is an error generating the JSON data
     */
    public static void outputGraph(final Graph graph, final OutputStream jsonOutputStream, final boolean adjacencyListFormat) throws IOException {
        final GraphSONLegacyWriter writer = new GraphSONLegacyWriter(graph);
        writer.outputGraph(jsonOutputStream, null, null, GraphSONMode.NORMAL, adjacencyListFormat);
    }

    /**
     * Write the data in a Graph to a JSON OutputStream. All keys are written to JSON. Utilizing
     * GraphSONMode.NORMAL.
     *
     * @param graph    the graph to serialize to JSON
     * @param filename the JSON file to write the Graph data to
     * @param adjacencyListFormat whether to output in adjacency list or vertices/edges format
     * @throws IOException thrown if there is an error generating the JSON data
     */
    public static void outputGraph(final Graph graph, final String filename, final boolean adjacencyListFormat) throws IOException {
        final GraphSONLegacyWriter writer = new GraphSONLegacyWriter(graph);
        writer.outputGraph(filename, null, null, GraphSONMode.NORMAL, adjacencyListFormat);
    }

    /**
     * Write the data in a Graph to a JSON OutputStream. All keys are written to JSON.
     *
     * @param graph            the graph to serialize to JSON
     * @param jsonOutputStream the JSON OutputStream to write the Graph data to
     * @param mode             determines the format of the GraphSON
     * @param adjacencyListFormat whether to output in adjacency list or vertices/edges format
     * @throws IOException thrown if there is an error generating the JSON data
     */
    public static void outputGraph(final Graph graph, final OutputStream jsonOutputStream,
                                   final GraphSONMode mode, final boolean adjacencyListFormat) throws IOException {
        final GraphSONLegacyWriter writer = new GraphSONLegacyWriter(graph);
        writer.outputGraph(jsonOutputStream, null, null, mode, adjacencyListFormat);
    }

    /**
     * Write the data in a Graph to a JSON OutputStream. All keys are written to JSON.
     *
     * @param graph    the graph to serialize to JSON
     * @param filename the JSON file to write the Graph data to
     * @param mode     determines the format of the GraphSON
     * @param adjacencyListFormat whether to output in adjacency list or vertices/edges format
     * @throws IOException thrown if there is an error generating the JSON data
     */
    public static void outputGraph(final Graph graph, final String filename,
                                   final GraphSONMode mode, final boolean adjacencyListFormat) throws IOException {
        final GraphSONLegacyWriter writer = new GraphSONLegacyWriter(graph);
        writer.outputGraph(filename, null, null, mode, adjacencyListFormat);
    }

    /**
     * Write the data in a Graph to a JSON OutputStream.
     *
     * @param graph              the graph to serialize to JSON
     * @param jsonOutputStream   the JSON OutputStream to write the Graph data to
     * @param vertexPropertyKeys the keys of the vertex elements to write to JSON
     * @param edgePropertyKeys   the keys of the edge elements to write to JSON
     * @param mode               determines the format of the GraphSON
     * @param adjacencyListFormat whether to output in adjacency list or vertices/edges format
     * @throws IOException thrown if there is an error generating the JSON data
     */
    public static void outputGraph(final Graph graph, final OutputStream jsonOutputStream,
                                   final Set<String> vertexPropertyKeys, final Set<String> edgePropertyKeys,
                                   final GraphSONMode mode, final boolean adjacencyListFormat) throws IOException {
        final GraphSONLegacyWriter writer = new GraphSONLegacyWriter(graph);
        writer.outputGraph(jsonOutputStream, vertexPropertyKeys, edgePropertyKeys, mode, adjacencyListFormat);
    }

    /**
     * Write the data in a Graph to a JSON OutputStream.
     *
     * @param graph              the graph to serialize to JSON
     * @param filename           the JSON file to write the Graph data to
     * @param vertexPropertyKeys the keys of the vertex elements to write to JSON
     * @param edgePropertyKeys   the keys of the edge elements to write to JSON
     * @param mode               determines the format of the GraphSON
     * @param adjacencyListFormat whether to output in adjacency list or vertices/edges format
     * @throws IOException thrown if there is an error generating the JSON data
     */
    public static void outputGraph(final Graph graph, final String filename,
                                   final Set<String> vertexPropertyKeys, final Set<String> edgePropertyKeys,
                                   final GraphSONMode mode, final boolean adjacencyListFormat) throws IOException {
        final GraphSONLegacyWriter writer = new GraphSONLegacyWriter(graph);
        writer.outputGraph(filename, vertexPropertyKeys, edgePropertyKeys, mode, adjacencyListFormat);
    }

}