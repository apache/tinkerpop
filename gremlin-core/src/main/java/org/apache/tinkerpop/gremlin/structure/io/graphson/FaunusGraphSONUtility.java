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

import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.io.graphson.TP2GraphSONUtility.ElementFactory;
import org.apache.tinkerpop.gremlin.structure.io.graphson.TP2GraphSONUtility.GraphSONMode;
import org.apache.tinkerpop.gremlin.structure.util.detached.DetachedEdge;
import org.apache.tinkerpop.gremlin.structure.util.detached.DetachedVertex;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.codehaus.jettison.json.JSONTokener;
import org.javatuples.Pair;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.*;

import static org.apache.tinkerpop.gremlin.structure.Direction.IN;
import static org.apache.tinkerpop.gremlin.structure.Direction.OUT;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 * @author Stephen Mallette (http://stephen.genoprime.com)
 * @author Edi Bice
 */
public class FaunusGraphSONUtility {

    //private static final String _OUT_E = "_outE";
    //private static final String _IN_E = "_inE";
    private static final String EMPTY_STRING = "";

    private static final Set<String> VERTEX_IGNORE = new HashSet<String>(Arrays.asList(GraphSONTokensTP2._TYPE, GraphSONTokensTP2._OUT_E, GraphSONTokensTP2._IN_E));
    private static final Set<String> EDGE_IGNORE = new HashSet<String>(Arrays.asList(GraphSONTokensTP2._TYPE, GraphSONTokensTP2._OUT_V, GraphSONTokensTP2._IN_V));

    private static final ElementFactory elementFactory = new MyElementFactory();

    private static final TP2GraphSONUtility graphson = new TP2GraphSONUtility(GraphSONMode.COMPACT, elementFactory,
            ElementPropertyConfig.excludeProperties(VERTEX_IGNORE, EDGE_IGNORE));

    public static List<Vertex> fromJSON(final InputStream in) throws IOException {
        final List<Vertex> vertices = new LinkedList<Vertex>();
        final BufferedReader bfs = new BufferedReader(new InputStreamReader(in));
        String line = "";
        while ((line = bfs.readLine()) != null) {
            vertices.add(FaunusGraphSONUtility.fromJSON(line));
        }
        bfs.close();
        return vertices;

    }

    public static Vertex fromJSON(String line) throws IOException {
        try {
            final JSONObject json = new JSONObject(new JSONTokener(line));
            line = EMPTY_STRING; // clear up some memory

            final Vertex vertex = graphson.vertexFromJson(json);

            fromJSONEdges(vertex, json.optJSONArray(GraphSONTokensTP2._OUT_E), OUT);
            json.remove(GraphSONTokensTP2._OUT_E); // clear up some memory
            fromJSONEdges(vertex, json.optJSONArray(GraphSONTokensTP2._IN_E), IN);
            json.remove(GraphSONTokensTP2._IN_E); // clear up some memory

            return vertex;
        } catch (Exception e) {
            throw new IOException(e.getMessage(), e);
        }
    }

    private static void fromJSONEdges(final Vertex vertex, final JSONArray edges, final Direction direction) throws JSONException, IOException {
        if (null != edges) {
            for (int i = 0; i < edges.length(); i++) {
                final JSONObject jsonEdge = edges.optJSONObject(i);
                Edge edge = null;
                if (direction.equals(Direction.IN)) {
                    DetachedVertex outVertex = new DetachedVertex(jsonEdge.optLong(GraphSONTokensTP2._OUT_V),null,new HashMap<>());
                    edge = graphson.edgeFromJson(jsonEdge, outVertex, vertex);
                    outVertex.addEdge(jsonEdge.optString(GraphSONTokensTP2._LABEL), vertex);
                } else if (direction.equals(Direction.OUT)) {
                    DetachedVertex inVertex = new DetachedVertex(jsonEdge.optLong(GraphSONTokensTP2._IN_V),null,new HashMap<>());
                    edge = graphson.edgeFromJson(jsonEdge, vertex, inVertex);
                    vertex.addEdge(jsonEdge.optString(GraphSONTokensTP2._LABEL), inVertex);
                }
            }
        }
    }

    public static JSONObject toJSON(final Vertex vertex) throws IOException {
        try {
            //final JSONObject object = TP2GraphSONUtility.jsonFromElement(vertex, getElementPropertyKeys(vertex, false), GraphSONMode.COMPACT);
            final JSONObject object = TP2GraphSONUtility.jsonFromElement(vertex, null, GraphSONMode.COMPACT);

            // force the ID to long.  with blueprints, most implementations will send back a long, but
            // some like TinkerGraph will return a string.  the same is done for edges below
            object.put(GraphSONTokensTP2._ID, Long.valueOf(object.remove(GraphSONTokensTP2._ID).toString()));

            List<Edge> edges =  new ArrayList<Edge>();
            vertex.edges(OUT).forEachRemaining(edges::add);
            if (!edges.isEmpty()) {
                final JSONArray outEdgesArray = new JSONArray();
                for (final Edge outEdge : edges) {
                    //final JSONObject edgeObject = TP2GraphSONUtility.jsonFromElement(outEdge, getElementPropertyKeys(outEdge, true), GraphSONMode.COMPACT);
                    final JSONObject edgeObject = TP2GraphSONUtility.jsonFromElement(outEdge, null, GraphSONMode.COMPACT);
                    outEdgesArray.put(edgeObject);
                }
                object.put(GraphSONTokensTP2._OUT_E, outEdgesArray);
            }

            vertex.edges(IN).forEachRemaining(edges::add);
            if (!edges.isEmpty()) {
                final JSONArray inEdgesArray = new JSONArray();
                for (final Edge inEdge : edges) {
                    //final JSONObject edgeObject = TP2GraphSONUtility.jsonFromElement(inEdge, getElementPropertyKeys(inEdge, false), GraphSONMode.COMPACT);
                    final JSONObject edgeObject = TP2GraphSONUtility.jsonFromElement(inEdge, null, GraphSONMode.COMPACT);
                    inEdgesArray.put(edgeObject);
                }
                object.put(GraphSONTokensTP2._IN_E, inEdgesArray);
            }

            return object;
        } catch (JSONException e) {
            throw new IOException(e);
        }
    }

    private static Set<String> getElementPropertyKeys(final Element element, final boolean edgeIn) {
        final Set<String> elementPropertyKeys = new HashSet<String>(element.keys()); // .getPropertyKeys());
        // exclude reserved keys? not really properties?
        /*elementPropertyKeys.add(GraphSONTokens.ID);
        if (element instanceof Edge) {
            if (edgeIn) {
                elementPropertyKeys.add(GraphSONTokens.IN);
            } else {
                elementPropertyKeys.add(GraphSONTokens.OUT);
            }

            elementPropertyKeys.add(GraphSONTokens.LABEL);
        }*/
        return elementPropertyKeys;
    }

    public static class MyElementFactory implements ElementFactory<Vertex, Edge> {

        @Override
        public Edge createEdge(final Object id, final Vertex out, final Vertex in, final String label) {
            Pair<Object, String> outPair = new Pair<Object, String>(out.id(), out.label());
            Pair<Object, String> inPair = new Pair<Object, String>(in.id(), in.label());
            return new DetachedEdge(id, label, Collections.EMPTY_MAP, outPair, inPair);
        }

        @Override
        public DetachedVertex createVertex(final Object id) {
            return new DetachedVertex(convertIdentifier(id), null, new HashMap<>());
        }

        private long convertIdentifier(final Object id) {
            if (id instanceof Long)
                return (Long) id;

            long identifier = -1l;
            if (id != null) {
                try {
                    identifier = Long.parseLong(id.toString());
                } catch (final NumberFormatException e) {
                    identifier = -1l;
                }
            }
            return identifier;
        }
    }
}