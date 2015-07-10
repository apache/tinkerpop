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
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.MappingJsonFactory;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.io.graphson.ElementPropertyConfig.ElementPropertiesRule;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.codehaus.jettison.json.JSONTokener;

import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Array;
import java.util.*;

//import com.tinkerpop.blueprints.Direction;
//import com.tinkerpop.blueprints.Element;

/**
 * Helps write individual graph elements to TinkerPop JSON format known as GraphSON.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class TP2GraphSONUtility {

    /**
     * Modes of operation of the GraphSONUtility.
     *
     * @author Stephen Mallette
     */
    public enum GraphSONMode {
        /**
         * COMPACT constructs GraphSON on the assumption that all property keys
         * are fair game for exclusion including _type, _inV, _outV, _label and _id.
         * It is possible to write GraphSON that cannot be read back into Graph,
         * if some or all of these keys are excluded.
         */
        COMPACT,

        /**
         * NORMAL includes the _type field and JSON data typing.
         */
        NORMAL,

        /**
         * EXTENDED includes the _type field and explicit data typing.
         */
        EXTENDED
    }

    /**
     * A factory responsible for creating graph elements.  Abstracts the way that graph elements are created. In
     * most cases a Graph is responsible for element creation, but there are cases where more control over
     * how vertices and edges are constructed.
     *
     * @author Stephen Mallette (http://stephen.genoprime.com)
     */
    public interface ElementFactory<V extends Vertex, E extends Edge> {
        /**
         * Creates a new Edge instance.
         */
        E createEdge(final Object id, final V out, final V in, final String label);

        /**
         * Creates a new Vertex instance.
         */
        V createVertex(final Object id);
    }

    private static final JsonNodeFactory jsonNodeFactory = JsonNodeFactory.instance;
    private static final JsonFactory jsonFactory = new MappingJsonFactory();
    private static final String EMPTY_STRING = "";

    private static final ObjectMapper mapper = new ObjectMapper();

    private final GraphSONMode mode;
    private final List<String> vertexPropertyKeys;
    private final List<String> edgePropertyKeys;
    private final ElementFactory factory;
    private final boolean hasEmbeddedTypes;
    private final ElementPropertiesRule vertexPropertiesRule;
    private final ElementPropertiesRule edgePropertiesRule;
    private final boolean normalized;

    private final boolean includeReservedVertexId;
    private final boolean includeReservedEdgeId;
    private final boolean includeReservedVertexType;
    private final boolean includeReservedEdgeType;
    private final boolean includeReservedEdgeLabel;
    private final boolean includeReservedEdgeOutV;
    private final boolean includeReservedEdgeInV;

    /**
     * A GraphSONUtiltiy that includes all properties of vertices and edges.
     */
    public TP2GraphSONUtility(final GraphSONMode mode, final ElementFactory factory) {
        this(mode, factory, ElementPropertyConfig.AllProperties);
    }

    /**
     * A GraphSONUtility that includes the specified properties.
     */
    public TP2GraphSONUtility(final GraphSONMode mode, final ElementFactory factory,
                           final Set<String> vertexPropertyKeys, final Set<String> edgePropertyKeys) {
        this(mode, factory, ElementPropertyConfig.includeProperties(vertexPropertyKeys, edgePropertyKeys));
    }

    public TP2GraphSONUtility(final GraphSONMode mode, final ElementFactory factory,
                           final ElementPropertyConfig config) {
        this.vertexPropertyKeys = config.getVertexPropertyKeys();
        this.edgePropertyKeys = config.getEdgePropertyKeys();
        this.vertexPropertiesRule = config.getVertexPropertiesRule();
        this.edgePropertiesRule = config.getEdgePropertiesRule();
        this.normalized = config.isNormalized();

        this.mode = mode;
        this.factory = factory;
        this.hasEmbeddedTypes = mode == GraphSONMode.EXTENDED;

        this.includeReservedVertexId = includeReservedKey(mode, GraphSONTokensTP2._ID, vertexPropertyKeys, this.vertexPropertiesRule);
        this.includeReservedEdgeId = includeReservedKey(mode, GraphSONTokensTP2._ID, edgePropertyKeys, this.edgePropertiesRule);
        this.includeReservedVertexType = includeReservedKey(mode, GraphSONTokensTP2._TYPE, vertexPropertyKeys, this.vertexPropertiesRule);
        this.includeReservedEdgeType = includeReservedKey(mode, GraphSONTokensTP2._TYPE, edgePropertyKeys, this.edgePropertiesRule);
        this.includeReservedEdgeLabel = includeReservedKey(mode, GraphSONTokensTP2._LABEL, edgePropertyKeys, this.edgePropertiesRule);
        this.includeReservedEdgeOutV = includeReservedKey(mode, GraphSONTokensTP2._OUT_V, edgePropertyKeys, this.edgePropertiesRule);
        this.includeReservedEdgeInV = includeReservedKey(mode, GraphSONTokensTP2._IN_V, edgePropertyKeys, this.edgePropertiesRule);
    }

    /**
     * Creates a vertex from GraphSON using settings supplied in the constructor.
     */
    public Vertex vertexFromJson(final JSONObject json) throws IOException {
        return this.vertexFromJson(json.toString());
    }

    /**
     * Creates a vertex from GraphSON using settings supplied in the constructor.
     */
    public Vertex vertexFromJson(final String json) throws IOException {
        final JsonParser jp = jsonFactory.createParser(json);
        final JsonNode node = jp.readValueAsTree();
        return this.vertexFromJson(node);
    }

    /**
     * Creates a vertex from GraphSON using settings supplied in the constructor.
     */
    public Vertex vertexFromJson(final InputStream json) throws IOException {
        final JsonParser jp = jsonFactory.createParser(json);
        final JsonNode node = jp.readValueAsTree();
        return this.vertexFromJson(node);
    }

    /**
     * Creates a vertex from GraphSON using settings supplied in the constructor.
     */
    public Vertex vertexFromJson(final JsonNode json) throws IOException {
        final Map<String, Object> props = readProperties(json, true, this.hasEmbeddedTypes);

        final Object vertexId = getTypedValueFromJsonNode(json.get(GraphSONTokensTP2._ID));
        final Vertex v = factory.createVertex(vertexId);

        for (Map.Entry<String, Object> entry : props.entrySet()) {
            //if (this.vertexPropertyKeys == null || vertexPropertyKeys.contains(entry.getKey())) {
            if (includeKey(entry.getKey(), vertexPropertyKeys, this.vertexPropertiesRule)) {
                v.property(entry.getKey(), entry.getValue());
            }
        }

        return v;
    }

    /**
     * Creates an edge from GraphSON using settings supplied in the constructor.
     */
    public Edge edgeFromJson(final JSONObject json, final Vertex out, final Vertex in) throws IOException {
        return this.edgeFromJson(json.toString(), out, in);
    }

    /**
     * Creates an edge from GraphSON using settings supplied in the constructor.
     */
    public Edge edgeFromJson(final String json, final Vertex out, final Vertex in) throws IOException {
        final JsonParser jp = jsonFactory.createParser(json);
        final JsonNode node = jp.readValueAsTree();
        return this.edgeFromJson(node, out, in);
    }

    /**
     * Creates an edge from GraphSON using settings supplied in the constructor.
     */
    public Edge edgeFromJson(final InputStream json, final Vertex out, final Vertex in) throws IOException {
        final JsonParser jp = jsonFactory.createParser(json);
        final JsonNode node = jp.readValueAsTree();
        return this.edgeFromJson(node, out, in);
    }

    /**
     * Creates an edge from GraphSON using settings supplied in the constructor.
     */
    public Edge edgeFromJson(final JsonNode json, final Vertex out, final Vertex in) throws IOException {
        final Map<String, Object> props = TP2GraphSONUtility.readProperties(json, true, this.hasEmbeddedTypes);

        final Object edgeId = getTypedValueFromJsonNode(json.get(GraphSONTokensTP2._ID));
        final JsonNode nodeLabel = json.get(GraphSONTokensTP2._LABEL);

        // assigned an empty string edge label in cases where one does not exist.  this gets around the requirement
        // that blueprints graphs have a non-null label while ensuring that GraphSON can stay flexible in parsing
        // partial bits from the JSON.  Not sure if there is any gotchas developing out of this.
        final String label = nodeLabel == null ? EMPTY_STRING : nodeLabel.textValue();

        final Edge e = factory.createEdge(edgeId, out, in, label);

        for (Map.Entry<String, Object> entry : props.entrySet()) {
            // if (this.edgePropertyKeys == null || this.edgePropertyKeys.contains(entry.getKey())) {
            if (includeKey(entry.getKey(), edgePropertyKeys, this.edgePropertiesRule)) {
                e.property(entry.getKey(), entry.getValue());
            }
        }

        return e;
    }

    /**
     * Creates GraphSON for a single graph element.
     */
    public JSONObject jsonFromElement(final Element element) throws JSONException {
        final ObjectNode objectNode = this.objectNodeFromElement(element);

        try {
            return new JSONObject(new JSONTokener(mapper.writeValueAsString(objectNode)));
        } catch (IOException ioe) {
            // repackage this as a JSONException...seems sensible as the caller will only know about
            // the jettison object not being created
            throw new JSONException(ioe);
        }
    }

    /**
     * Creates GraphSON for a single graph element.
     */
    public ObjectNode objectNodeFromElement(final Element element) {
        final boolean isEdge = element instanceof Edge;
        final boolean showTypes = mode == GraphSONMode.EXTENDED;
        final List<String> propertyKeys = isEdge ? this.edgePropertyKeys : this.vertexPropertyKeys;
        final ElementPropertiesRule elementPropertyConfig = isEdge ? this.edgePropertiesRule : this.vertexPropertiesRule;

        final ObjectNode jsonElement = createJSONMap(createPropertyMap(element, propertyKeys, elementPropertyConfig, normalized), propertyKeys, showTypes);

        //if ((isEdge && this.includeReservedEdgeId) || (!isEdge && this.includeReservedVertexId)) {
        //    putObject(jsonElement, GraphSONTokensTP2._ID, element.id());
        //}

        // it's important to keep the order of these straight.  check Edge first and then Vertex because there
        // are graph implementations that have Edge extend from Vertex
        if (isEdge) { //element instanceof Edge) {
            final Edge edge = (Edge) element;

            if (this.includeReservedEdgeId) {
                putObject(jsonElement, GraphSONTokensTP2._ID, element.id());
            }

            if (this.includeReservedEdgeType) {
                jsonElement.put(GraphSONTokensTP2._TYPE, GraphSONTokens.EDGE);
            }

            if (this.includeReservedEdgeOutV) {
                putObject(jsonElement, GraphSONTokensTP2._OUT_V, edge.outVertex().id());
            }

            if (this.includeReservedEdgeInV) {
                putObject(jsonElement, GraphSONTokensTP2._IN_V, edge.inVertex().id());
            }

            if (this.includeReservedEdgeLabel) {
                jsonElement.put(GraphSONTokensTP2._LABEL, edge.label());
            }
        } else if (element instanceof Vertex) {
            if (this.includeReservedVertexId) {
                putObject(jsonElement, GraphSONTokensTP2._ID, element.id());
            }

            if (this.includeReservedVertexType) {
                jsonElement.put(GraphSONTokensTP2._TYPE, GraphSONTokens.VERTEX);
            }
        }

        return jsonElement;
    }

    /**
     * Reads an individual Vertex from JSON.  The vertex must match the accepted GraphSON format.
     *
     * @param json         a single vertex in GraphSON format as Jettison JSONObject
     * @param factory      the factory responsible for constructing graph elements
     * @param mode         the mode of the GraphSON
     * @param propertyKeys a list of keys to include on reading of element properties
     */
    public static Vertex vertexFromJson(final JSONObject json, final ElementFactory factory, final GraphSONMode mode,
                                        final Set<String> propertyKeys) throws IOException {
        final TP2GraphSONUtility graphson = new TP2GraphSONUtility(mode, factory, propertyKeys, null);
        return graphson.vertexFromJson(json);
    }

    /**
     * Reads an individual Vertex from JSON.  The vertex must match the accepted GraphSON format.
     *
     * @param json         a single vertex in GraphSON format as a String.
     * @param factory      the factory responsible for constructing graph elements
     * @param mode         the mode of the GraphSON
     * @param propertyKeys a list of keys to include on reading of element properties
     */
    public static Vertex vertexFromJson(final String json, final ElementFactory factory, final GraphSONMode mode,
                                        final Set<String> propertyKeys) throws IOException {
        final TP2GraphSONUtility graphson = new TP2GraphSONUtility(mode, factory, propertyKeys, null);
        return graphson.vertexFromJson(json);
    }

    /**
     * Reads an individual Vertex from JSON.  The vertex must match the accepted GraphSON format.
     *
     * @param json         a single vertex in GraphSON format as an InputStream.
     * @param factory      the factory responsible for constructing graph elements
     * @param mode         the mode of the GraphSON
     * @param propertyKeys a list of keys to include on reading of element properties
     */
    public static Vertex vertexFromJson(final InputStream json, final ElementFactory factory, final GraphSONMode mode,
                                        final Set<String> propertyKeys) throws IOException {
        final TP2GraphSONUtility graphson = new TP2GraphSONUtility(mode, factory, propertyKeys, null);
        return graphson.vertexFromJson(json);
    }

    /**
     * Reads an individual Vertex from JSON.  The vertex must match the accepted GraphSON format.
     *
     * @param json         a single vertex in GraphSON format as Jackson JsonNode
     * @param factory      the factory responsible for constructing graph elements
     * @param mode         the mode of the GraphSON
     * @param propertyKeys a list of keys to include on reading of element properties
     */
    public static Vertex vertexFromJson(final JsonNode json, final ElementFactory factory, final GraphSONMode mode,
                                        final Set<String> propertyKeys) throws IOException {
        final TP2GraphSONUtility graphson = new TP2GraphSONUtility(mode, factory, propertyKeys, null);
        return graphson.vertexFromJson(json);
    }

    /**
     * Reads an individual Edge from JSON.  The edge must match the accepted GraphSON format.
     *
     * @param json         a single edge in GraphSON format as a Jettison JSONObject
     * @param factory      the factory responsible for constructing graph elements
     * @param mode         the mode of the GraphSON
     * @param propertyKeys a list of keys to include when reading of element properties
     */
    public static Edge edgeFromJson(final JSONObject json, final Vertex out, final Vertex in,
                                    final ElementFactory factory, final GraphSONMode mode,
                                    final Set<String> propertyKeys) throws IOException {
        final TP2GraphSONUtility graphson = new TP2GraphSONUtility(mode, factory, null, propertyKeys);
        return graphson.edgeFromJson(json, out, in);
    }

    /**
     * Reads an individual Edge from JSON.  The edge must match the accepted GraphSON format.
     *
     * @param json         a single edge in GraphSON format as a String
     * @param factory      the factory responsible for constructing graph elements
     * @param mode         the mode of the GraphSON
     * @param propertyKeys a list of keys to include when reading of element properties
     */
    public static Edge edgeFromJson(final String json, final Vertex out, final Vertex in,
                                    final ElementFactory factory, final GraphSONMode mode,
                                    final Set<String> propertyKeys) throws IOException {
        final TP2GraphSONUtility graphson = new TP2GraphSONUtility(mode, factory, null, propertyKeys);
        return graphson.edgeFromJson(json, out, in);
    }

    /**
     * Reads an individual Edge from JSON.  The edge must match the accepted GraphSON format.
     *
     * @param json         a single edge in GraphSON format as an InputStream
     * @param factory      the factory responsible for constructing graph elements
     * @param mode         the mode of the GraphSON
     * @param propertyKeys a list of keys to include when reading of element properties
     */
    public static Edge edgeFromJson(final InputStream json, final Vertex out, final Vertex in,
                                    final ElementFactory factory, final GraphSONMode mode,
                                    final Set<String> propertyKeys) throws IOException {
        final TP2GraphSONUtility graphson = new TP2GraphSONUtility(mode, factory, null, propertyKeys);
        return graphson.edgeFromJson(json, out, in);
    }

    /**
     * Reads an individual Edge from JSON.  The edge must match the accepted GraphSON format.
     *
     * @param json         a single edge in GraphSON format as a Jackson JsonNode
     * @param factory      the factory responsible for constructing graph elements
     * @param mode         the mode of the GraphSON
     * @param propertyKeys a list of keys to include when reading of element properties
     */
    public static Edge edgeFromJson(final JsonNode json, final Vertex out, final Vertex in,
                                    final ElementFactory factory, final GraphSONMode mode,
                                    final Set<String> propertyKeys) throws IOException {
        final TP2GraphSONUtility graphson = new TP2GraphSONUtility(mode, factory, null, propertyKeys);
        return graphson.edgeFromJson(json, out, in);
    }

    /**
     * Creates a Jettison JSONObject from a graph element.
     *
     * @param element      the graph element to convert to JSON.
     * @param propertyKeys The property keys at the root of the element to serialize.  If null, then all keys are serialized.
     * @param mode         the type of GraphSON to be generated.
     */
    public static JSONObject jsonFromElement(final Element element, final Set<String> propertyKeys,
                                             final GraphSONMode mode) throws JSONException {
        ElementFactory factory = new FaunusGraphSONUtility.MyElementFactory();
        final TP2GraphSONUtility graphson = element instanceof Edge ? new TP2GraphSONUtility(mode, factory, null, propertyKeys)
                : new TP2GraphSONUtility(mode, factory, propertyKeys, null);
        return graphson.jsonFromElement(element);
    }

    /**
     * Creates a Jackson ObjectNode from a graph element.
     *
     * @param element      the graph element to convert to JSON.
     * @param propertyKeys The property keys at the root of the element to serialize.  If null, then all keys are serialized.
     * @param mode         The type of GraphSON to generate.
     */
    public static ObjectNode objectNodeFromElement(final Element element, final Set<String> propertyKeys, final GraphSONMode mode) {
        ElementFactory factory = new FaunusGraphSONUtility.MyElementFactory();
        final TP2GraphSONUtility graphson = element instanceof Edge ? new TP2GraphSONUtility(mode, factory, null, propertyKeys)
                : new TP2GraphSONUtility(mode, factory, propertyKeys, null);
        return graphson.objectNodeFromElement(element);
    }

    private static ObjectNode objectNodeFromElement(final Element element, final List<String> propertyKeys, final GraphSONMode mode) {
        ElementFactory factory = new FaunusGraphSONUtility.MyElementFactory();
        Set<String> propKeySet = (propertyKeys!=null)? new HashSet<String>(propertyKeys) : null;
        final TP2GraphSONUtility graphson = element instanceof Edge ? new TP2GraphSONUtility(mode, factory, null, propKeySet)
                : new TP2GraphSONUtility(mode, factory, propKeySet, null);
        return graphson.objectNodeFromElement(element);
    }

    static Map<String, Object> readProperties(final JsonNode node, final boolean ignoreReservedKeys, final boolean hasEmbeddedTypes) {
        final Map<String, Object> map = new HashMap<String, Object>();

        final Iterator<Map.Entry<String, JsonNode>> iterator = node.fields();
        while (iterator.hasNext()) {
            final Map.Entry<String, JsonNode> entry = iterator.next();

            if (!ignoreReservedKeys || !isReservedKey(entry.getKey())) {
                // it generally shouldn't be as such but graphson containing null values can't be shoved into
                // element property keys or it will result in error
                final Object o = readProperty(entry.getValue(), hasEmbeddedTypes);
                if (o != null) {
                    map.put(entry.getKey(), o);
                }
            }
        }

        return map;
    }

    private static boolean includeReservedKey(final GraphSONMode mode, final String key,
                                              final List<String> propertyKeys,
                                              final ElementPropertiesRule rule) {
        // the key is always included in modes other than compact.  if it is compact, then validate that the
        // key is in the property key list
        return mode != GraphSONMode.COMPACT || includeKey(key, propertyKeys, rule);
    }

    private static boolean includeKey(final String key, final List<String> propertyKeys,
                                      final ElementPropertiesRule rule) {
        if (propertyKeys == null) {
            // when null always include the key and shortcut this piece
            return true;
        }

        // default the key situation.  if it's included then it should be explicitly defined in the
        // property keys list to be included or the reverse otherwise
        boolean keySituation = rule == ElementPropertiesRule.INCLUDE;

        switch (rule) {
            case INCLUDE:
                keySituation = propertyKeys.contains(key);
                break;
            case EXCLUDE:
                keySituation = !propertyKeys.contains(key);
                break;
        }

        return keySituation;
    }

    private static boolean isReservedKey(final String key) {
        return key.equals(GraphSONTokensTP2._ID) || key.equals(GraphSONTokensTP2._TYPE) || key.equals(GraphSONTokensTP2._LABEL)
                || key.equals(GraphSONTokensTP2._OUT_V) || key.equals(GraphSONTokensTP2._IN_V);
    }

    private static Object readProperty(final JsonNode node, final boolean hasEmbeddedTypes) {
        final Object propertyValue;

        if (hasEmbeddedTypes) {
            if (node.get(GraphSONTokensTP2._TYPE).textValue().equals(GraphSONTokensTP2.TYPE_UNKNOWN)) {
                propertyValue = null;
            } else if (node.get(GraphSONTokensTP2._TYPE).textValue().equals(GraphSONTokensTP2.TYPE_BOOLEAN)) {
                propertyValue = node.get(GraphSONTokensTP2.VALUE).booleanValue();
            } else if (node.get(GraphSONTokensTP2._TYPE).textValue().equals(GraphSONTokensTP2.TYPE_FLOAT)) {
                propertyValue = Float.parseFloat(node.get(GraphSONTokensTP2.VALUE).asText());
            } else if (node.get(GraphSONTokensTP2._TYPE).textValue().equals(GraphSONTokensTP2.TYPE_BYTE)) {
                propertyValue = Byte.parseByte(node.get(GraphSONTokensTP2.VALUE).asText());
            } else if (node.get(GraphSONTokensTP2._TYPE).textValue().equals(GraphSONTokensTP2.TYPE_SHORT)) {
                propertyValue = Short.parseShort(node.get(GraphSONTokensTP2.VALUE).asText());
            } else if (node.get(GraphSONTokensTP2._TYPE).textValue().equals(GraphSONTokensTP2.TYPE_DOUBLE)) {
                propertyValue = node.get(GraphSONTokensTP2.VALUE).doubleValue();
            } else if (node.get(GraphSONTokensTP2._TYPE).textValue().equals(GraphSONTokensTP2.TYPE_INTEGER)) {
                propertyValue = node.get(GraphSONTokensTP2.VALUE).intValue();
            } else if (node.get(GraphSONTokensTP2._TYPE).textValue().equals(GraphSONTokensTP2.TYPE_LONG)) {
                propertyValue = node.get(GraphSONTokensTP2.VALUE).longValue();
            } else if (node.get(GraphSONTokensTP2._TYPE).textValue().equals(GraphSONTokensTP2.TYPE_STRING)) {
                propertyValue = node.get(GraphSONTokensTP2.VALUE).textValue();
            } else if (node.get(GraphSONTokensTP2._TYPE).textValue().equals(GraphSONTokensTP2.TYPE_LIST)) {
                propertyValue = readProperties(node.get(GraphSONTokensTP2.VALUE).elements(), hasEmbeddedTypes);
            } else if (node.get(GraphSONTokensTP2._TYPE).textValue().equals(GraphSONTokensTP2.TYPE_MAP)) {
                propertyValue = readProperties(node.get(GraphSONTokensTP2.VALUE), false, hasEmbeddedTypes);
            } else {
                propertyValue = node.textValue();
            }
        } else {
            if (node.isNull()) {
                propertyValue = null;
            } else if (node.isBoolean()) {
                propertyValue = node.booleanValue();
            } else if (node.isDouble()) {
                propertyValue = node.doubleValue();
            } else if (node.isInt()) {
                propertyValue = node.intValue();
            } else if (node.isLong()) {
                propertyValue = node.longValue();
            } else if (node.isTextual()) {
                propertyValue = node.textValue();
            } else if (node.isArray()) {
                propertyValue = readProperties(node.elements(), hasEmbeddedTypes);
            } else if (node.isObject()) {
                propertyValue = readProperties(node, false, hasEmbeddedTypes);
            } else {
                propertyValue = node.textValue();
            }
        }

        return propertyValue;
    }

    private static List readProperties(final Iterator<JsonNode> listOfNodes, final boolean hasEmbeddedTypes) {
        final List array = new ArrayList();

        while (listOfNodes.hasNext()) {
            array.add(readProperty(listOfNodes.next(), hasEmbeddedTypes));
        }

        return array;
    }

    private static ArrayNode createJSONList(final List list, final List<String> propertyKeys, final boolean showTypes) {
        final ArrayNode jsonList = jsonNodeFactory.arrayNode();
        for (Object item : list) {
            if (item instanceof Element) {
                jsonList.add(objectNodeFromElement((Element) item, propertyKeys,
                        showTypes ? GraphSONMode.EXTENDED : GraphSONMode.NORMAL));
            } else if (item instanceof List) {
                jsonList.add(createJSONList((List) item, propertyKeys, showTypes));
            } else if (item instanceof Map) {
                jsonList.add(createJSONMap((Map) item, propertyKeys, showTypes));
            } else if (item != null && item.getClass().isArray()) {
                jsonList.add(createJSONList(convertArrayToList(item), propertyKeys, showTypes));
            } else {
                addObject(jsonList, item);
            }
        }
        return jsonList;
    }

    private static ObjectNode createJSONMap(final Map map, final List<String> propertyKeys, final boolean showTypes) {
        final ObjectNode jsonMap = jsonNodeFactory.objectNode();
        for (Object key : map.keySet()) {
            Object value = map.get(key);
            if (value != null) {
                if (value instanceof List) {
                    value = createJSONList((List) value, propertyKeys, showTypes);
                } else if (value instanceof Iterable){
                    value = createJSONList(getList((Iterable) value), propertyKeys, showTypes);
                } else if (value instanceof Iterator){
                    value = createJSONList(getList((Iterator) value), propertyKeys, showTypes);
                } else if (value instanceof Map) {
                    value = createJSONMap((Map) value, propertyKeys, showTypes);
                } else if (value instanceof Element) {
                    value = objectNodeFromElement((Element) value, propertyKeys,
                            showTypes ? GraphSONMode.EXTENDED : GraphSONMode.NORMAL);
                } else if (value.getClass().isArray()) {
                    value = createJSONList(convertArrayToList(value), propertyKeys, showTypes);
                }
            }

            putObject(jsonMap, key.toString(), getValue(value, showTypes));
        }
        return jsonMap;

    }

    private static List getList(final Iterable value) {
        return getList(value.iterator());
    }

    private static List getList(final Iterator value) {
        final List result = new ArrayList();
        while (value.hasNext()) {
            result.add(value.next());
        }
        return result;
    }

    private static void addObject(final ArrayNode jsonList, final Object value) {
        if (value == null) {
            jsonList.add((JsonNode) null);
        } else if (value.getClass() == Boolean.class) {
            jsonList.add((Boolean) value);
        } else if (value.getClass() == Long.class) {
            jsonList.add((Long) value);
        } else if (value.getClass() == Integer.class) {
            jsonList.add((Integer) value);
        } else if (value.getClass() == Float.class) {
            jsonList.add((Float) value);
        } else if (value.getClass() == Double.class) {
            jsonList.add((Double) value);
        } else if (value.getClass() == Byte.class) {
            jsonList.add((Byte) value);
        } else if (value.getClass() == Short.class) {
            jsonList.add((Short) value);
        } else if (value.getClass() == String.class) {
            jsonList.add((String) value);
        } else if (value instanceof ObjectNode) {
            jsonList.add((ObjectNode) value);
        } else if (value instanceof ArrayNode) {
            jsonList.add((ArrayNode) value);
        } else {
            jsonList.add(value.toString());
        }
    }

    private static void putObject(final ObjectNode jsonMap, final String key, final Object value) {
        if (value == null) {
            jsonMap.put(key, (JsonNode) null);
        } else if (value.getClass() == Boolean.class) {
            jsonMap.put(key, (Boolean) value);
        } else if (value.getClass() == Long.class) {
            jsonMap.put(key, (Long) value);
        } else if (value.getClass() == Integer.class) {
            jsonMap.put(key, (Integer) value);
        } else if (value.getClass() == Float.class) {
            jsonMap.put(key, (Float) value);
        } else if (value.getClass() == Double.class) {
            jsonMap.put(key, (Double) value);
        } else if (value.getClass() == Short.class) {
            jsonMap.put(key, (Short) value);
        } else if (value.getClass() == Byte.class) {
            jsonMap.put(key, (Byte) value);
        } else if (value.getClass() == String.class) {
            jsonMap.put(key, (String) value);
        } else if (value instanceof ObjectNode) {
            jsonMap.put(key, (ObjectNode) value);
        } else if (value instanceof ArrayNode) {
            jsonMap.put(key, (ArrayNode) value);
        } else {
            jsonMap.put(key, value.toString());
        }
    }

    private static Map createPropertyMap(final Element element, final List<String> propertyKeys,
                                         final ElementPropertiesRule rule, final boolean normalized) {
        final Map map = new HashMap<String, Object>();
        final List<String> propertyKeyList;
        if (normalized) {
            final List<String> sorted = new ArrayList<String>(element.keys());
            Collections.sort(sorted);
            propertyKeyList = sorted;
        } else
            propertyKeyList = new ArrayList<String>(element.keys());

        if (propertyKeys == null) {
            for (String key : propertyKeyList) {
                final Object valToPutInMap = element.property(key).value();
                if (valToPutInMap != null) {
                    map.put(key, valToPutInMap);
                }
            }
        } else {
            if (rule == ElementPropertiesRule.INCLUDE) {
                for (String key : propertyKeys) {
                    final Object valToPutInMap = element.property(key).value();
                    if (valToPutInMap != null) {
                        map.put(key, valToPutInMap);
                    }
                }
            } else {
                for (String key : propertyKeyList) {
                    if (!propertyKeys.contains(key)) {
                        final Object valToPutInMap = element.property(key).value();
                        if (valToPutInMap != null) {
                            map.put(key, valToPutInMap);
                        }
                    }
                }
            }
        }

        return map;
    }

    private static Object getValue(Object value, final boolean includeType) {

        Object returnValue = value;

        // if the includeType is set to true then show the data types of the properties
        if (includeType) {

            // type will be one of: map, list, string, long, int, double, float.
            // in the event of a complex object it will call a toString and store as a
            // string
            String type = determineType(value);

            ObjectNode valueAndType = jsonNodeFactory.objectNode();
            valueAndType.put(GraphSONTokens.TYPE, type);

            if (type.equals(GraphSONTokensTP2.TYPE_LIST)) {

                // values of lists must be accumulated as ObjectNode objects under the value key.
                // will return as a ArrayNode. called recursively to traverse the entire
                // object graph of each item in the array.
                ArrayNode list = (ArrayNode) value;

                // there is a set of values that must be accumulated as an array under a key
                ArrayNode valueArray = valueAndType.putArray(GraphSONTokens.VALUE);
                for (int ix = 0; ix < list.size(); ix++) {
                    // the value of each item in the array is a node object from an ArrayNode...must
                    // get the value of it.
                    addObject(valueArray, getValue(getTypedValueFromJsonNode(list.get(ix)), includeType));
                }

            } else if (type.equals(GraphSONTokensTP2.TYPE_MAP)) {

                // maps are converted to a ObjectNode.  called recursively to traverse
                // the entire object graph within the map.
                ObjectNode convertedMap = jsonNodeFactory.objectNode();
                ObjectNode jsonObject = (ObjectNode) value;
                Iterator keyIterator = jsonObject.fieldNames();
                while (keyIterator.hasNext()) {
                    Object key = keyIterator.next();

                    // no need to getValue() here as this is already a ObjectNode and should have type info
                    convertedMap.put(key.toString(), jsonObject.get(key.toString()));
                }

                valueAndType.put(GraphSONTokens.VALUE, convertedMap);
            } else {

                // this must be a primitive value or a complex object.  if a complex
                // object it will be handled by a call to toString and stored as a
                // string value
                putObject(valueAndType, GraphSONTokens.VALUE, value);
            }

            // this goes back as a JSONObject with data type and value
            returnValue = valueAndType;
        }

        return returnValue;
    }

    static Object getTypedValueFromJsonNode(JsonNode node) {
        Object theValue = null;

        if (node != null && !node.isNull()) {
            if (node.isBoolean()) {
                theValue = node.booleanValue();
            } else if (node.isDouble()) {
                theValue = node.doubleValue();
            } else if (node.isFloatingPointNumber()) {
                theValue = node.floatValue();
            } else if (node.isInt()) {
                theValue = node.intValue();
            } else if (node.isLong()) {
                theValue = node.longValue();
            } else if (node.isTextual()) {
                theValue = node.textValue();
            } else if (node.isArray()) {
                // this is an array so just send it back so that it can be
                // reprocessed to its primitive components
                theValue = node;
            } else if (node.isObject()) {
                // this is an object so just send it back so that it can be
                // reprocessed to its primitive components
                theValue = node;
            } else {
                theValue = node.textValue();
            }
        }

        return theValue;
    }

    private static List convertArrayToList(final Object value) {
        final ArrayList<Object> list = new ArrayList<Object>();
        int arrlength = Array.getLength(value);
        for (int i = 0; i < arrlength; i++) {
            Object object = Array.get(value, i);
            list.add(object);
        }
        return list;
    }

    private static String determineType(final Object value) {
        String type = GraphSONTokensTP2.TYPE_STRING;
        if (value == null) {
            type = GraphSONTokensTP2.TYPE_UNKNOWN;
        } else if (value.getClass() == Double.class) {
            type = GraphSONTokensTP2.TYPE_DOUBLE;
        } else if (value.getClass() == Float.class) {
            type = GraphSONTokensTP2.TYPE_FLOAT;
        } else if (value.getClass() == Byte.class) {
            type = GraphSONTokensTP2.TYPE_BYTE;
        } else if (value.getClass() == Short.class) {
            type = GraphSONTokensTP2.TYPE_SHORT;
        } else if (value.getClass() == Integer.class) {
            type = GraphSONTokensTP2.TYPE_INTEGER;
        } else if (value.getClass() == Long.class) {
            type = GraphSONTokensTP2.TYPE_LONG;
        } else if (value.getClass() == Boolean.class) {
            type = GraphSONTokensTP2.TYPE_BOOLEAN;
        } else if (value instanceof ArrayNode) {
            type = GraphSONTokensTP2.TYPE_LIST;
        } else if (value instanceof ObjectNode) {
            type = GraphSONTokensTP2.TYPE_MAP;
        }

        return type;
    }
}