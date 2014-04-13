package com.tinkerpop.gremlin.structure.io.graphml;

/**
 * A collection of tokens used for GraphML related data.
 *
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
class GraphMLTokens {
    public static final String XML_SCHEMA_NAMESPACE_TAG = "xsi";
    public static final String DEFAULT_GRAPHML_SCHEMA_LOCATION = "http://graphml.graphdrawing.org/xmlns/1.1/graphml.xsd";
    public static final String XML_SCHEMA_LOCATION_ATTRIBUTE = "schemaLocation";
    public static final String GRAPHML = "graphml";
    public static final String XMLNS = "xmlns";
    public static final String GRAPHML_XMLNS = "http://graphml.graphdrawing.org/xmlns";
    public static final String G = "G";
    public static final String EDGEDEFAULT = "edgedefault";
    public static final String DIRECTED = "directed";
    public static final String KEY = "key";
    public static final String FOR = "for";
    public static final String ID = "id";
    public static final String ATTR_NAME = "attr.name";
    public static final String ATTR_TYPE = "attr.type";
    public static final String GRAPH = "graph";
    public static final String NODE = "node";
    public static final String EDGE = "edge";
    public static final String SOURCE = "source";
    public static final String TARGET = "target";
    public static final String DATA = "data";
    public static final String LABEL_E = "labelE";
    public static final String LABEL_V = "labelV";
    public static final String STRING = "string";
    public static final String FLOAT = "float";
    public static final String DOUBLE = "double";
    public static final String LONG = "long";
    public static final String BOOLEAN = "boolean";
    public static final String INT = "int";
}
