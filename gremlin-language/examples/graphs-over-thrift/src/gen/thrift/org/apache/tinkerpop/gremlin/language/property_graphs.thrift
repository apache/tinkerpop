/**
 * Basic Property Graph data model
 * 
 * @name org/apache/tinkerpop/gremlin/language/property_graphs
 * @note This is an auto-generated file. Manual changes to the file may not be preserved.
 * @status development
 */

namespace java org.apache.tinkerpop.gremlin.language.property_graphs

/**
 * The label of an edge, sometimes indicating its type
 * 
 * @type string
 */
typedef string EdgeLabel

/**
 * The key of a property. Property keys are part of a graph's schema, like vertex and edge labels.
 * 
 * @type string
 */
typedef string PropertyKey

/**
 * The label of a vertex, sometimes indicating its type
 * 
 * @type string
 */
typedef string VertexLabel

/**
 * A simple value like a boolean, number, or string
 */
union AtomicValue {
    /**
     * A boolean (true/false) value
     * 
     * @type boolean
     */
    1: optional bool booleanEsc;
    /**
     * A byte (8-bit integer) value
     * 
     * @type integer:
     *         precision:
     *           bits: 8
     */
    2: optional i16 byteEsc;
    /**
     * A double-precision (64-bit) floating point value
     * 
     * @type float:
     *         precision:
     *           bits: 64
     */
    3: optional double doubleEsc;
    /**
     * A single-precision (32-bit) floating point value
     * 
     * @type float
     */
    4: optional double floatEsc;
    /**
     * A 32-bit integer value
     * 
     * @type integer
     */
    5: optional i32 integer;
    /**
     * A 64-bit integer value
     * 
     * @type integer:
     *         precision:
     *           bits: 64
     */
    6: optional i64 longEsc;
    /**
     * A string value
     * 
     * @type string
     */
    7: optional string stringEsc;
}

/**
 * The unique id of an edge
 * 
 * @type org/apache/tinkerpop/gremlin/language/property_graphs.AtomicValue
 */
typedef AtomicValue EdgeId

/**
 * A property, or key/value pair which may be attached to vertices, edges, and occasionally other properties.
 */
struct Property {
    /**
     * The property's key
     * 
     * @type org/apache/tinkerpop/gremlin/language/property_graphs.PropertyKey
     */
    1: required PropertyKey key;
    /**
     * The property's value
     * 
     * @type org/apache/tinkerpop/gremlin/language/property_graphs.Value
     */
    2: required Value value;
    /**
     * Any metaproperties (properties of a property)
     * 
     * @type set: org/apache/tinkerpop/gremlin/language/property_graphs.Property
     */
    3: required set<Property> metaproperties;
}

/**
 * An id or property value; either an atomic (simple) value, or a list, map, or set value
 */
union Value {
    /**
     * An atomic (simple) value
     * 
     * @type org/apache/tinkerpop/gremlin/language/property_graphs.AtomicValue
     */
    1: optional AtomicValue atomic;
    /**
     * An ordered list value
     * 
     * @type list: org/apache/tinkerpop/gremlin/language/property_graphs.Value
     */
    2: optional list<Value> listEsc;
    /**
     * An array value. Like a list, but also supporting efficient random access
     * 
     * @type list: org/apache/tinkerpop/gremlin/language/property_graphs.Value
     */
    3: optional list<Value> array;
    /**
     * A map of arbitrary string keys to arbitrary values
     * 
     * @type map:
     *         keys: string
     *         values: org/apache/tinkerpop/gremlin/language/property_graphs.Value
     */
    4: optional map<string, Value> mapEsc;
    /**
     * A set of unique values
     * 
     * @type set: org/apache/tinkerpop/gremlin/language/property_graphs.Value
     */
    5: optional set<Value> setEsc;
    /**
     * A serialized object which the application should be capable of decoding
     * 
     * @type string
     */
    6: optional string serialized;
}

/**
 * The unique id of a vertex
 * 
 * @type org/apache/tinkerpop/gremlin/language/property_graphs.AtomicValue
 */
typedef AtomicValue VertexId

/**
 * A edge, or binary relationship connecting two vertices
 */
struct Edge {
    /**
     * The unique id of the edge; no two edges in a graph may share the same id
     * 
     * @type org/apache/tinkerpop/gremlin/language/property_graphs.EdgeId
     * @unique true
     */
    1: required EdgeId id;
    /**
     * The label of the edge, sometimes indicating its type
     * 
     * @type org/apache/tinkerpop/gremlin/language/property_graphs.EdgeLabel
     */
    2: required EdgeLabel label;
    /**
     * Any properties (key/value pairs) of the edge
     * 
     * @type set: org/apache/tinkerpop/gremlin/language/property_graphs.Property
     */
    3: required set<Property> properties;
    /**
     * The id of the edge's out-vertex (tail)
     * 
     * @type org/apache/tinkerpop/gremlin/language/property_graphs.VertexId
     */
    4: required VertexId outVertexId;
    /**
     * The id if the edge's in-vertex (head)
     * 
     * @type org/apache/tinkerpop/gremlin/language/property_graphs.VertexId
     */
    5: required VertexId inVertexId;
}

/**
 * A vertex, or simple element in a graph; vertices are typically connected by edges
 */
struct Vertex {
    /**
     * The unique id of the vertex; no two vertices in a graph may share the same id
     * 
     * @type org/apache/tinkerpop/gremlin/language/property_graphs.VertexId
     * @unique true
     */
    1: required VertexId id;
    /**
     * The optional label of the vertex, sometimes indicating its type
     * 
     * @type optional: org/apache/tinkerpop/gremlin/language/property_graphs.VertexLabel
     */
    2: optional VertexLabel label;
    /**
     * Any properties (key/value pairs) of the vertex
     * 
     * @type set: org/apache/tinkerpop/gremlin/language/property_graphs.Property
     */
    3: required set<Property> properties;
}

/**
 * A graph, consisting of a set of vertices and a set of edges
 * 
 * @comments As a basic integrity constraint, the out- and in- vertex ids of the graph's edges must be among the ids of the graph's vertices.
 */
struct Graph {
    /**
     * The set of all vertices in the graph
     * 
     * @type set: org/apache/tinkerpop/gremlin/language/property_graphs.Vertex
     */
    1: required set<Vertex> vertices;
    /**
     * The set of all edges in the graph
     * 
     * @type set: org/apache/tinkerpop/gremlin/language/property_graphs.Edge
     */
    2: required set<Edge> edges;
}
