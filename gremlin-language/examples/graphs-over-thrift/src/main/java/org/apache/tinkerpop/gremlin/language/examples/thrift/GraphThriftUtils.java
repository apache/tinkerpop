package org.apache.tinkerpop.gremlin.language.examples.thrift;

import org.apache.tinkerpop.gremlin.language.property_graphs.AtomicValue;
import org.apache.tinkerpop.gremlin.language.property_graphs.Value;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class GraphThriftUtils {
    public static AtomicValue fromNativeAtomicValue(Object o) {
        if (o instanceof Boolean) {
            return AtomicValue.booleanEsc((Boolean) o);
        } else if (o instanceof Short) {
            return AtomicValue.byteEsc((Short) o);
        } else if (o instanceof Double) {
            return AtomicValue.doubleEsc((Double) o);
        } else if (o instanceof Float) {
            // Note: toString avoids float-to-double precision issues
            return AtomicValue.floatEsc(Double.parseDouble(o.toString()));
        } else if (o instanceof Integer) {
            return AtomicValue.integer((Integer) o);
        } else if (o instanceof Long) {
            return AtomicValue.longEsc((Long) o);
        } else if (o instanceof String) {
            return AtomicValue.stringEsc((String) o);
        } else {
            throw new IllegalArgumentException();
        }
    }

    public static org.apache.tinkerpop.gremlin.language.property_graphs.Graph fromNativeGraph(Graph graph) {
        Set<org.apache.tinkerpop.gremlin.language.property_graphs.Vertex> vertices0 = new HashSet<>();
        Iterator<Vertex> vertices = graph.vertices();
        while (vertices.hasNext()) {
            Vertex v = vertices.next();

            org.apache.tinkerpop.gremlin.language.property_graphs.Vertex v0
                    = new org.apache.tinkerpop.gremlin.language.property_graphs.Vertex();
            v0.setId(fromNativeAtomicValue(v.id()));
            v0.setLabel(v.label());

            Iterator<VertexProperty<Object>> props = v.properties();
            Set<org.apache.tinkerpop.gremlin.language.property_graphs.Property> props0 = new HashSet<>();
            while (props.hasNext()) {
                props0.add(fromNativeProperty(props.next()));
            }
            v0.setProperties(props0);

            vertices0.add(v0);
        }

        Set<org.apache.tinkerpop.gremlin.language.property_graphs.Edge> edges0 = new HashSet<>();
        Iterator<Edge> edges = graph.edges();
        while (edges.hasNext()) {
            Edge e = edges.next();

            org.apache.tinkerpop.gremlin.language.property_graphs.Edge e0
                    = new org.apache.tinkerpop.gremlin.language.property_graphs.Edge();
            e0.setId(fromNativeAtomicValue(e.id()));
            e0.setLabel(e.label());
            e0.setOutVertexId(fromNativeAtomicValue(e.outVertex().id()));
            e0.setInVertexId(fromNativeAtomicValue(e.inVertex().id()));

            Iterator<Property<Object>> props = e.properties();
            Set<org.apache.tinkerpop.gremlin.language.property_graphs.Property> props0 = new HashSet<>();
            while (props.hasNext()) {
                props0.add(fromNativeProperty(props.next()));
            }
            e0.setProperties(props0);

            edges0.add(e0);
        }

        org.apache.tinkerpop.gremlin.language.property_graphs.Graph graph0
                = new org.apache.tinkerpop.gremlin.language.property_graphs.Graph();
        graph0.setVertices(vertices0);
        graph0.setEdges(edges0);

        return graph0;
    }

    public static org.apache.tinkerpop.gremlin.language.property_graphs.Property fromNativeProperty(Property prop) {
        org.apache.tinkerpop.gremlin.language.property_graphs.Property p0
                = new org.apache.tinkerpop.gremlin.language.property_graphs.Property();
        p0.setKey(prop.key());
        p0.setValue(fromNativeValue(prop.value()));
        p0.setMetaproperties(new HashSet<>()); // don't bother with metaproperties for now

        //System.out.println("Native value: " + prop.value() + " ( " + prop.value().getClass() + ")"
        //        + " --> Thrift value: " + fromNativeValue(prop.value()));
        return p0;
    }

    public static Value fromNativeValue(Object o) {
        if (o.getClass().isArray()) {
            List<Value> coll = new ArrayList<>();
            for (Object o1 : (Object[]) o) {
                coll.add(fromNativeValue(o1));
            }
            return Value.array(coll);
        } else if (o instanceof List) {
            List<Value> coll = new ArrayList<>();
            for (Object o1 : (List<Object>) o) {
                coll.add(fromNativeValue(o1));
            }
            return Value.listEsc(coll);
        } else if (o instanceof Set) {
            Set<Value> coll = new HashSet<>();
            for (Object o1 : (Set<Object>) o) {
                coll.add(fromNativeValue(o1));
            }
            return Value.setEsc(coll);
        } else if (o instanceof Map) {
            Map<String, Value> coll = new HashMap<>();
            for (Map.Entry<String, Object> e : ((Map<String, Object>) o).entrySet()) {
                coll.put(e.getKey(), fromNativeValue(e.getValue()));
            }
            return Value.mapEsc(coll);
        } else {
            return Value.atomic(fromNativeAtomicValue(o));
        }
    }

    public static Object toNativeAtomicValue(AtomicValue v) {
        if (v.isSetBooleanEsc()) {
            return v.getBooleanEsc();
        } else if (v.isSetByteEsc()) {
            return v.getByteEsc();
        } else if (v.isSetDoubleEsc()) {
            return v.getDoubleEsc();
        } else if (v.isSetFloatEsc()) {
            return v.getFloatEsc();
        } else if (v.isSetInteger()) {
            return v.getInteger();
        } else if (v.isSetLongEsc()) {
            return v.getLongEsc();
        } else if (v.isSetStringEsc()) {
            return v.getStringEsc();
        } else {
            throw new IllegalArgumentException();
        }
    }

    public static Object toNativeValue(Value v) {
        if (v.isSetAtomic()) {
            return toNativeAtomicValue(v.getAtomic());
        } else if (v.isSetListEsc()) {
            List<Object> coll = new LinkedList<>();
            for (Value v1 : v.getListEsc()) {
                coll.add(toNativeValue(v1));
            }
            return coll;
        } else if (v.isSetArray()) {
            Object[] array = new Object[v.getArray().size()];
            int i = 0;
            for (Value v1 : v.getArray()) {
                array[i++] = toNativeValue(v1);
            }
            return array;
        } else if (v.isSetMapEsc()) {
            Map<String, Object> map = new HashMap<>();
            for (Map.Entry<String, Value> e : v.getMapEsc().entrySet()) {
                map.put(e.getKey(), toNativeValue(e.getValue()));
            }
            return map;
        } else if (v.isSetSetEsc()) {
            Set<Object> coll = new HashSet<>();
            for (Value v1 : v.getSetEsc()) {
                coll.add(toNativeValue(v1));
            }
            return coll;
        } else if (v.isSetSerialized()) {
            // Note: an appropriate deserializer should be used here
            return v.getSerialized();
        } else {
            throw new IllegalArgumentException();
        }
    }
}
