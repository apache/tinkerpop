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
package org.apache.tinkerpop.gremlin.util;

import org.apache.tinkerpop.gremlin.process.traversal.Path;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Comparator;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

/**
 * An implementation of the Comparability/Orderability semantics as defined in the Apache TinkerPop Provider
 * documentation.
 *
 * @author Mike Personick
 */
public class OrderabilityComparator implements Comparator<Object> {

    public static final Comparator<Object> INSTANCE = Comparator.comparing(
            // first transform (strip Traverser layer)
            OrderabilityComparator::transform,
                    // then handle nulls
                    Comparator.nullsFirst(
                            // then consider orderability
                            new OrderabilityComparator()));

    /**
     * Boolean, Date, String, UUID.
     */
    private static final Comparator<Comparable> naturalOrderComparator = Comparator.naturalOrder();

    /**
     * This comparator does not provide a stable order for numerics because of type promotion equivalence semantics.
     */
    private static final Comparator<Number> numberComparator = (f,s) -> NumberHelper.compare(f, s);

    /**
     * Sort Vertex, Edge, VertexProperty by id.
     */
    private static final Comparator<Element> elementComparator =
            Comparator.comparing(Element::id, INSTANCE);

    /**
     * Sort Property first by key, then by value.
     */
    private static final Comparator<Property> propertyComparator =
            Comparator.<Property,Object>comparing(Property::key, INSTANCE).thenComparing(Property::value, INSTANCE);

    /**
     * Sort List, Set, Path, and Map element-by-element in the order presented by their natural iterator.
     */
    private static final Comparator<Iterable> iteratableComparator = (f, s) -> {
        final Iterator fi = f.iterator();
        final Iterator si = s.iterator();

        while (fi.hasNext() && si.hasNext()) {
            final int i = INSTANCE.compare(fi.next(), si.next());
            if (i != 0) {
                return i;
            }
        }

        return fi.hasNext() ? 1 : si.hasNext() ? -1 : 0;
    };

    /**
     * Sort Map by entry-set.
     */
    private static final Comparator<Map> mapComparator =
            Comparator.comparing(Map::entrySet, iteratableComparator);

    /**
     * Sort Map.Entry first by key, then by value.
     */
    private static final Comparator<Map.Entry> entryComparator =
            Comparator.<Map.Entry,Object>comparing(Map.Entry::getKey, INSTANCE).thenComparing(Map.Entry::getValue, INSTANCE);

    /**
     * Sort unknown types first by classname, then by natural toString().
     */
    private static final Comparator<Object> unknownTypeComparator =
            Comparator.comparing(f -> f.getClass().getName()).thenComparing(Object::toString);

    /**
     * The typespace, along with their priorities and the comparators used to compare within each type.
     */
    private enum Type {
        Boolean         (Boolean.class, 0, naturalOrderComparator),
        Number          (Number.class, 1, numberComparator),
        Date            (Date.class, 2, naturalOrderComparator),
        String          (String.class, 3, naturalOrderComparator),
        UUID            (UUID.class, 4, naturalOrderComparator),
        Vertex          (Vertex.class, 5, elementComparator),
        Edge            (Edge.class, 6, elementComparator),
        VertexProperty  (VertexProperty.class, 7, elementComparator),
        Property        (Property.class, 8, propertyComparator),
        Path            (Path.class, 9, iteratableComparator),
        Set             (Set.class, 10, iteratableComparator),
        List            (List.class, 11, iteratableComparator),
        Map             (Map.class, 12, mapComparator),
        MapEntry        (Map.Entry.class, 13, entryComparator),
        Unknown         (Object.class, 14, unknownTypeComparator);

        /**
         * Lookup by instanceof semantics (not class equality). Current implementation will return first enum value
         * that matches the object's type.
         */
        public static Type type(final Object o) {
            final Type[] types = Type.values();
            for (int i = 0; i < types.length; i++) {
                if (types[i].type.isInstance(o)) {
                    return types[i];
                }
            }
            return Unknown;
        }

        Type(Class type, int priority, Comparator comparator) {
            this.type = type;
            this.priority = priority;
            this.comparator = comparator;
        }
        private final Class type;
        private final int priority;
        private final Comparator comparator;
    }

    /**
     * Strip the Traverser layer and convert Enum to string.
     */
    private static Object transform(Object o) {
        // we want to sort the underlying object contained by the traverser
        if (o instanceof Traverser)
            o = ((Traverser) o).get();
        // need to convert enum to string representations for comparison or else you can get cast exceptions.
        // this typically happens when sorting local on the keys of maps that contain T
        if (o instanceof Enum)
            o = ((Enum) o).name();
        return o;
    }

    private OrderabilityComparator() {}

    /**
     * Compare two non-null Gremlin value objects per the Comparability/Orderability semantics.
     */
    @Override
    public int compare(final Object f, final Object s) {
        final Type ft = Type.type(f);
        /*
         * Do a quick check to catch very common cases - class equality or both numerics. Even if these are false we
         * could still be dealing with the same type (e.g. different implementation of Vertex), but let the type lookup
         * method handle that.
         */
        final Type st = f.getClass().equals(s.getClass()) || (f instanceof Number && s instanceof Number) ?
                        ft : Type.type(s);

        return ft != st ? ft.priority - st.priority : ft.comparator.compare(f, s);
    }

}
