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
     * The order of this array determines the sort order across types.The index in this array corresponds directly to a
     * Comparator in the {@link #comparators} array below, so be sure to keep them in sync.
     */
    private static final Class[] types = new Class[] {
            Boolean.class,
            Number.class,
            Date.class,
            String.class,
            UUID.class,
            Vertex.class,
            Edge.class,
            VertexProperty.class,
            Property.class,
            Path.class,
            Set.class,
            List.class,
            Map.class,
            Map.Entry.class,
    };

    /**
     * The stable ordering for numeric types, should we choose to support stable ordering for numeric types.
     */
    private static final Class[] numerics = new Class[] {
            Byte.class,
            Short.class,
            Integer.class,
            Long.class,
            BigInteger.class,
            Float.class,
            Double.class,
            BigDecimal.class
    };

    private static final Comparator<Comparable> naturalOrderComparator = Comparator.naturalOrder();

    /**
     * This comparator does not provide a stable order for numerics because of type promotion equivalence semantics.
     */
    private static final Comparator<Number> numberComparator = (f,s) -> NumberHelper.compare(f, s);

    /**
     * This alternative numeric comparator will sort by numeric type in the case of type promotion equivalence.
     * Currently unused.
     */
    private static final Comparator<Number> stableOrderNumberComparator = (f,s) -> {
        final int i = NumberHelper.compare(f, s);
        if (i != 0 || f.getClass().equals(s.getClass())) {
            // they're not equal, or they are of the same numeric type
            return i;
        }
        // if they are equal but of different types, order by type
        final int ft = type(numerics, f);
        final int st = type(numerics, s);
        return ft - st;
    };

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
     * The comparators that will be used for the types defined above in the {@link #types} array. Be sure to keep them
     * in sync.
     */
    private static final Comparator[] comparators = new Comparator[] {
        naturalOrderComparator,          // Boolean.class,
        numberComparator,                // Number.class,
        naturalOrderComparator,          // Date.class,
        naturalOrderComparator,          // String.class,
        naturalOrderComparator,          // UUID.class,
        elementComparator,               // Vertex.class,
        elementComparator,               // Edge.class,
        elementComparator,               // VertexProperty.class,
        propertyComparator,              // Property.class,
        iteratableComparator,            // Path.class,
        iteratableComparator,            // Set.class,
        iteratableComparator,            // List.class,
        mapComparator,                   // Map.class,
        entryComparator,                 // Map.Entry.class,
    };

    /**
     * Marker priority for unknown types. A large number means they will appear after known types. Use -1 if you want
     * them to appear first.
     */
    private static final int UNKNOWN_TYPE = 0xffff;

    /**
     * Return the first index of the Class array for which the supplied object is an instance. Returns
     * {@link #UNKNOWN_TYPE} if not an instance of any of the supplied classes.
     */
    private static final int type(final Class[] types, final Object o) {
        for (int i = 0; i < types.length; i++) {
            if (types[i].isInstance(o)) {
                return i;
            }
        }
        return UNKNOWN_TYPE;
    }

    /**
     * Return the comparator for the supplied type, or the {@link #unknownTypeComparator} for unknown types.
     */
    private static final Comparator comparator(final int type) {
        return type >= 0 && type < types.length ? comparators[type] : unknownTypeComparator;
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
        final int ft = type(types, f);
        final int st = type(types, s);

        return ft != st ? ft - st : comparator(ft).compare(f, s);
    }

}
