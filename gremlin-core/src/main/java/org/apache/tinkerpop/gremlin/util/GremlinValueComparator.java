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

import org.apache.tinkerpop.gremlin.process.traversal.GremlinTypeErrorException;
import org.apache.tinkerpop.gremlin.process.traversal.Path;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;

import java.util.Comparator;
import java.util.Date;
import java.util.EnumMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import static org.apache.tinkerpop.gremlin.util.NumberHelper.eitherAreNaN;

/**
 * An implementation of the Comparability/Orderability semantics as defined in the Apache TinkerPop Provider
 * documentation.
 *
 * @author Mike Personick (http://github.com/mikepersonick)
 */
public abstract class GremlinValueComparator implements Comparator<Object> {

    /**
     * Orderability comparator allows for a total order across all types (no type error exceptions).
     */
    public static final GremlinValueComparator ORDER = new GremlinValueComparator() {

        /**
         * Compare two Gremlin value objects per the Orderability semantics.
         */
        @Override
        public int compare(final Object f, final Object s) {
            // nulls first
            if (f == null || s == null)
                return f == s ? 0 : f == null ? -1 : 1;

            final Type ft = Type.type(f);
            /*
             * Do a quick check to catch very common cases - class equality or both numerics. Even if these are false we
             * could still be dealing with the same type (e.g. different implementation of List), but let the type
             * lookup method handle that.
             */
            final Type st = f.getClass().equals(s.getClass()) || (f instanceof Number && s instanceof Number) ?
                    ft : Type.type(s);

            return ft != st ? ft.priority() - st.priority() : comparator(ft).compare(f, s);
        }

        /**
         * Test for equivalence (Orderability semantics).
         */
        public boolean equals(final Object f, final Object s) {
            return compare(f, s) == 0;
        }
    };

    /**
     * Compare has very similar semantics to orderability with the following exceptions:
     *
     * 1. NaN is not equal to anything, including itself, and cannot be compared to anything:
     *      equals(NaN, anything) = FALSE
     *      compare(NaN, anything) = ERROR
     * 2. Unlike Orderability, Comparability is limited to a single type space:
     *      compare(type1, type2) = ERROR
     *
     * Note that because of type errors for Comparability, equals(a,b) does not necessarily produce the same result
     * as compare(a,b) == 0. Make sure to use equals(a,b) for P.eq/neq.
     */
    public static final GremlinValueComparator COMPARE = new GremlinValueComparator() {

        /**
         * Compare two Gremlin value objects per the Comparability semantics. Throws type errors for NaN comparison
         * and for cross-type comparison (including nulltype).
         *
         * Use this method for P.lt/lte/gt/gte.
         */
        @Override
        public int compare(final Object f, final Object s) {
            // For Compare, NaN always produces ERROR
            if (eitherAreNaN(f, s))
                throwTypeError();

            // For Compare we do not cross type boundaries, including null
            if (!comparable(f, s))
                throwTypeError();

            // comparable(f, s) assures that type(f) == type(s)
            final Type type = Type.type(f);
            return comparator(type).compare(f, s);
        }

        /**
         * Test two Gremlin values for equality per the Comparability semantics. Returns false for NaN comparison
         * and for cross-type comparison (including nulltype).
         *
         * Use this method for P.eq/neq.
         */
        @Override
        public boolean equals(final Object f, final Object s) {
            try {
                return compare(f, s) == 0;
            } catch (GremlinTypeErrorException ex) {
                /**
                 * By routing through the compare(f, s) path we expose ourselves to type errors, which should be
                 * reduced to false for equality:
                 *
                 * compare(NaN, anything) -> ERROR -> FALSE for equality
                 * compare(Type1, Type2) -> ERROR -> FALSE for equality
                 *
                 * Can also happen for elements nested inside of collections.
                 */
                return false;
            }
        }
    };

    private static <T> T throwTypeError() {
        throw new GremlinTypeErrorException();
    }

    /**
     * Boolean, Date, String, UUID.
     */
    private final Comparator<Comparable> naturalOrderComparator = Comparator.naturalOrder();

    /**
     * This comparator does not provide a stable order for numerics because of type promotion equivalence semantics.
     */
    private final Comparator<Number> numberComparator = (f,s) -> NumberHelper.compare(f, s);

    /**
     * Sort Vertex, Edge, VertexProperty by id.
     */
    private final Comparator<Element> elementComparator =
            Comparator.comparing(Element::id, this);

    /**
     * Sort Property first by key, then by value.
     */
    private final Comparator<Property> propertyComparator =
            Comparator.<Property,Object>comparing(Property::key, this).thenComparing(Property::value, this);

    /**
     * Sort List, Set, Path, and Map element-by-element in the order presented by their natural iterator.
     */
    private final Comparator<Iterable> iterableComparator = (f, s) -> {
        final Iterator fi = f.iterator();
        final Iterator si = s.iterator();

        while (fi.hasNext() && si.hasNext()) {
            final int i = this.compare(fi.next(), si.next());
            if (i != 0) {
                return i;
            }
        }

        return fi.hasNext() ? 1 : si.hasNext() ? -1 : 0;
    };

    /**
     * Sort Map by entry-set.
     */
    private final Comparator<Map> mapComparator =
            Comparator.comparing(Map::entrySet, iterableComparator);

    /**
     * Sort Map.Entry first by key, then by value.
     */
    private final Comparator<Map.Entry> entryComparator =
            Comparator.<Map.Entry,Object>comparing(Map.Entry::getKey, this).thenComparing(Map.Entry::getValue, this);

    /**
     * Sort using either their natural order if they are Comparable or by classname then by toString() if they are
     * not naturally Comparable. Also handles the special case: f.equals(s) -> 0 even for objects without a natural
     * comparator.
     */
    private final Comparator<Object> unknownTypeComparator = (f, s) -> naturallyComparable(f, s)
        ? naturallyCompare(f, s)
        : Comparator.comparing(o -> o.getClass().getName()).thenComparing(Object::toString).compare(f, s);

    /**
     * Two nulls. Always 0.
     */
    private final Comparator<Object> nulltypeComparator = (f, s) -> 0;

    /**
     * The typespace. The ordinal of the type indicates its position in cross-type ordering.
     */
    public enum Type {
        Nulltype,
        Boolean         (Boolean.class),
        Number          (Number.class),
        Date            (Date.class),
        String          (String.class),
        UUID            (UUID.class),
        Vertex          (Vertex.class),
        Edge            (Edge.class),
        VertexProperty  (VertexProperty.class),
        Property        (Property.class),
        Path            (Path.class),
        Set             (Set.class),
        List            (List.class),
        Map             (Map.class),
        MapEntry        (Map.Entry.class),
        Unknown         (Object.class);

        /**
         * Lookup by instanceof semantics (not class equality). Current implementation will return first enum value
         * that matches the object's type.
         */
        public static Type type(final Object o) {
            if (o == null)
                return Nulltype;

            final Type[] types = Type.values();
            for (int i = 1; i < types.length; i++) {
                if (types[i].type.isInstance(o)) {
                    return types[i];
                }
            }
            return Unknown;
        }

        Type() {
            this.type = null;
        }
        Type(final Class type) {
            this.type = type;
        }
        private final Class type;

        public int priority() {
            return ordinal();
        }
    }

    /**
     * Compare the two objects using their natural comparator. Also handles the special case: f.equals(s) -> 0 even
     * for objects without a natural comparator.
     */
    private static int naturallyCompare(final Object f, final Object s) {
        if (f instanceof Comparable && s instanceof Comparable)
            return ((Comparable) f).compareTo(s);
        return f.equals(s) ? 0 : throwTypeError();
    }

    /**
     * Return true if the two objects in the UNKNOWN type space are equal or comparable via their own natural Comparator.
     */
    private static boolean naturallyComparable(final Object f, final Object s) {
        return (f instanceof Comparable && s instanceof Comparable
                && (f.getClass().isInstance(s) || s.getClass().isInstance(f)))
                || f.equals(s);
    }

    /**
     * Return true if the two objects are of the same comparison type (although they may not be the exact same Class)
     */
    private static boolean comparable(final Object f, final Object s) {
        if (f == null || s == null)
            return f == s; // true iff both in the null space

        final Type ft = Type.type(f);
        final Type st = Type.type(s);

        // Check for same type. If they're both the unknown type then return true iff they are naturally Comparable
        return ft == Type.Unknown && st == Type.Unknown ? naturallyComparable(f, s) : ft == st;
    }

    private final Map<Type, Comparator> comparators = new EnumMap<Type, Comparator>(Type.class) {{
        put(Type.Nulltype,       nulltypeComparator);
        put(Type.Boolean,        naturalOrderComparator);
        put(Type.Number,         numberComparator);
        put(Type.Date,           naturalOrderComparator);
        put(Type.String,         naturalOrderComparator);
        put(Type.UUID,           naturalOrderComparator);
        put(Type.Vertex,         elementComparator);
        put(Type.Edge,           elementComparator);
        put(Type.VertexProperty, elementComparator);
        put(Type.Property,       propertyComparator);
        put(Type.Path,           iterableComparator);
        put(Type.Set,            iterableComparator);
        put(Type.List,           iterableComparator);
        put(Type.Map,            mapComparator);
        put(Type.MapEntry,       entryComparator);
        put(Type.Unknown,        unknownTypeComparator);
    }};
    
    private GremlinValueComparator() {}

    protected Comparator comparator(final Type type) {
        return comparators.get(type);
    }

    /**
     * Compare(a,b) is defined differently for Comparablity vs. Orderability.
     */
    public abstract int compare(final Object f, final Object s);

    /**
     * Equals(a,b) is defined differently for Comparablity (equality) vs. Orderability (equivalence).
     */
    public abstract boolean equals(final Object f, final Object s);

}
