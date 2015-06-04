/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.tinkerpop.gremlin.process.traversal.step.map;

import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserRequirement;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;

import java.util.*;

/**
 * @author Daniel Kuppitz (http://gremlin.guru)
 */
public final class RangeLocalStep<S> extends MapStep<S, S> {

    private final long low;
    private final long high;

    public RangeLocalStep(final Traversal.Admin traversal, final long low, final long high) {
        super(traversal);
        if (low != -1 && high != -1 && low > high) {
            throw new IllegalArgumentException("Not a legal range: [" + low + ", " + high + ']');
        }
        this.low = low;
        this.high = high;
    }

    @Override
    protected S map(final Traverser.Admin<S> traverser) {
        final S start = traverser.get();
        return applyRange(start, this.low, this.high);
    }

    /**
     * Extracts the specified range of elements from a collection.
     * Return type depends on dynamic type of start.
     * <ul>
     * <li>
     * Map becomes Map (order-preserving)
     * </li>
     * <li>
     * Set becomes Set (order-preserving)
     * </li>
     * <li>
     * Other Collection types become List
     * </li>
     * </ul>
     */
    static <S> S applyRange(final S start, final long low, final long high) {
        if (start instanceof Map) {
            return (S) applyRangeMap((Map) start, low, high);
        } else if (start instanceof Collection) {
            return (S) applyRangeCollection((Collection) start, low, high);
        }
        return start;
    }

    /** Extracts specified range of elements from a Map. */
    private static Map applyRangeMap(final Map map, final long low, final long high) {
        final long capacity = (high != -1 ? high : map.size()) - low;
        final Map result = new LinkedHashMap((int) Math.min(capacity, map.size()));
        long c = 0L;
        for (final Object obj : map.entrySet()) {
            final Map.Entry entry = (Map.Entry) obj;
            if (c >= low) {
                if (c < high || high == -1) {
                    result.put(entry.getKey(), entry.getValue());
                } else break;
            }
            c++;
        }
        return result;
    }

    /** Extracts specified range of elements from a Collection. */
    private static Collection applyRangeCollection(final Collection collection, final long low, final long high) {
        final Collection result = (collection instanceof Set) ? new LinkedHashSet() : new LinkedList();
        long c = 0L;
        for (final Object item : collection) {
            if (c >= low) {
                if (c < high || high == -1) {
                    result.add(item);
                } else break;
            }
            c++;
        }
        return result;
    }

    @Override
    public String toString() {
        return StringFactory.stepString(this, this.low, this.high);
    }

    @Override
    public int hashCode() {
        return super.hashCode() ^ Long.hashCode(this.high) ^ Long.hashCode(this.low);
    }

    @Override
    public Set<TraverserRequirement> getRequirements() {
        return Collections.singleton(TraverserRequirement.OBJECT);
    }
}
