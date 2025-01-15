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
import org.apache.tinkerpop.gremlin.process.traversal.step.GValue;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserRequirement;
import org.apache.tinkerpop.gremlin.process.traversal.util.FastNoSuchElementException;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.Map;
import java.util.Set;

/**
 * @author Daniel Kuppitz (http://gremlin.guru)
 */
public final class RangeLocalStep<S> extends ScalarMapStep<S, S> {

    private final GValue<Long> low;
    private final GValue<Long> high;

    public RangeLocalStep(final Traversal.Admin traversal, final long low, final long high) {
        this(traversal, GValue.ofLong(null, low), GValue.ofLong(null, high));
    }

    public RangeLocalStep(final Traversal.Admin traversal, final GValue<Long> low, final GValue<Long> high) {
        super(traversal);
        if (low.get() != -1 && high.get() != -1 && low.get() > high.get()) {
            throw new IllegalArgumentException("Not a legal range: [" + low + ", " + high + ']');
        }
        this.low = low;
        this.high = high;
    }

    public long getLowRange() {
        return this.low.get();
    }
 
    public long getHighRange() {
        return this.high.get();
    } 
 

    @Override
    protected S map(final Traverser.Admin<S> traverser) {
        final S start = traverser.get();
        return applyRange(start, this.low.get(), this.high.get());
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
        } else if (start instanceof Iterable) {
            return (S) applyRangeIterable((Iterable) start, low, high);
        } else if (start != null && start.getClass().isArray()) {
            return (S) applyRangeIterable(IteratorUtils.asList(start), low, high);
        }
        return start;
    }

    /**
     * Extracts specified range of elements from a Map.
     */
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

    /**
     * Extracts specified range of elements from a Collection.
     */
    private static Object applyRangeIterable(final Iterable<Object> iterable, final long low, final long high) {
        // See if we only want a single item.  It is also possible that we will allow more than one item, but that the
        // incoming container is only capable of producing a single item.  In that case, we will still emit a
        // container.  This allows the result type to be predictable based on the step arguments.  It also allows us to
        // avoid creating the result container for the single case.
        boolean single = high != -1 ? (high - low == 1) : false;

        final Collection resultCollection =
                single ? null : (iterable instanceof Set) ? new LinkedHashSet() : new LinkedList();
        Object result = single ? null : resultCollection;
        long c = 0L;
        for (final Object item : iterable) {
            if (c >= low) {
                if (c < high || high == -1) {
                    if (single) {
                        result = item;
                        break;
                    } else {
                        resultCollection.add(item);
                    }
                } else break;
            }
            c++;
        }
        if (null == result)
            // We have nothing to emit, so stop traversal.
            throw FastNoSuchElementException.instance();
        return result;
    }

    @Override
    public String toString() {
        return StringFactory.stepString(this, this.low.get(), this.high.get());
    }

    @Override
    public int hashCode() {
        return super.hashCode() ^ Long.hashCode(this.high.get()) ^ Long.hashCode(this.low.get());
    }

    @Override
    public Set<TraverserRequirement> getRequirements() {
        return Collections.singleton(TraverserRequirement.OBJECT);
    }
}
