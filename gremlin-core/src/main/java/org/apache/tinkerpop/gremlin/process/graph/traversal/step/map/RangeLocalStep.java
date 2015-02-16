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

package org.apache.tinkerpop.gremlin.process.graph.traversal.step.map;

import org.apache.tinkerpop.gremlin.process.Traversal;
import org.apache.tinkerpop.gremlin.process.Traverser;

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
        if (start instanceof Map) {
            final Map map = (Map) start;
            final long capacity = (this.high != -1 ? this.high : map.size()) - this.low;
            final Map result = new LinkedHashMap((int) Math.min(capacity, map.size()));
            long c = 0L;
            for (final Object obj : map.entrySet()) {
                final Map.Entry entry = (Map.Entry) obj;
                if (c >= this.low) {
                    if (c < this.high || this.high == -1) {
                        result.put(entry.getKey(), entry.getValue());
                    } else break;
                }
                c++;
            }
            return (S) result;
        } else if (start instanceof Collection) {
            final Collection collection = (Collection) start;
            final Collection result = (collection instanceof Set) ? new TreeSet() : new LinkedList();
            long c = 0L;
            for (final Object item : collection) {
                if (c >= this.low) {
                    if (c < this.high || this.high == -1) {
                        result.add(item);
                    } else break;
                }
                c++;
            }
            return (S) result;
        }
        return start;
    }
}
