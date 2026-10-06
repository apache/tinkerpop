/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.tinkerpop.gremlin.structure.snapshot.process.op.spill;

import org.apache.tinkerpop.gremlin.structure.snapshot.graph.CsrElement;
import org.apache.tinkerpop.gremlin.structure.snapshot.process.exec.CsrExecutionContext;

import java.util.Collection;
import java.util.Map;

/**
 * Constants and estimates shared by the spillable operators. The estimates are deliberately a little high: the budget
 * is a promise about the state the operators own, not a measurement of the JVM heap.
 */
final class SpillSupport {

    /**
     * The deepest recursive re-partitioning before a partition that still does not fit raises the budget exception.
     */
    static final int MAX_DEPTH = 4;

    static final int MAX_BUFFER = 64 << 10;
    static final int MIN_BUFFER = 1 << 10;

    /**
     * A hash set entry: node, key wrapper, array header.
     */
    static final long SET_ENTRY = 96;
    /**
     * A hash map entry holding a counter or a group.
     */
    static final long MAP_ENTRY = 128;
    /**
     * A result map entry without the key and value objects.
     */
    static final long RESULT_ENTRY = 64;
    /**
     * A buffered member of a group or a sort record, without its bytes.
     */
    static final long RECORD = 48;

    private SpillSupport() {
    }

    /**
     * A buffer size such that {@code buffers} of them use at most a quarter of what the budget can still give, between
     * 1 KiB and 64 KiB.
     */
    static int bufferBytes(final CsrExecutionContext ctx, final int buffers) {
        final long share = ctx.budget().available() / 4 / Math.max(1, buffers);
        return (int) Math.max(MIN_BUFFER, Math.min(MAX_BUFFER, share));
    }

    /**
     * The maximum number of runs to merge at once, and the buffer size for each, for what the budget can give now.
     */
    static int[] mergePlan(final CsrExecutionContext ctx) {
        final long available = ctx.budget().available();
        final int buffer = (int) Math.max(MIN_BUFFER, Math.min(MAX_BUFFER, available / 128));
        final int fanIn = (int) Math.max(2, Math.min(64, available / 2 / buffer));
        return new int[]{fanIn, buffer};
    }

    static long estimate(final Object o) {
        if (o == null) return 0;
        if (o instanceof String) return 48 + 2L * ((String) o).length();
        if (o instanceof Number || o instanceof Boolean || o instanceof Character) return 24;
        if (o instanceof CsrElement) return 48;
        if (o instanceof Collection) {
            long n = 56;
            for (final Object e : (Collection<?>) o) n += 8 + estimate(e);
            return n;
        }
        if (o instanceof Map) {
            long n = 56;
            for (final Map.Entry<?, ?> e : ((Map<?, ?>) o).entrySet()) n += 48 + estimate(e.getKey()) + estimate(e.getValue());
            return n;
        }
        return 64;
    }
}
