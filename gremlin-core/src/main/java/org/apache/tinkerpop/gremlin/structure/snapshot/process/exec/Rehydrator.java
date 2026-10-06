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
package org.apache.tinkerpop.gremlin.structure.snapshot.process.exec;

import org.apache.tinkerpop.gremlin.structure.snapshot.graph.CsrElement;
import org.apache.tinkerpop.gremlin.structure.snapshot.graph.CsrProperty;
import org.apache.tinkerpop.gremlin.structure.snapshot.graph.CsrVertex;
import org.apache.tinkerpop.gremlin.structure.snapshot.graph.CsrVertexProperty;
import org.apache.tinkerpop.gremlin.structure.snapshot.graph.CsrEdge;

import java.util.HashMap;
import java.util.Map;

/**
 * Converts the objects of upstream traversers back into batch entries: a facade of the same snapshot gives its
 * ordinal, and equal {@code V} or {@code E} ordinals within one batch are merged by adding bulks. The strategy proved
 * provenance when it planned the region, so an object that cannot be rehydrated is a runtime error naming the step,
 * never a silent wrong answer. For a {@code VAL} lane any object is accepted as a decoded value.
 */
public final class Rehydrator {

    private final CsrExecutionContext ctx;
    private final Lane lane;
    private final Map<Integer, Integer> positions = new HashMap<>();

    public Rehydrator(final CsrExecutionContext ctx, final Lane lane) {
        this.ctx = ctx;
        this.lane = lane;
    }

    /**
     * Starts a new batch: forgets the ordinals merged so far.
     */
    public void startBatch() {
        positions.clear();
    }

    /**
     * Appends the object with the bulk to the batch, or adds the bulk to the entry of an equal ordinal.
     *
     * @throws IllegalStateException if the object is not an element or property of this snapshot of the lane's kind
     */
    public void add(final Object object, final long bulk, final Batch out) {
        switch (lane) {
            case V:
                if (object instanceof CsrVertex && same(object)) {
                    merge(((CsrVertex) object).ordinal(), bulk, out);
                    return;
                }
                break;
            case E:
                if (object instanceof CsrEdge && same(object)) {
                    merge(((CsrEdge) object).ordinal(), bulk, out);
                    return;
                }
                break;
            case VP:
                if (object instanceof CsrVertexProperty && same(((CsrVertexProperty<?>) object).element())) {
                    final CsrVertexProperty<?> p = (CsrVertexProperty<?>) object;
                    out.addVP(p.vertexOrdinal(), p.keyCode(), p.propertyOrdinal(), bulk);
                    return;
                }
                break;
            case EP:
                if (object instanceof CsrProperty && same(((CsrProperty<?>) object).element())) {
                    final CsrProperty<?> p = (CsrProperty<?>) object;
                    out.addEP(p.edgeOrdinal(), p.keyCode(), bulk);
                    return;
                }
                break;
            case VAL:
                out.addValue(object, bulk);
                return;
            default:
                break;
        }
        throw new IllegalStateException(ctx.stepName() + " cannot rehydrate " + (object == null ? "null"
                : object.getClass().getName()) + " into lane " + lane);
    }

    private boolean same(final Object element) {
        return element instanceof CsrElement && ((CsrElement) element).graph().snapshot() == ctx.snapshot();
    }

    private void merge(final int ordinal, final long bulk, final Batch out) {
        final Integer position = positions.get(ordinal);
        if (position != null) {
            out.bulk[position] += bulk;
            return;
        }
        positions.put(ordinal, out.n);
        if (lane == Lane.V) out.addV(ordinal, bulk);
        else out.addE(ordinal, bulk);
    }
}
