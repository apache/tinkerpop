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

/**
 * Turns batch entries into the objects a traversal works with: facades for element and property lanes, decoded values
 * for {@code VAL} and {@code SCALAR}. {@code CsrSuperStep} uses it to emit traversers, and terminals use it to build
 * {@code fold()} lists and the keys of {@code groupCount()} and {@code group()} maps.
 */
public final class Materializer {

    private Materializer() {
    }

    /**
     * The facade for entry {@code i} of a batch whose lane is an element or property lane.
     *
     * @throws IllegalArgumentException for the {@code VAL} and {@code SCALAR} lanes
     */
    public static Object facade(final CsrExecutionContext ctx, final Batch batch, final int i) {
        switch (batch.lane) {
            case V:
                return ctx.graph().vertexAt(batch.ord[i]);
            case E:
                return ctx.graph().edgeAt(batch.ord[i]);
            case VP:
                return ctx.graph().vertexPropertyAt(batch.ord[i], batch.key[i], batch.aux[i]);
            case EP:
                return ctx.graph().edgePropertyAt(batch.ord[i], batch.key[i]);
            case MP:
                return ctx.graph().metaPropertyAt(batch.ord[i], batch.key[i], batch.aux[i], batch.src[i]);
            default:
                throw new IllegalArgumentException("Lane " + batch.lane + " has no facade");
        }
    }

    /**
     * The value of entry {@code i} of a {@code VAL} or {@code SCALAR} batch: the decoded column entry, or the object.
     * May be null.
     */
    public static Object value(final CsrExecutionContext ctx, final Batch batch, final int i) {
        if (batch.lane == Lane.SCALAR) return batch.val[i];
        if (batch.lane != Lane.VAL) throw new IllegalArgumentException("Lane " + batch.lane + " has no value");
        final int column = batch.key[i];
        return column == Batch.DECODED ? batch.val[i] : ctx.columnValue(column, batch.aux[i]);
    }

    /**
     * {@link #facade} for facade lanes and {@link #value} for the others.
     */
    public static Object materialize(final CsrExecutionContext ctx, final Batch batch, final int i) {
        return batch.lane.isFacade() ? facade(ctx, batch, i) : value(ctx, batch, i);
    }
}
