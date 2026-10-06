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
package org.apache.tinkerpop.gremlin.structure.snapshot.process.op.value;

import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.snapshot.CsrSnapshot;
import org.apache.tinkerpop.gremlin.structure.snapshot.format.ColumnReader;
import org.apache.tinkerpop.gremlin.structure.snapshot.process.exec.Batch;
import org.apache.tinkerpop.gremlin.structure.snapshot.process.exec.CsrExecutionContext;
import org.apache.tinkerpop.gremlin.structure.snapshot.process.exec.CsrPipeline;
import org.apache.tinkerpop.gremlin.structure.snapshot.process.exec.FeedSupplier;
import org.apache.tinkerpop.gremlin.structure.snapshot.process.exec.Lane;
import org.apache.tinkerpop.gremlin.structure.snapshot.process.exec.Materializer;
import org.apache.tinkerpop.gremlin.structure.snapshot.process.op.Keys;

import java.util.Map;

/**
 * Evaluates one {@code by()} of a {@code project()} for an entry of a batch, with the semantics of the traversal it was
 * compiled from. {@link #UNPRODUCTIVE} stands for a traversal that produced nothing, which leaves the key out of the
 * map. Element ids and labels are read from the snapshot without a facade; facades are created only for
 * {@code Keys.Identity} on an element lane, because the map then contains the element. The reader owns the child
 * pipeline of a {@code Keys.Child}; {@link #close()} releases it.
 */
final class ProjectKeyReader implements AutoCloseable {

    static final Object UNPRODUCTIVE = new Object();

    private final CsrExecutionContext ctx;
    private final Keys.Key key;
    private final Lane lane;

    private FeedSupplier feed;
    private CsrPipeline child;
    private Batch one;
    private Batch childOut;

    ProjectKeyReader(final CsrExecutionContext ctx, final Keys.Key key, final Lane lane) {
        this.ctx = ctx;
        this.key = key;
        this.lane = lane;
        if (key instanceof Keys.Child) {
            final Keys.Child c = (Keys.Child) key;
            if (c.plan().inputLane() != lane) {
                throw new IllegalArgumentException("A project child reads lane " + c.plan().inputLane()
                        + " but the input is " + lane);
            }
            feed = new FeedSupplier();
            child = CsrPipeline.open(ctx, c.plan(), feed);
            one = new Batch(lane, 1);
            childOut = child.newOutputBatch(ctx.batchSize());
        }
    }

    Object read(final Batch b, final int i) {
        if (key instanceof Keys.Identity) return Materializer.materialize(ctx, b, i);
        if (key instanceof Keys.Const) return ((Keys.Const) key).value();
        if (key instanceof Keys.Token) return token(((Keys.Token) key).token(), b, i);
        if (key instanceof Keys.Value) return value((Keys.Value) key, b, i);
        return child(b, i);
    }

    void reset() {
        if (child != null) child.reset();
    }

    @Override
    public void close() {
        if (child != null) {
            child.close();
            child = null;
        }
    }

    private Object child(final Batch b, final int i) {
        one.clear();
        one.copyEntry(b, i);
        feed.set(one);
        child.reset();
        if (child.next(childOut) && childOut.n > 0) return Materializer.materialize(ctx, childOut, 0);
        return UNPRODUCTIVE;
    }

    private Object token(final T token, final Batch b, final int i) {
        final CsrSnapshot snapshot = ctx.snapshot();
        if (lane == Lane.V) {
            if (token == T.id) return snapshot.vertexId(b.ord[i]);
            if (token == T.label) return snapshot.vertexLabel(b.ord[i]);
        } else if (lane == Lane.E) {
            if (token == T.id) return snapshot.edgeId(b.ord[i]);
            if (token == T.label) return snapshot.edgeLabel(b.ord[i]);
        }
        if (!lane.isFacade()) {
            throw new IllegalStateException("TokenTraversal support of a value does not allow selection by " + token);
        }
        final Object facade = Materializer.facade(ctx, b, i);
        if (facade instanceof Element) return token.apply((Element) facade);
        if (token == T.key) return ((Property<?>) facade).key();
        if (token == T.value) return ((Property<?>) facade).value();
        throw new IllegalStateException("TokenTraversal support of Property does not allow selection by " + token);
    }

    private Object value(final Keys.Value k, final Batch b, final int i) {
        final CsrSnapshot snapshot = ctx.snapshot();
        switch (lane) {
            case V: {
                final int code = k.keyCode();
                if (code < 0) return absent(k);
                final int vertex = b.ord[i];
                final long start = snapshot.vertexPropertyStart(code, vertex);
                final long end = snapshot.vertexPropertyEnd(code, vertex);
                if (end <= start) return absent(k);
                if (end - start > 1) throw Vertex.Exceptions.multiplePropertiesExistForProvidedKey(k.name());
                return snapshot.vertexPropertyValue(code, start);
            }
            case E: {
                final int code = k.keyCode();
                if (code < 0) return absent(k);
                final ColumnReader column = ctx.graph().edgeColumn(code);
                final int edge = b.ord[i];
                if (!column.isPresent(edge)) return absent(k);
                return column.get(edge);
            }
            case VP: {
                final Property<?> p = ((Element) Materializer.facade(ctx, b, i)).property(k.name());
                return p.isPresent() ? p.value() : absent(k);
            }
            case VAL: {
                final Object v = Materializer.value(ctx, b, i);
                if (v instanceof Map) return ((Map<?, ?>) v).get(k.name());
                throw new IllegalStateException(String.format("The by(\"%s\") modulator can only be applied to a "
                        + "traverser that is an Element or a Map - it is being applied to [%s] a %s class instead",
                        k.name(), v, v == null ? "null" : v.getClass().getSimpleName()));
            }
            default:
                throw new IllegalStateException(String.format("The by(\"%s\") modulator can only be applied to a "
                        + "traverser that is an Element or a Map - it is being applied to a property of lane %s",
                        k.name(), lane));
        }
    }

    private static Object absent(final Keys.Value k) {
        return k.productive() ? null : UNPRODUCTIVE;
    }
}
