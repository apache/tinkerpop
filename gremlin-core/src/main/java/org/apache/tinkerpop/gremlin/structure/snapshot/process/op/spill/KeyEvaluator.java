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

import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.snapshot.process.exec.Batch;
import org.apache.tinkerpop.gremlin.structure.snapshot.process.exec.CsrExecutionContext;
import org.apache.tinkerpop.gremlin.structure.snapshot.process.exec.CsrPipeline;
import org.apache.tinkerpop.gremlin.structure.snapshot.process.exec.FeedSupplier;
import org.apache.tinkerpop.gremlin.structure.snapshot.process.exec.Lane;
import org.apache.tinkerpop.gremlin.structure.snapshot.process.exec.Materializer;
import org.apache.tinkerpop.gremlin.structure.snapshot.process.op.Keys;

import java.util.Map;

/**
 * Evaluates a {@link Keys.Key} for the entries of a batch of one lane, the way {@code TokenTraversal},
 * {@code ValueTraversal} and child traversals do, with bulk 1. The last result is kept as a batch entry or as an object
 * and can then be encoded or materialized. Self-contained copy of what the in-memory operators do, so that the
 * spillable operators do not depend on them.
 */
final class KeyEvaluator implements AutoCloseable {

    private final CsrExecutionContext ctx;
    private final KeyCodec codec;
    private final Keys.Key key;
    private final Lane lane;

    private FeedSupplier feed;
    private CsrPipeline child;
    private Batch one;
    private Batch childOut;

    private Batch resultBatch;
    private int resultIndex;
    private Object resultObject;
    private boolean resultIsObject;

    KeyEvaluator(final CsrExecutionContext ctx, final KeyCodec codec, final Keys.Key key, final Lane lane) {
        this.ctx = ctx;
        this.codec = codec;
        this.key = key;
        this.lane = lane;
    }

    /**
     * Whether the key is the entry of a vertex or edge lane itself, which is keyed by ordinal.
     */
    boolean isOrdinalIdentity() {
        return key instanceof Keys.Identity && lane.isElement();
    }

    void open() {
        if (key instanceof Keys.Child) {
            feed = new FeedSupplier();
            one = new Batch(lane, 1, false);
            child = CsrPipeline.open(ctx, ((Keys.Child) key).plan(), feed);
            childOut = child.newOutputBatch(ctx.batchSize());
        }
    }

    void reset() {
        if (child != null) child.reset();
    }

    @Override
    public void close() {
        if (child != null) {
            final CsrPipeline c = child;
            child = null;
            c.close();
        }
    }

    /**
     * Evaluates the key for entry {@code i}.
     *
     * @return false if the key is non-productive, which filters the entry
     */
    boolean evaluate(final Batch in, final int i) {
        resultIsObject = false;
        resultBatch = null;
        resultObject = null;
        if (key instanceof Keys.Identity) {
            resultBatch = in;
            resultIndex = i;
            return true;
        }
        if (key instanceof Keys.Const) {
            return object(((Keys.Const) key).value());
        }
        if (key instanceof Keys.Token) {
            return object(token(((Keys.Token) key).token(), subject(in, i)));
        }
        if (key instanceof Keys.Value) {
            return value((Keys.Value) key, subject(in, i));
        }
        if (key instanceof Keys.Child) {
            one.clear();
            one.copyEntry(in, i, 1);
            feed.set(one);
            child.reset();
            if (!child.next(childOut)) return false;
            resultBatch = childOut;
            resultIndex = 0;
            return true;
        }
        throw new IllegalStateException("Unknown key " + key);
    }

    private boolean object(final Object o) {
        resultIsObject = true;
        resultObject = o;
        return true;
    }

    private Object subject(final Batch in, final int i) {
        return in.lane.isFacade() ? Materializer.facade(ctx, in, i) : Materializer.value(ctx, in, i);
    }

    private static Object token(final T t, final Object s) {
        if (s instanceof Element) return t.apply((Element) s);
        if (s instanceof Property) {
            if (t == T.key) return ((Property<?>) s).key();
            if (t == T.value) return ((Property<?>) s).value();
            throw new IllegalStateException(String.format(
                    "TokenTraversal support of Property does not allow selection by %s", t));
        }
        throw new IllegalStateException(String.format("TokenTraversal support of %s does not allow selection by %s",
                s == null ? "null" : s.getClass().getName(), t));
    }

    private boolean value(final Keys.Value k, final Object s) {
        if (s instanceof Element) {
            final Property<?> p = ((Element) s).property(k.name());
            if (p.isPresent()) return object(p.value());
            return k.productive() && object(null);
        }
        if (s instanceof Map) return object(((Map<?, ?>) s).get(k.name()));
        throw new IllegalStateException(String.format(
                "The by(\"%s\") modulator can only be applied to a traverser that is an Element or a Map - it is being applied to [%s] a %s class instead",
                k.name(), s, s == null ? "null" : s.getClass().getSimpleName()));
    }

    /**
     * Encodes the last result as a canonical key and, where the canonical form cannot rebuild it, a representative.
     */
    void encode(final Bytes canon, final Bytes rep) {
        if (resultIsObject) codec.encodeKey(resultObject, canon, rep);
        else codec.encodeKey(resultBatch, resultIndex, canon, rep);
    }

    /**
     * The last result as the object the standard step would use: a facade, a value, or null.
     */
    Object object() {
        return resultIsObject ? resultObject : Materializer.materialize(ctx, resultBatch, resultIndex);
    }
}
