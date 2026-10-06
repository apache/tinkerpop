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
package org.apache.tinkerpop.gremlin.structure.snapshot.process.op.nav;

import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.snapshot.CsrSnapshot;
import org.apache.tinkerpop.gremlin.structure.snapshot.format.ColumnReader;
import org.apache.tinkerpop.gremlin.structure.snapshot.graph.CsrGraph;
import org.apache.tinkerpop.gremlin.structure.snapshot.process.exec.Batch;
import org.apache.tinkerpop.gremlin.structure.snapshot.process.exec.CsrPipeline;
import org.apache.tinkerpop.gremlin.structure.snapshot.process.exec.FeedSupplier;
import org.apache.tinkerpop.gremlin.structure.snapshot.process.exec.Lane;
import org.apache.tinkerpop.gremlin.structure.snapshot.process.exec.Materializer;
import org.apache.tinkerpop.gremlin.structure.snapshot.process.exec.OperatorSpec;
import org.apache.tinkerpop.gremlin.structure.snapshot.process.op.Keys;
import org.apache.tinkerpop.gremlin.structure.snapshot.process.op.Ops;

import java.util.HashSet;
import java.util.Set;

/**
 * {@code dedup()} and {@code dedup().by(key)}: passes the first entry for each distinct key with bulk 1. Three
 * representations hold the seen keys:
 * <ul>
 *     <li>a bitset over the ordinals, for vertices and edges deduplicated by identity or {@code T.id};</li>
 *     <li>a bitset over the label codes, for {@code by(T.label)} on vertices and edges;</li>
 *     <li>an in-heap hash set of the key objects for everything else, with Java {@code equals} like
 *     {@code DedupGlobalStep}'s {@code HashSet}. Its memory is estimated and reserved from the budget in chunks; a
 *     spillable version replaces this one in the spill package.</li>
 * </ul>
 * The state is kept until {@code reset()}, so it persists over the levels of a {@code repeat()}.
 */
final class DedupOperator extends EntryStreamOperator {

    private static final Object NON_PRODUCTIVE = new Object();
    private static final long CHUNK = 64L * 1024;
    private static final long ENTRY_OVERHEAD = 64;

    private enum Mode {ORDINAL, LABEL, HASH}

    private final Keys.Key key;
    private Mode mode;
    private CsrSnapshot snapshot;
    private CsrGraph graph;
    private Lane lane;

    private long[] bits;
    private long bitsReserved;
    private int noLabelSlot;

    private Set<Object> seen;
    private long hashReserved;
    private long hashUsed;

    private FeedSupplier feed;
    private CsrPipeline child;
    private Batch feedBatch;
    private Batch childOut;

    DedupOperator(final Ops.Dedup node, final OperatorSpec spec) {
        super(spec);
        this.key = node.key();
    }

    @Override
    protected void onOpen() {
        snapshot = ctx.snapshot();
        graph = ctx.graph();
        lane = spec.inputLane();
        mode = Mode.HASH;
        if (lane.isElement()) {
            if (key instanceof Keys.Identity || (key instanceof Keys.Token token && token.token() == T.id)) {
                mode = Mode.ORDINAL;
            } else if (key instanceof Keys.Token token && token.token() == T.label) {
                mode = Mode.LABEL;
            }
        }
        if (mode == Mode.ORDINAL) {
            final int universe = lane == Lane.V ? snapshot.vertexCount() : snapshot.edgeCount();
            allocateBits(universe);
        } else if (mode == Mode.LABEL) {
            final int labels = lane == Lane.V ? snapshot.vertexLabels().size() : snapshot.edgeLabels().size();
            if (lane == Lane.V) {
                final int empty = snapshot.vertexLabelCodeOf("");
                noLabelSlot = empty >= 0 ? empty : labels;
            }
            allocateBits(labels + 1);
        } else {
            seen = new HashSet<>();
            if (key instanceof Keys.Child childKey) {
                feed = new FeedSupplier();
                child = CsrPipeline.open(ctx, childKey.plan(), feed);
                feedBatch = new Batch(lane, 1);
                childOut = child.newOutputBatch(ctx.batchSize());
            }
        }
    }

    private void allocateBits(final int size) {
        final int words = (int) (((long) size + 63) >>> 6);
        bitsReserved = 8L * words;
        ctx.budget().reserve(bitsReserved, owner("seen bits"));
        bits = new long[words];
    }

    @Override
    protected boolean process(final Batch in, final int i, final Batch out) {
        switch (mode) {
            case ORDINAL:
                return setBit(in.ord[i], in, i, out);
            case LABEL:
                return setBit(labelSlot(in.ord[i]), in, i, out);
            default:
                final Object k = hashKey(in, i);
                if (k == NON_PRODUCTIVE) return true;
                reserveFor(k);
                if (seen.add(k)) out.copyEntry(in, i, 1L);
                return true;
        }
    }

    private boolean setBit(final int slot, final Batch in, final int i, final Batch out) {
        final int word = slot >>> 6;
        final long mask = 1L << (slot & 63);
        if ((bits[word] & mask) == 0) {
            bits[word] |= mask;
            out.copyEntry(in, i, 1L);
        }
        return true;
    }

    private int labelSlot(final int ordinal) {
        if (lane == Lane.E) return snapshot.edgeLabelCode(ordinal);
        final int code = snapshot.vertexLabelCode(ordinal);
        return code < 0 ? noLabelSlot : code;
    }

    private void reserveFor(final Object k) {
        hashUsed += ENTRY_OVERHEAD + sizeOf(k);
        if (hashUsed > hashReserved) {
            final long more = Math.max(CHUNK, hashUsed - hashReserved);
            ctx.budget().reserve(more, owner("seen keys"));
            hashReserved += more;
        }
    }

    private static long sizeOf(final Object k) {
        if (k == null) return 0;
        if (k instanceof String s) return 48 + s.length();
        if (k instanceof Number || k instanceof Boolean || k instanceof Character) return 24;
        return 64;
    }

    // ---------------------------------------------------------------- the key of an entry

    private Object hashKey(final Batch in, final int i) {
        if (key instanceof Keys.Identity) return identity(in, i);
        if (key instanceof Keys.Const constant) return constant.value();
        if (key instanceof Keys.Token token) return token(token.token(), in, i);
        if (key instanceof Keys.Value value) return propertyValue(value, in, i);
        return childKey(in, i);
    }

    private Object identity(final Batch in, final int i) {
        return Materializer.materialize(ctx, in, i);
    }

    private Object token(final T token, final Batch in, final int i) {
        switch (lane) {
            case V:
                if (token == T.id) return snapshot.vertexId(in.ord[i]);
                if (token == T.label) return snapshot.vertexLabel(in.ord[i]);
                break;
            case E:
                if (token == T.id) return snapshot.edgeId(in.ord[i]);
                if (token == T.label) return snapshot.edgeLabel(in.ord[i]);
                break;
            case VP:
                switch (token) {
                    case id:
                        return snapshot.vertexPropertyIdentifier(in.key[i], in.aux[i]);
                    case key:
                    case label:
                        return graph.vertexKey(in.key[i]);
                    default:
                        return snapshot.vertexPropertyValue(in.key[i], in.aux[i]);
                }
            case EP:
                if (token == T.key) return graph.edgeKey(in.key[i]);
                if (token == T.value) return graph.edgeColumn(in.key[i]).get(in.ord[i]);
                break;
            case MP:
                if (token == T.key) return graph.metaKey(in.src[i]);
                if (token == T.value) return snapshot.metaPropertyValue(in.key[i], in.src[i], in.aux[i]);
                break;
            default:
                break;
        }
        throw new IllegalStateException("TokenTraversal support of " + lane + " does not allow selection by " + token);
    }

    private Object propertyValue(final Keys.Value value, final Batch in, final int i) {
        final int code = value.keyCode();
        if (lane == Lane.V) {
            if (code >= 0) {
                final long start = snapshot.vertexPropertyStart(code, in.ord[i]);
                final long end = snapshot.vertexPropertyEnd(code, in.ord[i]);
                if (end - start > 1) throw Vertex.Exceptions.multiplePropertiesExistForProvidedKey(value.name());
                if (end > start) return snapshot.vertexPropertyValue(code, start);
            }
        } else if (lane == Lane.E) {
            if (code >= 0) {
                final ColumnReader column = graph.edgeColumn(code);
                final long entry = column == null ? -1 : column.entryIndex(in.ord[i]);
                if (entry >= 0) return column.getAt(entry);
            }
        } else {
            throw new IllegalStateException("The by(\"" + value.name()
                    + "\") modulator can only be applied to a traverser that is an Element or a Map");
        }
        return value.productive() ? null : NON_PRODUCTIVE;
    }

    private Object childKey(final Batch in, final int i) {
        feedBatch.clear();
        feedBatch.copyEntry(in, i, 1L);
        feed.set(feedBatch);
        child.reset();
        Object result = NON_PRODUCTIVE;
        if (child.next(childOut) && childOut.n > 0) result = Materializer.materialize(ctx, childOut, 0);
        return result;
    }

    @Override
    protected void onReset() {
        if (bits != null) java.util.Arrays.fill(bits, 0L);
        if (seen != null) {
            seen.clear();
            if (hashReserved > 0) ctx.budget().release(hashReserved, owner("seen keys"));
            hashReserved = 0;
            hashUsed = 0;
        }
    }

    @Override
    protected void onClose() {
        try {
            if (child != null) child.close();
        } finally {
            child = null;
            if (bitsReserved > 0) ctx.budget().release(bitsReserved, owner("seen bits"));
            if (hashReserved > 0) ctx.budget().release(hashReserved, owner("seen keys"));
            bits = null;
            bitsReserved = 0;
            seen = null;
            hashReserved = 0;
            hashUsed = 0;
            snapshot = null;
            graph = null;
        }
    }
}
