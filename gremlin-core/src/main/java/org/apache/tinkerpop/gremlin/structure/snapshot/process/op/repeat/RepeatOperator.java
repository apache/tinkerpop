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
package org.apache.tinkerpop.gremlin.structure.snapshot.process.op.repeat;

import org.apache.tinkerpop.gremlin.structure.snapshot.process.exec.AbstractCsrOperator;
import org.apache.tinkerpop.gremlin.structure.snapshot.process.exec.Batch;
import org.apache.tinkerpop.gremlin.structure.snapshot.process.exec.BatchSupplier;
import org.apache.tinkerpop.gremlin.structure.snapshot.process.exec.CsrPipeline;
import org.apache.tinkerpop.gremlin.structure.snapshot.process.exec.FeedSupplier;
import org.apache.tinkerpop.gremlin.structure.snapshot.process.exec.Frontier;
import org.apache.tinkerpop.gremlin.structure.snapshot.process.exec.Lane;
import org.apache.tinkerpop.gremlin.structure.snapshot.process.exec.OperatorSpec;
import org.apache.tinkerpop.gremlin.structure.snapshot.process.op.CsrOp;
import org.apache.tinkerpop.gremlin.structure.snapshot.process.op.CsrPlan;
import org.apache.tinkerpop.gremlin.structure.snapshot.process.op.Ops;
import org.apache.tinkerpop.gremlin.structure.snapshot.process.op.Sources;
import org.apache.tinkerpop.gremlin.structure.snapshot.process.op.spill.SpillFrontier;
import org.apache.tinkerpop.gremlin.structure.snapshot.process.op.spill.SpillPersistentDedup;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * {@code repeat()} with {@code times}, {@code until} and {@code emit}, as a level-synchronous loop over a frontier of
 * vertices or edges with merged bulks (section 2.7 of the design). One level is:
 * <pre>
 *   F := the entries of the current level, bulks merged per ordinal
 *   untilFirst: entries for which until(F) holds (or loops == times) leave the loop and are emitted downstream
 *   emitFirst:  the remaining entries for which emit holds are emitted downstream as well
 *   G := merge(body(remaining F))
 *   !untilFirst: entries of G for which until(G) holds (or loops == times) leave the loop and are emitted downstream
 *   !emitFirst:  the remaining entries of G for which emit holds are emitted downstream as well
 *   F := the remaining entries of G, loops + 1
 * </pre>
 * which is what {@code RepeatStep} and {@code RepeatEndStep} do for one traverser, in the same order of tests; an
 * exiting traverser is never emitted a second time. The multiset of results is that of the standard step, the order
 * is by level and then by ordinal.
 * <p/>
 * Memory: two {@link SpillFrontier}s (current and next level), which move between dense, sparse and scratch runs
 * within the budget, a bitset of the entries that survive an {@code until} test that runs first, and the state of the
 * body. The input of the operator is consumed completely before the first result.
 * <p/>
 * Body state across levels. The body pipelines are {@code reset()} before every level, because a pipeline whose input
 * ended can only be revived that way. That gives {@code range()} inside the body a fresh counter for each level, which
 * is the per-loop counter of {@code RangeGlobalStep}, and {@code dedup()} inside the body would lose its visited set.
 * Therefore the body is cut at each of its top-level dedups into pipeline pieces that are connected directly, and each
 * dedup is a {@link SpillPersistentDedup} stage that is cleared only when the operator itself is reset (and not when an
 * enclosing repeat resets it to start its next level).
 */
final class RepeatOperator extends AbstractCsrOperator {

    private enum State {LOAD, PRE, BODY, POST, DONE}

    private final Ops.Repeat node;
    private final boolean untilFirst;
    private final boolean emitFirst;
    private final boolean emitAll;
    private final int maxLoops;

    private Lane lane;
    private int universe;
    private int batchSize;
    private LoopStack stack;
    private LoopStack.Frame frame;

    private Batch in;
    private Batch chunk;
    private Batch filterBatch;
    private Batch bodyOut;
    private Frontier current;
    private Frontier following;
    private long[] survivors;
    private long survivorsReserved;
    private Frontier staying;

    private final List<CsrPipeline> bodyPipelines = new ArrayList<>();
    private final List<SpillPersistentDedup> dedups = new ArrayList<>();
    private BatchSupplier bodyEnd;
    private Probe untilProbe;
    private Probe emitProbe;

    private State state;
    private int level;
    private int chunkPos;
    private int bodyPos;
    private boolean bodyStarted;
    private boolean exitAllFirst;
    private long peakDistinct;
    private long exited;
    private long emitted;

    RepeatOperator(final Ops.Repeat node, final OperatorSpec spec) {
        super(spec);
        this.node = node;
        this.untilFirst = node.untilFirst();
        this.emitFirst = node.emitFirst();
        this.emitAll = node.emitAll();
        this.maxLoops = node.maxLoops();
        if (!spec.inputLane().isElement()) {
            throw new UnsupportedOperationException("A native repeat() over " + spec.inputLane() + " entries is not supported");
        }
        if (spec.inputRecordsSource() || spec.outputRecordsSource()) {
            throw new IllegalStateException("A native repeat() cannot merge edges that record their source vertex");
        }
    }

    // ---------------------------------------------------------------- lifecycle

    @Override
    protected void doOpen() {
        try {
            lane = spec.inputLane();
            universe = lane == Lane.V ? ctx.snapshot().vertexCount() : ctx.snapshot().edgeCount();
            batchSize = ctx.batchSize();
            stack = LoopStack.of(ctx);
            frame = new LoopStack.Frame(node.loopName());
            in = spec.newInputBatch(batchSize);
            chunk = new Batch(lane, batchSize);
            current = SpillFrontier.create(ctx, owner("frontier A"), lane, universe, 0);
            following = SpillFrontier.create(ctx, owner("frontier B"), lane, universe, 0);
            if (untilFirst && node.until() != null) {
                final int words = (int) (((long) universe + 63) >>> 6);
                if (ctx.tryReserveWithinQuota(8L * words, owner("survivors"))) {
                    survivorsReserved = 8L * words;
                    survivors = new long[words];
                } else {
                    // the entries that stay are collected in a frontier that spills, instead of marked in a bitset
                    staying = SpillFrontier.create(ctx, owner("staying"), lane, universe, 0);
                }
            }
            if (node.until() != null) untilProbe = new Probe(node.until());
            if (node.emit() != null) emitProbe = new Probe(node.emit());
            buildBody();
            resetState();
        } catch (RuntimeException e) {
            try {
                release();
            } catch (RuntimeException suppressed) {
                e.addSuppressed(suppressed);
            }
            throw e;
        }
    }

    private void resetState() {
        state = State.LOAD;
        level = 0;
        chunkPos = 0;
        bodyPos = 0;
        bodyStarted = false;
        exitAllFirst = false;
        peakDistinct = 0;
        exited = 0;
        emitted = 0;
    }

    @Override
    protected void doReset() {
        current.clear();
        following.clear();
        in.n = 0;
        chunk.clear();
        bodyOut.clear();
        resetBodyPipelines();
        if (!stack.keepsState()) {
            for (final SpillPersistentDedup dedup : dedups) dedup.clear();
        } else {
            for (final SpillPersistentDedup dedup : dedups) dedup.abandonLevel();
        }
        if (staying != null) staying.clear();
        resetState();
    }

    @Override
    protected void doClose() {
        release();
    }

    private void release() {
        RuntimeException failure = null;
        try {
            if (emitProbe != null) emitProbe.pipe.close();
            if (untilProbe != null) untilProbe.pipe.close();
        } catch (RuntimeException e) {
            failure = e;
        }
        for (int i = bodyPipelines.size() - 1; i >= 0; i--) {
            try {
                bodyPipelines.get(i).close();
            } catch (RuntimeException e) {
                if (failure == null) failure = e;
                else failure.addSuppressed(e);
            }
        }
        for (final SpillPersistentDedup dedup : dedups) {
            try {
                dedup.close();
            } catch (RuntimeException e) {
                if (failure == null) failure = e;
                else failure.addSuppressed(e);
            }
        }
        bodyPipelines.clear();
        dedups.clear();
        emitProbe = null;
        untilProbe = null;
        bodyEnd = null;
        if (current != null) current.release();
        if (following != null) following.release();
        if (staying != null) staying.release();
        current = null;
        following = null;
        staying = null;
        if (survivorsReserved > 0) ctx.budget().release(survivorsReserved, owner("survivors"));
        survivorsReserved = 0;
        survivors = null;
        in = null;
        chunk = null;
        filterBatch = null;
        bodyOut = null;
        stack = null;
        if (failure != null) throw failure;
    }

    // ---------------------------------------------------------------- the body

    /**
     * Builds the stages of the body: pipelines over the pieces between top-level dedups, chained by suppliers, and the
     * persistent dedup stages in between.
     */
    private void buildBody() {
        final CsrPlan plan = node.body();
        final List<CsrOp> nodes = plan.nodes();
        BatchSupplier upstream = this::levelInput;
        Lane pieceLane = lane;
        final List<CsrOp> piece = new ArrayList<>();
        boolean lastWasPipeline = false;
        for (int i = 1; i < nodes.size(); i++) {
            final CsrOp op = nodes.get(i);
            if (op instanceof Ops.Dedup dedup) {
                final Lane at = plan.laneAfter(i - 1);
                if (plan.recordsSourceAfter(i - 1)) {
                    throw new IllegalStateException("A dedup of edges that record their source inside a fused repeat() is not supported");
                }
                if (!piece.isEmpty()) {
                    upstream = openPiece(pieceLane, piece, upstream);
                    piece.clear();
                }
                final SpillPersistentDedup stage = new SpillPersistentDedup(ctx, owner("dedup " + dedups.size()), dedup, at, upstream);
                dedups.add(stage);
                upstream = stage;
                pieceLane = at;
                lastWasPipeline = false;
            } else {
                piece.add(op);
            }
        }
        if (!piece.isEmpty() || (dedups.isEmpty() && bodyPipelines.isEmpty())) {
            upstream = openPiece(pieceLane, piece, upstream);
            lastWasPipeline = true;
        }
        bodyEnd = upstream;
        bodyOut = lastWasPipeline ? bodyPipelines.get(bodyPipelines.size() - 1).newOutputBatch(batchSize)
                : new Batch(lane, batchSize);
        if (bodyOut.lane != lane || bodyOut.recordSource) {
            throw new IllegalStateException("The body of " + describe() + " emits " + bodyOut.lane + " instead of " + lane);
        }
    }

    private BatchSupplier openPiece(final Lane pieceLane, final List<CsrOp> ops, final BatchSupplier upstream) {
        final List<CsrOp> nodes = new ArrayList<>(ops.size() + 1);
        nodes.add(new Sources.Input(pieceLane));
        nodes.addAll(ops);
        final CsrPipeline pipeline = CsrPipeline.open(ctx, new CsrPlan(nodes), upstream);
        bodyPipelines.add(pipeline);
        return pipeline::next;
    }

    private void resetBodyPipelines() {
        stack.beginLevelReset();
        try {
            for (final CsrPipeline pipeline : bodyPipelines) pipeline.reset();
        } finally {
            stack.endLevelReset();
        }
    }

    /**
     * The input of the body: the entries of the current level that did not leave in the pre-body pass.
     */
    private boolean levelInput(final Batch out) {
        out.clear();
        if (staying != null) return staying.drain(out) > 0;
        if (survivors == null) return current.drain(out) > 0;
        if (filterBatch == null) filterBatch = new Batch(lane, out.capacity);
        while (true) {
            filterBatch.clear();
            if (current.drain(filterBatch) == 0) return false;
            for (int i = 0; i < filterBatch.n; i++) {
                final int ordinal = filterBatch.ord[i];
                if ((survivors[ordinal >>> 6] & (1L << (ordinal & 63))) != 0) out.copyEntry(filterBatch, i);
            }
            if (out.n > 0) return true;
            ctx.checkInterrupt();
        }
    }

    // ---------------------------------------------------------------- the until and emit tests

    private final class Probe {
        final FeedSupplier feed = new FeedSupplier();
        final CsrPipeline pipe;
        final Batch one;
        final Batch result;

        Probe(final CsrPlan plan) {
            pipe = CsrPipeline.open(ctx, plan, feed);
            one = new Batch(lane, 1);
            result = pipe.newOutputBatch(batchSize);
        }

        boolean test(final Batch from, final int i) {
            one.clear();
            one.copyEntry(from, i, 1L);
            feed.set(one);
            pipe.reset();
            return pipe.next(result) && result.n > 0;
        }
    }

    // ---------------------------------------------------------------- the loop

    @Override
    protected boolean produce(final Batch out) {
        while (true) {
            switch (state) {
                case LOAD:
                    load();
                    break;
                case PRE:
                    if (!prePass(out)) return true;
                    endPrePass();
                    break;
                case BODY:
                    bodyStep();
                    break;
                case POST:
                    if (!postPass(out)) return true;
                    state = State.BODY;
                    break;
                default:
                    return false;
            }
            ctx.checkInterrupt();
        }
    }

    private void load() {
        while (pull(in)) {
            current.addAll(in);
            ctx.checkInterrupt();
        }
        current.seal();
        level = 0;
        beginLevel();
    }

    private void beginLevel() {
        if (current.isEmpty()) {
            finish();
            return;
        }
        peakDistinct = Math.max(peakDistinct, current.distinct());
        exitAllFirst = untilFirst && maxLoops >= 0 && level >= maxLoops;
        final boolean emitTest = emitFirst && (emitAll || emitProbe != null);
        current.rewind();
        bodyStarted = false;
        if (exitAllFirst || (untilFirst && untilProbe != null) || emitTest) {
            if (survivors != null) Arrays.fill(survivors, 0L);
            if (staying != null) staying.clear();
            chunk.clear();
            chunkPos = 0;
            state = State.PRE;
        } else {
            state = State.BODY;
        }
    }

    private void endPrePass() {
        current.rewind();
        if (exitAllFirst) finish();
        else state = State.BODY;
    }

    private void finish() {
        state = State.DONE;
        stats().annotate("levels", level);
        stats().annotate("peakFrontierEntries", peakDistinct);
        stats().annotate("exited", exited);
        stats().annotate("emitted", emitted);
        stats().annotate("frontier", (current.isDense() ? "dense" : "sparse") + "/" + (following.isDense() ? "dense" : "sparse"));
    }

    /**
     * Tests the entries of the current level that run before the body: {@code until} (or {@code times}) and
     * {@code emit}. An entry that exits is appended to {@code out}; one that stays is marked as a survivor and, if
     * {@code emit} holds for it, appended as well.
     *
     * @return false if {@code out} is full and the pass must resume on the next call
     */
    private boolean prePass(final Batch out) {
        frame.loops = level;
        stack.push(frame);
        try {
            while (true) {
                if (chunkPos >= chunk.n) {
                    chunk.clear();
                    chunkPos = 0;
                    if (current.drain(chunk) == 0) return true;
                }
                while (chunkPos < chunk.n) {
                    if (out.isFull()) return false;
                    final int i = chunkPos++;
                    final boolean exit = exitAllFirst || (untilFirst && untilProbe != null && untilProbe.test(chunk, i));
                    if (exit) {
                        out.copyEntry(chunk, i);
                        exited++;
                        continue;
                    }
                    if (survivors != null) {
                        final int ordinal = chunk.ord[i];
                        survivors[ordinal >>> 6] |= 1L << (ordinal & 63);
                    } else if (staying != null) {
                        staying.add(chunk.ord[i], chunk.bulk[i]);
                    }
                    if (emitFirst && (emitAll || (emitProbe != null && emitProbe.test(chunk, i)))) {
                        out.copyEntry(chunk, i);
                        emitted++;
                    }
                }
                ctx.checkInterrupt();
            }
        } finally {
            stack.pop();
        }
    }

    private void bodyStep() {
        if (!bodyStarted) {
            following.clear();
            resetBodyPipelines();
            bodyStarted = true;
        }
        frame.loops = level;
        stack.push(frame);
        final boolean more;
        try {
            more = bodyEnd.next(bodyOut);
        } finally {
            stack.pop();
        }
        if (more) {
            bodyPos = 0;
            state = State.POST;
            return;
        }
        following.seal();
        current.clear();
        final Frontier swap = current;
        current = following;
        following = swap;
        level++;
        beginLevel();
    }

    /**
     * Handles the entries the body emitted: tests {@code until} (or {@code times}) and {@code emit} after the body
     * and adds the entries that stay to the frontier of the next level.
     *
     * @return false if {@code out} is full and the pass must resume on the next call
     */
    private boolean postPass(final Batch out) {
        frame.loops = level + 1;
        stack.push(frame);
        try {
            final boolean timesExit = !untilFirst && maxLoops >= 0 && level + 1 >= maxLoops;
            while (bodyPos < bodyOut.n) {
                if (out.isFull()) return false;
                final int i = bodyPos++;
                final boolean exit = timesExit || (!untilFirst && untilProbe != null && untilProbe.test(bodyOut, i));
                if (exit) {
                    out.copyEntry(bodyOut, i);
                    exited++;
                    continue;
                }
                following.add(bodyOut.ord[i], bodyOut.bulk[i]);
                if (!emitFirst && (emitAll || (emitProbe != null && emitProbe.test(bodyOut, i)))) {
                    out.copyEntry(bodyOut, i);
                    emitted++;
                }
            }
            return true;
        } finally {
            stack.pop();
        }
    }
}
