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
package org.apache.tinkerpop.gremlin.structure.snapshot.process;

import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalSideEffects;
import org.apache.tinkerpop.gremlin.process.traversal.step.Profiling;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.AbstractStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.EmptyStep;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserRequirement;
import org.apache.tinkerpop.gremlin.process.traversal.util.FastNoSuchElementException;
import org.apache.tinkerpop.gremlin.process.traversal.util.MutableMetrics;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalMetrics;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.snapshot.graph.CsrGraph;
import org.apache.tinkerpop.gremlin.structure.snapshot.process.exec.Batch;
import org.apache.tinkerpop.gremlin.structure.snapshot.process.exec.BatchSupplier;
import org.apache.tinkerpop.gremlin.structure.snapshot.process.exec.CsrExecutionContext;
import org.apache.tinkerpop.gremlin.structure.snapshot.process.exec.CsrOperator;
import org.apache.tinkerpop.gremlin.structure.snapshot.process.exec.CsrOperatorFactory;
import org.apache.tinkerpop.gremlin.structure.snapshot.process.exec.CsrPipeline;
import org.apache.tinkerpop.gremlin.structure.snapshot.process.exec.CsrRuntime;
import org.apache.tinkerpop.gremlin.structure.snapshot.process.exec.CsrSettings;
import org.apache.tinkerpop.gremlin.structure.snapshot.process.exec.Lane;
import org.apache.tinkerpop.gremlin.structure.snapshot.process.exec.Materializer;
import org.apache.tinkerpop.gremlin.structure.snapshot.process.exec.OperatorStats;
import org.apache.tinkerpop.gremlin.structure.snapshot.process.exec.Rehydrator;
import org.apache.tinkerpop.gremlin.structure.snapshot.process.op.CsrPlan;

import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * A run of standard steps replaced by one native plan over ordinals, bulks and typed columns (section 2 of the spike
 * document). It is an ordinary step: downstream steps pull {@link #processNextStart()}, and when its output buffer is
 * empty it pulls the next batch from the operator pipeline, so execution is lazy and bounded by the batch size.
 * <p/>
 * <b>Input.</b> When the plan starts with {@code Input(lane)}, upstream traversers are rehydrated into batches: a
 * {@code CsrElement} of the same snapshot gives its ordinal and equal ordinals in one batch are merged (see
 * {@link Rehydrator}). Anything else throws {@code IllegalStateException} naming this step. A plan that starts with a
 * scan or lookup ignores upstream.
 * <p/>
 * <b>Output.</b> The {@link EmitMode} must match the plan's output lane: {@code FACADE} for {@code V}, {@code E},
 * {@code VP}, {@code EP} and {@code MP} (one facade per entry with the entry's bulk), {@code VALUE} for {@code VAL}
 * (the decoded value, possibly null, with the entry's bulk) and {@code SCALAR} for {@code SCALAR} (the single value
 * with bulk 1). A plan that ends in a {@code Drain} emits nothing whatever its mode.
 * <p/>
 * <b>State.</b> The {@link CsrExecutionContext} and the operator pipeline are created on the first
 * {@code processNextStart()} ("bind") and discarded on exhaustion, {@link #reset()} and {@link #close()}, which also
 * deletes scratch files once no step of the traversal is bound. The memory budget and scratch space are shared by all
 * native steps of the root traversal ({@link CsrRuntime}). {@link #clone()} copies the plan and settings and no
 * state. The step is not a
 * {@code TraversalParent}, so strategies and {@code ProfileStrategy} do not descend into the plan.
 * <p/>
 * <b>Requirements.</b> {@code BULK}, plus {@code OBJECT} when it consumes upstream traversers; never {@code PATH}.
 * <p/>
 * <b>Profiling.</b> As a {@link Profiling} step it adds one nested metrics entry per top-level operator, with the
 * traverser count, element count (bulk), exclusive time and the operator's annotations, and annotates the step with the
 * budget peak and scratch use.
 */
public final class CsrSuperStep<S, E> extends AbstractStep<S, E> implements Profiling, AutoCloseable {

    /**
     * How the plan's output entries become traversers.
     */
    public enum EmitMode {
        FACADE, VALUE, SCALAR
    }

    private final CsrPlan plan;
    private final List<Step<?, ?>> fused;
    private final EmitMode emitMode;
    private final CsrSettings settings;
    private final CsrOperatorFactory factory;

    private State state;
    private boolean done;
    private MutableMetrics metrics;
    private long peakBytes = -1L;
    private long scratchBytes = -1L;

    /**
     * The per-execution state. The owner check lets {@link #clone()} share the field copy safely: a clone that finds a
     * state it does not own just drops the reference.
     */
    private static final class State {
        private final CsrSuperStep<?, ?> owner;
        private CsrExecutionContext ctx;
        private CsrPipeline pipeline;
        private Batch out;
        private int position;
        private Rehydrator rehydrator;
        private MutableMetrics[] nested;

        private State(final CsrSuperStep<?, ?> owner) {
            this.owner = owner;
        }
    }

    /**
     * @param plan     the plan to execute
     * @param fused    clones of the replaced steps, used only by {@code toString()} and {@code explain()}
     * @param emitMode how output entries become traversers; must match {@code plan.outputLane()}
     * @param settings budget, scratch directory and batch size
     */
    public CsrSuperStep(final Traversal.Admin traversal, final CsrPlan plan, final List<Step<?, ?>> fused,
                        final EmitMode emitMode, final CsrSettings settings) {
        this(traversal, plan, fused, emitMode, settings, CsrOperatorFactory.shared());
    }

    public CsrSuperStep(final Traversal.Admin traversal, final CsrPlan plan, final List<Step<?, ?>> fused,
                        final EmitMode emitMode, final CsrSettings settings, final CsrOperatorFactory factory) {
        super(traversal);
        this.plan = Objects.requireNonNull(plan);
        this.fused = List.copyOf(fused);
        this.emitMode = Objects.requireNonNull(emitMode);
        this.settings = Objects.requireNonNull(settings);
        this.factory = Objects.requireNonNull(factory);
        final Lane lane = plan.outputLane();
        final boolean matches = emitMode == EmitMode.FACADE ? lane.isFacade()
                : emitMode == EmitMode.VALUE ? lane == Lane.VAL : lane == Lane.SCALAR;
        if (!matches) throw new IllegalArgumentException("Emit mode " + emitMode + " does not fit output lane " + lane);
    }

    public CsrPlan plan() {
        return plan;
    }

    /**
     * The clones of the steps this step replaced, unmodifiable.
     */
    public List<Step<?, ?>> fusedSteps() {
        return fused;
    }

    public EmitMode emitMode() {
        return emitMode;
    }

    /**
     * The lane read from upstream traversers, or null if the plan has its own source.
     */
    public Lane inputLane() {
        return plan.inputLane();
    }

    public Lane outputLane() {
        return plan.outputLane();
    }

    public CsrSettings settings() {
        return settings;
    }

    /**
     * The most bytes the memory budget held at once during the last execution, or -1 if the step has not run. The
     * budget is shared by the whole root traversal, so this is a traversal-wide peak as of this step's last pull. It
     * survives exhaustion and failure, unlike the execution state.
     */
    public long peakBytes() {
        return peakBytes;
    }

    /**
     * The bytes written to scratch files by the root traversal in the last execution, or -1 if the step has not run.
     * Like {@link #peakBytes()} it survives exhaustion and failure.
     */
    public long scratchBytes() {
        return scratchBytes;
    }

    // ---------------------------------------------------------------- execution

    @Override
    @SuppressWarnings("unchecked")
    protected Traverser.Admin<E> processNextStart() {
        while (true) {
            if (state != null && state.position < state.out.n) {
                final int i = state.position++;
                final Object object = emitMode == EmitMode.FACADE ? Materializer.facade(state.ctx, state.out, i)
                        : Materializer.value(state.ctx, state.out, i);
                final long bulk = emitMode == EmitMode.SCALAR ? 1L : state.out.bulk[i];
                return (Traverser.Admin<E>) this.getTraversal().getTraverserGenerator().generate(object,
                        (Step) this, bulk);
            }
            if (done) throw FastNoSuchElementException.instance();
            if (state == null) bind();
            state.out.clear();
            state.position = 0;
            final boolean has;
            try {
                has = state.pipeline.next(state.out);
            } finally {
                peakBytes = Math.max(peakBytes, state.ctx.budget().peak());
                scratchBytes = Math.max(scratchBytes, state.ctx.scratch().bytesCreated());
            }
            publishMetrics();
            if (!has) {
                done = true;
                releaseState();
                throw FastNoSuchElementException.instance();
            }
        }
    }

    private void bind() {
        final Graph graph = this.getTraversal().getGraph().orElse(null);
        if (!(graph instanceof CsrGraph)) {
            throw new IllegalStateException(this + " needs a traversal over a CsrGraph");
        }
        final State s = new State(this);
        s.ctx = new CsrExecutionContext((CsrGraph) graph, settings, factory, this.getTraversal().getSideEffects(),
                this.toString(), CsrRuntime.acquire(rootSideEffects(), settings), plan);
        try {
            BatchSupplier input = null;
            if (plan.inputLane() != null) {
                s.rehydrator = new Rehydrator(s.ctx, plan.inputLane());
                input = this::pullUpstream;
            }
            s.pipeline = CsrPipeline.open(s.ctx, plan, input);
            s.out = s.pipeline.newOutputBatch(settings.batchSize());
        } catch (RuntimeException e) {
            s.ctx.close();
            throw e;
        }
        state = s;
        attachMetrics();
    }

    // the Input source: rehydrates up to one batch of upstream traversers
    private boolean pullUpstream(final Batch out) {
        out.clear();
        state.rehydrator.startBatch();
        while (!out.isFull() && this.starts.hasNext()) {
            final Traverser.Admin<S> traverser = this.starts.next();
            state.rehydrator.add(traverser.get(), traverser.bulk(), out);
        }
        return out.n > 0;
    }

    // the budget and scratch are per root traversal; child traversals share its side effects
    private TraversalSideEffects rootSideEffects() {
        return TraversalHelper.getRootTraversal(this.getTraversal()).getSideEffects();
    }

    // a root-level step ends the traversal's runtime (and resets its budget peak) when it is reset or closed
    private void endRuntime() {
        if (this.getTraversal().getParent() instanceof EmptyStep) CsrRuntime.forget(rootSideEffects());
    }

    private void releaseState() {
        final State s = state;
        state = null;
        if (s == null || s.owner != this) return;
        try {
            if (s.pipeline != null) s.pipeline.close();
        } finally {
            s.ctx.close();
        }
    }

    @Override
    public void reset() {
        super.reset();
        releaseState();
        endRuntime();
        done = false;
        peakBytes = -1L;
        scratchBytes = -1L;
    }

    @Override
    public void close() {
        releaseState();
        endRuntime();
    }

    @Override
    public CsrSuperStep<S, E> clone() {
        final CsrSuperStep<S, E> clone = (CsrSuperStep<S, E>) super.clone();
        clone.state = null;
        clone.done = false;
        clone.metrics = null;
        clone.peakBytes = -1L;
        clone.scratchBytes = -1L;
        return clone;
    }

    @Override
    public Set<TraverserRequirement> getRequirements() {
        return plan.inputLane() == null ? EnumSet.of(TraverserRequirement.BULK)
                : EnumSet.of(TraverserRequirement.BULK, TraverserRequirement.OBJECT);
    }

    // ---------------------------------------------------------------- profiling

    @Override
    public void setMetrics(final MutableMetrics metrics) {
        this.metrics = metrics;
        attachMetrics();
    }

    private void attachMetrics() {
        if (metrics == null || state == null || state.nested != null || metrics.isFinalized()) return;
        final List<CsrOperator> operators = state.pipeline.operators();
        state.nested = new MutableMetrics[operators.size()];
        for (int i = 0; i < state.nested.length; i++) {
            state.nested[i] = new MutableMetrics(this.getId() + "." + i, operators.get(i).node().toString());
            metrics.addNested(state.nested[i]);
        }
    }

    private void publishMetrics() {
        if (metrics == null || state == null || state.nested == null || metrics.isFinalized()) return;
        final List<CsrOperator> operators = state.pipeline.operators();
        for (int i = 0; i < state.nested.length; i++) {
            final MutableMetrics nested = state.nested[i];
            if (nested.isFinalized()) continue;
            final OperatorStats stats = operators.get(i).stats();
            nested.setDuration(stats.nanos(), TimeUnit.NANOSECONDS);
            nested.setCount(TraversalMetrics.TRAVERSER_COUNT_ID, stats.entriesOut());
            nested.setCount(TraversalMetrics.ELEMENT_COUNT_ID, stats.bulkOut());
            for (final Map.Entry<String, Object> annotation : stats.annotations().entrySet()) {
                nested.setAnnotation(annotation.getKey(), annotation.getValue());
            }
        }
        metrics.setAnnotation("csr.memoryPeakBytes", state.ctx.budget().peak());
        metrics.setAnnotation("csr.scratchBytes", state.ctx.scratch().bytesCreated());
    }

    // ---------------------------------------------------------------- description

    @Override
    public String toString() {
        return fused.isEmpty() ? "CsrSuperStep[" + plan + "]" : "CsrSuperStep" + fused;
    }

    @Override
    public int hashCode() {
        return super.hashCode() ^ plan.toString().hashCode();
    }
}
