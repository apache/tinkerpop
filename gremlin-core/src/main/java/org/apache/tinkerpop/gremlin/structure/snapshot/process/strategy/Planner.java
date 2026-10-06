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
package org.apache.tinkerpop.gremlin.structure.snapshot.process.strategy;

import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.lambda.AbstractLambdaTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.step.TraversalParent;
import org.apache.tinkerpop.gremlin.process.traversal.step.branch.BranchStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.branch.LocalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.branch.OptionalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.branch.RepeatStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.branch.UnionStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.ConnectiveStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.DedupGlobalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.FilterStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.HasStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.NotStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.TailGlobalStepContract;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.TraversalFilterStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.CoalesceStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.EdgeOtherVertexStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.EdgeVertexStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.ElementStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.GraphStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.GroupCountStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.GroupStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.NoOpBarrierStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.OrderGlobalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.ProjectStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.PropertiesStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.TraversalFlatMapStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.TraversalMapStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.VertexStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.AggregateStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.GroupCountSideEffectStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.GroupSideEffectStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.IdentityStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.SideEffectStep;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import org.apache.tinkerpop.gremlin.structure.PropertyType;
import org.apache.tinkerpop.gremlin.structure.snapshot.graph.CsrGraph;
import org.apache.tinkerpop.gremlin.structure.snapshot.process.CsrSuperStep;
import org.apache.tinkerpop.gremlin.structure.snapshot.process.exec.CsrOperatorFactory;
import org.apache.tinkerpop.gremlin.structure.snapshot.process.exec.CsrSettings;
import org.apache.tinkerpop.gremlin.structure.snapshot.process.exec.Lane;
import org.apache.tinkerpop.gremlin.structure.snapshot.process.op.CsrPlan;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Set;

/**
 * Region detection and the all-or-nothing rewrite of section 3.4 of the spike document. {@link #plan()} walks the root
 * and the children of the steps that stay standard from left to right, grows a maximal run of compilable steps from
 * every step that can start one, and records the replacement as an {@link Edit} without touching anything.
 * {@link #apply} performs the edits once the whole tree has planned without error.
 * <p/>
 * A region starts at the root's start {@code GraphStep}, or at a step whose input is provably a facade of this graph
 * (its provenance, computed from the standard steps before it) when no label that is read is live. It ends at the first
 * step that does not compile, after a terminal, after a side-effect writer, or after a step whose label is read: that
 * label moves to the {@code CsrSuperStep}. A region that would do nothing but scan or limit is not rewritten.
 */
final class Planner {

    /**
     * One replacement: {@code count} steps from {@code first} in {@code traversal} become {@code step}.
     */
    static final class Edit {
        private final Traversal.Admin<?, ?> traversal;
        private final Step<?, ?> first;
        private final Step<?, ?> last;
        private final int count;
        private final CsrSuperStep<?, ?> step;

        private Edit(final Traversal.Admin<?, ?> traversal, final Step<?, ?> first, final Step<?, ?> last,
                     final int count, final CsrSuperStep<?, ?> step) {
            this.traversal = traversal;
            this.first = first;
            this.last = last;
            this.count = count;
            this.step = step;
        }
    }

    private final Traversal.Admin<?, ?> root;
    private final Analysis analysis;
    private final CsrSettings settings;
    private final StepCompiler compiler;
    private final List<Edit> edits = new ArrayList<>();
    private final List<String> notes = new ArrayList<>();

    Planner(final Traversal.Admin<?, ?> root, final CsrGraph graph, final Analysis analysis, final CsrSettings settings,
            final CsrOperatorFactory factory) {
        this.root = root;
        this.analysis = analysis;
        this.settings = settings;
        this.compiler = new StepCompiler(root, graph, analysis, factory);
    }

    /**
     * Plans the whole tree without changing it.
     */
    List<Edit> plan() {
        planTraversal(root, null, Collections.emptySet(), true);
        return edits;
    }

    /**
     * The reasons regions and steps stayed on facades, for the debug log.
     */
    List<String> notes() {
        return notes;
    }

    // ---------------------------------------------------------------- planning

    private void planTraversal(final Traversal.Admin<?, ?> t, final Lane entry, final Set<String> inherited,
                               final boolean isRoot) {
        final List<Step> steps = new ArrayList<>(t.getSteps());
        final int n = steps.size();
        final Lane[] lanes = new Lane[n];
        for (int k = 0; k < n; k++) lanes[k] = provenance(steps.get(k), k == 0 ? entry : lanes[k - 1]);
        final Set<String> defined = new HashSet<>(inherited);
        int i = 0;
        while (i < n) {
            final Lane in = i == 0 ? entry : lanes[i - 1];
            final int end = region(t, steps, i, in, defined, isRoot);
            if (end > i) {
                for (int k = i; k < end; k++) addLabels(defined, steps.get(k));
                i = end;
            } else {
                planChildren(steps.get(i), in, defined);
                addLabels(defined, steps.get(i));
                i++;
            }
        }
    }

    private static void addLabels(final Set<String> defined, final Step<?, ?> step) {
        for (final String label : step.getLabels()) {
            if (!org.apache.tinkerpop.gremlin.structure.Graph.Hidden.isHidden(label)) defined.add(label);
        }
    }

    // the lanes a region may start from: the ones a Rehydrator can read
    private static boolean isEntryLane(final Lane lane) {
        return lane == Lane.V || lane == Lane.E || lane == Lane.VP || lane == Lane.EP;
    }

    /**
     * Tries to grow a region from the step at the index.
     *
     * @return the index after the region's last step, or -1 if no region starts here
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    private int region(final Traversal.Admin<?, ?> t, final List<Step> steps, final int i, final Lane in,
                       final Set<String> defined, final boolean isRoot) {
        final Step<?, ?> first = steps.get(i);
        final PlanBuilder b;
        final int pos;
        try {
            if (isRoot && i == 0 && first instanceof GraphStep && ((GraphStep<?, ?>) first).isStartStep()) {
                b = new PlanBuilder(compiler.startSource(first));
                pos = 1;
            } else {
                if (!isEntryLane(in)) return -1;
                if (analysis.anyReferenced(defined)) {
                    note(first, i, "a label that is read is live here, so a region cannot start");
                    return -1;
                }
                b = new PlanBuilder(compiler.inputSource(in));
                pos = i;
            }
        } catch (final Reject r) {
            note(first, i, r.getMessage());
            return -1;
        }

        final StepCompiler.Scope scope = StepCompiler.Scope.region(t);
        int j = pos;
        while (j < steps.size()) {
            final PlanBuilder saved = b.copy();
            try {
                final int used = compiler.compile(steps, j, b, scope);
                boolean close = false;
                for (int k = 0; k < used; k++) {
                    if (analysis.anyReferenced(steps.get(j + k).getLabels())) {
                        if (k < used - 1) throw new Reject("the label of a fused step is read");
                        close = true;
                    }
                }
                j += used;
                if (close || b.isClosed()) break;
            } catch (final Reject r) {
                b.commit(saved);
                note(steps.get(j), j, r.getMessage());
                break;
            }
        }
        if (!b.isWorthwhile()) {
            if (j > pos) note(first, i, "a region of only a scan, merge or range is not worth running natively");
            return -1;
        }
        final CsrPlan plan;
        try {
            plan = b.build();
        } catch (final Reject r) {
            note(first, i, r.getMessage());
            return -1;
        }
        final CsrSuperStep.EmitMode mode = plan.outputLane().isFacade() ? CsrSuperStep.EmitMode.FACADE
                : plan.outputLane() == Lane.VAL ? CsrSuperStep.EmitMode.VALUE : CsrSuperStep.EmitMode.SCALAR;
        final List<Step<?, ?>> fused = new ArrayList<>();
        for (int k = i; k < j; k++) fused.add(steps.get(k));
        edits.add(new Edit(t, first, steps.get(j - 1), j - i,
                new CsrSuperStep(t, plan, fused, mode, settings)));
        return j;
    }

    // the children of a step that stays standard get regions of their own
    private void planChildren(final Step<?, ?> step, final Lane in, final Set<String> definedBefore) {
        if (!(step instanceof TraversalParent)) return;
        final TraversalParent parent = (TraversalParent) step;
        final Lane childLane = passesInput(step) && isEntryLane(in) ? in : null;
        final Set<String> inherited = new HashSet<>(definedBefore);
        if (step instanceof RepeatStep) {
            // a loop carries the labels of its body back to its start
            for (final Traversal.Admin<?, ?> c : parent.<Object, Object>getGlobalChildren()) inherited.addAll(TraversalHelper.getLabels(c));
            for (final Traversal.Admin<?, ?> c : parent.<Object, Object>getLocalChildren()) inherited.addAll(TraversalHelper.getLabels(c));
        }
        final List<Traversal.Admin<?, ?>> children = new ArrayList<>();
        children.addAll(parent.<Object, Object>getLocalChildren());
        children.addAll(parent.<Object, Object>getGlobalChildren());
        for (final Traversal.Admin<?, ?> c : children) {
            if (c instanceof AbstractLambdaTraversal) continue;
            planTraversal(c, step instanceof RepeatStep ? null : childLane, inherited, false);
        }
    }

    // the steps whose children start from the step's own input traverser
    private static boolean passesInput(final Step<?, ?> s) {
        return s instanceof TraversalFilterStep || s instanceof NotStep || s instanceof ConnectiveStep
                || s instanceof LocalStep || s instanceof TraversalMapStep || s instanceof TraversalFlatMapStep
                || s instanceof UnionStep || s instanceof CoalesceStep || s instanceof OptionalStep
                || s instanceof BranchStep || s instanceof ProjectStep || s instanceof DedupGlobalStep
                || s instanceof OrderGlobalStep || s instanceof GroupStep || s instanceof GroupCountStep
                || s instanceof GroupCountSideEffectStep || s instanceof GroupSideEffectStep
                || s instanceof AggregateStep || s instanceof HasStep;
    }

    /**
     * The lane of the elements a standard step emits when that is provable: the step reads this graph or extends an
     * input lane that is itself proven, and passes elements through or maps them to a known kind.
     */
    private static Lane provenance(final Step<?, ?> s, final Lane in) {
        if (s instanceof GraphStep) return ((GraphStep<?, ?>) s).returnsVertex() ? Lane.V : Lane.E;
        if (s instanceof CsrSuperStep) {
            final CsrSuperStep<?, ?> ss = (CsrSuperStep<?, ?>) s;
            return ss.emitMode() == CsrSuperStep.EmitMode.FACADE ? ss.outputLane() : null;
        }
        if (in == null) return null;
        if (s instanceof VertexStep) return ((VertexStep<?>) s).returnsVertex() ? Lane.V : Lane.E;
        if (s instanceof EdgeVertexStep || s instanceof EdgeOtherVertexStep) return in == Lane.E ? Lane.V : null;
        if (s instanceof PropertiesStep) {
            if (((PropertiesStep<?>) s).getReturnType() != PropertyType.PROPERTY) return null;
            return in == Lane.V ? Lane.VP : in == Lane.E ? Lane.EP : in == Lane.VP ? Lane.MP : null;
        }
        if (s instanceof ElementStep) return in == Lane.VP ? Lane.V : in == Lane.EP ? Lane.E : in == Lane.MP ? Lane.VP : null;
        if (s instanceof FilterStep || s instanceof SideEffectStep || s instanceof IdentityStep
                || s instanceof NoOpBarrierStep || s instanceof OrderGlobalStep || s instanceof AggregateStep
                || s instanceof GroupCountSideEffectStep || s instanceof GroupSideEffectStep
                || s instanceof TailGlobalStepContract) {
            return in;
        }
        return null;
    }

    private void note(final Step<?, ?> step, final int index, final String reason) {
        notes.add(step.getClass().getSimpleName() + " at " + index + ": " + reason);
    }

    // ---------------------------------------------------------------- rewrite

    /**
     * Whether an {@code otherV()} would stay standard next to a rewrite. The standard step reads the path, which a
     * {@code CsrSuperStep} does not carry, so the traversal is then left as it is.
     */
    boolean leavesOtherVUnfused(final List<Edit> planned) {
        final Set<Step<?, ?>> fused = Collections.newSetFromMap(new IdentityHashMap<>());
        for (final Edit e : planned) fused.addAll(e.step.fusedSteps());
        for (final Step<?, ?> other : analysis.otherVSteps()) {
            Step<?, ?> cur = other;
            boolean inside = false;
            while (true) {
                if (fused.contains(cur)) {
                    inside = true;
                    break;
                }
                final Traversal.Admin<?, ?> owner = cur.getTraversal();
                if (owner.isRoot()) break;
                cur = owner.getParent().asStep();
            }
            if (!inside) return true;
        }
        return false;
    }

    /**
     * Performs the planned replacements; the last fused step's labels move to the new step.
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    void apply(final List<Edit> planned) {
        for (final Edit e : planned) {
            final int index = Analysis.indexOf(e.traversal, e.first);
            for (int k = 0; k < e.count; k++) e.traversal.removeStep(index);
            e.traversal.addStep(index, (Step) e.step);
            TraversalHelper.copyLabels(e.last, e.step, false);
        }
    }
}
