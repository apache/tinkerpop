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

import org.apache.tinkerpop.gremlin.process.traversal.Order;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.Pick;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.lambda.AbstractLambdaTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.lambda.ColumnTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.lambda.ConstantTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.lambda.GValueConstantTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.lambda.IdentityTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.lambda.LoopTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.lambda.PredicateTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.lambda.TokenTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.lambda.TrueTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.lambda.ValueTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.step.Deleting;
import org.apache.tinkerpop.gremlin.process.traversal.step.GValue;
import org.apache.tinkerpop.gremlin.process.traversal.step.GValueHolder;
import org.apache.tinkerpop.gremlin.process.traversal.step.LambdaHolder;
import org.apache.tinkerpop.gremlin.process.traversal.step.Mutating;
import org.apache.tinkerpop.gremlin.process.traversal.step.Parameterizing;
import org.apache.tinkerpop.gremlin.process.traversal.step.ReadWriting;
import org.apache.tinkerpop.gremlin.process.traversal.step.Writing;
import org.apache.tinkerpop.gremlin.process.traversal.step.branch.BranchStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.branch.ChooseStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.branch.LocalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.branch.OptionalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.branch.RepeatStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.branch.UnionStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.AndStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.ConnectiveStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.DedupGlobalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.DiscardStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.HasStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.IsStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.NotStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.RangeGlobalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.TraversalFilterStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.CoalesceStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.ConstantStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.CountGlobalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.CountLocalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.EdgeOtherVertexStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.EdgeVertexStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.ElementMapStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.ElementStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.FoldStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.GraphStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.GroupCountStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.GroupStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.IdStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.LabelStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.LabelsStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.LoopsStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.MaxGlobalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.MaxLocalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.MeanGlobalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.MinGlobalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.NoOpBarrierStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.OrderGlobalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.ProjectStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.PropertiesStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.PropertyKeyStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.PropertyMapStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.PropertyValueStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.SumGlobalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.TraversalFlatMapStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.TraversalMapStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.VertexStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.AggregateStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.GroupCountSideEffectStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.GroupSideEffectStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.IdentityStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.ComputerAwareStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.process.traversal.util.ConnectiveP;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.PropertyType;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.snapshot.CsrSnapshot;
import org.apache.tinkerpop.gremlin.structure.snapshot.format.SnapshotLayout;
import org.apache.tinkerpop.gremlin.structure.snapshot.graph.CsrGraph;
import org.apache.tinkerpop.gremlin.structure.snapshot.process.exec.CsrOperatorFactory;
import org.apache.tinkerpop.gremlin.structure.snapshot.process.exec.Lane;
import org.apache.tinkerpop.gremlin.structure.snapshot.process.op.CsrOp;
import org.apache.tinkerpop.gremlin.structure.snapshot.process.op.CsrPlan;
import org.apache.tinkerpop.gremlin.structure.snapshot.process.op.Keys;
import org.apache.tinkerpop.gremlin.structure.snapshot.process.op.Ops;
import org.apache.tinkerpop.gremlin.structure.snapshot.process.op.Preds;
import org.apache.tinkerpop.gremlin.structure.snapshot.process.op.Sources;
import org.apache.tinkerpop.gremlin.structure.snapshot.process.op.Terminals;
import org.apache.tinkerpop.gremlin.structure.snapshot.process.op.branch.DegreeFilter;
import org.apache.tinkerpop.gremlin.structure.snapshot.process.op.value.MapOps;
import org.javatuples.Pair;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.BiPredicate;

/**
 * The compile rules of the spike document (sections 1 and 3): turns one standard step, with the children it absorbs,
 * into IR nodes appended to a {@link PlanBuilder}. Anything that is not provably equivalent throws {@link Reject}, and
 * the planner then ends the region before the step. Every node a rule emits is checked against
 * {@link CsrOperatorFactory#isImplemented}, so a step whose operator does not exist yet stays on facades.
 */
final class StepCompiler {

    /**
     * Where a step is compiled: the traversal it sits in, whether it is a direct step of a region, and the enclosing
     * fused repeats (innermost last; a null name is an unnamed loop).
     */
    static final class Scope {
        private final Traversal.Admin<?, ?> traversal;
        private final boolean topLevel;
        private final boolean bodyDirect;
        private final List<String> loops;

        private Scope(final Traversal.Admin<?, ?> traversal, final boolean topLevel, final boolean bodyDirect,
                      final List<String> loops) {
            this.traversal = traversal;
            this.topLevel = topLevel;
            this.bodyDirect = bodyDirect;
            this.loops = loops;
        }

        /**
         * The scope of the direct steps of a region in the traversal.
         */
        static Scope region(final Traversal.Admin<?, ?> traversal) {
            return new Scope(traversal, true, false, List.of());
        }

        Scope child(final Traversal.Admin<?, ?> child) {
            return new Scope(child, false, false, loops);
        }

        Scope loop(final Traversal.Admin<?, ?> child, final String name, final boolean direct) {
            final List<String> all = new ArrayList<>(loops);
            all.add(name);
            return new Scope(child, false, direct, Collections.unmodifiableList(all));
        }

        boolean insideRepeat() {
            return !loops.isEmpty();
        }
    }

    private final Analysis analysis;
    private final CsrGraph graph;
    private final CsrSnapshot snapshot;
    private final CsrOperatorFactory factory;
    private final Traversal.Admin<?, ?> root;
    private final boolean hasVariables;
    private final boolean identity;
    private final boolean full;

    StepCompiler(final Traversal.Admin<?, ?> root, final CsrGraph graph, final Analysis analysis,
                 final CsrOperatorFactory factory) {
        this.root = root;
        this.graph = graph;
        this.snapshot = graph.snapshot();
        this.analysis = analysis;
        this.factory = factory;
        this.hasVariables = root.getGValueManager().hasVariables();
        this.identity = snapshot.layout() != SnapshotLayout.TOPOLOGY;
        this.full = snapshot.layout() == SnapshotLayout.FULL;
    }

    // ---------------------------------------------------------------- entry points

    /**
     * The source of a region that starts at the root traversal's start {@code GraphStep}.
     */
    CsrOp.Source startSource(final Step<?, ?> step) {
        if (!(step instanceof GraphStep)) throw new Reject(name(step) + " is not a GraphStep");
        final GraphStep<?, ?> g = (GraphStep<?, ?>) step;
        checkGeneral(g);
        if (g.getIdTraversal() != null) throw new Reject("V(traversal) is not supported");
        final Lane lane = g.returnsVertex() ? Lane.V : Lane.E;
        if (lane == Lane.E) requireIdentity("edges");
        final Object[] ids = g.getIds();
        final CsrOp.Source source;
        if (ids.length == 0) {
            source = new Sources.Scan(lane);
        } else {
            requireIdentity("identifier lookup");
            if (lane == Lane.E && !snapshot.hasEdgeIdIndex()) throw new Reject("the snapshot has no edge identifier index");
            final List<Object> list = new ArrayList<>(ids.length);
            for (final Object id : ids) list.add(id instanceof Element ? ((Element) id).id() : id);
            source = new Sources.Lookup(lane, list);
        }
        checkImplemented(source);
        return source;
    }

    /**
     * A region source that reads the upstream traversers.
     */
    CsrOp.Source inputSource(final Lane lane) {
        final CsrOp.Source source = new Sources.Input(lane);
        checkImplemented(source);
        return source;
    }

    /**
     * Compiles the step at the index, and possibly the steps after it that it absorbs, into the builder.
     *
     * @return the number of steps consumed
     * @throws Reject if the step does not compile; the builder is left unchanged
     */
    int compile(final List<Step> steps, final int index, final PlanBuilder builder, final Scope scope) {
        final PlanBuilder work = builder.copy();
        final int before = work.size();
        final int used;
        try {
            used = compileStep(steps, index, work, scope);
        } catch (final IllegalArgumentException | UnsupportedOperationException | IllegalStateException
                 | ClassCastException | NullPointerException e) {
            throw new Reject("the step does not compile: " + e);
        }
        for (int i = Math.max(1, before - 1); i < work.size(); i++) checkImplemented(work.nodes().get(i));
        builder.commit(work);
        return used;
    }

    // ---------------------------------------------------------------- the rules

    private int compileStep(final List<Step> steps, final int index, final PlanBuilder b, final Scope sc) {
        final Step<?, ?> s = steps.get(index);
        checkGeneral(s);
        final Lane lane = b.lane();

        if (s instanceof GraphStep) {
            final GraphStep<?, ?> g = (GraphStep<?, ?>) s;
            if (g.isStartStep()) throw new Reject("a start GraphStep can only start the root traversal");
            if (g.getIdTraversal() != null) throw new Reject("V(traversal) is not supported");
            if (g.getIds().length != 0) throw new Reject("mid-traversal V(ids) is not supported");
            if (!g.returnsVertex()) requireIdentity("edges");
            b.add(new Sources.MidScan(g.returnsVertex() ? Lane.V : Lane.E));
            return 1;
        }
        if (s instanceof VertexStep) {
            final VertexStep<?> v = (VertexStep<?>) s;
            if (lane != Lane.V) throw new Reject("a vertex step needs vertices");
            final Lane emit = v.returnsVertex() ? Lane.V : Lane.E;
            final String[] labels = v.getEdgeLabels();
            if (emit == Lane.E || labels.length > 0) requireIdentity("edge labels");
            b.add(new Ops.Expand(v.getDirection(), labels.length == 0 ? null : edgeLabelCodes(labels), emit, false));
            return 1;
        }
        if (s instanceof EdgeVertexStep) {
            if (lane != Lane.E) throw new Reject("an edge endpoint needs edges");
            requireIdentity("edge endpoints");
            b.add(new Ops.Endpoint(((EdgeVertexStep) s).getDirection()));
            return 1;
        }
        if (s instanceof EdgeOtherVertexStep) {
            otherV(b);
            return 1;
        }
        if (s instanceof HasStep) {
            for (final HasContainer hc : ((HasStep<?>) s).getHasContainers()) has(hc, b);
            return 1;
        }
        if (s instanceof IdentityStep) return 1;
        if (s instanceof NoOpBarrierStep) {
            if (sc.topLevel && lane.isElement() && !b.recordsSource()) b.add(new Ops.Merge());
            return 1;
        }
        if (s instanceof RangeGlobalStep) {
            final RangeGlobalStep<?> r = (RangeGlobalStep<?>) s;
            if (sc.insideRepeat() && !sc.bodyDirect) throw new Reject("a range nested in a repeat body");
            final long lo = Math.max(0L, r.getLowRange());
            b.add(new Ops.Range(lo, r.getHighRange(), sc.insideRepeat()));
            return 1;
        }
        if (s instanceof DedupGlobalStep) {
            dedup((DedupGlobalStep<?>) s, b, sc);
            return 1;
        }
        if (s instanceof TraversalFilterStep) {
            final Traversal.Admin<?, ?> child = ((TraversalFilterStep<?>) s).getFilterTraversal();
            requireFacadeLane(lane, "an existence filter");
            if (!degreeFilter(child, b) && !presence(child, b, true)) b.add(new Ops.Exists(child(child, lane, sc, false)));
            return 1;
        }
        if (s instanceof NotStep) {
            final Traversal.Admin<?, ?> child = ((NotStep<?>) s).<Object, Object>getLocalChildren().get(0);
            requireFacadeLane(lane, "an existence filter");
            if (!presence(child, b, false)) b.add(new Ops.NotExists(child(child, lane, sc, false)));
            return 1;
        }
        if (s instanceof ConnectiveStep) {
            requireFacadeLane(lane, "a connective filter");
            final List<CsrPlan> plans = new ArrayList<>();
            for (final Traversal.Admin<?, ?> c : ((ConnectiveStep<?>) s).<Object, Object>getLocalChildren()) {
                plans.add(child(c, lane, sc, false));
            }
            if (plans.isEmpty()) throw new Reject("a connective without children");
            b.add(s instanceof AndStep ? new Ops.And(plans) : new Ops.Or(plans));
            return 1;
        }
        if (s instanceof IsStep) {
            final P<?> p = ((IsStep<?>) s).getPredicate();
            if (lane != Lane.VAL) throw new Reject("is() needs values");
            if (p.hasTraversal()) throw new Reject("a predicate with a traversal");
            b.add(new Ops.Filter(new Preds.ValuePred(p)));
            return 1;
        }
        if (s instanceof PropertiesStep) {
            properties((PropertiesStep<?>) s, b);
            return 1;
        }
        if (s instanceof PropertyValueStep) {
            b.add(new Ops.PropValue());
            return 1;
        }
        if (s instanceof PropertyKeyStep) {
            b.add(new Ops.PropKey());
            return 1;
        }
        if (s instanceof IdStep) {
            requireIdentity("identifiers");
            b.add(new Ops.Id());
            return 1;
        }
        if (s instanceof LabelStep) {
            requireIdentity("labels");
            b.add(new Ops.Label());
            return 1;
        }
        if (s instanceof LabelsStep) {
            requireIdentity("labels");
            b.add(new Ops.Labels());
            return 1;
        }
        if (s instanceof ElementStep) {
            b.add(new Ops.Element());
            return 1;
        }
        if (s instanceof ConstantStep) {
            final GValue<?> constant = ((ConstantStep<?, ?>) s).getConstantGValue();
            if (constant.isVariable()) throw new Reject("a constant bound to a variable");
            b.add(new Ops.Constant(constant.get()));
            return 1;
        }
        if (s instanceof LoopsStep) {
            loops((LoopsStep<?>) s, b, sc);
            return 1;
        }
        if (s instanceof PropertyMapStep) {
            propertyMap((PropertyMapStep<?, ?>) s, b);
            return 1;
        }
        if (s instanceof ElementMapStep) {
            final ElementMapStep<?, ?> m = (ElementMapStep<?, ?>) s;
            if (m.isOnGraphComputer()) throw new Reject("elementMap on a graph computer");
            requireMapLane(lane);
            b.add(new MapOps.ElementMap(mapKeyCodes(m.getPropertyKeys(), lane), TraversalHelper.isMultilabelEnabled(root)));
            return 1;
        }
        if (s instanceof ProjectStep) {
            project((ProjectStep<?, ?>) s, b, sc);
            return 1;
        }
        if (s instanceof LocalStep) {
            requireFacadeLane(lane, "local()");
            b.add(new Ops.Local(child(singleChild(s), lane, sc, false)));
            return 1;
        }
        if (s instanceof TraversalFlatMapStep) {
            requireFacadeLane(lane, "flatMap()");
            b.add(new Ops.FlatMap(child(singleChild(s), lane, sc, false)));
            return 1;
        }
        if (s instanceof TraversalMapStep) {
            traversalMap((TraversalMapStep<?, ?>) s, b, sc);
            return 1;
        }
        if (s instanceof UnionStep) {
            final UnionStep<?, ?> u = (UnionStep<?, ?>) s;
            if (u.isStart()) throw new Reject("a start union");
            requireFacadeLane(lane, "union()");
            final List<CsrPlan> plans = new ArrayList<>();
            for (final Traversal.Admin<?, ?> c : u.<Object, Object>getGlobalChildren()) plans.add(child(c, lane, sc, false));
            if (plans.isEmpty()) throw new Reject("a union without branches");
            b.add(new Ops.Union(plans));
            return 1;
        }
        if (s instanceof CoalesceStep) {
            requireFacadeLane(lane, "coalesce()");
            final List<CsrPlan> plans = new ArrayList<>();
            for (final Traversal.Admin<?, ?> c : ((CoalesceStep<?, ?>) s).<Object, Object>getLocalChildren()) {
                plans.add(child(c, lane, sc, false));
            }
            if (plans.isEmpty()) throw new Reject("a coalesce without branches");
            b.add(new Ops.Coalesce(plans));
            return 1;
        }
        if (s instanceof OptionalStep) {
            requireFacadeLane(lane, "optional()");
            b.add(new Ops.Optional(child(singleChild(s), lane, sc, false)));
            return 1;
        }
        if (s instanceof BranchStep) {
            choose((BranchStep<?, ?, ?>) s, b, sc);
            return 1;
        }
        if (s instanceof RepeatStep) {
            repeat((RepeatStep<?>) s, b, sc);
            return 1;
        }
        if (s instanceof AggregateStep || s instanceof GroupCountSideEffectStep || s instanceof GroupSideEffectStep) {
            writer(s, steps, index, b, sc);
            return 1;
        }
        if (s instanceof CountGlobalStep) {
            b.add(new Terminals.Count());
            return 1;
        }
        if (s instanceof CountLocalStep) {
            b.add(new MapOps.CountLocal());
            return 1;
        }
        if (s instanceof SumGlobalStep) {
            requireValues(lane, "sum()");
            b.add(new Terminals.Sum());
            return 1;
        }
        if (s instanceof MinGlobalStep) {
            requireValues(lane, "min()");
            b.add(new Terminals.Min());
            return 1;
        }
        if (s instanceof MaxGlobalStep) {
            requireValues(lane, "max()");
            b.add(new Terminals.Max());
            return 1;
        }
        if (s instanceof MaxLocalStep) {
            b.add(new MapOps.MaxLocal());
            return 1;
        }
        if (s instanceof MeanGlobalStep) {
            requireValues(lane, "mean()");
            b.add(new Terminals.Mean());
            return 1;
        }
        if (s instanceof FoldStep) {
            if (!((FoldStep<?, ?>) s).isListFold()) throw new Reject("fold() with a seed");
            b.add(new Terminals.Fold());
            return 1;
        }
        if (s instanceof DiscardStep) {
            b.add(new Terminals.Drain());
            return 1;
        }
        if (s instanceof GroupCountStep) {
            final List<Traversal.Admin<Object, Object>> children = ((GroupCountStep<Object, Object>) s).getLocalChildren();
            b.add(new Terminals.GroupCount(key(children.isEmpty() ? null : children.get(0), lane, sc)));
            return 1;
        }
        if (s instanceof GroupStep) {
            final GroupStep<?, ?, ?> g = (GroupStep<?, ?, ?>) s;
            b.add(new Terminals.Group(key(g.getKeyTraversal(), lane, sc), reducer(g.getValueTraversal(), lane, sc)));
            return 1;
        }
        if (s instanceof OrderGlobalStep) {
            return order(steps, index, b, sc);
        }
        throw new Reject("unsupported step " + name(s));
    }

    // ---------------------------------------------------------------- general checks

    private void checkGeneral(final Step<?, ?> s) {
        if (s instanceof GValueHolder && ((GValueHolder<?, ?>) s).isParameterized()) throw new Reject(name(s) + " is an unreduced placeholder");
        if (s instanceof LambdaHolder) throw new Reject(name(s) + " holds a lambda");
        if (s instanceof Mutating || s instanceof Writing || s instanceof Deleting || s instanceof ReadWriting) {
            throw new Reject(name(s) + " writes");
        }
        if (s instanceof Parameterizing && !((Parameterizing) s).getParameters().isEmpty()) {
            throw new Reject(name(s) + " has parameters");
        }
    }

    private void checkImplemented(final CsrOp node) {
        if (!factory.isImplemented(node.getClass())) throw new Reject("no native operator for " + node.name());
        for (final CsrPlan child : node.children()) {
            for (final CsrOp inner : child.nodes()) checkImplemented(inner);
        }
    }

    private void requireIdentity(final String what) {
        if (!identity) throw new Reject("a topology snapshot has no " + what);
    }

    private void requireFull(final String what) {
        if (!full) throw new Reject("only a full snapshot has " + what);
    }

    private static void requireFacadeLane(final Lane lane, final String what) {
        if (lane == null || !lane.isFacade()) throw new Reject(what + " needs elements or properties, not " + lane);
    }

    private static void requireValues(final Lane lane, final String what) {
        if (lane != Lane.VAL) throw new Reject(what + " needs values, not " + lane);
    }

    private static void requireDecoded(final Lane lane, final String what) {
        if (lane != Lane.VAL && lane != Lane.SCALAR) throw new Reject(what + " needs a decoded value, not " + lane);
    }

    private static void requireMapLane(final Lane lane) {
        if (lane != Lane.V && lane != Lane.E) throw new Reject("a map step needs vertices or edges, not " + lane);
    }

    private static String name(final Step<?, ?> s) {
        return s.getClass().getSimpleName();
    }

    @SuppressWarnings("unchecked")
    private static Traversal.Admin<?, ?> singleChild(final Step<?, ?> s) {
        final List<Traversal.Admin<Object, Object>> children = ((org.apache.tinkerpop.gremlin.process.traversal.step.TraversalParent) s).getLocalChildren();
        if (children.size() != 1) throw new Reject(name(s) + " does not have exactly one child");
        return children.get(0);
    }

    // ---------------------------------------------------------------- navigation

    private int[] edgeLabelCodes(final String[] labels) {
        final TreeSet<Integer> codes = new TreeSet<>();
        for (final String label : labels) {
            final int code = snapshot.edgeLabelCodeOf(label);
            if (code >= 0) codes.add(code);
        }
        final int[] out = new int[codes.size()];
        int i = 0;
        for (final int code : codes) out[i++] = code;
        return out;
    }

    // the edge has to remember the vertex it came from, so the Expand that produced it is replaced by one that records it
    private void otherV(final PlanBuilder b) {
        if (b.lane() != Lane.E) throw new Reject("otherV() needs edges");
        requireIdentity("edge endpoints");
        if (!b.recordsSource()) {
            int k = -1;
            for (int i = b.size() - 1; i >= 1; i--) {
                final CsrOp op = b.nodes().get(i);
                if (op instanceof Ops.Expand && ((Ops.Expand) op).emit() == Lane.E) {
                    k = i;
                    break;
                }
                if (op.outputLane(Lane.E) != Lane.E || !op.outputRecordsSource(true)) {
                    throw new Reject("otherV() is separated from its edge expansion by " + op.name());
                }
            }
            if (k < 0) throw new Reject("otherV() needs an edge expansion in the same region");
            final Ops.Expand e = (Ops.Expand) b.nodes().get(k);
            b.replace(k, new Ops.Expand(e.direction(), e.labelCodes(), Lane.E, true));
        }
        b.add(new Ops.OtherV());
    }

    // ---------------------------------------------------------------- has()

    private void has(final HasContainer hc, final PlanBuilder b) {
        if (hc.hasTraversal()) throw new Reject("a predicate with a traversal");
        final Lane lane = b.lane();
        final String key = hc.getKey();
        final P<?> p = hc.getPredicate();
        if (lane == Lane.V || lane == Lane.E) {
            final boolean vertex = lane == Lane.V;
            if (T.id.getAccessor().equals(key)) {
                requireIdentity("identifiers");
                b.add(new Ops.Filter(idPred(p, vertex)));
            } else if (T.label.getAccessor().equals(key)) {
                requireIdentity("labels");
                b.add(new Ops.Filter(labelSet(p, vertex)));
            } else if (key == null || T.key.getAccessor().equals(key) || T.value.getAccessor().equals(key)) {
                throw new Reject("has() on " + key + " of " + lane);
            } else {
                requireFull("properties");
                final int code = vertex ? graph.vertexKeyCode(key) : graph.edgeKeyCode(key);
                b.add(new Ops.Filter(code < 0 ? nothing() : new Preds.PropPred(code, p)));
            }
        } else if (lane == Lane.VP || lane == Lane.EP || lane == Lane.MP) {
            if (T.value.getAccessor().equals(key)) b.add(new Ops.Filter(new Preds.ValuePred(p)));
            else if (T.key.getAccessor().equals(key)) b.add(new Ops.Filter(new Preds.KeyPred(p)));
            else throw new Reject("has() on " + key + " of " + lane);
        } else {
            throw new Reject("has() on " + lane);
        }
    }

    private static Preds.Pred nothing() {
        return new Preds.IdIn(new int[0]);
    }

    private Preds.Pred idPred(final P<?> p, final boolean vertex) {
        final BiPredicate<?, ?> bp = p.getBiPredicate();
        final Object value = p.getValue();
        if (!hasVariables && !(p instanceof ConnectiveP) && (bp == org.apache.tinkerpop.gremlin.process.traversal.Compare.eq
                || bp == org.apache.tinkerpop.gremlin.process.traversal.Contains.within)) {
            final Collection<?> values = bp == org.apache.tinkerpop.gremlin.process.traversal.Compare.eq
                    ? Collections.singletonList(value) : value instanceof Collection ? (Collection<?>) value : null;
            final boolean exact = vertex ? snapshot.manifest().isVertexIdIndexExact()
                    : snapshot.hasEdgeIdIndex() && snapshot.manifest().isEdgeIdIndexExact();
            if (values != null && exact && allIntegral(values)) {
                final TreeSet<Integer> ordinals = new TreeSet<>();
                for (final Object v : values) {
                    final int ordinal = vertex ? snapshot.vertexOrdinalCoerced(v) : snapshot.edgeOrdinalCoerced(v);
                    if (ordinal >= 0) ordinals.add(ordinal);
                }
                final int[] out = new int[ordinals.size()];
                int i = 0;
                for (final int ordinal : ordinals) out[i++] = ordinal;
                return new Preds.IdIn(out);
            }
        }
        return new Preds.IdPred(p, isStringTest(value));
    }

    private static boolean allIntegral(final Collection<?> values) {
        for (final Object v : values) {
            if (!(v instanceof Long || v instanceof Integer || v instanceof Short || v instanceof Byte)) return false;
        }
        return true;
    }

    // the rule of HasContainer: strings are compared through the toString of the identifier
    private static boolean isStringTest(final Object value) {
        if (value instanceof Collection) {
            final Collection<?> c = (Collection<?>) value;
            if (!c.isEmpty()) {
                for (final Object o : c) {
                    if (o != null && !(o instanceof String)) return false;
                }
                return true;
            }
        }
        return value instanceof String;
    }

    @SuppressWarnings("unchecked")
    private Preds.Pred labelSet(final P<?> p, final boolean vertex) {
        if (hasVariables) throw new Reject("a label predicate over variables");
        final List<String> dictionary = vertex ? snapshot.vertexLabels() : snapshot.edgeLabels();
        final boolean[] byCode = new boolean[dictionary.size()];
        try {
            for (int i = 0; i < byCode.length; i++) byCode[i] = ((P<Object>) p).test(dictionary.get(i));
        } catch (final RuntimeException e) {
            throw new Reject("a label predicate that throws: " + e.getMessage());
        }
        return new Preds.LabelSet(byCode);
    }

    // filter(outE(labels).count().is(P)) and its variants: the degree of the vertex tested against P. CountStrategy
    // puts limit(n) before the count; the test on the limited count is the same as on the degree when P does not
    // change beyond n, which is checked on a few larger degrees
    @SuppressWarnings("unchecked")
    private boolean degreeFilter(final Traversal.Admin<?, ?> child, final PlanBuilder b) {
        final List<Step> st = child.getSteps();
        final int n = st.size();
        if ((n != 3 && n != 4) || b.lane() != Lane.V) return false;
        if (!(st.get(0) instanceof VertexStep) || !(st.get(n - 2) instanceof CountGlobalStep)
                || !(st.get(n - 1) instanceof IsStep)) {
            return false;
        }
        for (final Step<?, ?> s : st) {
            if (!s.getLabels().isEmpty()) return false;
            checkGeneral(s);
        }
        final P<?> p = ((IsStep<?>) st.get(n - 1)).getPredicate();
        if (p.hasTraversal()) return false;
        if (n == 4) {
            if (!(st.get(1) instanceof RangeGlobalStep)) return false;
            final RangeGlobalStep<?> r = (RangeGlobalStep<?>) st.get(1);
            if (r.getLowRange() > 0 || r.getHighRange() < 0) return false;
            final long limit = r.getHighRange();
            try {
                final boolean atLimit = ((P<Object>) p).test(limit);
                for (final long degree : new long[]{limit + 1L, limit + 2L, limit + 1000L, Long.MAX_VALUE / 2L}) {
                    if (((P<Object>) p).test(degree) != atLimit) return false;
                }
            } catch (final RuntimeException e) {
                return false;
            }
        }
        requireIdentity("edge labels");
        final VertexStep<?> v = (VertexStep<?>) st.get(0);
        final String[] labels = v.getEdgeLabels();
        b.add(new DegreeFilter(v.getDirection(), labels.length == 0 ? null : edgeLabelCodes(labels), p));
        return true;
    }

    // has(key) and hasNot(key) are filters over values(key), which is a presence test
    private boolean presence(final Traversal.Admin<?, ?> child, final PlanBuilder b, final boolean positive) {
        final List<Step> steps = child.getSteps();
        if (steps.size() != 1 || !(steps.get(0) instanceof PropertiesStep)) return false;
        final PropertiesStep<?> ps = (PropertiesStep<?>) steps.get(0);
        if (ps.getReturnType() != PropertyType.VALUE || ps.getPropertyKeys().length != 1
                || !ps.getLabels().isEmpty() || !ps.getParameters().isEmpty()
                || (b.lane() != Lane.V && b.lane() != Lane.E) || !full) {
            return false;
        }
        final String key = ps.getPropertyKeys()[0];
        final int code = b.lane() == Lane.V ? graph.vertexKeyCode(key) : graph.edgeKeyCode(key);
        if (positive) b.add(new Ops.Filter(code < 0 ? nothing() : new Preds.Presence(code)));
        else if (code >= 0) return false;
        return true;
    }

    // ---------------------------------------------------------------- dedup, keys and ordering

    private void dedup(final DedupGlobalStep<?> d, final PlanBuilder b, final Scope sc) {
        final Set<String> labels = d.getScopeKeys();
        if (labels != null && !labels.isEmpty()) throw new Reject("dedup over labels");
        final Lane lane = b.lane();
        if (lane == null || lane == Lane.SCALAR) throw new Reject("dedup of " + lane);
        final List<Traversal.Admin<Object, Object>> children = ((DedupGlobalStep<Object>) d).getLocalChildren() == null
                ? List.of() : castChildren(d);
        final Keys.Key key = key(children.isEmpty() ? null : children.get(0), lane, sc);
        if (sc.insideRepeat()) {
            if (!sc.bodyDirect) throw new Reject("a dedup nested in a repeat body");
            if (!lane.isElement() || b.recordsSource()) throw new Reject("a persistent dedup over " + lane);
            final boolean supported = key instanceof Keys.Identity || key instanceof Keys.Const
                    || key instanceof Keys.Value || key instanceof Keys.Child
                    || (key instanceof Keys.Token && (((Keys.Token) key).token() == T.id
                    || ((Keys.Token) key).token() == T.label));
            if (!supported) throw new Reject("a persistent dedup by " + key);
        }
        b.add(new Ops.Dedup(key));
    }

    @SuppressWarnings("unchecked")
    private static List<Traversal.Admin<Object, Object>> castChildren(final DedupGlobalStep<?> d) {
        return (List) d.getLocalChildren();
    }

    /**
     * A {@code by()} modulator as a key: absent and identity, tokens, property values (with or without the productive
     * wrapper), constants, or a child plan whose first result is the key.
     */
    private Keys.Key key(final Traversal.Admin<?, ?> by, final Lane lane, final Scope sc) {
        if (lane == null) throw new Reject("a key over " + lane);
        if (by == null || by instanceof IdentityTraversal) return new Keys.Identity();
        if (by instanceof TokenTraversal) {
            if (lane == Lane.SCALAR) throw new Reject("by(token) over " + lane);
            final T token = ((TokenTraversal<?, ?>) by).getToken();
            final boolean ok;
            if (token == T.id) ok = lane == Lane.V || lane == Lane.E || lane == Lane.VP;
            else if (token == T.label) ok = lane == Lane.V || lane == Lane.E;
            else ok = lane.isProperty();
            if (!ok) throw new Reject("by(" + token + ") over " + lane);
            if (token == T.id || token == T.label) requireIdentity("identifiers and labels");
            return new Keys.Token(token);
        }
        if (by instanceof ValueTraversal) {
            final ValueTraversal<?, ?> vt = (ValueTraversal<?, ?>) by;
            final boolean productive = vt.getBypassTraversal() != null;
            if (productive && !isProductiveBypass(vt)) throw new Reject("an unrecognized by() bypass");
            final String name = vt.getPropertyKey();
            if (lane == Lane.VAL || lane == Lane.SCALAR) return new Keys.Value(-1, name, productive);
            if (lane != Lane.V && lane != Lane.E) throw new Reject("by(key) over " + lane);
            requireFull("properties");
            final int code = lane == Lane.V ? graph.vertexKeyCode(name) : graph.edgeKeyCode(name);
            if (code < 0) {
                if (productive) return new Keys.Const(null);
                throw new Reject("by(" + name + ") over a key the snapshot does not have");
            }
            return new Keys.Value(code, name, productive);
        }
        if (by instanceof ConstantTraversal) {
            final GValue<?> end = ((ConstantTraversal<?, ?>) by).getEnd();
            if (end.isVariable()) throw new Reject("a constant bound to a variable");
            return new Keys.Const(end.get());
        }
        if (by instanceof GValueConstantTraversal) {
            final GValue<?> end = ((GValueConstantTraversal<?, ?>) by).getGValue();
            if (end.isVariable()) throw new Reject("a constant bound to a variable");
            return new Keys.Const(end.get());
        }
        if (by instanceof AbstractLambdaTraversal) {
            throw new Reject("the by() form " + by.getClass().getSimpleName());
        }
        return new Keys.Child(child(by, lane, sc, true));
    }

    // ProductiveByStrategy wraps a value traversal as coalesce(values(key), constant(null))
    private static boolean isProductiveBypass(final ValueTraversal<?, ?> vt) {
        final Traversal.Admin<?, ?> bypass = vt.getBypassTraversal();
        final List<Step> steps = bypass.getSteps();
        if (steps.size() != 1 || !(steps.get(0) instanceof CoalesceStep)) return false;
        final List<Traversal.Admin<Object, Object>> branches = ((CoalesceStep<Object, Object>) steps.get(0)).getLocalChildren();
        if (branches.size() != 2) return false;
        final Traversal.Admin<?, ?> last = branches.get(1);
        final boolean nullBranch = last instanceof ConstantTraversal && ((ConstantTraversal<?, ?>) last).getEnd().get() == null;
        return nullBranch && branches.get(0) instanceof ValueTraversal
                && ((ValueTraversal<?, ?>) branches.get(0)).getBypassTraversal() == null
                && ((ValueTraversal<?, ?>) branches.get(0)).getPropertyKey().equals(vt.getPropertyKey());
    }

    private int order(final List<Step> steps, final int index, final PlanBuilder b, final Scope sc) {
        final OrderGlobalStep<?, ?> o = (OrderGlobalStep<?, ?>) steps.get(index);
        if (o.getLimit() != Long.MAX_VALUE) throw new Reject("an order() with a limit");
        final Lane lane = b.lane();
        if (lane == null || lane == Lane.SCALAR) throw new Reject("order() of " + lane);
        final List<Keys.Key> keys = new ArrayList<>();
        final List<Order> orders = new ArrayList<>();
        for (final Pair<Traversal.Admin, java.util.Comparator> c : (List<Pair<Traversal.Admin, java.util.Comparator>>) (List) o.getComparators()) {
            final Object comparator = c.getValue1();
            if (comparator != Order.asc && comparator != Order.desc) throw new Reject("the comparator " + comparator);
            keys.add(key(c.getValue0(), lane, sc));
            orders.add((Order) comparator);
        }
        if (index + 1 < steps.size() && steps.get(index + 1) instanceof RangeGlobalStep) {
            final RangeGlobalStep<?> r = (RangeGlobalStep<?>) steps.get(index + 1);
            if (r.getHighRange() >= 0 && !(sc.insideRepeat())) {
                b.add(new Terminals.TopK(keys, orders, Math.max(0L, r.getLowRange()), r.getHighRange()));
                return 2;
            }
        }
        b.add(new Terminals.Sort(keys, orders));
        return 1;
    }

    // ---------------------------------------------------------------- children

    /**
     * Compiles a child traversal to a plan that starts with {@code Input(lane)}.
     */
    private CsrPlan child(final Traversal.Admin<?, ?> t, final Lane lane, final Scope sc, final boolean allowTerminal) {
        if (t instanceof AbstractLambdaTraversal) throw new Reject("a lambda traversal child");
        if (lane == null) throw new Reject("a child over " + lane);
        final CsrOp.Source input = new Sources.Input(lane);
        checkImplemented(input);
        final PlanBuilder b = new PlanBuilder(input);
        final Scope inner = sc.child(t);
        final List<Step> steps = t.getSteps();
        int i = 0;
        while (i < steps.size()) {
            final Step<?, ?> s = steps.get(i);
            if (s instanceof ComputerAwareStep.EndStep || s instanceof RepeatStep.RepeatEndStep) {
                i++;
                continue;
            }
            if (analysis.anyReferenced(s.getLabels())) throw new Reject("the label of " + name(s) + " is read");
            i += compile(steps, i, b, inner);
        }
        if (b.endsInTerminal() && !allowTerminal) throw new Reject("a reducing child");
        return b.build();
    }

    private void traversalMap(final TraversalMapStep<?, ?> s, final PlanBuilder b, final Scope sc) {
        final Traversal.Admin<?, ?> child = singleChild(s);
        if (!(child instanceof AbstractLambdaTraversal)) {
            requireFacadeLane(b.lane(), "map()");
            b.add(new Ops.MapFirst(child(child, b.lane(), sc, false)));
            return;
        }
        final AbstractLambdaTraversal<?, ?> lt = (AbstractLambdaTraversal<?, ?>) child;
        if (lt.getBypassTraversal() != null) throw new Reject("a map over a bypass traversal");
        if (lt instanceof IdentityTraversal) return;
        final Lane lane = b.lane();
        if (lt instanceof ColumnTraversal) {
            requireDecoded(lane, "select(column)");
            b.add(new MapOps.SelectColumn(((ColumnTraversal) lt).getColumn()));
            return;
        }
        if (lt instanceof TokenTraversal) {
            final T token = ((TokenTraversal<?, ?>) lt).getToken();
            if (token == T.id) {
                requireIdentity("identifiers");
                b.add(new Ops.Id());
            } else if (token == T.label) {
                requireIdentity("labels");
                b.add(new Ops.Label());
            } else if (token == T.key) {
                b.add(new Ops.PropKey());
            } else {
                b.add(new Ops.PropValue());
            }
            return;
        }
        if (lt instanceof ValueTraversal) {
            final String name = ((ValueTraversal<?, ?>) lt).getPropertyKey();
            if (lane != Lane.V && lane != Lane.E) throw new Reject("map(key) over " + lane);
            requireFull("properties");
            final int code = lane == Lane.V ? graph.vertexKeyCode(name) : graph.edgeKeyCode(name);
            if (code < 0 || (lane == Lane.V && snapshot.isMultiProperty(code))) {
                throw new Reject("map(" + name + ") over an unknown or multi-valued key");
            }
            b.add(new Ops.Props(new int[]{code}, Ops.PropMode.VALUE));
            return;
        }
        if (lt instanceof ConstantTraversal || lt instanceof GValueConstantTraversal) {
            final GValue<?> end = lt instanceof ConstantTraversal ? ((ConstantTraversal<?, ?>) lt).getEnd()
                    : ((GValueConstantTraversal<?, ?>) lt).getGValue();
            if (end.isVariable()) throw new Reject("a constant bound to a variable");
            b.add(new Ops.Constant(end.get()));
            return;
        }
        throw new Reject("the map form " + lt.getClass().getSimpleName());
    }

    /**
     * The value traversal of {@code group()}: a plan from {@code Input(lane)} that ends in a reducing terminal.
     */
    private CsrPlan reducer(final Traversal.Admin<?, ?> value, final Lane lane, final Scope sc) {
        if (value == null) throw new Reject("group() without a value traversal");
        final CsrPlan plan = child(value, lane, sc, true);
        final CsrOp.Terminal t = plan.terminal();
        if (!(t instanceof Terminals.Count || t instanceof Terminals.Fold || t instanceof Terminals.Sum
                || t instanceof Terminals.Min || t instanceof Terminals.Max || t instanceof Terminals.Mean)) {
            throw new Reject("a group value that does not end in count, fold, sum, min, max or mean");
        }
        return plan;
    }

    private void project(final ProjectStep<?, ?> p, final PlanBuilder b, final Scope sc) {
        final Lane lane = b.lane();
        if (lane == null) throw new Reject("project() of " + lane);
        final List<String> names = p.getProjectKeys();
        final List<? extends Traversal.Admin<?, ?>> ring = p.getTraversalRing().getTraversals();
        final List<Keys.Key> keys = new ArrayList<>(names.size());
        for (int i = 0; i < names.size(); i++) {
            keys.add(key(ring.isEmpty() ? null : ring.get(i % ring.size()), lane, sc));
        }
        b.add(new MapOps.Project(names, keys));
    }

    private void propertyMap(final PropertyMapStep<?, ?> m, final PlanBuilder b) {
        if (!m.<Object, Object>getLocalChildren().isEmpty()) throw new Reject("valueMap()/propertyMap() with by() or a property traversal");
        final Lane lane = b.lane();
        requireMapLane(lane);
        final int[] codes = mapKeyCodes(m.getPropertyKeys(), lane);
        if (m.getReturnType() == PropertyType.VALUE) {
            b.add(new MapOps.ValueMap(codes, m.getIncludedTokens(), TraversalHelper.isMultilabelEnabled(root)));
        } else {
            b.add(new MapOps.PropertyMap(codes));
        }
    }

    // the key codes of a map step, in argument order; a key the snapshot does not have is -1, which the operators skip
    private int[] mapKeyCodes(final String[] keys, final Lane lane) {
        requireFull("properties");
        requireIdentity("identifiers");
        return propertyCodes(keys, lane);
    }

    private int[] propertyCodes(final String[] keys, final Lane lane) {
        final int[] codes = new int[keys.length];
        final Set<String> seen = new TreeSet<>();
        for (int i = 0; i < keys.length; i++) {
            if (!seen.add(keys[i])) throw new Reject("a repeated property key " + keys[i]);
            codes[i] = lane == Lane.V ? graph.vertexKeyCode(keys[i]) : graph.edgeKeyCode(keys[i]);
        }
        return codes;
    }

    private void properties(final PropertiesStep<?> p, final PlanBuilder b) {
        final Lane lane = b.lane();
        if (lane != Lane.V && lane != Lane.E) throw new Reject("properties of " + lane);
        requireFull("properties");
        b.add(new Ops.Props(propertyCodes(p.getPropertyKeys(), lane),
                p.getReturnType() == PropertyType.VALUE ? Ops.PropMode.VALUE : Ops.PropMode.PROPERTY));
    }

    // ---------------------------------------------------------------- branches and loops

    @SuppressWarnings("unchecked")
    private void choose(final BranchStep<?, ?, ?> step, final PlanBuilder b, final Scope sc) {
        final Lane lane = b.lane();
        requireFacadeLane(lane, "a branch");
        if (step instanceof UnionStep) throw new Reject("a union handled elsewhere");
        if (step instanceof ChooseStep
                && ((ChooseStep<?, ?, ?>) step).getChooseSemantics() == ChooseStep.ChooseSemantics.IF_THEN) {
            throw new Reject("a choose with a predicate traversal has no native form");
        }
        final Map<Object, CsrPlan> options = new LinkedHashMap<>();
        final Keys.Key key = key(step.getBranchTraversal(), lane, sc);
        for (final Map.Entry<Pick, List<Traversal.Admin>> e : (Set<Map.Entry<Pick, List<Traversal.Admin>>>) (Set) step
                .getTraversalPickOptions().entrySet()) {
            final Pick pick = e.getKey();
            if (pick == Pick.any) throw new Reject("a choose with the any option");
            if (e.getValue().size() != 1) throw new Reject("several options for " + pick);
            options.put(pick, child(e.getValue().get(0), lane, sc, false));
        }
        for (final Pair<Traversal.Admin<Object, ?>, Traversal.Admin<Object, Object>> option
                : (List<Pair<Traversal.Admin<Object, ?>, Traversal.Admin<Object, Object>>>) (List) step.getTraversalOptions()) {
            if (!(option.getValue0() instanceof PredicateTraversal)) throw new Reject("an option that is not a value");
            final Object predicate = ((PredicateTraversal<?>) (Object) option.getValue0()).getPredicate();
            if (!(predicate instanceof P) || ((P<?>) predicate).getBiPredicate() != org.apache.tinkerpop.gremlin.process.traversal.Compare.eq) {
                throw new Reject("an option that is not an equality");
            }
            final Object literal = ((P<?>) predicate).getValue();
            if (!(literal instanceof String || literal instanceof Boolean)) throw new Reject("an option key " + literal);
            if (options.containsKey(literal)) throw new Reject("several options for " + literal);
            options.put(literal, child(option.getValue1(), lane, sc, false));
        }
        if (options.isEmpty()) throw new Reject("a choose without options");
        b.add(new Ops.Choose(key, options));
    }

    private void repeat(final RepeatStep<?> r, final PlanBuilder b, final Scope sc) {
        final Lane lane = b.lane();
        if (lane != Lane.V && lane != Lane.E) throw new Reject("repeat() over " + lane);
        if (b.recordsSource()) throw new Reject("repeat() over edges that record their source");
        if (r.getRepeatTraversal() == null) throw new Reject("repeat() without a body");
        final String loopName = r.getLoopName();
        final CsrPlan body = loopChild(r.getRepeatTraversal(), lane, sc, loopName, true);
        if (body.outputRecordsSource()) throw new Reject("a repeat body that records its source");
        CsrPlan until = null;
        int maxLoops = -1;
        final Traversal.Admin<?, ?> u = r.getUntilTraversal();
        if (u instanceof LoopTraversal) {
            final long max = ((LoopTraversal<?>) u).getMaxLoops();
            if (max < 0 || max > Integer.MAX_VALUE) throw new Reject("times(" + max + ")");
            maxLoops = (int) max;
        } else if (u instanceof TrueTraversal) {
            until = CsrPlan.of(new Sources.Input(lane));
        } else if (u != null) {
            until = loopChild(u, lane, sc, loopName, false);
        }
        CsrPlan emit = null;
        boolean emitAll = false;
        final Traversal.Admin<?, ?> e = r.getEmitTraversal();
        if (e instanceof TrueTraversal) emitAll = true;
        else if (e != null) emit = loopChild(e, lane, sc, loopName, false);
        b.add(new Ops.Repeat(body, until, r.untilFirst, emit, emitAll, r.emitFirst, maxLoops, loopName));
    }

    private CsrPlan loopChild(final Traversal.Admin<?, ?> t, final Lane lane, final Scope sc, final String loopName,
                              final boolean direct) {
        if (t instanceof AbstractLambdaTraversal) throw new Reject("a lambda traversal child");
        final CsrOp.Source input = new Sources.Input(lane);
        checkImplemented(input);
        final PlanBuilder b = new PlanBuilder(input);
        final Scope inner = sc.loop(t, loopName, direct);
        final List<Step> steps = t.getSteps();
        int i = 0;
        while (i < steps.size()) {
            final Step<?, ?> s = steps.get(i);
            if (s instanceof ComputerAwareStep.EndStep || s instanceof RepeatStep.RepeatEndStep) {
                i++;
                continue;
            }
            if (analysis.anyReferenced(s.getLabels())) throw new Reject("the label of " + name(s) + " is read");
            i += compile(steps, i, b, inner);
        }
        if (b.endsInTerminal()) throw new Reject("a reducing loop child");
        return b.build();
    }

    // loops() and loops(name): LoopsStep keeps the name private, so the candidates are compared with equals()
    @SuppressWarnings("unchecked")
    private void loops(final LoopsStep<?> s, final PlanBuilder b, final Scope sc) {
        if (!sc.insideRepeat()) throw new Reject("loops() outside a fused repeat");
        for (int i = sc.loops.size() - 1; i >= 0; i--) {
            final String name = sc.loops.get(i);
            if (name != null && new LoopsStep<>((Traversal.Admin) s.getTraversal(), name).equals(s)) {
                b.add(new Ops.Loops(name));
                return;
            }
        }
        if (new LoopsStep<>((Traversal.Admin) s.getTraversal(), null).equals(s)) {
            b.add(new Ops.Loops(null));
            return;
        }
        throw new Reject("loops() of an unknown loop name");
    }

    // ---------------------------------------------------------------- side-effect writers

    @SuppressWarnings("unchecked")
    private void writer(final Step<?, ?> s, final List<Step> steps, final int index, final PlanBuilder b, final Scope sc) {
        if (!sc.topLevel || sc.traversal != root) throw new Reject("a side-effect writer below the root traversal");
        final Lane lane = b.lane();
        if (lane == null || lane == Lane.SCALAR) throw new Reject("a side-effect writer over " + lane);
        if (s instanceof AggregateStep) {
            final AggregateStep<?> a = (AggregateStep<?>) s;
            final List<Traversal.Admin<Object, Object>> by = ((AggregateStep<Object>) a).getLocalChildren();
            if (!by.isEmpty() && !(by.get(0) instanceof IdentityTraversal)) throw new Reject("aggregate() by a modulator");
            b.add(new Ops.AggregateSideEffect(a.getSideEffectKey()));
        } else if (s instanceof GroupCountSideEffectStep) {
            final GroupCountSideEffectStep<?, ?> g = (GroupCountSideEffectStep<?, ?>) s;
            checkWriter(g.getSideEffectKey(), s, steps, index);
            final List<Traversal.Admin<Object, Object>> by = (List) g.getLocalChildren();
            b.add(new Ops.GroupCountSideEffect(g.getSideEffectKey(), key(by.isEmpty() ? null : by.get(0), lane, sc)));
        } else {
            final GroupSideEffectStep<?, ?, ?> g = (GroupSideEffectStep<?, ?, ?>) s;
            checkWriter(g.getSideEffectKey(), s, steps, index);
            b.add(new Ops.GroupSideEffect(g.getSideEffectKey(), key(g.getKeyTraversal(), lane, sc),
                    reducer(g.getValueTraversal(), lane, sc)));
        }
        // the writers write when their input is exhausted, so nothing after one in the region may stop pulling early
        b.close();
    }

    // group('x') and groupCount('x') update the side effect as traversers pass: no reader may see it before the end,
    // and nothing after the writer may end the traversal early
    private void checkWriter(final String key, final Step<?, ?> s, final List<Step> steps, final int index) {
        if (analysis.sideEffectIsRead(key, root, s)) throw new Reject("the side effect " + key + " is read before the traversal ends");
        for (int i = index + 1; i < steps.size(); i++) {
            final Step<?, ?> next = steps.get(i);
            if (!(next instanceof org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.SideEffectCapStep
                    || next instanceof DiscardStep)) {
                throw new Reject("a step after group('" + key + "') could end the traversal early");
            }
        }
    }
}
