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
package org.apache.tinkerpop.gremlin.structure.snapshot.process.op;

import org.apache.tinkerpop.gremlin.process.traversal.Pick;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.snapshot.process.exec.Lane;
import org.apache.tinkerpop.gremlin.structure.snapshot.process.op.Keys.Key;
import org.apache.tinkerpop.gremlin.structure.snapshot.process.op.Preds.Pred;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * The middle nodes of a plan, see {@link CsrOp}:
 * <pre>
 * Op := Expand(dir, labelCodes?, emit V|E, recordSource) | Endpoint(OUT|IN|BOTH) | OtherV
 *     | Filter(pred) | Exists(Plan) | NotExists(Plan) | And(Plan...) | Or(Plan...)
 *     | Merge | Range(lo, hi, perLevel) | Dedup(key)
 *     | Props(keys, VALUE|PROPERTY) | PropKey | PropValue | Id | Label | Labels | Element | Constant(v)
 *     | Local(Plan) | Union(Plan...) | Coalesce(Plan...) | Optional(Plan) | Choose(key, Map&lt;Object,Plan&gt;)
 *     | MapFirst(Plan) | FlatMap(Plan)
 *     | Repeat(body, until?, untilFirst, emit?, emitFirst, maxLoops?, loopName) | Loops(name)
 * </pre>
 * plus the side-effect writers of section 2.11, which pass their input through: {@link AggregateSideEffect},
 * {@link GroupCountSideEffect} and {@link GroupSideEffect}. A child plan starts with {@link Sources.Input} of the lane
 * of the node that holds it.
 */
public final class Ops {

    private Ops() {
    }

    private static void requireLane(final CsrOp node, final Lane input, final Lane... allowed) {
        for (final Lane lane : allowed) {
            if (lane == input) return;
        }
        throw CsrOp.badLane(node, input);
    }

    private static void requireChild(final CsrOp node, final CsrPlan child, final Lane input) {
        if (child.inputLane() != input) {
            throw new IllegalArgumentException(node.name() + " child reads lane " + child.inputLane()
                    + " but the node input is " + input);
        }
    }

    private static Lane sameOutput(final CsrOp node, final List<CsrPlan> plans, final Lane input) {
        Lane out = null;
        for (final CsrPlan plan : plans) {
            requireChild(node, plan, input);
            if (out != null && out != plan.outputLane()) {
                throw new IllegalArgumentException(node.name() + " branches emit different lanes");
            }
            out = plan.outputLane();
        }
        return out;
    }

    // ---------------------------------------------------------------- navigation

    /**
     * {@code out}, {@code in}, {@code both}, {@code outE}, {@code inE}, {@code bothE}: from each input vertex, the
     * adjacent vertices or edges. {@code BOTH} emits the out-adjacency, then the in-adjacency, so a self-loop appears
     * twice. Bulk is the input bulk.
     *
     * @param labelCodes   edge label codes to follow, or null for every label; an empty array matches nothing (labels
     *                     missing from the dictionary are dropped by the planner)
     * @param emit         {@code V} for neighbors, {@code E} for incident edges
     * @param recordSource when emitting {@code E}, record the input vertex in {@code Batch.src} so a following
     *                     {@link OtherV} can find the other end
     */
    public record Expand(Direction direction, int[] labelCodes, Lane emit, boolean recordSource) implements CsrOp {
        public Expand {
            Objects.requireNonNull(direction);
            if (!Objects.requireNonNull(emit).isElement()) throw new IllegalArgumentException("Expand emits V or E");
            if (recordSource && emit != Lane.E) throw new IllegalArgumentException("Only edges record a source");
            labelCodes = labelCodes == null ? null : labelCodes.clone();
        }

        @Override
        public boolean outputRecordsSource(final boolean inputRecordsSource) {
            return recordSource;
        }

        @Override
        public Lane outputLane(final Lane input) {
            requireLane(this, input, Lane.V);
            return emit;
        }

        @Override
        public String toString() {
            return "Expand(" + direction + "," + (labelCodes == null ? "*" : Arrays.toString(labelCodes)) + ","
                    + emit + (recordSource ? ",src" : "") + ")";
        }
    }

    /**
     * {@code outV}, {@code inV}, {@code bothV}: the end vertices of each edge; {@code BOTH} emits out, then in.
     */
    public record Endpoint(Direction direction) implements CsrOp {
        public Endpoint {
            Objects.requireNonNull(direction);
        }

        @Override
        public Lane outputLane(final Lane input) {
            requireLane(this, input, Lane.E);
            return Lane.V;
        }

        @Override
        public String toString() {
            return "Endpoint(" + direction + ")";
        }
    }

    /**
     * {@code otherV}: for an edge reached through an {@link Expand} that recorded its source, the end vertex that is not
     * the source. The input batch must carry {@code src}.
     */
    public record OtherV() implements CsrOp {
        @Override
        public Lane outputLane(final Lane input) {
            requireLane(this, input, Lane.E);
            return Lane.V;
        }

        @Override
        public String toString() {
            return "OtherV";
        }
    }

    // ---------------------------------------------------------------- filters

    /**
     * Keeps the entries that satisfy the predicate; the lane is unchanged.
     */
    public record Filter(Pred pred) implements CsrOp {
        public Filter {
            Objects.requireNonNull(pred);
        }

        @Override
        public boolean outputRecordsSource(final boolean inputRecordsSource) {
            return inputRecordsSource;
        }

        @Override
        public Lane outputLane(final Lane input) {
            return input;
        }

        @Override
        public String toString() {
            return "Filter(" + pred + ")";
        }
    }

    /**
     * {@code filter(t)} and {@code where(t)} without labels: keeps the entries for which the child plan produces at
     * least one result. The child is fed with bulk 1.
     */
    public record Exists(CsrPlan plan) implements CsrOp {
        public Exists {
            Objects.requireNonNull(plan);
        }

        @Override
        public boolean outputRecordsSource(final boolean inputRecordsSource) {
            return inputRecordsSource;
        }

        @Override
        public Lane outputLane(final Lane input) {
            requireChild(this, plan, input);
            return input;
        }

        @Override
        public List<CsrPlan> children() {
            return List.of(plan);
        }

        @Override
        public String toString() {
            return "Exists(" + plan + ")";
        }
    }

    /**
     * {@code not(t)}: keeps the entries for which the child plan produces no result.
     */
    public record NotExists(CsrPlan plan) implements CsrOp {
        public NotExists {
            Objects.requireNonNull(plan);
        }

        @Override
        public boolean outputRecordsSource(final boolean inputRecordsSource) {
            return inputRecordsSource;
        }

        @Override
        public Lane outputLane(final Lane input) {
            requireChild(this, plan, input);
            return input;
        }

        @Override
        public List<CsrPlan> children() {
            return List.of(plan);
        }

        @Override
        public String toString() {
            return "NotExists(" + plan + ")";
        }
    }

    /**
     * {@code and(t...)}: keeps the entries for which every child produces a result.
     */
    public record And(List<CsrPlan> plans) implements CsrOp {
        public And {
            plans = List.copyOf(plans);
            if (plans.isEmpty()) throw new IllegalArgumentException("And needs a child");
        }

        @Override
        public boolean outputRecordsSource(final boolean inputRecordsSource) {
            return inputRecordsSource;
        }

        @Override
        public Lane outputLane(final Lane input) {
            sameOutput(this, plans, input);
            return input;
        }

        @Override
        public List<CsrPlan> children() {
            return plans;
        }

        @Override
        public String toString() {
            return "And" + plans;
        }
    }

    /**
     * {@code or(t...)}: keeps the entries for which some child produces a result.
     */
    public record Or(List<CsrPlan> plans) implements CsrOp {
        public Or {
            plans = List.copyOf(plans);
            if (plans.isEmpty()) throw new IllegalArgumentException("Or needs a child");
        }

        @Override
        public boolean outputRecordsSource(final boolean inputRecordsSource) {
            return inputRecordsSource;
        }

        @Override
        public Lane outputLane(final Lane input) {
            sameOutput(this, plans, input);
            return input;
        }

        @Override
        public List<CsrPlan> children() {
            return plans;
        }

        @Override
        public String toString() {
            return "Or" + plans;
        }
    }

    // ---------------------------------------------------------------- stateful

    /**
     * Where a {@code NoOpBarrierStep} stood, and implied before stateful operators: adds the bulks of equal
     * {@code V} or {@code E} entries through a {@code Frontier} (dense or sparse), emitting them in ascending ordinal
     * order. The lane is unchanged; an {@code E} input must not record sources.
     */
    public record Merge() implements CsrOp {
        @Override
        public Lane outputLane(final Lane input) {
            requireLane(this, input, Lane.V, Lane.E);
            return input;
        }

        @Override
        public boolean isStateful() {
            return true;
        }

        @Override
        public boolean isBulkLinear() {
            return false;
        }

        @Override
        public String toString() {
            return "Merge";
        }
    }

    /**
     * {@code limit}, {@code range}, {@code skip} (global): passes the entries with rank in {@code [lo, hi)}, counting
     * bulk and splitting a bulk at the boundaries exactly as {@code RangeGlobalStep} does.
     *
     * @param hi       the exclusive upper rank, or -1 for no upper bound
     * @param perLevel inside a {@link Repeat} body, keep one counter per loop level rather than one for the execution
     */
    public record Range(long lo, long hi, boolean perLevel) implements CsrOp {
        public Range {
            if (lo < 0 || (hi >= 0 && hi < lo)) throw new IllegalArgumentException("Bad range [" + lo + ", " + hi + ")");
        }

        @Override
        public boolean outputRecordsSource(final boolean inputRecordsSource) {
            return inputRecordsSource;
        }

        @Override
        public Lane outputLane(final Lane input) {
            return input;
        }

        @Override
        public boolean isStateful() {
            return true;
        }

        @Override
        public boolean isBulkLinear() {
            return false;
        }

        @Override
        public String toString() {
            return "Range([" + lo + "," + (hi < 0 ? "inf" : String.valueOf(hi)) + ")" + (perLevel ? ",perLevel" : "") + ")";
        }
    }

    /**
     * {@code dedup()} and {@code dedup().by(key)}: passes the first entry for each distinct key, with bulk 1. State
     * persists across loop levels inside a {@link Repeat}.
     */
    public record Dedup(Key key) implements CsrOp {
        public Dedup {
            Objects.requireNonNull(key);
        }

        @Override
        public boolean outputRecordsSource(final boolean inputRecordsSource) {
            return inputRecordsSource;
        }

        @Override
        public Lane outputLane(final Lane input) {
            return input;
        }

        @Override
        public boolean isStateful() {
            return true;
        }

        @Override
        public boolean isBulkLinear() {
            return false;
        }

        @Override
        public List<CsrPlan> children() {
            return key.children();
        }

        @Override
        public String toString() {
            return "Dedup(" + key + ")";
        }
    }

    // ---------------------------------------------------------------- properties, values, tokens

    /**
     * Whether {@link Props} emits values or property entries.
     */
    public enum PropMode {
        VALUE, PROPERTY
    }

    /**
     * {@code values(k...)} ({@code VALUE}: lazy {@code VAL} entries, present nulls included) or
     * {@code properties(k...)} ({@code PROPERTY}: {@code VP} or {@code EP} entries). From {@code V} or {@code E}; the
     * key codes are vertex or edge codes by the input lane. Empty means every key, in key-code order.
     */
    public record Props(int[] keyCodes, PropMode mode) implements CsrOp {
        public Props {
            keyCodes = keyCodes.clone();
            Objects.requireNonNull(mode);
        }

        @Override
        public Lane outputLane(final Lane input) {
            requireLane(this, input, Lane.V, Lane.E);
            if (mode == PropMode.VALUE) return Lane.VAL;
            return input == Lane.V ? Lane.VP : Lane.EP;
        }

        @Override
        public String toString() {
            return "Props(" + (keyCodes.length == 0 ? "*" : Arrays.toString(keyCodes)) + "," + mode + ")";
        }
    }

    /**
     * {@code key()}: the key name of a property entry.
     */
    public record PropKey() implements CsrOp {
        @Override
        public Lane outputLane(final Lane input) {
            requireLane(this, input, Lane.VP, Lane.EP, Lane.MP);
            return Lane.VAL;
        }

        @Override
        public String toString() {
            return "PropKey";
        }
    }

    /**
     * {@code value()}: the value of a property entry.
     */
    public record PropValue() implements CsrOp {
        @Override
        public Lane outputLane(final Lane input) {
            requireLane(this, input, Lane.VP, Lane.EP, Lane.MP);
            return Lane.VAL;
        }

        @Override
        public String toString() {
            return "PropValue";
        }
    }

    /**
     * {@code id()}: the identifier of a vertex, edge or vertex property.
     */
    public record Id() implements CsrOp {
        @Override
        public Lane outputLane(final Lane input) {
            requireLane(this, input, Lane.V, Lane.E, Lane.VP);
            return Lane.VAL;
        }

        @Override
        public String toString() {
            return "Id";
        }
    }

    /**
     * {@code label()}: the first label of a vertex (empty string when it has none), the label of an edge.
     */
    public record Label() implements CsrOp {
        @Override
        public Lane outputLane(final Lane input) {
            requireLane(this, input, Lane.V, Lane.E);
            return Lane.VAL;
        }

        @Override
        public String toString() {
            return "Label";
        }
    }

    /**
     * {@code labels()}: each label of a vertex, or the label of an edge, in source order.
     */
    public record Labels() implements CsrOp {
        @Override
        public Lane outputLane(final Lane input) {
            requireLane(this, input, Lane.V, Lane.E);
            return Lane.VAL;
        }

        @Override
        public String toString() {
            return "Labels";
        }
    }

    /**
     * {@code element()}: the owner of a property entry: {@code VP} to {@code V}, {@code EP} to {@code E}, {@code MP} to
     * its {@code VP}.
     */
    public record Element() implements CsrOp {
        @Override
        public Lane outputLane(final Lane input) {
            requireLane(this, input, Lane.VP, Lane.EP, Lane.MP);
            return input == Lane.VP ? Lane.V : input == Lane.EP ? Lane.E : Lane.VP;
        }

        @Override
        public String toString() {
            return "Element";
        }
    }

    /**
     * {@code constant(v)}: the value with the input bulk, for any input lane.
     */
    public record Constant(Object value) implements CsrOp {
        @Override
        public Lane outputLane(final Lane input) {
            return Lane.VAL;
        }

        @Override
        public String toString() {
            return "Constant(" + value + ")";
        }
    }

    // ---------------------------------------------------------------- children and branches

    /**
     * {@code local(t)}: per distinct input, runs the child with bulk 1 and multiplies the outputs by the input bulk.
     */
    public record Local(CsrPlan plan) implements CsrOp {
        public Local {
            Objects.requireNonNull(plan);
        }

        @Override
        public Lane outputLane(final Lane input) {
            requireChild(this, plan, input);
            return plan.outputLane();
        }

        @Override
        public List<CsrPlan> children() {
            return List.of(plan);
        }

        @Override
        public String toString() {
            return "Local(" + plan + ")";
        }
    }

    /**
     * {@code union(t...)}: each branch over the same input, outputs concatenated. All branches emit the same lane.
     */
    public record Union(List<CsrPlan> plans) implements CsrOp {
        public Union {
            plans = List.copyOf(plans);
            if (plans.isEmpty()) throw new IllegalArgumentException("Union needs a branch");
        }

        @Override
        public Lane outputLane(final Lane input) {
            return sameOutput(this, plans, input);
        }

        @Override
        public boolean isBulkLinear() {
            for (final CsrPlan plan : plans) {
                if (!isLinear(plan)) return false;
            }
            return true;
        }

        @Override
        public List<CsrPlan> children() {
            return plans;
        }

        @Override
        public String toString() {
            return "Union" + plans;
        }
    }

    /**
     * {@code coalesce(t...)}: per distinct input, the outputs of the first branch that produces any.
     */
    public record Coalesce(List<CsrPlan> plans) implements CsrOp {
        public Coalesce {
            plans = List.copyOf(plans);
            if (plans.isEmpty()) throw new IllegalArgumentException("Coalesce needs a branch");
        }

        @Override
        public Lane outputLane(final Lane input) {
            return sameOutput(this, plans, input);
        }

        @Override
        public List<CsrPlan> children() {
            return plans;
        }

        @Override
        public String toString() {
            return "Coalesce" + plans;
        }
    }

    /**
     * {@code optional(t)}: the child outputs, or the input entry if there are none. The child emits the input lane.
     */
    public record Optional(CsrPlan plan) implements CsrOp {
        public Optional {
            Objects.requireNonNull(plan);
        }

        @Override
        public Lane outputLane(final Lane input) {
            requireChild(this, plan, input);
            if (plan.outputLane() != input) throw new IllegalArgumentException("Optional child must emit " + input);
            return input;
        }

        @Override
        public List<CsrPlan> children() {
            return List.of(plan);
        }

        @Override
        public String toString() {
            return "Optional(" + plan + ")";
        }
    }

    /**
     * {@code choose(p, t, f)}, {@code choose(t).option(...)} and {@code branch(t).option(...)}: computes the key of
     * each entry and routes it through the option plan registered for that value. Option keys are literal values,
     * {@link Pick#none} (no match) and {@link Pick#unproductive} (the key was non-productive). An entry whose key has
     * no option and no {@code Pick.none} option is dropped.
     */
    public record Choose(Key key, Map<Object, CsrPlan> options) implements CsrOp {
        public Choose {
            Objects.requireNonNull(key);
            options = new LinkedHashMap<>(options);
            if (options.isEmpty()) throw new IllegalArgumentException("Choose needs an option");
        }

        @Override
        public Lane outputLane(final Lane input) {
            return sameOutput(this, new ArrayList<>(options.values()), input);
        }

        @Override
        public List<CsrPlan> children() {
            final List<CsrPlan> all = new ArrayList<>(key.children());
            all.addAll(options.values());
            return all;
        }

        @Override
        public String toString() {
            return "Choose(" + key + "," + options + ")";
        }
    }

    /**
     * {@code map(t)}: the first result of the child for each entry; an entry with no result is dropped.
     */
    public record MapFirst(CsrPlan plan) implements CsrOp {
        public MapFirst {
            Objects.requireNonNull(plan);
        }

        @Override
        public Lane outputLane(final Lane input) {
            requireChild(this, plan, input);
            return plan.outputLane();
        }

        @Override
        public List<CsrPlan> children() {
            return List.of(plan);
        }

        @Override
        public String toString() {
            return "MapFirst(" + plan + ")";
        }
    }

    /**
     * {@code flatMap(t)}: every result of the child for each entry, fed with bulk 1 and multiplied by the input bulk.
     */
    public record FlatMap(CsrPlan plan) implements CsrOp {
        public FlatMap {
            Objects.requireNonNull(plan);
        }

        @Override
        public Lane outputLane(final Lane input) {
            requireChild(this, plan, input);
            return plan.outputLane();
        }

        @Override
        public List<CsrPlan> children() {
            return List.of(plan);
        }

        @Override
        public String toString() {
            return "FlatMap(" + plan + ")";
        }
    }

    // ---------------------------------------------------------------- repeat

    /**
     * {@code repeat(body)} with {@code until}, {@code emit} and {@code times}, as a level-synchronous frontier loop
     * (section 2.7). Body, until and emit plans start with {@link Sources.Input} of the loop lane; the body emits the
     * loop lane; {@code until} and {@code emit} are existence tests.
     *
     * @param body       the loop body
     * @param until      the exit test, or null for none
     * @param untilFirst true if the test runs before the body ({@code until().repeat()}), false after
     * @param emit       the emit test, or null; see {@code emitAll}
     * @param emitAll    true for an unconditional {@code emit()}, in which case {@code emit} is null
     * @param emitFirst  true if the emit test runs before the body ({@code emit().repeat()}), false after
     * @param maxLoops   the number of iterations after which the loop exits ({@code times(n)}), or -1 for none
     * @param loopName   the name of the loop for {@code loops(name)}, or null
     */
    public record Repeat(CsrPlan body, CsrPlan until, boolean untilFirst, CsrPlan emit, boolean emitAll,
                         boolean emitFirst, int maxLoops, String loopName) implements CsrOp {
        public Repeat {
            Objects.requireNonNull(body);
            if (emitAll && emit != null) throw new IllegalArgumentException("emitAll excludes an emit plan");
        }

        @Override
        public Lane outputLane(final Lane input) {
            requireChild(this, body, input);
            if (body.outputLane() != input) throw new IllegalArgumentException("Repeat body must emit " + input);
            if (until != null) requireChild(this, until, input);
            if (emit != null) requireChild(this, emit, input);
            return input;
        }

        @Override
        public boolean isStateful() {
            return true;
        }

        @Override
        public boolean isBulkLinear() {
            return false;
        }

        @Override
        public List<CsrPlan> children() {
            final List<CsrPlan> all = new ArrayList<>();
            all.add(body);
            if (until != null) all.add(until);
            if (emit != null) all.add(emit);
            return all;
        }

        @Override
        public String toString() {
            return "Repeat(" + body + (until != null ? ",until" + (untilFirst ? "First" : "") + "(" + until + ")" : "")
                    + (emitAll ? ",emit" + (emitFirst ? "First" : "") : "")
                    + (emit != null ? ",emit" + (emitFirst ? "First" : "") + "(" + emit + ")" : "")
                    + (maxLoops >= 0 ? ",times(" + maxLoops + ")" : "")
                    + (loopName != null ? "," + loopName : "") + ")";
        }
    }

    /**
     * {@code loops()} and {@code loops(name)}: the loop level of the enclosing fused {@link Repeat} as a value.
     *
     * @param name the loop name, or null for the innermost loop
     */
    public record Loops(String name) implements CsrOp {
        @Override
        public Lane outputLane(final Lane input) {
            return Lane.VAL;
        }

        @Override
        public String toString() {
            return "Loops(" + name + ")";
        }
    }

    // ---------------------------------------------------------------- side-effect writers

    /**
     * {@code aggregate('x')}: consumes all input, adds the materialized entries to the side effect once, then
     * re-emits the input (section 2.11). The lane is unchanged.
     */
    public record AggregateSideEffect(String sideEffectKey) implements CsrOp {
        public AggregateSideEffect {
            Objects.requireNonNull(sideEffectKey);
        }

        @Override
        public boolean outputRecordsSource(final boolean inputRecordsSource) {
            return inputRecordsSource;
        }

        @Override
        public Lane outputLane(final Lane input) {
            return input;
        }

        @Override
        public boolean isStateful() {
            return true;
        }

        @Override
        public boolean isBulkLinear() {
            return false;
        }

        @Override
        public String toString() {
            return "AggregateSideEffect(" + sideEffectKey + ")";
        }
    }

    /**
     * {@code groupCount('x')}: accumulates like {@link Terminals.GroupCount}, writes the map to the side effect once
     * when the region finishes, and passes the input through unchanged.
     */
    public record GroupCountSideEffect(String sideEffectKey, Key key) implements CsrOp {
        public GroupCountSideEffect {
            Objects.requireNonNull(sideEffectKey);
            Objects.requireNonNull(key);
        }

        @Override
        public boolean outputRecordsSource(final boolean inputRecordsSource) {
            return inputRecordsSource;
        }

        @Override
        public Lane outputLane(final Lane input) {
            return input;
        }

        @Override
        public boolean isStateful() {
            return true;
        }

        @Override
        public boolean isBulkLinear() {
            return false;
        }

        @Override
        public List<CsrPlan> children() {
            return key.children();
        }

        @Override
        public String toString() {
            return "GroupCountSideEffect(" + sideEffectKey + "," + key + ")";
        }
    }

    /**
     * {@code group('x')}: accumulates like {@link Terminals.Group}, writes the map to the side effect once when the
     * region finishes, and passes the input through unchanged.
     */
    public record GroupSideEffect(String sideEffectKey, Key key, CsrPlan reducer) implements CsrOp {
        public GroupSideEffect {
            Objects.requireNonNull(sideEffectKey);
            Objects.requireNonNull(key);
            Objects.requireNonNull(reducer);
            if (reducer.inputLane() == null || !reducer.isGroupReducer()) {
                throw new IllegalArgumentException("A group reducer must start with Input and end in count, fold, "
                        + "sum, min, max or mean");
            }
        }

        @Override
        public boolean outputRecordsSource(final boolean inputRecordsSource) {
            return inputRecordsSource;
        }

        @Override
        public Lane outputLane(final Lane input) {
            if (reducer.inputLane() != input) throw new IllegalArgumentException("Group reducer reads lane "
                    + reducer.inputLane() + " but the group input is " + input);
            return input;
        }

        @Override
        public boolean isStateful() {
            return true;
        }

        @Override
        public boolean isBulkLinear() {
            return false;
        }

        @Override
        public List<CsrPlan> children() {
            final List<CsrPlan> all = new ArrayList<>(key.children());
            all.add(reducer);
            return all;
        }

        @Override
        public String toString() {
            return "GroupSideEffect(" + sideEffectKey + "," + key + "," + reducer + ")";
        }
    }

    private static boolean isLinear(final CsrPlan plan) {
        for (final CsrOp op : plan.ops()) {
            if (!op.isBulkLinear()) return false;
        }
        return plan.terminal() == null;
    }
}
