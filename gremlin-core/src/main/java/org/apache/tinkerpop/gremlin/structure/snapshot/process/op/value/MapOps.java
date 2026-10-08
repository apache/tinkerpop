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

import org.apache.tinkerpop.gremlin.structure.Column;
import org.apache.tinkerpop.gremlin.structure.snapshot.process.exec.Lane;
import org.apache.tinkerpop.gremlin.structure.snapshot.process.op.CsrOp;
import org.apache.tinkerpop.gremlin.structure.snapshot.process.op.CsrPlan;
import org.apache.tinkerpop.gremlin.structure.snapshot.process.op.Keys;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;

/**
 * The IR nodes of the native map emission, which the shared IR in {@code Ops} has no form for: {@code valueMap()},
 * {@code propertyMap()}, {@code elementMap()}, {@code project()} and local maps over decoded values. Each node emits
 * on the {@code VAL} lane with the entry's bulk. Element maps are built directly from owner ranges and columns. Key
 * codes are vertex or edge key codes by the input lane; an empty array means every key in key-code order, and negative
 * codes (keys the snapshot does not have) are skipped. The planner must only emit a node after checking
 * {@code CsrOperatorFactory#isImplemented}.
 */
public final class MapOps {

    private MapOps() {
    }

    private static void requireLane(final CsrOp node, final Lane input, final Lane... allowed) {
        for (final Lane lane : allowed) {
            if (lane == input) return;
        }
        throw CsrOp.badLane(node, input);
    }

    /**
     * {@code valueMap(k...)} with {@code WithOptions.tokens}, no {@code by()}: {@code T.id} and {@code T.label} first
     * when the {@code ids} and {@code labels} bits of {@code tokens} are set, then one entry per key. A vertex maps a
     * key to the list of its values (one per vertex property, in source order, nulls included); an edge maps it to the
     * value. {@code T.label} is omitted when the label is empty, unless {@code multiLabel} (the {@code multilabel}
     * option of the source), when it is the set of labels. The {@code keys} and {@code values} bits concern vertex
     * properties and are ignored on these lanes.
     *
     * @param tokens the {@code WithOptions} bit set
     */
    public record ValueMap(int[] keyCodes, int tokens, boolean multiLabel) implements CsrOp {
        public ValueMap {
            keyCodes = keyCodes.clone();
        }

        @Override
        public Lane outputLane(final Lane input) {
            requireLane(this, input, Lane.V, Lane.E);
            return Lane.VAL;
        }

        @Override
        public String toString() {
            return "ValueMap(" + (keyCodes.length == 0 ? "*" : Arrays.toString(keyCodes)) + ",tokens=" + tokens
                    + (multiLabel ? ",multiLabel" : "") + ")";
        }
    }

    /**
     * {@code propertyMap(k...)}: like {@link ValueMap} without tokens, but the values are the property objects
     * ({@code VertexProperty} lists for a vertex, {@code Property} for an edge), so the facades are created here.
     */
    public record PropertyMap(int[] keyCodes) implements CsrOp {
        public PropertyMap {
            keyCodes = keyCodes.clone();
        }

        @Override
        public Lane outputLane(final Lane input) {
            requireLane(this, input, Lane.V, Lane.E);
            return Lane.VAL;
        }

        @Override
        public String toString() {
            return "PropertyMap(" + (keyCodes.length == 0 ? "*" : Arrays.toString(keyCodes)) + ")";
        }
    }

    /**
     * {@code elementMap(k...)}: {@code T.id}, then {@code T.label} (omitted when empty, the label set when
     * {@code multiLabel}), for an edge {@code Direction.IN} and {@code Direction.OUT} maps of the end vertex's
     * {@code T.id} and {@code T.label}, then one entry per key with a single value, the last one for a multi-property.
     */
    public record ElementMap(int[] keyCodes, boolean multiLabel) implements CsrOp {
        public ElementMap {
            keyCodes = keyCodes.clone();
        }

        @Override
        public Lane outputLane(final Lane input) {
            requireLane(this, input, Lane.V, Lane.E);
            return Lane.VAL;
        }

        @Override
        public String toString() {
            return "ElementMap(" + (keyCodes.length == 0 ? "*" : Arrays.toString(keyCodes)) + ")";
        }
    }

    /**
     * {@code project(names...).by(keys...)}: a map from each name to its key, in name order, with a key the entry does
     * not produce left out. The keys are evaluated by lane as in {@code Keys}; {@code Keys.Identity} on an element lane
     * puts the element (a facade) in the map, so prefer tokens and values. Keys are matched to names by position; the
     * planner expands a shorter {@code by()} ring before building the node.
     *
     * @param keys child plans start with {@code Input(lane)}, and their first result is the value
     */
    public record Project(List<String> names, List<Keys.Key> keys) implements CsrOp {
        public Project {
            names = List.copyOf(names);
            keys = List.copyOf(keys);
            if (names.size() != keys.size()) throw new IllegalArgumentException("Project needs one key per name");
            if (new HashSet<>(names).size() != names.size()) {
                throw new IllegalArgumentException("keys must be unique in ProjectStep");
            }
            Objects.requireNonNull(names);
        }

        @Override
        public Lane outputLane(final Lane input) {
            requireLane(this, input, Lane.V, Lane.E, Lane.VP, Lane.EP, Lane.MP, Lane.VAL, Lane.SCALAR);
            for (final Keys.Key key : keys) {
                for (final CsrPlan child : key.children()) {
                    if (child.inputLane() != input) {
                        throw new IllegalArgumentException(name() + " child reads lane " + child.inputLane()
                                + " but the node input is " + input);
                    }
                }
            }
            return Lane.VAL;
        }

        @Override
        public List<CsrPlan> children() {
            final List<CsrPlan> plans = new ArrayList<>();
            for (final Keys.Key key : keys) plans.addAll(key.children());
            return plans;
        }

        @Override
        public String toString() {
            return "Project(" + names + "," + keys + ")";
        }
    }

    /**
     * {@code select(keys)} or {@code select(values)} over a decoded {@link java.util.Map},
     * {@link java.util.Map.Entry} or {@link org.apache.tinkerpop.gremlin.process.traversal.Path}. The standard
     * {@link Column} implementation defines both the accepted inputs and the returned collection/value.
     */
    public record SelectColumn(Column column) implements CsrOp {
        public SelectColumn {
            Objects.requireNonNull(column);
        }

        @Override
        public Lane outputLane(final Lane input) {
            requireLane(this, input, Lane.VAL, Lane.SCALAR);
            return Lane.VAL;
        }
    }

    /**
     * {@code count(local)} over a decoded value. The operator follows {@code CountLocalStep}: collections, maps and
     * paths use their sizes, trees use their recursive node count, and other values are viewed through
     * {@code IteratorUtils.asIterator}.
     */
    public record CountLocal() implements CsrOp {
        @Override
        public Lane outputLane(final Lane input) {
            Objects.requireNonNull(input);
            return Lane.VAL;
        }
    }

    /**
     * {@code max(local)} over a decoded local value. Empty inputs are unproductive, as in the standard step.
     */
    public record MaxLocal() implements CsrOp {
        @Override
        public Lane outputLane(final Lane input) {
            Objects.requireNonNull(input);
            return Lane.VAL;
        }
    }
}
