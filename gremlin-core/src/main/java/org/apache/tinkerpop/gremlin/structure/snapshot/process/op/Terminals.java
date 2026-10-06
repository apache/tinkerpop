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

import org.apache.tinkerpop.gremlin.process.traversal.Order;
import org.apache.tinkerpop.gremlin.structure.snapshot.process.exec.Lane;
import org.apache.tinkerpop.gremlin.structure.snapshot.process.op.Keys.Key;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * The nodes that end a plan: {@code Terminal := Count | Sum | Min | Max | Mean | GroupCount(key) | Group(key, Reducer) |
 * TopK(keys[], orders[], n, lo) | Sort(keys[], orders[]) | Fold | Drain}. A terminal consumes its whole input. Those
 * that emit one result emit it as a single {@code SCALAR} entry of bulk 1, and emit nothing for an empty input where
 * the standard step does (sum, min, max, mean).
 */
public final class Terminals {

    private Terminals() {
    }

    private static void requireValues(final CsrOp node, final Lane input) {
        if (input != Lane.VAL) throw CsrOp.badLane(node, input);
    }

    /**
     * {@code count()}: the sum of bulks of any lane, 0 for an empty input.
     */
    public record Count() implements CsrOp.Terminal {
        @Override
        public Lane outputLane(final Lane input) {
            return Lane.SCALAR;
        }

        @Override
        public String toString() {
            return "Count";
        }
    }

    /**
     * {@code sum()} over a {@code VAL} lane.
     */
    public record Sum() implements CsrOp.Terminal {
        @Override
        public Lane outputLane(final Lane input) {
            requireValues(this, input);
            return Lane.SCALAR;
        }

        @Override
        public String toString() {
            return "Sum";
        }
    }

    /**
     * {@code min()} over a {@code VAL} lane.
     */
    public record Min() implements CsrOp.Terminal {
        @Override
        public Lane outputLane(final Lane input) {
            requireValues(this, input);
            return Lane.SCALAR;
        }

        @Override
        public String toString() {
            return "Min";
        }
    }

    /**
     * {@code max()} over a {@code VAL} lane.
     */
    public record Max() implements CsrOp.Terminal {
        @Override
        public Lane outputLane(final Lane input) {
            requireValues(this, input);
            return Lane.SCALAR;
        }

        @Override
        public String toString() {
            return "Max";
        }
    }

    /**
     * {@code mean()} over a {@code VAL} lane.
     */
    public record Mean() implements CsrOp.Terminal {
        @Override
        public Lane outputLane(final Lane input) {
            requireValues(this, input);
            return Lane.SCALAR;
        }

        @Override
        public String toString() {
            return "Mean";
        }
    }

    /**
     * {@code groupCount()} or {@code groupCount().by(key)}: one {@code Map<Object, Long>} as the scalar. Map keys are
     * built at the end with {@code Materializer}.
     */
    public record GroupCount(Key key) implements CsrOp.Terminal {
        public GroupCount {
            Objects.requireNonNull(key);
        }

        @Override
        public Lane outputLane(final Lane input) {
            return Lane.SCALAR;
        }

        @Override
        public List<CsrPlan> children() {
            return key.children();
        }

        @Override
        public String toString() {
            return "GroupCount(" + key + ")";
        }
    }

    /**
     * {@code group().by(key).by(reducer)}: one {@code Map<Object, Object>} as the scalar. The reducer is a plan that
     * starts with {@link Sources.Input} of the lane the group reads and ends in a reducing terminal ({@link Count},
     * {@link Fold}, {@link Sum}, {@link Min}, {@link Max}, {@link Mean}), run once per group over that group's
     * members; it is how {@code count()}, {@code fold()}, {@code values(x).sum()} and {@code dedup().count()} are
     * expressed.
     */
    public record Group(Key key, CsrPlan reducer) implements CsrOp.Terminal {
        public Group {
            Objects.requireNonNull(key);
            Objects.requireNonNull(reducer);
            if (reducer.inputLane() == null || !reducer.isReducing()) {
                throw new IllegalArgumentException("A group reducer must start with Input and end in a terminal");
            }
        }

        @Override
        public Lane outputLane(final Lane input) {
            if (reducer.inputLane() != input) throw new IllegalArgumentException("Group reducer reads lane "
                    + reducer.inputLane() + " but the group input is " + input);
            return Lane.SCALAR;
        }

        @Override
        public List<CsrPlan> children() {
            final List<CsrPlan> all = new ArrayList<>(key.children());
            all.add(reducer);
            return all;
        }

        @Override
        public String toString() {
            return "Group(" + key + "," + reducer + ")";
        }
    }

    /**
     * {@code order().by(...)...range(lo, hi)}, fused into a bounded heap: emits the entries with rank in
     * {@code [lo, hi)}, counting bulk, in sort order with ties by arrival, on the input lane. {@code limit(n)} is
     * {@code lo = 0, hi = n}.
     */
    public record TopK(List<Key> keys, List<Order> orders, long lo, long hi) implements CsrOp.Terminal {
        public TopK {
            keys = List.copyOf(keys);
            orders = List.copyOf(orders);
            if (keys.isEmpty() || keys.size() != orders.size()) {
                throw new IllegalArgumentException("TopK needs one order per key and at least one key");
            }
            if (lo < 0 || hi < lo) throw new IllegalArgumentException("Bad TopK range [" + lo + ", " + hi + ")");
        }

        @Override
        public Lane outputLane(final Lane input) {
            return input;
        }

        @Override
        public List<CsrPlan> children() {
            final List<CsrPlan> all = new ArrayList<>();
            for (final Key key : keys) all.addAll(key.children());
            return all;
        }

        @Override
        public String toString() {
            return "TopK(" + keys + "," + orders + ",[" + lo + "," + hi + "))";
        }
    }

    /**
     * {@code order().by(...)} with no limit: an external sort, emitting every entry in sort order on the input lane.
     */
    public record Sort(List<Key> keys, List<Order> orders) implements CsrOp.Terminal {
        public Sort {
            keys = List.copyOf(keys);
            orders = List.copyOf(orders);
            if (keys.isEmpty() || keys.size() != orders.size()) {
                throw new IllegalArgumentException("Sort needs one order per key and at least one key");
            }
        }

        @Override
        public Lane outputLane(final Lane input) {
            return input;
        }

        @Override
        public List<CsrPlan> children() {
            final List<CsrPlan> all = new ArrayList<>();
            for (final Key key : keys) all.addAll(key.children());
            return all;
        }

        @Override
        public String toString() {
            return "Sort(" + keys + "," + orders + ")";
        }
    }

    /**
     * {@code fold()}: a {@code List} of the materialized entries, each repeated by its bulk, as the scalar.
     */
    public record Fold() implements CsrOp.Terminal {
        @Override
        public Lane outputLane(final Lane input) {
            return Lane.SCALAR;
        }

        @Override
        public String toString() {
            return "Fold";
        }
    }

    /**
     * {@code iterate()} and {@code discard()}: runs the region for its exceptions and side effects and emits nothing.
     * The output lane is the input lane, with no entries.
     */
    public record Drain() implements CsrOp.Terminal {
        @Override
        public Lane outputLane(final Lane input) {
            return input;
        }

        @Override
        public String toString() {
            return "Drain";
        }
    }
}
