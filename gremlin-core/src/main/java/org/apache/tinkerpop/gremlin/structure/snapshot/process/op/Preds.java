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

import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.structure.snapshot.process.exec.Lane;

import java.util.Arrays;
import java.util.Objects;

/**
 * The predicate forms of a {@link Ops.Filter}. A predicate is a pure descriptor: the operator that evaluates it
 * derives typed fast paths from the column it reads when it is bound. {@code P} instances may be updated between
 * executions, so an operator reads their values at bind time and again after {@code reset()}, never at planning time.
 */
public final class Preds {

    private Preds() {
    }

    /**
     * A predicate over the entries of a lane.
     */
    public interface Pred {
    }

    /**
     * True for an element whose label (any label, for a multi-label vertex) has a true flag; the strategy evaluates the
     * {@code P} once per dictionary code. Applies to {@code V} and {@code E}.
     */
    public record LabelSet(boolean[] byCode) implements Pred {
        public LabelSet {
            byCode = byCode.clone();
        }

        @Override
        public String toString() {
            return "LabelSet" + Arrays.toString(byCode);
        }
    }

    /**
     * True for an element whose ordinal is among the given sorted, distinct ordinals: {@code has(T.id, eq/within)}
     * resolved through the identifier index. Applies to {@code V} and {@code E}.
     */
    public record IdIn(int[] ordinals) implements Pred {
        public IdIn {
            ordinals = ordinals.clone();
        }

        @Override
        public String toString() {
            return "IdIn" + Arrays.toString(ordinals);
        }
    }

    /**
     * {@code P.test} over the decoded identifier, or over its {@code toString()} when {@code stringTest}, which is how
     * {@code HasContainer} compares identifiers with string predicates. Applies to {@code V}, {@code E} and {@code VP}.
     */
    public record IdPred(P<?> predicate, boolean stringTest) implements Pred {
        public IdPred {
            Objects.requireNonNull(predicate);
        }
    }

    /**
     * True if some value of the property with the key code satisfies the predicate: a vertex property per entry in the
     * vertex's range, the single entry of an edge property. An absent property is false; a present null is tested with
     * {@code test(null)}. Applies to {@code V} and {@code E}; the key code is by the lane.
     */
    public record PropPred(int keyCode, P<?> predicate) implements Pred {
        public PropPred {
            Objects.requireNonNull(predicate);
        }
    }

    /**
     * True if the element has at least one value, which may be null, for the key code: {@code has(key)} and the
     * complement of {@code hasNot(key)}. Applies to {@code V} and {@code E}.
     */
    public record Presence(int keyCode) implements Pred {
    }

    /**
     * True for entries of the given lane. Used for {@code ClassFilterStep} and, when the lane is statically known,
     * folds to a constant.
     */
    public record LaneType(Lane lane) implements Pred {
        public LaneType {
            Objects.requireNonNull(lane);
        }
    }

    /**
     * {@code P.test} over the entry's value. Applies to {@code VAL} and {@code SCALAR} ({@code is(P)}) and, for
     * {@code hasValue}, to property lanes ({@code VP}, {@code EP}, {@code MP}).
     */
    public record ValuePred(P<?> predicate) implements Pred {
        public ValuePred {
            Objects.requireNonNull(predicate);
        }
    }

    /**
     * {@code P.test} over the key of a property entry: {@code hasKey}. Applies to {@code VP}, {@code EP} and
     * {@code MP}.
     */
    public record KeyPred(P<?> predicate) implements Pred {
        public KeyPred {
            Objects.requireNonNull(predicate);
        }
    }
}
