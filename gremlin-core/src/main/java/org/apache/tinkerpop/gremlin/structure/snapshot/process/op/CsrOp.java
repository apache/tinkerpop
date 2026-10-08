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

import org.apache.tinkerpop.gremlin.structure.snapshot.process.exec.Lane;

import java.util.List;

/**
 * A node of the native operator IR:
 * <pre>
 * Plan := Source Node*
 * </pre>
 * A barrier {@link Terminal} consumes its whole input and may either end a plan or feed later nodes. Reducing
 * terminals emit one {@link Lane#SCALAR} entry; ordering terminals preserve their input lane.
 * Nodes are immutable descriptors built by the strategy; they hold no state and no reference to a graph. The nodes are
 * grouped in {@link Sources}, {@link Ops} and {@link Terminals}, with the key forms in {@link Keys} and the predicate
 * forms in {@link Preds}. Code that executes a node never switches on its class: it is created through the
 * {@code CsrOperatorFactory}, which maps the node class to an operator.
 * <p/>
 * Codes in nodes are snapshot codes: label codes index the snapshot's label dictionaries and key codes index the
 * vertex or edge key dictionary, whichever matches the lane the node reads.
 */
public interface CsrOp {

    /**
     * A short name for plans, profiles and messages, the simple class name of the node by default.
     */
    default String name() {
        return getClass().getSimpleName();
    }

    /**
     * The lane this node emits when fed the given lane. A source ignores the argument, which is null for the first node
     * of a plan.
     *
     * @throws IllegalArgumentException if the node cannot read the lane
     */
    Lane outputLane(Lane input);

    /**
     * Whether the batches this node emits carry the {@code E} source vertex ({@code Batch.src}), given whether its
     * input batches do. False unless the node is {@code Expand} with {@code recordSource} or passes its input
     * through unchanged (filters, range, dedup, the side-effect writers), which return the argument.
     */
    default boolean outputRecordsSource(final boolean inputRecordsSource) {
        return false;
    }

    /**
     * Whether the node keeps state across batches (dedup, range, merge, side-effect writers and every terminal), so its
     * operator reserves from the memory budget.
     */
    default boolean isStateful() {
        return false;
    }

    /**
     * Whether evaluating the node is a per-entry map, flat map or filter, so a child made only of such nodes can
     * be evaluated once per distinct input and the outputs multiplied by the input bulk.
     */
    default boolean isBulkLinear() {
        return true;
    }

    /**
     * The nested plans of the node, in evaluation order.
     */
    default List<CsrPlan> children() {
        return List.of();
    }

    /**
     * A node that starts a plan.
     */
    interface Source extends CsrOp {

        /**
         * The lane the source emits.
         */
        Lane outputLane();

        @Override
        default Lane outputLane(final Lane input) {
            return outputLane();
        }
    }

    /**
     * A barrier node: it consumes its whole input before emitting results.
     */
    interface Terminal extends CsrOp {

        @Override
        default boolean isStateful() {
            return true;
        }

        @Override
        default boolean isBulkLinear() {
            return false;
        }
    }

    static IllegalArgumentException badLane(final CsrOp node, final Lane input) {
        return new IllegalArgumentException(node.name() + " cannot read lane " + input);
    }
}
