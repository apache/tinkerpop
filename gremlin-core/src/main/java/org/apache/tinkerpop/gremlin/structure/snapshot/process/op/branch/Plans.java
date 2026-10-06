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
package org.apache.tinkerpop.gremlin.structure.snapshot.process.op.branch;

import org.apache.tinkerpop.gremlin.structure.snapshot.CsrSnapshot;
import org.apache.tinkerpop.gremlin.structure.snapshot.process.exec.Lane;
import org.apache.tinkerpop.gremlin.structure.snapshot.process.op.CsrOp;
import org.apache.tinkerpop.gremlin.structure.snapshot.process.op.CsrPlan;
import org.apache.tinkerpop.gremlin.structure.snapshot.process.op.Ops;

/**
 * Questions about child plans that decide how a branch operator may run them.
 */
final class Plans {

    private Plans() {
    }

    /**
     * Whether a run over part of the input differs from a run over all of it: the plan, or a nested plan that is not
     * reset for every entry, holds state across entries (dedup, range, merge, a terminal, a side-effect writer). Such a
     * plan sees the whole stream, like a child traversal of {@code union()} or {@code choose()} does.
     */
    static boolean holdsState(final CsrPlan plan) {
        for (final CsrOp node : plan.nodes()) {
            if (node.isStateful()) return true;
            if (node instanceof Ops.Choose) {
                for (final CsrPlan option : ((Ops.Choose) node).options().values()) {
                    if (holdsState(option)) return true;
                }
            } else if (!resetsPerEntry(node)) {
                for (final CsrPlan child : node.children()) {
                    if (holdsState(child)) return true;
                }
            }
        }
        return false;
    }

    /**
     * Whether the plan, or a nested one, writes a side effect, so running it fewer times would change the result.
     */
    static boolean writesSideEffects(final CsrPlan plan) {
        for (final CsrOp node : plan.nodes()) {
            if (node instanceof Ops.AggregateSideEffect || node instanceof Ops.GroupCountSideEffect
                    || node instanceof Ops.GroupSideEffect) {
                return true;
            }
            for (final CsrPlan child : node.children()) {
                if (writesSideEffects(child)) return true;
            }
        }
        return false;
    }

    /**
     * The number of entries of the lane's universe, or -1 if entries of the lane have no dense ordinal.
     */
    static int universe(final CsrSnapshot snapshot, final Lane lane) {
        switch (lane) {
            case V:
                return snapshot.vertexCount();
            case E:
                return snapshot.edgeCount();
            default:
                return -1;
        }
    }

    // the nodes whose children are reset for every entry, so their state never spans entries
    private static boolean resetsPerEntry(final CsrOp node) {
        return node instanceof Ops.Exists || node instanceof Ops.NotExists || node instanceof Ops.And
                || node instanceof Ops.Or || node instanceof Ops.Local || node instanceof Ops.Coalesce
                || node instanceof Ops.Optional || node instanceof Ops.MapFirst || node instanceof Ops.FlatMap;
    }
}
