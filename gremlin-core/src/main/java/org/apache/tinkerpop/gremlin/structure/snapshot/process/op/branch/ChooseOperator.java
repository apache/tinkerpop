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

import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.Pick;
import org.apache.tinkerpop.gremlin.structure.snapshot.process.exec.Batch;
import org.apache.tinkerpop.gremlin.structure.snapshot.process.exec.OperatorSpec;
import org.apache.tinkerpop.gremlin.structure.snapshot.process.op.CsrPlan;
import org.apache.tinkerpop.gremlin.structure.snapshot.process.op.Ops;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Predicate;

/**
 * {@code Choose}: {@code choose(p, t, f)}, {@code choose(t).option(...)} and {@code branch(t).option(...)}. The choice
 * of each entry is read from the key (fed bulk 1); the entry, with its bulk, goes to every option whose key equals the
 * choice (like {@code P.eq}, or a {@code Predicate} key), else to the {@code Pick.none} option, and to the
 * {@code Pick.any} option unless the choice is {@code Pick.any}. An unproductive key is the choice
 * {@code Pick.unproductive}. An entry with no option is dropped. See {@link RoutingOperator} for stateful options.
 */
final class ChooseOperator extends RoutingOperator {

    private final Ops.Choose node;
    private final List<Object> optionKeys;
    private ChoiceKey key;
    private Batch[] routed;
    private int[] targets;
    private int noneOption;
    private int anyOption;

    ChooseOperator(final Ops.Choose node, final OperatorSpec spec) {
        super(new ArrayList<>(node.options().values()), spec);
        this.node = node;
        this.optionKeys = new ArrayList<>(node.options().keySet());
    }

    @Override
    protected void doOpen() {
        openRuns(false);
        try {
            key = new ChoiceKey(ctx, node.key(), spec.inputLane(), owner("key"));
            routed = new Batch[runs.length];
            for (int k = 0; k < runs.length; k++) {
                if (streaming[k]) routed[k] = new Batch(spec.inputLane(), ctx.batchSize(), false);
            }
            targets = new int[runs.length];
            noneOption = optionKeys.indexOf(Pick.none);
            anyOption = optionKeys.indexOf(Pick.any);
        } catch (RuntimeException e) {
            closeRuns();
            throw e;
        }
    }

    @Override
    protected Batch batchFor(final int branch) {
        return routed[branch];
    }

    @Override
    protected void route(final Batch batch) {
        for (final Batch b : routed) {
            if (b != null) b.clear();
        }
        for (int i = 0; i < batch.n; i++) {
            final Object read = key.read(batch, i);
            final Object choice = read == ChoiceKey.UNPRODUCTIVE ? Pick.unproductive : read;
            int count = 0;
            for (int k = 0; k < optionKeys.size(); k++) {
                if (matches(optionKeys.get(k), choice)) targets[count++] = k;
            }
            if (count == 0 && noneOption >= 0) targets[count++] = noneOption;
            if (choice != Pick.any && anyOption >= 0) targets[count++] = anyOption;
            for (int t = 0; t < count; t++) {
                final int option = targets[t];
                if (streaming[option]) routed[option].copyEntry(batch, i);
                else retention[option].add(batch, i);
            }
        }
    }

    // like BranchStep.pickBranches: a Pick choice selects only the option with that Pick, any other choice is
    // tested against the option keys that are not Picks
    @SuppressWarnings("unchecked")
    private static boolean matches(final Object optionKey, final Object choice) {
        if (choice instanceof Pick) return optionKey == choice;
        if (optionKey instanceof Pick) return false;
        final Predicate<Object> predicate = optionKey instanceof Predicate ? (Predicate<Object>) optionKey
                : (Predicate<Object>) (Predicate<?>) P.eq(optionKey);
        return predicate.test(choice);
    }

    @Override
    protected void closeRuns() {
        try {
            if (key != null) key.close();
        } finally {
            key = null;
            routed = null;
            super.closeRuns();
        }
    }
}
