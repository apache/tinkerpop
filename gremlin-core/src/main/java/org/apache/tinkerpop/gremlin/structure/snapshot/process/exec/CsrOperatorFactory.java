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
package org.apache.tinkerpop.gremlin.structure.snapshot.process.exec;

import org.apache.tinkerpop.gremlin.structure.snapshot.CsrSnapshot;
import org.apache.tinkerpop.gremlin.structure.snapshot.process.op.CsrOp;
import org.apache.tinkerpop.gremlin.structure.snapshot.process.op.filter.FilterOperators;
import org.apache.tinkerpop.gremlin.structure.snapshot.process.op.nav.NavigationOperators;
import org.apache.tinkerpop.gremlin.structure.snapshot.process.op.repeat.RepeatOperators;
import org.apache.tinkerpop.gremlin.structure.snapshot.process.op.spill.SpillOperators;
import org.apache.tinkerpop.gremlin.structure.snapshot.process.op.terminal.SideEffectOperators;
import org.apache.tinkerpop.gremlin.structure.snapshot.process.op.terminal.TerminalOperators;
import org.apache.tinkerpop.gremlin.structure.snapshot.process.op.branch.BranchOperators;
import org.apache.tinkerpop.gremlin.structure.snapshot.process.op.value.ValueOperators;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Maps IR node classes to the operators that execute them, and to optional plan-time estimators. Registration is by
 * exact node class, and registering again replaces the previous entry, so a later registrar can override an earlier
 * one (the spill work package replaces in-memory operators with spillable ones). {@link #standard()} builds a factory
 * from the registrars in a fixed order, starting with stubs that throw {@link UnsupportedOperationException} for every
 * node, so a node without a real operator fails at bind time with a clear message and
 * {@link #isImplemented(Class)} tells the planner which nodes it may emit.
 */
public final class CsrOperatorFactory {

    /**
     * Creates the operator for a node of type {@code N}.
     */
    @FunctionalInterface
    public interface Creator<N extends CsrOp> {
        CsrOperator create(N node, OperatorSpec spec);
    }

    /**
     * Estimates the output of a node of type {@code N} from the estimate of its input.
     */
    @FunctionalInterface
    public interface Estimator<N extends CsrOp> {
        OpEstimate estimate(N node, CsrSnapshot snapshot, OpEstimate input);
    }

    private static final class Entry {
        private final Creator<CsrOp> creator;
        private final boolean stub;

        private Entry(final Creator<CsrOp> creator, final boolean stub) {
            this.creator = creator;
            this.stub = stub;
        }
    }

    private final Map<Class<?>, Entry> creators = new HashMap<>();
    private final Map<Class<?>, Estimator<CsrOp>> estimators = new HashMap<>();
    private final Set<Class<?>> stubs = new HashSet<>();

    /**
     * The registrars of {@link #standard()}, in registration order. Later ones override earlier ones.
     */
    private static List<OperatorRegistrar> registrars() {
        return List.of(
                new StubOperators(),
                new CoreOperators(),
                new NavigationOperators(),
                new FilterOperators(),
                new TerminalOperators(),
                new SideEffectOperators(),
                new BranchOperators(),
                new RepeatOperators(),
                new ValueOperators(),
                new SpillOperators());
    }

    /**
     * A new factory with every standard registrar applied.
     */
    public static CsrOperatorFactory standard() {
        final CsrOperatorFactory factory = new CsrOperatorFactory();
        for (final OperatorRegistrar registrar : registrars()) registrar.register(factory);
        return factory;
    }

    private static final class Holder {
        private static final CsrOperatorFactory SHARED = standard();
    }

    /**
     * The shared standard factory. It is immutable in practice: nothing registers with it after class initialization.
     */
    public static CsrOperatorFactory shared() {
        return Holder.SHARED;
    }

    /**
     * Registers the operator for a node class, replacing any previous registration.
     */
    @SuppressWarnings("unchecked")
    public <N extends CsrOp> void register(final Class<N> nodeClass, final Creator<N> creator) {
        Objects.requireNonNull(nodeClass);
        Objects.requireNonNull(creator);
        creators.put(nodeClass, new Entry((Creator<CsrOp>) creator, false));
        stubs.remove(nodeClass);
    }

    /**
     * Registers a placeholder for a node class that has no real operator; see {@link StubOperators}.
     */
    @SuppressWarnings("unchecked")
    public <N extends CsrOp> void registerStub(final Class<N> nodeClass, final Creator<N> creator) {
        creators.put(nodeClass, new Entry((Creator<CsrOp>) creator, true));
        stubs.add(nodeClass);
    }

    /**
     * Registers the plan-time estimator for a node class, replacing any previous one.
     */
    @SuppressWarnings("unchecked")
    public <N extends CsrOp> void registerEstimator(final Class<N> nodeClass, final Estimator<N> estimator) {
        estimators.put(Objects.requireNonNull(nodeClass), (Estimator<CsrOp>) Objects.requireNonNull(estimator));
    }

    /**
     * Whether the node class has a real operator, that is one that is registered and not a stub.
     */
    public boolean isImplemented(final Class<? extends CsrOp> nodeClass) {
        final Entry entry = creators.get(nodeClass);
        return entry != null && !entry.stub;
    }

    /**
     * Creates the operator for the node of the spec.
     *
     * @throws IllegalArgumentException if no operator is registered for the node class
     */
    public CsrOperator create(final OperatorSpec spec) {
        final Entry entry = creators.get(spec.node().getClass());
        if (entry == null) {
            throw new IllegalArgumentException("No operator is registered for " + spec.node().getClass().getName());
        }
        return entry.creator.create(spec.node(), spec);
    }

    /**
     * Estimates the output of the node from the estimate of its input; the input estimate unchanged if the node class
     * has no estimator.
     */
    public OpEstimate estimate(final CsrOp node, final CsrSnapshot snapshot, final OpEstimate input) {
        final Estimator<CsrOp> estimator = estimators.get(node.getClass());
        return estimator == null ? input : estimator.estimate(node, snapshot, input);
    }
}
