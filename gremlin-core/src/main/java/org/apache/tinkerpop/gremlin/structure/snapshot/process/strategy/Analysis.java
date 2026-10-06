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
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.step.GValueHolder;
import org.apache.tinkerpop.gremlin.process.traversal.step.LambdaHolder;
import org.apache.tinkerpop.gremlin.process.traversal.step.Parameterizing;
import org.apache.tinkerpop.gremlin.process.traversal.step.Scoping;
import org.apache.tinkerpop.gremlin.process.traversal.step.TraversalParent;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.EdgeOtherVertexStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.AggregateStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.GroupCountSideEffectStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.GroupSideEffectStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.SideEffectCapStep;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.provider.ProviderGValueReductionStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserRequirement;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import org.apache.tinkerpop.gremlin.structure.Graph;

import java.util.ArrayList;
import java.util.Collection;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * The whole-traversal walk of section 3.2 of the spike document, over the root and every local and global child: the
 * union of the step requirements with the spike's vetoes, left-over GValue placeholders, provider strategies that
 * might expect the original steps, lambdas that could read labels or side effects without saying so, and the labels
 * and side-effect keys that steps read. Nothing is rewritten unless {@link #vetoReason()} is null.
 */
final class Analysis {

    private final Set<String> referenced = new HashSet<>();
    private final Set<String> scopeKeys = new HashSet<>();
    private final List<SideEffectCapStep<?, ?>> caps = new ArrayList<>();
    private final Set<Step<?, ?>> otherVs = new HashSet<>();
    private boolean lambdas;
    private String veto;

    private Analysis() {
    }

    static Analysis of(final Traversal.Admin<?, ?> root) {
        final Analysis a = new Analysis();
        for (final TraversalStrategy<?> strategy : root.getStrategies().toList()) {
            if (strategy instanceof TraversalStrategy.ProviderOptimizationStrategy
                    && !(strategy instanceof CsrNativeStrategy) && !(strategy instanceof ProviderGValueReductionStrategy)) {
                a.veto("the provider strategy " + strategy.getClass().getSimpleName() + " may expect the original steps");
            }
        }
        if (root.getSideEffects().getSackInitialValue() != null) a.veto("the traversal has a sack");
        a.walk(root);
        if (a.lambdas) {
            if (TraversalHelper.hasLabels(root)) a.veto("a lambda could read a label");
            else if (!root.getSideEffects().keys().isEmpty()) a.veto("a lambda could read a side effect");
        }
        return a;
    }

    private void veto(final String reason) {
        if (veto == null) veto = reason;
    }

    private void walk(final Traversal.Admin<?, ?> traversal) {
        for (final Step<?, ?> step : traversal.getSteps()) {
            if (step instanceof GValueHolder && ((GValueHolder<?, ?>) step).isParameterized()) veto("the placeholder " + step + " was not reduced");
            if (step instanceof LambdaHolder) lambdas = true;
            if (step instanceof Scoping) {
                final Set<String> keys = ((Scoping) step).getScopeKeys();
                if (keys != null) {
                    scopeKeys.addAll(keys);
                    addReferenced(keys);
                }
            }
            if (step instanceof Parameterizing) {
                final Set<String> labels = ((Parameterizing) step).getParameters().getReferencedLabels();
                if (labels != null) addReferenced(labels);
            }
            if (step instanceof SideEffectCapStep) caps.add((SideEffectCapStep<?, ?>) step);
            if (step instanceof EdgeOtherVertexStep) otherVs.add(step);
            checkRequirements(step);
            if (step instanceof TraversalParent) {
                final TraversalParent parent = (TraversalParent) step;
                for (final Traversal.Admin<?, ?> child : parent.<Object, Object>getLocalChildren()) walk(child);
                for (final Traversal.Admin<?, ?> child : parent.<Object, Object>getGlobalChildren()) walk(child);
            }
        }
    }

    private void addReferenced(final Collection<String> labels) {
        for (final String label : labels) {
            if (!Graph.Hidden.isHidden(label)) referenced.add(label);
        }
    }

    // a parent reports the requirements of its children, so only what the children do not explain counts as its own
    private void checkRequirements(final Step<?, ?> step) {
        final Set<TraverserRequirement> own = EnumSet.noneOf(TraverserRequirement.class);
        own.addAll(step.getRequirements());
        if (step instanceof TraversalParent) {
            final TraversalParent parent = (TraversalParent) step;
            for (final Traversal.Admin<?, ?> child : parent.<Object, Object>getLocalChildren()) removeChildRequirements(own, child);
            for (final Traversal.Admin<?, ?> child : parent.<Object, Object>getGlobalChildren()) removeChildRequirements(own, child);
        }
        if (own.contains(TraverserRequirement.ONE_BULK)) veto(step + " requires one-bulk traversers");
        if (own.contains(TraverserRequirement.SACK)) veto(step + " requires a sack");
        if (own.contains(TraverserRequirement.PATH) && !(step instanceof EdgeOtherVertexStep)) {
            veto(step + " requires paths");
        }
        if (own.contains(TraverserRequirement.SIDE_EFFECTS) && !isFusableSideEffectStep(step)) {
            veto(step + " requires side effects");
        }
    }

    private static void removeChildRequirements(final Set<TraverserRequirement> own, final Traversal.Admin<?, ?> child) {
        for (final Step<?, ?> s : child.getSteps()) own.removeAll(s.getRequirements());
    }

    private static boolean isFusableSideEffectStep(final Step<?, ?> step) {
        return step instanceof AggregateStep || step instanceof GroupCountSideEffectStep
                || step instanceof GroupSideEffectStep || step instanceof SideEffectCapStep;
    }

    /**
     * The reason the whole traversal must stay on facades, or null.
     */
    String vetoReason() {
        return veto;
    }

    /**
     * Whether some step reads the label (or a side effect of that name) at all.
     */
    boolean isReferenced(final String label) {
        return referenced.contains(label);
    }

    /**
     * Whether any of the labels is read by some step.
     */
    boolean anyReferenced(final Collection<String> labels) {
        for (final String label : labels) {
            if (referenced.contains(label)) return true;
        }
        return false;
    }

    /**
     * Whether a step other than a terminal {@code cap} can observe the side effect before the traversal ends. A
     * {@code cap} is harmless when it is a step of the same traversal after the writer, because it drains its input
     * before it reads.
     */
    boolean sideEffectIsRead(final String key, final Traversal.Admin<?, ?> writerTraversal, final Step<?, ?> writer) {
        if (scopeKeys.contains(key)) return true;
        for (final SideEffectCapStep<?, ?> cap : caps) {
            if (!cap.getSideEffectKeys().contains(key)) continue;
            if (cap.getTraversal() != writerTraversal) return true;
            if (indexOf(writerTraversal, cap) <= indexOf(writerTraversal, writer)) return true;
        }
        return false;
    }

    /**
     * The {@code otherV()} steps of the traversal, which need paths unless fused with their edges.
     */
    Set<Step<?, ?>> otherVSteps() {
        return otherVs;
    }

    static int indexOf(final Traversal.Admin<?, ?> traversal, final Step<?, ?> step) {
        final List<Step> steps = traversal.getSteps();
        for (int i = 0; i < steps.size(); i++) {
            if (steps.get(i) == step) return i;
        }
        return -1;
    }
}
