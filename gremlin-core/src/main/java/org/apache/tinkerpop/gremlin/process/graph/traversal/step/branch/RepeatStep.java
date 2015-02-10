/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.tinkerpop.gremlin.process.graph.traversal.step.branch;

import org.apache.tinkerpop.gremlin.process.Step;
import org.apache.tinkerpop.gremlin.process.Traversal;
import org.apache.tinkerpop.gremlin.process.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.TraversalParent;
import org.apache.tinkerpop.gremlin.process.graph.traversal.step.util.ComputerAwareStep;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalUtil;
import org.apache.tinkerpop.gremlin.process.traverser.TraverserRequirement;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class RepeatStep<S> extends ComputerAwareStep<S, S> implements TraversalParent {

    private Traversal.Admin<S, S> repeatTraversal = null;
    private Traversal.Admin<S, ?> untilTraversal = null;
    private Traversal.Admin<S, ?> emitTraversal = null;
    public boolean untilFirst = false;
    public boolean emitFirst = false;

    public RepeatStep(final Traversal.Admin traversal) {
        super(traversal);
    }

    @Override
    public Set<TraverserRequirement> getRequirements() {
        final Set<TraverserRequirement> requirements = this.getSelfAndChildRequirements(TraverserRequirement.BULK);
        if (requirements.contains(TraverserRequirement.SINGLE_LOOP))
            requirements.add(TraverserRequirement.NESTED_LOOP);
        requirements.add(TraverserRequirement.SINGLE_LOOP);
        return requirements;
    }

    @SuppressWarnings("unchecked")
    public void setRepeatTraversal(final Traversal.Admin<S, S> repeatTraversal) {
        this.repeatTraversal = repeatTraversal; // .clone();
        this.repeatTraversal.addStep(new RepeatEndStep(this.repeatTraversal));
        this.integrateChild(this.repeatTraversal, TYPICAL_GLOBAL_OPERATIONS);
    }

    public void setUntilTraversal(final Traversal.Admin<S, ?> untilTraversal) {
        if (null == this.repeatTraversal) this.untilFirst = true;
        this.integrateChild(this.untilTraversal = untilTraversal, TYPICAL_LOCAL_OPERATIONS);
    }

    public void setEmitTraversal(final Traversal.Admin<S, ?> emitTraversal) {
        if (null == this.repeatTraversal) this.emitFirst = true;
        this.integrateChild(this.emitTraversal = emitTraversal, TYPICAL_LOCAL_OPERATIONS);
    }

    public List<Traversal.Admin<S, S>> getGlobalChildren() {
        return null == this.repeatTraversal ? Collections.emptyList() : Collections.singletonList(this.repeatTraversal);
    }

    public List<Traversal.Admin<S, ?>> getLocalChildren() {
        final List<Traversal.Admin<S, ?>> list = new ArrayList<>();
        if (null != this.untilTraversal)
            list.add(this.untilTraversal);
        if (null != this.emitTraversal)
            list.add(this.emitTraversal);
        return list;
    }

    public final boolean doUntil(final Traverser.Admin<S> traverser, boolean utilFirst) {
        return utilFirst == this.untilFirst && null != this.untilTraversal && TraversalUtil.test(traverser, this.untilTraversal);
    }

    public final boolean doEmit(final Traverser.Admin<S> traverser, boolean emitFirst) {
        return emitFirst == this.emitFirst && null != this.emitTraversal && TraversalUtil.test(traverser, this.emitTraversal);
    }

    @Override
    public String toString() {
        if (this.untilFirst && this.emitFirst)
            return TraversalHelper.makeStepString(this, untilString(), emitString(), this.repeatTraversal);
        else if (this.emitFirst)
            return TraversalHelper.makeStepString(this, emitString(), this.repeatTraversal, untilString());
        else if (this.untilFirst)
            return TraversalHelper.makeStepString(this, untilString(), this.repeatTraversal, emitString());
        else
            return TraversalHelper.makeStepString(this, this.repeatTraversal, untilString(), emitString());
    }

    private final String untilString() {
        return null == this.untilTraversal ? "until(false)" : "until(" + this.untilTraversal + ')';
    }

    private final String emitString() {
        return null == this.emitTraversal ? "emit(false)" : "emit(" + this.emitTraversal + ')';
    }

    /////////////////////////

    @Override
    public RepeatStep<S> clone() throws CloneNotSupportedException {
        final RepeatStep<S> clone = (RepeatStep<S>) super.clone();
        clone.repeatTraversal = clone.integrateChild(this.repeatTraversal.clone(), TYPICAL_GLOBAL_OPERATIONS);
        if (null != this.untilTraversal)
            clone.untilTraversal = clone.integrateChild(this.untilTraversal.clone(), TYPICAL_LOCAL_OPERATIONS);
        if (null != this.emitTraversal)
            clone.emitTraversal = clone.integrateChild(this.emitTraversal.clone(), TYPICAL_LOCAL_OPERATIONS);
        return clone;
    }

    @Override
    protected Iterator<Traverser<S>> standardAlgorithm() throws NoSuchElementException {
        while (true) {
            if (this.repeatTraversal.getEndStep().hasNext()) {
                return this.repeatTraversal.getEndStep();
            } else {
                final Traverser.Admin<S> start = this.starts.next();
                if (doUntil(start, true)) {
                    start.resetLoops();
                    return IteratorUtils.of(start);
                }
                this.repeatTraversal.addStart(start);
                if (doEmit(start, true)) {
                    final Traverser.Admin<S> emitSplit = start.split();
                    emitSplit.resetLoops();
                    return IteratorUtils.of(emitSplit);
                }
            }
        }
    }

    @Override
    protected Iterator<Traverser<S>> computerAlgorithm() throws NoSuchElementException {
        final Traverser.Admin<S> start = this.starts.next();
        if (doUntil(start, true)) {
            start.resetLoops();
            start.setStepId(this.getNextStep().getId());
            return IteratorUtils.of(start);
        } else {
            start.setStepId(this.repeatTraversal.getStartStep().getId());
            if (doEmit(start, true)) {
                final Traverser.Admin<S> emitSplit = start.split();
                emitSplit.resetLoops();
                emitSplit.setStepId(this.getNextStep().getId());
                return IteratorUtils.of(start, emitSplit);
            } else {
                return IteratorUtils.of(start);
            }
        }
    }

    /////////////////////////

    public static <A, B, C extends Traversal<A, B>> C addRepeatToTraversal(final C traversal, final Traversal.Admin<B, B> repeatTraversal) {
        final Step<?, B> step = traversal.asAdmin().getEndStep();
        if (step instanceof RepeatStep && null == ((RepeatStep) step).repeatTraversal) {
            ((RepeatStep<B>) step).setRepeatTraversal(repeatTraversal);
        } else {
            final RepeatStep<B> repeatStep = new RepeatStep<>(traversal.asAdmin());
            repeatStep.setRepeatTraversal(repeatTraversal);
            traversal.asAdmin().addStep(repeatStep);
        }
        return traversal;
    }

    public static <A, B, C extends Traversal<A, B>> C addUntilToTraversal(final C traversal, final Traversal.Admin<B, ?> untilPredicate) {
        final Step<?, B> step = traversal.asAdmin().getEndStep();
        if (step instanceof RepeatStep && null == ((RepeatStep) step).untilTraversal) {
            ((RepeatStep<B>) step).setUntilTraversal(untilPredicate);
        } else {
            final RepeatStep<B> repeatStep = new RepeatStep<>(traversal.asAdmin());
            repeatStep.setUntilTraversal(untilPredicate);
            traversal.asAdmin().addStep(repeatStep);
        }
        return traversal;
    }

    public static <A, B, C extends Traversal<A, B>> C addEmitToTraversal(final C traversal, final Traversal.Admin<B, ?> emitPredicate) {
        final Step<?, B> step = traversal.asAdmin().getEndStep();
        if (step instanceof RepeatStep && null == ((RepeatStep) step).emitTraversal) {
            ((RepeatStep<B>) step).setEmitTraversal(emitPredicate);
        } else {
            final RepeatStep<B> repeatStep = new RepeatStep<>(traversal.asAdmin());
            repeatStep.setEmitTraversal(emitPredicate);
            traversal.asAdmin().addStep(repeatStep);
        }
        return traversal;
    }

    ///////////////////////////////////

    public class RepeatEndStep extends ComputerAwareStep<S, S> {

        public RepeatEndStep(final Traversal.Admin traversal) {
            super(traversal);
        }

        @Override
        protected Iterator<Traverser<S>> standardAlgorithm() throws NoSuchElementException {
            while (true) {
                final Traverser.Admin<S> start = this.starts.next();
                start.incrLoops(this.getId());
                if (doUntil(start, false)) {
                    start.resetLoops();
                    return IteratorUtils.of(start);
                } else {
                    if (!RepeatStep.this.untilFirst && !RepeatStep.this.emitFirst)
                        RepeatStep.this.repeatTraversal.addStart(start);
                    else
                        RepeatStep.this.addStart(start);
                    if (doEmit(start, false)) {
                        final Traverser.Admin<S> emitSplit = start.split();
                        emitSplit.resetLoops();
                        return IteratorUtils.of(emitSplit);
                    }
                }
            }
        }

        @Override
        protected Iterator<Traverser<S>> computerAlgorithm() throws NoSuchElementException {
            final Traverser.Admin<S> start = this.starts.next();
            start.incrLoops(RepeatStep.this.getId());
            if (doUntil(start, false)) {
                start.resetLoops();
                start.setStepId(RepeatStep.this.getNextStep().getId());
                return IteratorUtils.of(start);
            } else {
                start.setStepId(RepeatStep.this.getId());
                if (doEmit(start, false)) {
                    final Traverser.Admin<S> emitSplit = start.split();
                    emitSplit.resetLoops();
                    emitSplit.setStepId(RepeatStep.this.getNextStep().getId());
                    return IteratorUtils.of(start, emitSplit);
                }
                return IteratorUtils.of(start);
            }
        }
    }

}
