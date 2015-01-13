package com.tinkerpop.gremlin.process.graph.strategy;

import com.tinkerpop.gremlin.process.Step;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.TraversalEngine;
import com.tinkerpop.gremlin.process.TraversalStrategy;
import com.tinkerpop.gremlin.process.Traverser;
import com.tinkerpop.gremlin.process.graph.step.branch.BranchStep;
import com.tinkerpop.gremlin.process.graph.step.branch.RepeatStep;
import com.tinkerpop.gremlin.process.graph.step.branch.util.IncrLoopsStep;
import com.tinkerpop.gremlin.process.graph.step.branch.util.ResetLoopsStep;
import com.tinkerpop.gremlin.process.util.TraversalHelper;
import com.tinkerpop.gremlin.util.function.CloneableFunction;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Function;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class RepeatLinearStrategy extends AbstractTraversalStrategy {

    private static final RepeatLinearStrategy INSTANCE = new RepeatLinearStrategy();

    private static final Set<Class<? extends TraversalStrategy>> POSTS = new HashSet<>(Arrays.asList(
            ChooseLinearStrategy.class,
            UnionLinearStrategy.class,
            EngineDependentStrategy.class));

    private RepeatLinearStrategy() {
    }


    public void apply(final Traversal<?, ?> traversal, final TraversalEngine engine) {
        if (engine.equals(TraversalEngine.STANDARD) || !TraversalHelper.hasStepOfClass(RepeatStep.class, traversal))
            return;

        for (final RepeatStep<?> repeatStep : TraversalHelper.getStepsOfClass(RepeatStep.class, traversal)) {
            if (repeatStep.getTraversals().isEmpty())
                throw new IllegalStateException("No traversal to repeat was provided to RepeatStep");

            Step currentStep = repeatStep.getPreviousStep();
            final Step firstStep = currentStep;
            TraversalHelper.removeStep(repeatStep, traversal);
            currentStep = TraversalHelper.insertTraversal(repeatStep.getTraversals().get(0), currentStep, traversal);
            final IncrLoopsStep<?> incrLoopStep = new IncrLoopsStep<>(traversal);
            TraversalHelper.insertAfterStep(incrLoopStep, currentStep, traversal);
            /////////
            final BranchStep<?> leftBranchStep = new BranchStep<>(traversal);
            final BranchStep<?> rightBranchStep = new BranchStep<>(traversal);
            TraversalHelper.insertAfterStep(leftBranchStep, firstStep, traversal);
            TraversalHelper.insertAfterStep(rightBranchStep, incrLoopStep, traversal);
            /////////
            final ResetLoopsStep<?> resetLoopStep = new ResetLoopsStep<>(traversal);
            TraversalHelper.insertAfterStep(resetLoopStep, rightBranchStep, traversal);

            leftBranchStep.setFunction((Function) new RepeatBranchFunction<>(repeatStep, step -> traverser -> {
                final List<String> stepLabels = new ArrayList<>(2);
                if (step.isUntilFirst()) {    // left until
                    if (step.doUntil((Traverser) traverser)) {
                        stepLabels.add(resetLoopStep.getPreviousStep().getLabel());
                        return stepLabels;
                    } else {
                        stepLabels.add("");
                        if (step.isEmitFirst() && step.doEmit((Traverser) traverser))
                            stepLabels.add(resetLoopStep.getPreviousStep().getLabel());
                        return stepLabels;
                    }
                } else {  // right until
                    stepLabels.add("");
                    if (step.isEmitFirst() && step.doEmit((Traverser) traverser))
                        stepLabels.add(resetLoopStep.getPreviousStep().getLabel());
                    return stepLabels;
                }
            }));

            rightBranchStep.setFunction((Function) new RepeatBranchFunction<>(repeatStep, step -> traverser -> {
                final List<String> stepLabels = new ArrayList<>(2);
                if (!step.isUntilFirst()) {      // right until
                    if (step.doUntil((Traverser) traverser)) {
                        stepLabels.add("");
                        return stepLabels;
                    } else {
                        stepLabels.add(leftBranchStep.getPreviousStep().getLabel());
                        if (!step.isEmitFirst() && step.doEmit((Traverser) traverser))
                            stepLabels.add("");
                        return stepLabels;
                    }

                } else { // left until
                    stepLabels.add(leftBranchStep.getPreviousStep().getLabel());
                    if (!step.isEmitFirst() && step.doEmit((Traverser) traverser)) {
                        stepLabels.add("");
                    }
                    return stepLabels;
                }
            }));
        }
    }

    public static RepeatLinearStrategy instance() {
        return INSTANCE;
    }

    @Override
    public Set<Class<? extends TraversalStrategy>> applyPost() {
        return POSTS;
    }

    public static class RepeatBranchFunction<S> implements CloneableFunction<Traverser<S>, Collection<String>> {

        private RepeatStep<S> repeatStep;
        private Function<Traverser<S>, Collection<String>> function;
        private final Function<RepeatStep<S>, Function<Traverser<S>, Collection<String>>> generatingFunction;

        public RepeatBranchFunction(final RepeatStep<S> repeatStep, final Function<RepeatStep<S>, Function<Traverser<S>, Collection<String>>> generatingFunction) {
            this.repeatStep = repeatStep;
            this.generatingFunction = generatingFunction;
            this.function = this.generatingFunction.apply(this.repeatStep);
        }

        @Override
        public Collection<String> apply(final Traverser<S> traverser) {
            return this.function.apply(traverser);
        }

        @Override
        public RepeatBranchFunction<S> clone() throws CloneNotSupportedException {
            final RepeatBranchFunction<S> clone = (RepeatBranchFunction<S>) super.clone();
            clone.repeatStep = this.repeatStep.clone();
            clone.function = this.generatingFunction.apply(clone.repeatStep);
            return clone;
        }

        /*@Override
        public String toString() {
            return this.repeatStep.toString();
        }*/
    }


}
