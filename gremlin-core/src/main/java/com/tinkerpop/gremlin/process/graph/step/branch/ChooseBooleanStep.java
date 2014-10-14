package com.tinkerpop.gremlin.process.graph.step.branch;

import com.tinkerpop.gremlin.process.Step;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.TraversalStrategy;
import com.tinkerpop.gremlin.process.Traverser;
import com.tinkerpop.gremlin.process.graph.marker.EngineDependent;
import com.tinkerpop.gremlin.process.graph.marker.StrategyProvider;
import com.tinkerpop.gremlin.process.graph.step.map.FlatMapStep;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.IdentityStep;
import com.tinkerpop.gremlin.process.util.TraversalHelper;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.Predicate;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class ChooseBooleanStep<S, E> extends FlatMapStep<S, E> implements StrategyProvider {

    private final Predicate<Traverser<S>> choosePredicate;
    private final Traversal<S, E> trueBranch;
    private final Traversal<S, E> falseBranch;

    public ChooseBooleanStep(final Traversal traversal, final Predicate<Traverser<S>> choosePredicate, final Traversal<S, E> trueBranch, final Traversal<S, E> falseBranch) {
        super(traversal);
        this.choosePredicate = choosePredicate;
        this.trueBranch = trueBranch;
        this.falseBranch = falseBranch;
        this.setFunction(traverser -> {
            final Traversal<S, E> branch = this.choosePredicate.test(traverser) ? this.trueBranch : this.falseBranch;
            branch.addStart(traverser);
            return branch;
        });
    }

    public List<TraversalStrategy> getStrategies(final EngineDependent.Engine engine) {
        return engine.equals(EngineDependent.Engine.COMPUTER) ? Arrays.asList(ChooseBooleanStrategy.instance()) : Collections.emptyList();
    }

    public static class ChooseBooleanStrategy implements TraversalStrategy.NoDependencies {

        private static final ChooseBooleanStrategy INSTANCE = new ChooseBooleanStrategy();

        private ChooseBooleanStrategy() {
        }

        // x.choose(p){a}{b}.y
        // x.branch(p ? this : z).a.branch(end).as(z).b.as(end).y
        public void apply(final Traversal<?, ?> traversal) {
            TraversalHelper.getStepsOfClass(ChooseBooleanStep.class, traversal).forEach(step -> {
                final BranchStep<?> branchStep = new BranchStep<>(traversal);
                branchStep.setFunctions(traverser -> step.choosePredicate.test(traverser) ? BranchStep.THIS_LABEL : "b");
                TraversalHelper.replaceStep(step, branchStep, traversal);

                Step currentStep = branchStep;
                for (final Step trueStep : (List<Step>) step.trueBranch.getSteps()) {
                    TraversalHelper.insertAfterStep(trueStep, currentStep, traversal);
                    currentStep = trueStep;
                }
                final BranchStep breakStep = new BranchStep(traversal);
                breakStep.setFunctions(new BranchStep.GoToLabel("end"));
                breakStep.setLabel("b");
                TraversalHelper.insertAfterStep(breakStep, currentStep, traversal);

                currentStep = breakStep;
                for (final Step falseStep : (List<Step>) step.falseBranch.getSteps()) {
                    TraversalHelper.insertAfterStep(falseStep, currentStep, traversal);
                    currentStep = falseStep;
                }
                final IdentityStep finalStep = new IdentityStep(traversal);
                finalStep.setLabel("end");
                TraversalHelper.insertAfterStep(finalStep, currentStep, traversal);

            });
        }

        public static ChooseBooleanStrategy instance() {
            return INSTANCE;
        }
    }
}